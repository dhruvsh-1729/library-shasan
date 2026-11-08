// Line numbers as a reader counts them on the printed page.
//
// OCR page text has blank lines between paragraphs and verses, and starts
// with the page's header: the page number and the running head ("३५०" /
// "षड्दर्शन समुच्चय भाग - १, परिशिष्ट - ५…"), sometimes on separate lines,
// sometimes framed ("॥ ३३२ ॥", "[सू० ५८४-५८५ /१]"). A reader counts lines of
// text from the first line under the header, so the line list's "[३९, ३५०]"
// sent Sahebji twelve lines too far down page 350 (8 Nov 2026: the word is on
// its 27th text line). Here a line's number is its place among the page's
// non-blank lines once the header is left out.
//
// The header is found per book: its running heads repeat from page to page,
// so the top lines that recur on many pages of the granth (digits ignored)
// are heads; so is a line holding only the page number, or the printed page
// number of this very page.

import { getTursoClient } from "@/lib/turso";

/** How far down a page a header can reach, in non-blank lines. */
const HEADER_LINES = 4;
const SCAN_FOOTER = /^(?:Jain\s+Education\s+International|For\s+Private\s*(?:&|and)\s*Personal\s+Use\s+Only|www\.jainelibrary\.org)$/i;
const DIGITS = /[0-9०-९૦-૯]/g;

function asciiDigits(text: string) {
  return text.replace(/[०-९૦-૯]/g, (ch) => String(ch.charCodeAt(0) - (ch <= "९" ? 0x0966 : 0x0ae6)));
}

/** A top line with its digits, punctuation and spacing taken out: the same on every page of a running head. */
export function headSignature(line: string) {
  return line
    .normalize("NFC")
    .replace(DIGITS, "")
    .replace(/[\s|।॥.,:;'"“”‘’()[\]{}/\\\-–—_=*~+<>]+/g, "")
    .toLowerCase();
}

/** Only the page number, perhaps framed: "३५०", "॥ ३३२ ॥", "( ७४ )", "|| ૭૨૨". */
function isPageNumberLine(line: string) {
  return /^[\s|।॥()[\]\-–—.]*[0-9०-९૦-૯]{1,4}[\s|।॥()[\]\-–—.]*$/.test(line);
}

function holdsPrintedNumber(line: string, printed: string | null) {
  if (!printed) return false;
  const numbers = asciiDigits(printed).match(/\d+/g) ?? [];
  const inLine = new Set(asciiDigits(line).match(/\d+/g) ?? []);
  return numbers.some((n) => inLine.has(n));
}

const headCache = new Map<string, { at: number; value: Promise<Set<string>> }>();
const HEAD_TTL_MS = 6 * 60 * 60 * 1000;

/** Signatures of the top lines that recur on many pages of one granth (its running heads). */
export function runningHeads(sourceRelPath: string): Promise<Set<string>> {
  const hit = headCache.get(sourceRelPath);
  if (hit && Date.now() - hit.at < HEAD_TTL_MS) return hit.value;
  const value = (async () => {
    const result = await getTursoClient().execute({
      sql: `SELECT substr(p.content, 1, 400) AS top
            FROM ocr_pages p JOIN ocr_granths g ON g.granth_key = p.granth_key
            WHERE g.source_rel_path = ?`,
      args: [sourceRelPath],
    });
    const counts = new Map<string, number>();
    for (const row of result.rows) {
      const seen = new Set<string>();
      for (const line of String(row.top ?? "").split("\n").filter((l) => l.trim()).slice(0, HEADER_LINES)) {
        const sig = headSignature(line);
        if (sig.length >= 3 && !seen.has(sig)) {
          seen.add(sig);
          counts.set(sig, (counts.get(sig) ?? 0) + 1);
        }
      }
    }
    const pages = result.rows.length;
    const need = Math.max(3, Math.ceil(pages * 0.04));
    return new Set([...counts].filter(([, n]) => n >= need).map(([sig]) => sig));
  })().catch(() => new Set<string>());
  headCache.set(sourceRelPath, { at: Date.now(), value });
  if (headCache.size > 200) headCache.delete(headCache.keys().next().value!);
  return value;
}

/**
 * For one page: a function from the OCR's 1-based line number to the line's
 * number on the printed page (its place among the text lines under the
 * header). A line inside the header, or blank, takes the number of the next
 * text line, never 0.
 */
export function bookLineNumbers(content: string, heads: Set<string>, printedPage: string | null) {
  const lines = String(content ?? "").replace(/\r\n?/g, "\n").split("\n");
  // A line of text has letters: not "-", "**********" or the scanner's footer.
  const isText = (line: string) => /\p{L}/u.test(line) && !SCAN_FOOTER.test(line.trim());
  const textIndexes = lines.map((line, i) => (isText(line) || isPageNumberLine(line.trim()) ? i : -1)).filter((i) => i >= 0);
  const top = textIndexes.slice(0, HEADER_LINES).map((i) => lines[i].trim());
  // The page number (or this page's printed number on a short line) marks the
  // header: everything above it is header too ("स्वोपज्ञवृत्तिविभूषितं
  // योगशास्त्रम्" / "॥ १०१० ॥").
  let headerEnd = 0;
  top.forEach((line, k) => {
    if (isPageNumberLine(line) || (holdsPrintedNumber(line, printedPage) && line.length <= 90)) headerEnd = k + 1;
  });
  // Running heads directly above or under it.
  while (headerEnd < top.length && heads.has(headSignature(top[headerEnd]))) headerEnd += 1;
  const numberOf = new Map<number, number>();
  // page-number lines below the header (a verse number on its own line) are not text lines either
  const counted = textIndexes.filter((index, k) => k >= headerEnd && isText(lines[index]));
  textIndexes.slice(0, headerEnd).forEach((index) => numberOf.set(index, 1));
  counted.forEach((index, k) => numberOf.set(index, k + 1));
  const lineOf = (rawLine: number) => {
    for (let i = rawLine - 1; i < lines.length; i += 1) {
      const n = numberOf.get(i);
      if (n != null) return n;
    }
    return Math.max(1, counted.length);
  };
  /** 0-based indexes of the header's lines, for leaving them out of quoted context. */
  lineOf.header = new Set(textIndexes.slice(0, headerEnd));
  return lineOf;
}
