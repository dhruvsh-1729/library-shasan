// The book's own Gujarati for a box passage, cut from the pages around the
// place the passage is printed (lib/anuvaad/locate). No server code: the
// browser page uses it.
//
// Maharaj Saheb, 18 Nov 2026, on the anuvaad taken from our books:
//   - "the starting point is not proper, about 3-4 lines are missed": a
//     gatha's Gujarati often starts at the foot of a page that is mostly
//     Sanskrit, so whole "Gujarati pages" began too late;
//   - "the exact part and also the Gujarati before and after comes, sometimes
//     two or three pages: make our main text bold and the rest normal";
//   - "in Pravinbhai Mota's books we do not want the ભાવાર્થ, only the anuvaad".
//
// So the pages are read line by line, in order, as one stream. The box's
// Granth column names the gathas ("गाथा 1-6", "श्लोक 28"); the books print
// those numbers as ॥४२॥ after a verse, "(૪૨)" or "(૧-૨)" around their
// Gujarati, "(૧) ગાથાર્થ" before it, "।।૨૭||" after it. The main part runs
// from the Gujarati after the first gatha's verse (or its own "(૧)" mark)
// to just before the next gatha's mark. Without the numbers, the lines that
// quote the passage's words lead.

import type { LocatedPage } from "@/lib/anuvaad/locate";
import { pageText } from "@/lib/anuvaad/pages";
import { WORD_TOKEN_PATTERN, foldSanskrit } from "@/lib/sanskrit-fold.mjs";

export type BookText = {
  /** Markup for AnuvaadView / the PDF: "> " lines are the main part. */
  markup: string;
  /** PDF pages the lines come from. */
  pages: number[];
  /** How the main part was found. */
  by: "gatha" | "words";
};

const MIN_WORD = 4;

/** The passage's distinct folded words of 4+ letters, longest first (lib/anuvaad/locate searches by them). */
export function passageWords(passage: string) {
  const words = (foldSanskrit(passage).match(WORD_TOKEN_PATTERN) ?? []).filter((w) => /[ऀ-ॿ]/.test(w) && [...w].length >= MIN_WORD);
  return [...new Set(words)].sort((a, b) => [...b].length - [...a].length);
}

// ---------------------------------------------------------------- numbers
const DIGITS: Record<string, string> = {};
"०१२३४५६७८९".split("").forEach((d, i) => (DIGITS[d] = String(i)));
"૦૧૨૩૪૫૬૭૮૯".split("").forEach((d, i) => (DIGITS[d] = String(i)));
const ascii = (text: string) => text.replace(/[०-९૦-૯]/g, (d) => DIGITS[d] ?? d);

/**
 * The gatha / shloka / sutra numbers the Granth column names, smallest to
 * largest: "पंचाशक मूल, 04 - पूजाविधि, गाथा 42" → [42, 42];
 * "गाथा 290-291, … गाथा 298-303" → [290, 303]. Null when it names none.
 */
export function gathaRange(granth: string): [number, number] | null {
  const text = ascii(granth).replace(/[–—]/g, "-");
  const nums: number[] = [];
  const re = /(?:गाथा|गा\.|श्लोक|श्लो\.|सूत्र|कारिका|ગાથા|શ્લોક|સૂત્ર)\s*[-:.]?\s*((?:\d+\s*(?:-\s*\d+)?\s*,?\s*)+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text))) {
    for (const part of m[1].split(",")) {
      const r = part.match(/(\d+)\s*(?:-\s*(\d+))?/);
      if (!r) continue;
      const a = Number(r[1]);
      const b = r[2] ? Number(r[2]) : a;
      // "गाथा 1 0" is 10 read with a space; a range is small
      if (a > 0 && b >= a && b - a <= 60) nums.push(a, b);
    }
  }
  if (!nums.length) return null;
  // a chapter number caught after a comma ("गाथा 290-291, 01 – देवस्वरूप") is far from the rest
  const sorted = [...nums].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  const kept = nums.filter((n) => Math.abs(n - median) <= 60);
  return [Math.min(...kept), Math.max(...kept)];
}

type Mark = { from: number; to: number; style: "open" | "close" };

/**
 * Gatha numbers a line prints: "॥४२॥", "।।૨૭||", "II૨૭॥", "|| ४.४२ ||" close a
 * verse; "(૪૨)" or "(૪૧-૪૨)" closes its Gujarati; "(૧) ગાથાર્થ" at the start
 * opens it. Page and chapter numbers ("ગાથા-૩૮-૩૯" running heads,
 * "(૪૦મી ગાથામાં)") are not marks.
 */
export function gathaMarks(line: string): Mark[] {
  const text = ascii(line);
  const out: Mark[] = [];
  const close = /(?:॥|।।|\|\||II|ll)\s*(?:\d+\s*[./]\s*)?(\d{1,4})(?:\s*-\s*(\d{1,4}))?\s*(?:॥|।।|\|\||II|ll|।)/g;
  let m: RegExpExecArray | null;
  while ((m = close.exec(text))) out.push({ from: Number(m[1]), to: Number(m[2] ?? m[1]), style: "close" });
  const paren = /\(\s*(\d{1,4})(?:\s*-\s*(\d{1,4}))?\s*\)/g;
  while ((m = paren.exec(text))) {
    // "(૧) ગાથાર્થ" opens; "(૧૯૯)." alone on its line closes
    const opens = m.index <= 1 && /[઀-૿ऀ-ॿ]{2}/.test(text.slice(m.index + m[0].length));
    out.push({ from: Number(m[1]), to: Number(m[2] ?? m[1]), style: opens ? "open" : "close" });
  }
  return out.filter((k) => k.to >= k.from && k.to - k.from <= 60);
}

// ---------------------------------------------------------------- lines
type Line = { page: number; text: string; gu: boolean; dev: boolean; marks: Mark[]; head: boolean };

const GU = /[઀-૿]/g;
const DEV = /[ऀ-ॿ]/g;

/** Section labels of the Gujarati books: ગાથાર્થ–, ટીકાર્થ :-, શ્લોક :- … */
const LABEL = /^(?:ગાથા|ગાથાર્થ|ગાંથાર્થ|ટીકા|ટીકાર્થ|શ્લોક|શ્લોકાર્થ|અવતરણિકા|અવતરણિકાર્થ|અન્વયાર્થ|ઉત્થાન|સૂત્ર|સૂત્રાર્થ|મૂળ|ભાવાર્થ|भावार्थ|ઉપન્યાસ|વિશેષાર્થ|પ્રશ્ન|ઉત્તર)\s*[:：\-–—]/;
const BHAVARTH = /^(?:ભાવાર્થ|भावार्थ)\s*[:：\-–—]/;
/** A label that ends a ભાવાર્થ: the next shloka / gatha / tika / avataranika. */
const AFTER_BHAVARTH = /^(?:ગાથા|ગાથાર્થ|ટીકા|ટીકાર્થ|શ્લોક|શ્લોકાર્થ|અવતરણિકા|અવતરણિકાર્થ|અન્વયાર્થ|ઉત્થાન|સૂત્ર|સૂત્રાર્થ|મૂળ)\s*[:：\-–—]/;

/** A running head with its numbers taken out, to see it repeat from page to page. */
const headKey = (text: string) => ascii(text).replace(/[\d\s.\-–/]+/g, "");

/** "૧.લલિતવિસ્તરા લોગસ્સસૂત્ર.", "* જુઓ …", "1. …": a footnote's first line. */
const FOOTNOTE = /^(?:\*+|[૧-૯1-9१-९]\s*[.)]|\(\s*[૧-૯1-9१-९]\s*\))\s*[઀-૿ऀ-ॿ]{2}/;

/**
 * The footnotes at a page's foot (Panchashak p.195: "૧.લલિતવિસ્તરા
 * લોગસ્સસૂત્ર." was read into the middle of the passage's bold sentence).
 * Within the page's last four lines, a line opening "૧." or "*" and all
 * after it, when that is three lines or fewer.
 */
export function footnoteLines(raw: string[]): Set<number> {
  const out = new Set<number>();
  const solid = raw.map((t, i) => ({ t, i })).filter((x) => x.t.replace(/\s/g, "").length > 3);
  const tail = solid.slice(-4);
  const at = tail.findIndex((x) => FOOTNOTE.test(x.t) && !gathaMarks(x.t).some((k) => k.style === "open"));
  if (at < 0 || tail.length - at > 3) return out;
  for (let i = tail[at].i; i < raw.length; i += 1) out.add(i);
  return out;
}

export function linesOf(pages: LocatedPage[]): Line[] {
  const out: Line[] = [];
  const perPage = pages.map((p) => ({
    page: p.page,
    raw: pageText(p.text)
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => (l.match(GU)?.length ?? 0) + (l.match(DEV)?.length ?? 0) > 0),
  }));
  // short lines found on two or more of these pages are running heads ("દશવૈકાલિકસૂત્ર ભાગ-૨", "વાદદ્વાત્રિંશિકા/શ્લોક-૨૮")
  const seen = new Map<string, Set<number>>();
  for (const p of perPage) for (const t of p.raw) if (t.length < 45) seen.set(headKey(t), (seen.get(headKey(t)) ?? new Set()).add(p.page));
  for (const p of perPage) {
    // the page's edges, counted in lines of some length (not the one-letter ornaments)
    const solid = p.raw.map((t, i) => ({ t, i })).filter((x) => x.t.length > 3);
    const top = new Set(solid.slice(0, 2).map((x) => x.i));
    const foot = new Set(solid.slice(-1).map((x) => x.i));
    const notes = footnoteLines(p.raw);
    p.raw.forEach((text, i) => {
      if (notes.has(i)) {
        out.push({ page: p.page, text, gu: false, dev: false, marks: [], head: true });
        return;
      }
      const gu = text.match(GU)?.length ?? 0;
      const dev = text.match(DEV)?.length ?? 0;
      const sentence = /[.?!।॥)]\s*$/.test(text) || LABEL.test(text) || /^\(/.test(text);
      const repeated = text.length < 45 && (seen.get(headKey(text))?.size ?? 0) > 1;
      // ornaments of single letters down the margin ("न मे ऽस्तु", "जि न") are not text
      const ornament = text.replace(/\s/g, "").length < 8 && !/[.?!।॥)]\s*$/.test(text);
      // "ગાથા-૩૧-૩૩", "અધ્ય. ૪ ગાથા-૧ થી ૯": a name and numbers only
      // or a page number then the book's name: "૧૪૪ સૂત્રકૃતાંગ."
      const numbered =
        (/^[઀-૿ऀ-ॿ\s.]+[\s\-–./:]*[0-9૦-૯०-९][0-9૦-૯०-९\s\-–.,/થીसे]*$/.test(text) || /^[0-9૦-૯०-९]{1,4}\s+[઀-૿ऀ-ॿ][઀-૿ऀ-ॿ\s.\-]*$/.test(text)) && text.length < 30;
      const head = ornament || numbered || (!sentence && text.length < 40 && (top.has(i) || foot.has(i))) || (repeated && !sentence);
      out.push({ page: p.page, text, gu: gu >= 3 && gu >= dev, dev: dev > gu, marks: head ? [] : gathaMarks(text), head });
    });
  }
  return out;
}

const covers = (k: Mark, n: number) => k.from <= n && n <= k.to;
/** Two Sanskrit lines in a row (ornaments and heads aside): a verse or tika block. */
const verseAt = (lines: Line[], i: number) => lines[i]?.dev && !lines[i].head && lines.slice(i + 1).find((l) => !l.head)?.dev === true;

/** [start, end] line indexes of the passage's Gujarati, by its gatha numbers. */
function byGatha(lines: Line[], [first, last]: [number, number], near: number): [number, number] | null {
  // a long tika can run four pages past its verse
  const near2 = (l: Line) => l.page >= near - 4 && l.page <= near + 2;
  // the first gatha's first mark near the place the passage was found
  const s = lines.findIndex((l) => near2(l) && l.marks.some((k) => covers(k, first)));
  if (s < 0) return null;
  let start: number;
  // 1. books that number each gatha's Gujarati: "(૮) ગાથાર્થ"
  const open = lines.findIndex((l) => near2(l) && l.gu && l.marks.some((k) => k.style === "open" && covers(k, first)));
  if (open >= 0) {
    start = open;
    // its short lead-in ("આચાર્ય કહે છે કે"), not the gatha before it
    while (start > 0 && open - start < 2 && lines[start - 1].gu && !lines[start - 1].marks.length && !LABEL.test(lines[start - 1].text) && lines[start - 1].text.length < 50) start -= 1;
  } else {
    // 2. right after the previous gatha's closing mark ("।।૩।।", "॥४१॥"), else after this gatha's verse
    let prev = -1;
    for (let i = s - 1; i >= 0 && lines[i].page >= lines[s].page - 2; i -= 1) {
      if (lines[i].marks.some((k) => k.style === "close" && k.to < first && k.to >= first - 3)) {
        prev = i;
        break;
      }
    }
    start = (prev >= 0 ? prev : s) + 1;
    while (start < lines.length && (!lines[start].gu || lines[start].head)) start += 1;
  }
  if (start >= lines.length) return null;
  // ends before the next gatha's first mark
  let end = -1;
  for (let i = start + 1; i < lines.length; i += 1) {
    if (lines[i].marks.some((k) => k.from > last && k.from <= last + 6)) {
      end = i - 1;
      // a verse block that starts the next gatha belongs to it, and so does the Gujarati lead-in to that verse
      while (end > start && lines[end].dev) end -= 1;
      if (lines[i].dev) while (end > start && lines[end].gu && !lines[end].marks.length && /[–—-]\s*$/.test(lines[end].text)) end -= 1;
      // the next gatha's lead-in ("પ્રશ્ન: જો આ રીતે …? તો.") just before its "(૭)"
      if (lines[i].gu && lines[i].marks.some((k) => k.style === "open")) {
        let b = end;
        while (b > start && end - b < 2 && lines[b].gu && !lines[b].marks.length && lines[b].text.length < 70) b -= 1;
        // only when the gatha's own text had ended there ("(૬) ગાથાર્થ: સ્પષ્ટ છે.")
        if (b < end && lines[b].marks.length && /[.।?)]\s*$/.test(lines[b].text)) end = b;
      }
      break;
    }
  }
  if (end < 0) {
    // no next mark on these pages: up to the last mark of the last gatha
    for (let i = lines.length - 1; i > start; i -= 1) if (lines[i].gu && lines[i].marks.some((k) => covers(k, last))) { end = i; break; }
  }
  if (end < start) return null;
  return [start, end];
}

/** [start, end] by the passage's words quoted in the lines (Devanagari, or their first letters in Gujarati). */
function byWords(lines: Line[], words: string[], near: number): [number, number] | null {
  if (!words.length) return null;
  const stems = words.slice(0, 30).map((w) => [...w].slice(0, 4).join(""));
  const gstems = stems.map((s) => s.replace(/[ऀ-ॿ]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) + 0x180)));
  const score = (l: Line) => stems.filter((s, i) => l.text.includes(s) || l.text.includes(gstems[i])).length;
  // the Sanskrit lines that print the passage
  const dev = lines.map((l, i) => (l.dev && score(l) >= 2 ? i : -1)).filter((i) => i >= 0);
  let start = -1;
  if (dev.length) {
    // the passage's own verse block nearest the place it was found
    const at = dev.sort((a, b) => Math.abs(lines[a].page - near) - Math.abs(lines[b].page - near) || a - b)[0];
    let s = at;
    while (s + 1 < lines.length && lines[s + 1].dev) s += 1;
    start = s + 1;
    while (start < lines.length && (!lines[start].gu || lines[start].head)) start += 1;
  }
  if (start < 0 || start >= lines.length) {
    const gu = lines.map((l, i) => (l.gu && score(l) >= 2 ? i : -1)).filter((i) => i >= 0);
    if (!gu.length) return null;
    start = gu[0];
  }
  // the Gujarati runs on until the next verse block of two lines or more
  let end = start;
  for (let i = start + 1; i < lines.length; i += 1) {
    if (verseAt(lines, i)) break;
    if (lines[i].gu && !lines[i].head) end = i;
  }
  return [start, end];
}

/**
 * Within a gatha's part, the Gujarati of the box passage itself: the lines
 * after each Sanskrit block that prints the passage's words, up to the next
 * Sanskrit block. A box passage is often one piece of a tika that runs over
 * pages; its own ટીકાર્થ is what goes bold. Null when no block prints it.
 */
function narrow(lines: Line[], [start, end]: [number, number], words: string[]): Set<number> | null {
  if (words.length < 3) return null;
  const want = new Set(words);
  const hits = (l: Line) => {
    const own = new Set(foldSanskrit(l.text).match(WORD_TOKEN_PATTERN) ?? []);
    let n = 0;
    for (const w of own) if (want.has(w)) n += 1;
    return n;
  };
  // the Sanskrit blocks from the gatha's verse (just before `start`) to its end
  let from = start;
  while (from > 0 && lines[from - 1].page >= lines[start].page - 4 && !lines[from - 1].gu) from -= 1;
  const out = new Set<number>();
  for (let i = from; i <= end; i += 1) {
    if (!lines[i].dev || hits(lines[i]) < 2) continue;
    let j = i;
    while (j + 1 <= end && !lines[j + 1].gu) j += 1;
    for (let k = j + 1; k <= end && !verseAt(lines, k); k += 1) if (lines[k].gu && k >= start) out.add(k);
    i = j;
  }
  return out.size ? out : null;
}

/** A ભાવાર્થ section, up to the next shloka/gatha/tika label or verse. */
function bhavarthLines(lines: Line[]) {
  const skip = new Set<number>();
  let inside = false;
  lines.forEach((l, i) => {
    if (BHAVARTH.test(l.text)) inside = true;
    else if (inside && (AFTER_BHAVARTH.test(l.text) || verseAt(lines, i))) inside = false;
    if (inside) skip.add(i);
  });
  return skip;
}

/** Lines into paragraphs: a new one at a label, an opening mark, or after a sentence that ends a line short of the measure. */
function paragraphs(lines: Line[]) {
  const paras: string[] = [];
  let cur = "";
  for (const l of lines) {
    const fresh = LABEL.test(l.text) || l.marks.some((k) => k.style === "open");
    if (cur && fresh) {
      paras.push(cur);
      cur = "";
    }
    cur = cur ? (/[-–]$/.test(cur) && !/\s[-–]$/.test(cur) ? cur.replace(/[-–]$/, "") + l.text : `${cur} ${l.text}`) : l.text;
    if (/[.?!।)]\s*$/.test(l.text) && l.text.length < 45) {
      paras.push(cur);
      cur = "";
    }
  }
  if (cur) paras.push(cur);
  // a label left alone ("ટીકા :-" before Sanskrit that is not kept) says nothing
  return paras.filter((p) => !(LABEL.test(p) && p.replace(LABEL, "").trim().length < 3));
}

/**
 * The book's Gujarati for the passage, or null when the pages have none.
 * `pages` are the located page and its neighbours; `place` the page the
 * passage is printed on; `granth` the box's Granth column; `words` the
 * passage's words (passageWords).
 */
export function bookGujaratiText(pages: LocatedPage[], place: number, granth: string, words: string[]): BookText | null {
  const ordered = [...pages].sort((a, b) => a.page - b.page);
  const lines = linesOf(ordered);
  if (!lines.some((l) => l.gu)) return null;
  const range = gathaRange(granth);
  const skip = bhavarthLines(lines);
  const mainOf = ([start, end]: [number, number]) => lines.slice(start, end + 1).filter((l, i) => l.gu && !l.head && !skip.has(start + i));
  const enough = (span: [number, number] | null) => span !== null && mainOf(span).reduce((n, l) => n + l.text.length, 0) >= 60;
  let span = range ? byGatha(lines, range, place) : null;
  let by: BookText["by"] = "gatha";
  if (!enough(span)) {
    span = byWords(lines, words, place);
    by = "words";
  }
  if (!span || !enough(span)) return null;
  const [start, end] = span;
  // the passage's own lines inside its gatha's part, when that is clearly smaller
  const own = by === "gatha" ? narrow(lines, span, words) : null;
  const ownLines = own ? mainOf(span).filter((l) => own.has(lines.indexOf(l))) : [];
  const mainLines = ownLines.reduce((n, l) => n + l.text.length, 0) >= 60 ? ownLines : mainOf(span);
  // the pages the main part is on, all their Gujarati in order: the main part bold, the rest as it is
  const main = new Set(mainLines);
  const firstPage = mainLines[0].page;
  const lastPage = mainLines[mainLines.length - 1].page;
  const shown = lines.filter((l, i) => l.page >= firstPage && l.page <= lastPage && l.gu && !l.head && !skip.has(i));
  const out: string[] = [];
  for (let i = 0; i < shown.length; ) {
    const bold = main.has(shown[i]);
    let j = i;
    while (j < shown.length && main.has(shown[j]) === bold) j += 1;
    for (const p of paragraphs(shown.slice(i, j))) out.push(bold ? `> ${p}` : p);
    i = j;
  }
  const pageNumbers = [...new Set(shown.map((l) => l.page))].sort((a, b) => a - b);
  void start;
  void end;
  return { markup: out.join("\n"), pages: pageNumbers, by };
}

/**
 * A whole page's own Gujarati ("Pages of a PDF", when the page is one of our
 * books): every Gujarati line printed on it, running heads, footnotes and the
 * ભાવાર્થ aside. Null when it has less than a few lines of Gujarati.
 */
export function pageGujaratiText(pages: LocatedPage[], page: number): BookText | null {
  const ordered = [...pages].sort((a, b) => a.page - b.page);
  const lines = linesOf(ordered);
  const skip = bhavarthLines(lines);
  const own = lines.filter((l, i) => l.page === page && l.gu && !l.head && !skip.has(i));
  if (own.reduce((n, l) => n + (l.text.match(GU)?.length ?? 0), 0) < 200) return null;
  return { markup: paragraphs(own).join("\n"), pages: [page], by: "words" };
}
