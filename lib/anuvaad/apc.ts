// APC files ("Gitarth Ganga" code files), read without any AI (Maharaj Saheb
// and Diksha, 17-18 Nov 2026: the society does not want these files to touch
// AI; "the system should split them, find the pages and print the PDF pages").
//
// An APC page lists entries like this (sample of 17 Nov 2026):
//
//   (क्र.-1)
//   ग्रंथ :- अनुयोगद्वारसूत्र जिनदासगणिमहत्तर चूर्णि भाग-1 (प्रकाशक:-महावीर जैन विद्यालय,
//   LibNo:-B010073)
//   Adhyay :- ग्रंथ प्रारंभ,सूत्र-14
//   Pointers :- 12049 to 12343
//   तमेवं आवस्सए त्ति सुतपदं …            ← the śāstra text
//   See Code Anuvaad :- …                  ← left out
//   UQ (परिणाम, धर्मकथा NDMS 12-2-26)      ← left out
//
// Her marks keep the granth's name, the adhyay and the śāstra text, and strike
// the rest (Sr. no., pointers, codes, "See Code Anuvaad", UQ lines). The page
// heads ("गीतार्थ गंगा", "APC - 1", "SPK>…", "Vishay Search…") and feet
// ("Dt.17/2/26", "Page 1 of 254") are not entries. No server code here.

export type ApcEntry = {
  /** "1" from "(क्र.-1)". */
  no: string;
  /** The granth's name, without the publisher and library number. */
  granth: string;
  /** "ग्रंथ प्रारंभ, सूत्र-14". */
  adhyay: string;
  /** The śāstra text, as printed. */
  text: string;
  /** The file's pages it is on (1-based). */
  pages: number[];
};

const ENTRY = /^\(?\s*क्र\s*\.?\s*[-–:]?\s*([0-9०-९]+)\s*\)?\s*$/;
const ENTRY_INLINE = /\(\s*क्र\s*\.?\s*[-–:]?\s*([0-9०-९]+)\s*\)/;
const GRANTH = /^ग्र(?:ं|न्)थ\s*[:：]?\s*[-–]?\s*(.*)$/;
const ADHYAY = /^Adhyay\s*[:：]?\s*[-–]?\s*(.*)$/i;
const POINTERS = /^Pointers\b/i;
const STOP = /^(?:See\s+Code\s+Anuvaad|UQ\b|\d+\.\s*Pointers\b)/i;
/** Page heads and feet, and lines that are only codes. */
const NOISE = /^(?:गीतार्थ\s*गंगा|Gitarth\s*Ganga|APC\s*[-–]?\s*\d+|SPK\s*>|Vishay\s+Search|User\s+Query|\d+\.\s*Dummy|Dt\.\s*\d|Page\s+\d+\s+of\s+\d+|LibNo\b)/i;

const DIGITS: Record<string, string> = {};
"०१२३४५६७८९".split("").forEach((d, i) => (DIGITS[d] = String(i)));
const asciiDigits = (s: string) => s.replace(/[०-९]/g, (d) => DIGITS[d] ?? d);

const DEV = /[ऀ-ॿ]/g;
const GU = /[઀-૿]/g;
/** A line of the śāstra: mostly Devanagari letters, not Gujarati (a handwritten note in the margin is Gujarati). */
const isText = (line: string) => {
  const dev = line.match(DEV)?.length ?? 0;
  const gu = line.match(GU)?.length ?? 0;
  const latin = line.match(/[A-Za-z]/g)?.length ?? 0;
  return dev >= 8 && dev > gu * 3 && dev > latin * 3;
};

/** "अनुयोगद्वारसूत्र … भाग-1 (प्रकाशक:-…" → "अनुयोगद्वारसूत्र … भाग-1". */
function granthName(raw: string) {
  return raw
    .replace(/\(\s*प्रकाशक[\s\S]*$/, "")
    .replace(/LibNo[\s\S]*$/i, "")
    .replace(/[(,\s]+$/, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Lines of a page as read (Markdown from the reader, or a PDF's own text): no headings' #, no blank runs. */
function pageLines(text: string) {
  return text
    .replace(/\r/g, "")
    .split("\n")
    .map((l) => l.replace(/^#+\s*/, "").replace(/^\*\*(.*)\*\*$/, "$1").trim());
}

/**
 * The entries of an APC file, in order, from its pages' text. An entry's text
 * may run on to the next page; within a page it is the block that follows
 * "Pointers" (or "Adhyay"), up to a blank line, so a handwritten note in the
 * margin read after it is not taken.
 */
export function parseApcPages(pages: Array<{ page: number; text: string }>): ApcEntry[] {
  const entries: ApcEntry[] = [];
  let cur: ApcEntry | null = null;
  // where we are inside the current entry
  let state: "head" | "text" | "after" = "head";
  // within a page, the text block has ended at a blank line
  let blockDone = false;

  const start = (no: string, page: number) => {
    cur = { no: asciiDigits(no), granth: "", adhyay: "", text: "", pages: [page] };
    entries.push(cur);
    state = "head";
    blockDone = false;
  };

  for (const { page, text } of pages) {
    // a new page: an entry's text may carry on at its top
    blockDone = false;
    let seenText = false;
    for (const line of pageLines(text)) {
      const entry = (line.match(ENTRY) ?? line.match(ENTRY_INLINE))?.[1];
      if (entry) {
        start(entry, page);
        continue;
      }
      if (!cur) continue;
      const e = cur as ApcEntry;
      if (!line) {
        if (state === "text" && seenText) blockDone = true;
        continue;
      }
      if (NOISE.test(line)) continue;
      const g = line.match(GRANTH);
      if (g && !e.granth) {
        e.granth = granthName(g[1]);
        continue;
      }
      const a = line.match(ADHYAY);
      if (a) {
        e.adhyay = a[1].replace(/\s*,\s*/g, ", ").trim();
        state = "text";
        continue;
      }
      if (POINTERS.test(line)) {
        state = "text";
        continue;
      }
      if (STOP.test(line)) {
        state = "after";
        continue;
      }
      if (state === "head" && e.granth && !e.adhyay && /^[^:：]*\)\s*$/.test(line)) continue; // the publisher's line ending the granth
      if (state !== "text" || blockDone) continue;
      if (!isText(line)) {
        if (seenText) blockDone = true;
        continue;
      }
      e.text = e.text ? (/[-–]$/.test(e.text) ? `${e.text.replace(/[-–]$/, "")}${line}` : `${e.text} ${line}`) : line;
      if (!e.pages.includes(page)) e.pages.push(page);
      seenText = true;
    }
  }
  return entries.filter((e) => e.text.length >= 20);
}

/** The entry as anuvaad markup: the granth as heading, the adhyay, the śāstra text in a box. */
export function apcEntryMarkup(entry: ApcEntry) {
  return [`# ${entry.granth || "ગ્રંથ"}`, entry.adhyay ? `**Adhyay:** ${entry.adhyay}` : "", `| ${entry.text}`].filter(Boolean).join("\n");
}
