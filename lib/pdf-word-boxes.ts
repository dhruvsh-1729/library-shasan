// Where the searched word sits on a PDF page, from the page's text layer.
//
// Shared by the PDF viewer (red rings drawn over the rendered page) and the
// PDF export (red ellipses drawn onto the exported page), so both mark the
// same places. The page's text items are joined into one string, the search
// matcher (lib/ocr-search, with its Sanskrit folding, forms and script
// filter) runs over it, and each match is mapped back to the items it covers
// and to a box in PDF user space.
//
// Positions inside an item are estimated from the item's width shared out by
// character; for a word that is enough to put a ring around it.

import { type OCRSearchMode, type OCRSearchScripts, findOCRSearchMatchesForQueries } from "@/lib/ocr-search";

/** The parts of a pdf.js TextItem this needs. */
export type PdfTextItem = {
  str: string;
  transform: number[];
  width: number;
  height: number;
  hasEOL?: boolean;
};

/** A box in PDF user space: x, y of the lower-left corner, with the font size. */
export type WordBox = { x: number; y: number; width: number; height: number; fontSize: number; text: string };

type Piece = { item: PdfTextItem; start: number; end: number; text: string };

function fontSizeOf(item: PdfTextItem) {
  const [a, b, c, d] = item.transform;
  return Math.max(Math.hypot(c, d), Math.hypot(a, b) * 0.5, item.height || 0, 1);
}

/**
 * Joins items into page text. Items on one line that touch are joined
 * directly (a word split across two items stays one word); a gap or a line
 * break becomes a space.
 */
function joinItems(items: PdfTextItem[]) {
  const pieces: Piece[] = [];
  let text = "";
  let prev: PdfTextItem | null = null;
  for (const item of items) {
    const str = String(item.str ?? "").normalize("NFC");
    if (!str) {
      if (item.hasEOL && text && !text.endsWith(" ")) text += " ";
      continue;
    }
    // A conjunct split across items ("પ્" + "ર") stays one word.
    const afterVirama = /[\u094D\u0ACD]$/.test(text);
    if (prev && text && !text.endsWith(" ") && !afterVirama) {
      const size = fontSizeOf(item);
      const sameLine = Math.abs(item.transform[5] - prev.transform[5]) < size * 0.4;
      const gap = item.transform[4] - (prev.transform[4] + prev.width);
      if (!sameLine || gap > size * 0.18 || prev.hasEOL) text += " ";
    }
    pieces.push({ item, start: text.length, end: text.length + str.length, text: str });
    text += str;
    prev = item;
  }
  return { text, pieces };
}

// ---------------------------------------------------------------- visual order
//
// Text layers made with legacy (non-Unicode) Devanagari and Gujarati fonts come
// out of pdf.js in the order the glyphs are drawn, not the order the letters
// are read: the short i sign before its consonant (િમથ્યાત્વ for મિથ્યાત્વ)
// and the r sign after the consonant it belongs to. That r is either a
// sub-script r (પર્કાર for પ્રકાર) or a reph over the next letter (सवर्गुण
// for सर्वगुण), which the glyph order cannot tell apart, so both readings are
// tried. Every repair only reorders letters, so offsets into the text stay
// valid for mapping a match back to its place on the page.

const CONS = "[\\u0915-\\u0939\\u0958-\\u095F\\u0A95-\\u0AB9]";
const VIRAMA = "[\\u094D\\u0ACD]";
const I_SIGN_FIRST = new RegExp(`([\\u093F\\u0ABF])(${CONS}(?:${VIRAMA}${CONS})*)`, "gu");
const R_AFTER = new RegExp(`(${CONS})([\\u0930\\u0AB0])(${VIRAMA})`, "gu");

/** The raw text and its repaired readings, all the same length as the raw text. */
export function readingOrders(text: string) {
  if (!/[\u093F\u0ABF\u0930\u0AB0]/.test(text)) return [text];
  const iFixed = text.replace(I_SIGN_FIRST, "$2$1");
  const subscriptR = iFixed.replace(R_AFTER, "$1$3$2");
  const reph = iFixed.replace(R_AFTER, "$2$3$1");
  return [...new Set([text, iFixed, subscriptR, reph])];
}

const graphemes = new Intl.Segmenter(undefined, { granularity: "grapheme" });

/**
 * How wide a printed syllable is, in rough units: one letter with its vowel
 * signs is one unit, each extra consonant of a conjunct adds a little, a
 * spacing vowel sign (ા, ી) adds a little, a space is narrower. Indic text is
 * drawn syllable by syllable, so sharing an item's width this way puts a ring
 * on the word rather than beside it.
 */
export function syllableWidth(cluster: string): number {
  if (/^\s+$/.test(cluster)) return 0.45;
  if (/^[\u200b-\u200d]+$/.test(cluster)) return 0;
  const consonants = (cluster.match(/[\u0915-\u0939\u0958-\u095F\u0A95-\u0AB9]/gu) ?? []).length;
  const spacingSigns = (cluster.match(/\p{Mc}/gu) ?? []).length;
  return 1 + Math.max(0, consonants - 1) * 0.35 + spacingSigns * 0.3;
}

/** The box a character range covers within one item. */
function boxInItem(piece: Piece, from: number, to: number): Omit<WordBox, "text"> {
  const { item } = piece;
  const size = fontSizeOf(item);
  const localFrom = Math.max(0, from - piece.start);
  const localTo = Math.min(piece.text.length, to - piece.start);
  let total = 0;
  let before = 0;
  let inside = 0;
  for (const { segment, index } of graphemes.segment(piece.text)) {
    const w = syllableWidth(segment);
    total += w;
    const end = index + segment.length;
    if (end <= localFrom) before += w;
    else if (index < localTo) inside += w;
  }
  total ||= 1;
  const x0 = item.transform[4] + (item.width * before) / total;
  const x1 = item.transform[4] + (item.width * (before + inside)) / total;
  // Devanagari and Gujarati marks reach well above and below the baseline.
  return { x: x0, y: item.transform[5] - size * 0.3, width: Math.max(size * 0.5, x1 - x0), height: size * 1.3, fontSize: size };
}

/**
 * Boxes around every match of the queries on a page, one per match (a match
 * that runs across two lines gets one box per line).
 */
export function findWordBoxes(
  items: PdfTextItem[],
  queries: string[],
  mode: OCRSearchMode,
  scripts: OCRSearchScripts = null
): WordBox[] {
  if (!items.length || !queries.length) return [];
  const { text, pieces } = joinItems(items);
  // Matches from every reading of the text, each place counted once.
  const seen = new Set<string>();
  const matches = readingOrders(text).flatMap((reading) =>
    findOCRSearchMatchesForQueries(reading, queries, mode, scripts).filter((m) => {
      const key = `${m.start}:${m.end}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
  );
  const boxes: WordBox[] = [];
  for (const match of matches) {
    const covered = pieces.filter((p) => p.end > match.start && p.start < match.end);
    // Merge the pieces of one line into one box.
    const lines: Array<Omit<WordBox, "text"> & { baseline: number }> = [];
    for (const piece of covered) {
      const box = boxInItem(piece, match.start, match.end);
      const baseline = piece.item.transform[5];
      const last = lines[lines.length - 1];
      if (last && Math.abs(baseline - last.baseline) < box.fontSize * 0.4) {
        const x0 = Math.min(last.x, box.x);
        const x1 = Math.max(last.x + last.width, box.x + box.width);
        lines[lines.length - 1] = {
          ...last,
          x: x0,
          width: x1 - x0,
          fontSize: Math.max(last.fontSize, box.fontSize),
          height: Math.max(last.height, box.height),
        };
      } else {
        lines.push({ ...box, baseline });
      }
    }
    for (const { baseline: _baseline, ...line } of lines) boxes.push({ ...line, text: match.text });
  }
  return boxes;
}
