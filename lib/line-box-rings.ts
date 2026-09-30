// Where a searched word sits on a scanned page, from our own OCR's line boxes.
//
// The invisible text layers in many PDFs do not follow the print: the one our
// OCR pipeline laid in drew every line at a fixed size with the font's own
// widths (so a line ran long or short, sometimes past the page edge), and some
// publishers' layers put whole lines off the page. Rings built from those land
// beside the word. The OCR itself knows each printed line's box, and the page
// text search counts is that OCR's text, one stored line per box. So a match in
// the stored text is placed on its line's box, and along the line by syllable
// widths; the ring count is exactly the page's occurrence count.

import { type OCRSearchMode, type OCRSearchScripts, findOCRSearchMatchesForQueries } from "@/lib/ocr-search";
import { type WordBox, syllableWidth } from "@/lib/pdf-word-boxes";

/** A printed line's box as fractions of the page: [left, top, right, bottom], top-left origin. */
export type LineBox = [number, number, number, number];

const graphemes = new Intl.Segmenter(undefined, { granularity: "grapheme" });

/** The page text's non-empty lines with their offsets, in the order the boxes are stored. */
function textLines(content: string) {
  const lines: Array<{ start: number; end: number; text: string }> = [];
  let offset = 0;
  for (const raw of content.split("\n")) {
    const start = offset;
    offset += raw.length + 1;
    if (!raw.trim()) continue;
    lines.push({ start, end: start + raw.length, text: raw });
  }
  return lines;
}

/** How far along a line (0..1) a character offset in it falls, weighted by syllable. */
function fractionAt(text: string, offset: number) {
  let total = 0;
  let before = 0;
  for (const { segment, index } of graphemes.segment(text)) {
    const w = syllableWidth(segment);
    if (index < offset) before += Math.min(w, w * ((offset - index) / segment.length));
    total += w;
  }
  return total ? before / total : 0;
}

/**
 * Boxes (PDF user space, lower-left origin) for every match of the queries on
 * one page, or null when the stored lines and boxes do not line up.
 */
export function lineBoxRings(
  content: string,
  boxes: LineBox[],
  page: { width: number; height: number },
  queries: string[],
  mode: OCRSearchMode,
  scripts: OCRSearchScripts = null
): WordBox[] | null {
  const lines = textLines(content);
  if (!lines.length || lines.length !== boxes.length) return null;
  const out: WordBox[] = [];
  for (const match of findOCRSearchMatchesForQueries(content, queries, mode, scripts)) {
    lines.forEach((line, i) => {
      if (match.end <= line.start || match.start >= line.end) return;
      const [left, top, right, bottom] = boxes[i];
      const from = fractionAt(line.text, Math.max(0, match.start - line.start));
      const to = fractionAt(line.text, Math.min(line.text.length, match.end - line.start));
      const height = (bottom - top) * page.height;
      const x0 = (left + (right - left) * from) * page.width;
      const x1 = (left + (right - left) * to) * page.width;
      out.push({
        x: x0,
        y: (1 - bottom) * page.height,
        width: Math.max(height * 0.5, x1 - x0),
        height,
        fontSize: height * 0.8,
        text: match.text,
      });
    });
  }
  return out;
}
