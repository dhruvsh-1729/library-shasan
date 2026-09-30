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
/**
 * A line's ink runs from the page image (scripts/word_segments.py): flat
 * [x0, x1, x0, x1, ...] in 1/2000ths of the page width, letters already
 * joined; split into words here at the widest gaps.
 */
export type LineRuns = number[];

const RUN_UNIT = 2000;

/** The printed words of a line: its runs split at the n-1 widest gaps, or null if there are too few. */
function printedWords(runs: LineRuns | undefined, n: number): Array<[number, number]> | null {
  if (!runs || runs.length < 2 || n < 1) return null;
  const spans: Array<[number, number]> = [];
  for (let i = 0; i + 1 < runs.length; i += 2) spans.push([runs[i] / RUN_UNIT, runs[i + 1] / RUN_UNIT]);
  if (spans.length < n) return null;
  const gaps = spans.slice(1).map((span, i) => ({ i, size: span[0] - spans[i][1] }));
  const cuts = new Set(gaps.sort((a, b) => b.size - a.size).slice(0, n - 1).map((g) => g.i));
  const words: Array<[number, number]> = [];
  let start = spans[0][0];
  spans.forEach((span, i) => {
    if (cuts.has(i) || i === spans.length - 1) {
      words.push([start, span[1]]);
      if (i + 1 < spans.length) start = spans[i + 1][0];
    }
  });
  return words;
}

/** The whitespace-separated words of a line of text, with their offsets. */
function textWords(text: string) {
  return [...text.matchAll(/\S+/gu)].map((m) => ({ start: m.index ?? 0, end: (m.index ?? 0) + m[0].length }));
}

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
  scripts: OCRSearchScripts = null,
  runs: Array<LineRuns | null> | null = null
): WordBox[] | null {
  const lines = textLines(content);
  if (!lines.length || lines.length !== boxes.length) return null;
  const out: WordBox[] = [];
  for (const match of findOCRSearchMatchesForQueries(content, queries, mode, scripts)) {
    lines.forEach((line, i) => {
      if (match.end <= line.start || match.start >= line.end) return;
      const [left, top, right, bottom] = boxes[i];
      // A stored line that found no printed line when aligned has no box.
      if (!(right > left && bottom > top)) return;
      const localStart = Math.max(0, match.start - line.start);
      const localEnd = Math.min(line.text.length, match.end - line.start);
      const from = fractionAt(line.text, localStart);
      const to = fractionAt(line.text, localEnd);
      const height = (bottom - top) * page.height;
      // Estimated along the line by syllable widths...
      let fx0 = left + (right - left) * from;
      let fx1 = left + (right - left) * to;
      // ...then put on the printed word itself when the line's ink splits into
      // as many words as its text has, and the word found is where the
      // estimate expects it (a text that differs from the print is not trusted).
      const lineRuns = runs?.[i] ?? undefined;
      const words = textWords(line.text);
      const printed = printedWords(lineRuns, words.length);
      const first = words.findIndex((w) => w.end > localStart);
      const last = words.findIndex((w) => w.end >= localEnd);
      if (printed && first >= 0 && last >= first) {
        const px0 = printed[first][0];
        const px1 = printed[last][1];
        const tolerance = Math.max(0.2 * (right - left), 3 * (px1 - px0));
        if (Math.abs((px0 + px1) / 2 - (fx0 + fx1) / 2) <= tolerance) {
          fx0 = px0;
          fx1 = px1;
        }
      } else if (lineRuns && lineRuns.length >= 2) {
        // Too few runs to split: snap the estimate to the ink it overlaps.
        let a = Infinity;
        let b = -Infinity;
        for (let k = 0; k + 1 < lineRuns.length; k += 2) {
          const r0 = lineRuns[k] / RUN_UNIT;
          const r1 = lineRuns[k + 1] / RUN_UNIT;
          if (r1 > fx0 && r0 < fx1) {
            a = Math.min(a, r0);
            b = Math.max(b, r1);
          }
        }
        if (a < b && b - a < 2.5 * (fx1 - fx0) + 0.02) {
          fx0 = a;
          fx1 = b;
        }
      }
      const x0 = fx0 * page.width;
      const x1 = fx1 * page.width;
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
