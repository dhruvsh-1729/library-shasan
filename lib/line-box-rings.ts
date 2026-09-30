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

/**
 * The printed words of a line: its runs grouped into as many words as the
 * line's text has. The grouping (a dynamic programme over where to cut) keeps
 * each word near where the text puts it along the line (`expected`, fractions
 * of the page width) and prefers cutting at wide gaps; cutting only at the
 * widest gaps shifted every later word by one when one gap was odd (a wide
 * space before a danda, a compound printed without its space).
 */
function printedWords(runs: LineRuns | undefined, expected: Array<[number, number]>): Array<[number, number]> | null {
  const n = expected.length;
  if (!runs || runs.length < 2 || n < 1) return null;
  const spans: Array<[number, number]> = [];
  for (let i = 0; i + 1 < runs.length; i += 2) spans.push([runs[i] / RUN_UNIT, runs[i + 1] / RUN_UNIT]);
  const m = spans.length;
  if (m < n) return null;
  const lineWidth = Math.max(1e-6, spans[m - 1][1] - spans[0][0]);
  // gap before run j, as a share of the line
  const gap = spans.map((span, j) => (j ? (span[0] - spans[j - 1][1]) / lineWidth : 0));
  const maxGap = Math.max(1e-6, ...gap);
  // cost[k][j]: best cost of the first k words using the first j runs
  const INF = Number.POSITIVE_INFINITY;
  const cost = Array.from({ length: n + 1 }, () => new Float64Array(m + 1).fill(INF));
  const back = Array.from({ length: n + 1 }, () => new Int32Array(m + 1));
  cost[0][0] = 0;
  for (let k = 1; k <= n; k += 1) {
    const [ea, eb] = expected[k - 1];
    for (let j = k; j <= m - (n - k); j += 1) {
      for (let i = k - 1; i < j; i += 1) {
        if (cost[k - 1][i] === INF) continue;
        const a = spans[i][0];
        const b = spans[j - 1][1];
        const place = (Math.abs(a - ea) + Math.abs(b - eb)) / lineWidth;
        // a cut at a narrow gap is unlikely to be a word break
        const cut = k > 1 ? 0.5 * (1 - gap[i] / maxGap) : 0;
        const c = cost[k - 1][i] + place + cut;
        if (c < cost[k][j]) {
          cost[k][j] = c;
          back[k][j] = i;
        }
      }
    }
  }
  if (cost[n][m] === INF) return null;
  const words: Array<[number, number]> = new Array(n);
  let j = m;
  for (let k = n; k >= 1; k -= 1) {
    const i = back[k][j];
    words[k - 1] = [spans[i][0], spans[j - 1][1]];
    j = i;
  }
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
  // A box much taller than the page's usual line spans two printed lines (an
  // alignment that joined them): not ringed rather than ringed in between.
  const heights = boxes.map((b) => b[3] - b[1]).filter((h) => h > 0).sort((x, y) => x - y);
  const usual = heights.length ? heights[Math.floor(heights.length / 2)] : 0;
  const out: WordBox[] = [];
  for (const match of findOCRSearchMatchesForQueries(content, queries, mode, scripts)) {
    lines.forEach((line, i) => {
      if (match.end <= line.start || match.start >= line.end) return;
      const [left, top, right, bottom] = boxes[i];
      // A stored line that found no printed line when aligned has no box, and
      // a box taller than a few lines (a joined paragraph) is not one line.
      if (!(right > left && bottom > top) || bottom - top > 0.12 || (usual && bottom - top > usual * 1.8)) return;
      const localStart = Math.max(0, match.start - line.start);
      const localEnd = Math.min(line.text.length, match.end - line.start);
      const from = fractionAt(line.text, localStart);
      const to = fractionAt(line.text, localEnd);
      const height = (bottom - top) * page.height;
      // The line's printed extent: its ink when measured, else its box.
      const lineRuns = runs?.[i] ?? undefined;
      const inkLeft = lineRuns && lineRuns.length >= 2 ? lineRuns[0] / RUN_UNIT : left;
      const inkRight = lineRuns && lineRuns.length >= 2 ? lineRuns[lineRuns.length - 1] / RUN_UNIT : right;
      // Estimated along the line by syllable widths...
      let fx0 = inkLeft + (inkRight - inkLeft) * from;
      let fx1 = inkLeft + (inkRight - inkLeft) * to;
      // ...then put on the printed word itself when the line's ink splits into
      // as many words as its text has, and the word found is where the
      // estimate expects it (a text that differs from the print is not trusted).
      const words = textWords(line.text);
      const at = (offset: number) => inkLeft + (inkRight - inkLeft) * fractionAt(line.text, offset);
      const printed = lineRuns && lineRuns.length / 2 <= 400 ? printedWords(lineRuns, words.map((w) => [at(w.start), at(w.end)])) : null;
      const first = words.findIndex((w) => w.end > localStart);
      const last = words.findIndex((w) => w.end >= localEnd);
      if (printed && first >= 0 && last >= first) {
        const px0 = printed[first][0];
        const px1 = printed[last][1];
        const tolerance = Math.max(0.2 * (inkRight - inkLeft), 3 * (px1 - px0));
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
