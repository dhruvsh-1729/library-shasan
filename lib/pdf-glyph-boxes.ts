// Exact places of a searched word on a PDF page, glyph by glyph.
//
// pdf.js's text content gives a line of text and its total width, so a word
// inside a line can only be placed by guessing how wide its letters are;
// with the legacy Gujarati/Devanagari fonts many granths use, that guess can
// land a word away. The page's operator list has what drawing it uses: every
// glyph with its Unicode text and its real advance width, inside the text
// positioning operators. Replaying those operators puts every glyph where it
// is drawn, so a ring goes around exactly the letters that matched.
//
// Works on a pdf.js PDFPageProxy in the browser and in Node alike. Returns
// null when the page cannot be replayed, and callers fall back to the
// text-content estimate in lib/pdf-word-boxes.

import { type OCRSearchMode, type OCRSearchScripts, findOCRSearchMatchesForQueries } from "@/lib/ocr-search";
import { type WordBox, readingOrders } from "@/lib/pdf-word-boxes";

type Matrix = [number, number, number, number, number, number];
type Glyph = { unicode?: string; width?: number; isSpace?: boolean };

/** The pdf.js pieces this needs, so the module works with either build. */
export type GlyphPage = {
  getOperatorList: () => Promise<{ fnArray: number[]; argsArray: unknown[][] }>;
  commonObjs?: { has: (id: string) => boolean; get: (id: string) => unknown };
};
export type PdfOps = Record<string, number>;

/** One drawn character, with the x range it covers and the line it sits on, in PDF user space. */
type PlacedChar = { ch: string; x0: number; x1: number; y: number; size: number };

const IDENTITY: Matrix = [1, 0, 0, 1, 0, 0];

function multiply(m: Matrix, n: Matrix): Matrix {
  return [
    m[0] * n[0] + m[1] * n[2],
    m[0] * n[1] + m[1] * n[3],
    m[2] * n[0] + m[3] * n[2],
    m[2] * n[1] + m[3] * n[3],
    m[4] * n[0] + m[5] * n[2] + n[4],
    m[4] * n[1] + m[5] * n[3] + n[5],
  ];
}

function apply(m: Matrix, x: number, y: number): [number, number] {
  return [x * m[0] + y * m[2] + m[4], x * m[1] + y * m[3] + m[5]];
}

/** Every character the page draws, in drawing order, placed in user space. */
async function placeCharacters(page: GlyphPage, OPS: PdfOps): Promise<PlacedChar[]> {
  const { fnArray, argsArray } = await page.getOperatorList();
  const chars: PlacedChar[] = [];
  let ctm: Matrix = IDENTITY;
  const stack: Matrix[] = [];
  let tm: Matrix = IDENTITY;
  let tlm: Matrix = IDENTITY;
  let fontSize = 1;
  let fontMatrixX = 0.001;
  let charSpacing = 0;
  let wordSpacing = 0;
  let hScale = 1;
  let leading = 0;
  let rise = 0;

  const moveText = (tx: number, ty: number) => {
    tlm = multiply([1, 0, 0, 1, tx, ty], tlm);
    tm = tlm;
  };

  for (let i = 0; i < fnArray.length; i += 1) {
    const fn = fnArray[i];
    const args = (argsArray[i] ?? []) as unknown[];
    switch (fn) {
      case OPS.save:
        stack.push(ctm);
        break;
      case OPS.restore:
        ctm = stack.pop() ?? IDENTITY;
        break;
      case OPS.transform:
        ctm = multiply(args as Matrix, ctm);
        break;
      case OPS.beginText:
        tm = IDENTITY;
        tlm = IDENTITY;
        break;
      case OPS.setFont: {
        fontSize = Number(args[1]) || 1;
        const id = String(args[0] ?? "");
        const font = page.commonObjs?.has(id) ? (page.commonObjs.get(id) as { fontMatrix?: number[] } | null) : null;
        fontMatrixX = font?.fontMatrix?.[0] ?? 0.001;
        break;
      }
      case OPS.setTextMatrix:
        tm = args as Matrix;
        tlm = tm;
        break;
      case OPS.moveText:
        moveText(Number(args[0]), Number(args[1]));
        break;
      case OPS.setLeadingMoveText:
        leading = -Number(args[1]);
        moveText(Number(args[0]), Number(args[1]));
        break;
      case OPS.nextLine:
        moveText(0, -leading);
        break;
      case OPS.setLeading:
        leading = Number(args[0]);
        break;
      case OPS.setCharSpacing:
        charSpacing = Number(args[0]);
        break;
      case OPS.setWordSpacing:
        wordSpacing = Number(args[0]);
        break;
      case OPS.setHScale:
        hScale = Number(args[0]) / 100;
        break;
      case OPS.setTextRise:
        rise = Number(args[0]);
        break;
      case OPS.showText:
      case OPS.showSpacedText:
      case OPS.nextLineShowText:
      case OPS.nextLineSetSpacingShowText: {
        if (fn === OPS.nextLineShowText) moveText(0, -leading);
        if (fn === OPS.nextLineSetSpacingShowText) {
          wordSpacing = Number(args[0]);
          charSpacing = Number(args[1]);
          moveText(0, -leading);
        }
        const glyphs = (fn === OPS.nextLineSetSpacingShowText ? args[2] : args[0]) as Array<Glyph | number> | undefined;
        if (!Array.isArray(glyphs)) break;
        const trm = multiply(tm, ctm);
        const size = Math.abs(fontSize) * Math.hypot(trm[2], trm[3]);
        let tx = 0;
        for (const glyph of glyphs) {
          if (typeof glyph === "number") {
            tx -= (glyph / 1000) * fontSize * hScale;
            continue;
          }
          const advance = ((glyph.width ?? 0) * fontMatrixX * fontSize + charSpacing + (glyph.isSpace ? wordSpacing : 0)) * hScale;
          const text = String(glyph.unicode ?? "").normalize("NFC");
          if (text) {
            const [ax, ay] = apply(trm, tx, rise);
            const [bx] = apply(trm, tx + advance, rise);
            // A glyph that stands for several letters (a conjunct) shares its width.
            const parts = [...text];
            parts.forEach((ch, k) => {
              const from = ax + ((bx - ax) * k) / parts.length;
              const to = ax + ((bx - ax) * (k + 1)) / parts.length;
              chars.push({ ch, x0: Math.min(from, to), x1: Math.max(from, to), y: ay, size });
            });
          }
          tx += advance;
        }
        // Text space advances by what was drawn.
        tm = multiply([1, 0, 0, 1, tx, 0], tm);
        break;
      }
      default:
        break;
    }
  }
  return chars;
}

/** Page text from placed characters, with a space wherever the drawing leaves a gap or starts a new line. */
function pageText(chars: PlacedChar[]) {
  let text = "";
  const at: Array<PlacedChar | null> = [];
  let prev: PlacedChar | null = null;
  for (const c of chars) {
    if (prev) {
      const newLine = Math.abs(c.y - prev.y) > prev.size * 0.4;
      const gap = c.x0 - prev.x1;
      const afterVirama = /[्્]$/.test(text);
      if (!text.endsWith(" ") && !/\s/.test(c.ch) && (newLine || (!afterVirama && gap > prev.size * 0.2))) {
        text += " ";
        at.push(null);
      }
    }
    text += c.ch;
    at.push(c);
    prev = c;
  }
  return { text, at };
}

/**
 * Boxes (PDF user space) around every match of the queries, one per match per
 * line, from the glyphs themselves. Null if the page could not be replayed.
 */
export async function findGlyphWordBoxes(
  page: GlyphPage,
  OPS: PdfOps,
  queries: string[],
  mode: OCRSearchMode,
  scripts: OCRSearchScripts = null
): Promise<WordBox[] | null> {
  if (!queries.length) return [];
  let chars: PlacedChar[];
  try {
    chars = await placeCharacters(page, OPS);
  } catch {
    return null;
  }
  if (!chars.length) return null;
  const { text, at } = pageText(chars);
  const seen = new Set<string>();
  const boxes: WordBox[] = [];
  for (const reading of readingOrders(text)) {
    for (const match of findOCRSearchMatchesForQueries(reading, queries, mode, scripts)) {
      const key = `${match.start}:${match.end}`;
      if (seen.has(key)) continue;
      seen.add(key);
      // Group the matched characters by line.
      const lines: PlacedChar[][] = [];
      for (let i = match.start; i < match.end; i += 1) {
        const c = at[i];
        if (!c) continue;
        const line = lines[lines.length - 1];
        if (line && Math.abs(line[0].y - c.y) <= c.size * 0.4) line.push(c);
        else lines.push([c]);
      }
      for (const line of lines) {
        const x0 = Math.min(...line.map((c) => c.x0));
        const x1 = Math.max(...line.map((c) => c.x1));
        const size = Math.max(...line.map((c) => c.size));
        const y = line[0].y;
        boxes.push({ x: x0, y: y - size * 0.3, width: Math.max(size * 0.5, x1 - x0), height: size * 1.3, fontSize: size, text: match.text });
      }
    }
  }
  return boxes;
}
