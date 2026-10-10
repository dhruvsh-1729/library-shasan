// The word-list PDF: for each granth, the searched word, the granth name and
// the number of occurrences, then a table of क्रमः | शब्दः | पृष्ठम् with one row per
// occurrence (the word as printed and the granth's printed page number).
//
// pdf-lib cannot shape Devanagari or Gujarati (its fontkit fails on Indic
// scripts), so text is shaped with HarfBuzz and drawn glyph by glyph from
// fonts embedded here as CID fonts. Each drawn string carries its Unicode text
// as /ActualText, so words copy and search correctly in PDF readers.
import { readFile } from "node:fs/promises";
import path from "node:path";
// harfbuzzjs initialises its WASM with a top-level await, so it cannot be
// require()d; it is imported on first use instead (see loadFaces).
import type * as HarfBuzz from "harfbuzzjs";
import {
  PDFArray,
  PDFDocument,
  PDFHexString,
  PDFName,
  PDFNumber,
  PDFOperator,
  PDFOperatorNames,
  type PDFPage,
  type PDFRef,
  PDFString,
  beginText,
  endText,
  popGraphicsState,
  pushGraphicsState,
  rgb,
  setFontAndSize,
  setTextMatrix,
  showText,
} from "pdf-lib";

export type WordListRow = {
  word: string;
  /** Printed page ("91", or "73-74" for two book pages per scan); null when unknown. */
  printedPage: string | null;
  pdfPage: number;
};

export type WordListSection = {
  granthName: string;
  rows: WordListRow[];
};

const FONT_DIR = path.join(process.cwd(), "data", "fonts");
const FONT_FILES = {
  devanagari: { regular: "NotoSerifDevanagari-Regular.ttf", bold: "NotoSerifDevanagari-Bold.ttf" },
  gujarati: { regular: "NotoSerifGujarati-Regular.ttf", bold: "NotoSerifGujarati-Bold.ttf" },
} as const;
type Script = keyof typeof FONT_FILES;
export type Weight = "regular" | "bold";

export const A4: [number, number] = [595.28, 841.89];
export const MARGIN = 42.5; // 15 mm
const TITLE_SIZE = 16;
const TEXT_SIZE = 12;
const MIN_WORD_SIZE = 7;
const CELL_PAD_X = 8.5; // 3 mm
const ROW_HEIGHT = 24;
const BORDER = 0.75;
const SERIAL_WIDTH = 34; // 12 mm
const PAGE_WIDTH_COL = 85; // 30 mm

const DEVANAGARI_DIGITS = "०१२३४५६७८९";
export const toDevanagariDigits = (value: string | number) =>
  String(value).replace(/[0-9]/g, (d) => DEVANAGARI_DIGITS[Number(d)]);

// ------------------------------------------------------------------ fonts
// (The fonts, shaping, embedding and wrapping below are shared with the
// vyutpatti PDF, lib/vyutpatti/pdf.ts.)

export type LoadedFace = {
  bytes: Uint8Array;
  font: HarfBuzz.Font;
  upem: number;
  ascender: number;
  descender: number;
  bbox: [number, number, number, number];
  postscriptName: string;
  unicodeOfGlyph: Map<number, string>;
};

let facesPromise: Promise<Record<Script, Record<Weight, LoadedFace>>> | null = null;

let hb: typeof HarfBuzz;

function readHeadBBox(face: HarfBuzz.Face): [number, number, number, number] {
  const head = face.referenceTable("head");
  if (!head || head.length < 44) return [-500, -500, 1500, 1200];
  const view = new DataView(head.buffer, head.byteOffset, head.byteLength);
  return [view.getInt16(36), view.getInt16(38), view.getInt16(40), view.getInt16(42)];
}

async function loadFace(file: string): Promise<LoadedFace> {
  const bytes = new Uint8Array(await readFile(path.join(FONT_DIR, file)));
  const face = new hb.Face(new hb.Blob(bytes));
  const font = new hb.Font(face);
  const unicodeOfGlyph = new Map<number, string>();
  for (const codePoint of face.collectUnicodes()) {
    const gid = font.glyph(codePoint);
    if (gid != null && gid > 0 && !unicodeOfGlyph.has(gid)) unicodeOfGlyph.set(gid, String.fromCodePoint(codePoint));
  }
  const extents = font.hExtents();
  return {
    bytes,
    font,
    upem: face.upem,
    ascender: extents.ascender,
    descender: extents.descender,
    bbox: readHeadBBox(face),
    postscriptName: file.replace(/\.ttf$/, ""),
    unicodeOfGlyph,
  };
}

export function loadFaces() {
  facesPromise ??= (async () => {
    hb = await import("harfbuzzjs");
    const out = {} as Record<Script, Record<Weight, LoadedFace>>;
    for (const script of Object.keys(FONT_FILES) as Script[]) {
      out[script] = {
        regular: await loadFace(FONT_FILES[script].regular),
        bold: await loadFace(FONT_FILES[script].bold),
      };
    }
    return out;
  })();
  return facesPromise;
}

// ------------------------------------------------------------------ shaping

type ShapedGlyph = { gid: number; xAdvance: number; xOffset: number; yOffset: number; cluster: number };
export type ShapedRun = { face: LoadedFace; text: string; glyphs: ShapedGlyph[]; width: number };

const GUJARATI = /[઀-૿]/;
const NEUTRAL = /[\s\d.,:;|()[\]\-–—'"“”‘’/!?+*=%&]/;

/** Splits text into runs of one script; spaces, digits and punctuation join the run they sit in. */
function scriptRuns(text: string): Array<{ script: Script; text: string }> {
  const runs: Array<{ script: Script; text: string }> = [];
  for (const char of text) {
    const script: Script | null = GUJARATI.test(char) ? "gujarati" : NEUTRAL.test(char) ? null : "devanagari";
    const last = runs[runs.length - 1];
    if (last && (script === null || script === last.script)) last.text += char;
    else runs.push({ script: script ?? last?.script ?? "devanagari", text: char });
  }
  return runs;
}

function shapeRun(face: LoadedFace, text: string): ShapedRun {
  const buffer = new hb.Buffer();
  buffer.addText(text);
  buffer.guessSegmentProperties();
  hb.shape(face.font, buffer);
  const infos = buffer.getGlyphInfos();
  const positions = buffer.getGlyphPositions();
  const glyphs = infos.map((info, i) => ({
    gid: info.codepoint,
    xAdvance: positions[i].xAdvance,
    xOffset: positions[i].xOffset,
    yOffset: positions[i].yOffset,
    cluster: info.cluster,
  }));
  return { face, text, glyphs, width: glyphs.reduce((sum, g) => sum + g.xAdvance, 0) / face.upem };
}

// ------------------------------------------------------------------ embedding

type EmbeddedFont = { face: LoadedFace; ref: PDFRef; widths: Map<number, number>; toUnicode: Map<number, string> };

export class FontEmbedder {
  private readonly fonts = new Map<LoadedFace, EmbeddedFont>();
  private readonly pageNames = new Map<string, PDFName>();
  private readonly doc: PDFDocument;

  constructor(doc: PDFDocument) {
    this.doc = doc;
  }

  /** Records the glyphs of a shaped run, for the width table and ToUnicode map. */
  use(run: ShapedRun) {
    const embedded = this.embedded(run.face);
    const clusterGlyphs = new Map<number, number>();
    for (const g of run.glyphs) clusterGlyphs.set(g.cluster, (clusterGlyphs.get(g.cluster) ?? 0) + 1);
    const clusterStarts = [...new Set(run.glyphs.map((g) => g.cluster))].sort((a, b) => a - b);
    const clusterText = (cluster: number) => {
      const next = clusterStarts.find((c) => c > cluster) ?? run.text.length;
      return run.text.slice(cluster, next);
    };
    for (const g of run.glyphs) {
      if (!embedded.widths.has(g.gid)) embedded.widths.set(g.gid, run.face.font.glyphHAdvance(g.gid));
      if (embedded.toUnicode.has(g.gid)) continue;
      const own = run.face.unicodeOfGlyph.get(g.gid);
      if (own) embedded.toUnicode.set(g.gid, own);
      else if (clusterGlyphs.get(g.cluster) === 1) embedded.toUnicode.set(g.gid, clusterText(g.cluster));
    }
    return embedded;
  }

  fontName(page: PDFPage, embedded: EmbeddedFont) {
    const key = `${page.ref.toString()}|${embedded.ref.toString()}`;
    let name = this.pageNames.get(key);
    if (!name) {
      name = page.node.newFontDictionary(embedded.face.postscriptName.replace(/[^A-Za-z]/g, ""), embedded.ref);
      this.pageNames.set(key, name);
    }
    return name;
  }

  private embedded(face: LoadedFace) {
    let embedded = this.fonts.get(face);
    if (!embedded) {
      embedded = { face, ref: this.doc.context.nextRef(), widths: new Map(), toUnicode: new Map() };
      this.fonts.set(face, embedded);
    }
    return embedded;
  }

  /** Writes every used font as a Type0 / CIDFontType2 font with Identity-H encoding. */
  finish() {
    const ctx = this.doc.context;
    for (const { face, ref, widths, toUnicode } of this.fonts.values()) {
      const scale = 1000 / face.upem;
      const fontFile = ctx.register(ctx.flateStream(face.bytes, { Length1: face.bytes.length }));
      const descriptor = ctx.register(
        ctx.obj({
          Type: "FontDescriptor",
          FontName: face.postscriptName,
          Flags: 4,
          FontBBox: face.bbox.map((v) => Math.round(v * scale)),
          ItalicAngle: 0,
          Ascent: Math.round(face.ascender * scale),
          Descent: Math.round(face.descender * scale),
          CapHeight: Math.round(face.ascender * scale),
          StemV: 80,
          FontFile2: fontFile,
        })
      );
      const w = PDFArray.withContext(ctx);
      for (const gid of [...widths.keys()].sort((a, b) => a - b)) {
        w.push(PDFNumber.of(gid));
        w.push(ctx.obj([Math.round(widths.get(gid)! * scale)]));
      }
      const cidFont = ctx.register(
        ctx.obj({
          Type: "Font",
          Subtype: "CIDFontType2",
          BaseFont: face.postscriptName,
          CIDSystemInfo: { Registry: PDFString.of("Adobe"), Ordering: PDFString.of("Identity"), Supplement: 0 },
          FontDescriptor: descriptor,
          DW: 0,
          W: w,
          CIDToGIDMap: "Identity",
        })
      );

      const cmap = ctx.register(ctx.flateStream(buildToUnicodeCMap(toUnicode)));
      ctx.assign(
        ref,
        ctx.obj({
          Type: "Font",
          Subtype: "Type0",
          BaseFont: face.postscriptName,
          Encoding: "Identity-H",
          DescendantFonts: [cidFont],
          ToUnicode: cmap,
        })
      );
    }
  }
}

function utf16Hex(text: string) {
  let hex = "";
  for (let i = 0; i < text.length; i += 1) hex += text.charCodeAt(i).toString(16).padStart(4, "0");
  return hex.toUpperCase();
}

function buildToUnicodeCMap(map: Map<number, string>) {
  const entries = [...map.entries()].sort((a, b) => a[0] - b[0]);
  const blocks: string[] = [];
  for (let i = 0; i < entries.length; i += 100) {
    const chunk = entries.slice(i, i + 100);
    blocks.push(
      `${chunk.length} beginbfchar\n${chunk
        .map(([gid, text]) => `<${gid.toString(16).padStart(4, "0").toUpperCase()}> <${utf16Hex(text)}>`)
        .join("\n")}\nendbfchar`
    );
  }
  return [
    "/CIDInit /ProcSet findresource begin",
    "12 dict begin",
    "begincmap",
    "/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def",
    "/CMapName /Adobe-Identity-UCS def",
    "/CMapType 2 def",
    "1 begincodespacerange",
    "<0000> <FFFF>",
    "endcodespacerange",
    ...blocks,
    "endcmap",
    "CMapName currentdict /CMap defineresource pop",
    "end",
    "end",
  ].join("\n");
}

// ------------------------------------------------------------------ drawing

export class TextDrawer {
  private readonly faces: Record<Script, Record<Weight, LoadedFace>>;
  private readonly embedder: FontEmbedder;

  constructor(faces: Record<Script, Record<Weight, LoadedFace>>, embedder: FontEmbedder) {
    this.faces = faces;
    this.embedder = embedder;
  }

  layout(text: string, weight: Weight) {
    // Neither Noto Serif face has arrows (OCR writes "→" in some granths); they would print as boxes.
    text = text.replace(/[→⇒]/g, "->").replace(/[←⇐]/g, "<-");
    return scriptRuns(text).map((run) => shapeRun(this.faces[run.script][weight], run.text));
  }

  width(runs: ShapedRun[], size: number) {
    return runs.reduce((sum, run) => sum + run.width * size, 0);
  }

  /** Draws text with its baseline at y, starting at x; returns the pen position after it. */
  draw(page: PDFPage, runs: ShapedRun[], x: number, y: number, size: number) {
    const text = runs.map((run) => run.text).join("");
    const ops: PDFOperator[] = [
      PDFOperator.of(PDFOperatorNames.BeginMarkedContentSequence, [
        PDFName.of("Span"),
        // pdf-lib's operator typings omit dictionaries, but it writes them out as
        // inline << … >> properties, which is what BDC takes.
        page.doc.context.obj({ ActualText: PDFHexString.fromText(text) }) as unknown as PDFName,
      ]),
      beginText(),
    ];
    let penX = x;
    for (const run of runs) {
      const embedded = this.embedder.use(run);
      const name = this.embedder.fontName(page, embedded);
      const unit = size / run.face.upem;
      ops.push(setFontAndSize(name, size));
      for (const g of run.glyphs) {
        ops.push(setTextMatrix(1, 0, 0, 1, penX + g.xOffset * unit, y + g.yOffset * unit));
        ops.push(showText(PDFHexString.of(g.gid.toString(16).padStart(4, "0"))));
        penX += g.xAdvance * unit;
      }
    }
    ops.push(endText(), PDFOperator.of(PDFOperatorNames.EndMarkedContent));
    page.pushOperators(pushGraphicsState(), ...ops, popGraphicsState());
    return penX;
  }
}

export function strokeRect(page: PDFPage, x: number, y: number, width: number, height: number, borderWidth = BORDER) {
  page.drawRectangle({ x, y, width, height, borderWidth, borderColor: rgb(0, 0, 0) });
}

// ------------------------------------------------------------------ document

/** Baseline inside a row of the given height, centring the font's ascender–descender box. */
function baselineIn(rowBottom: number, height: number, face: LoadedFace, size: number) {
  const asc = (face.ascender / face.upem) * size;
  const desc = (-face.descender / face.upem) * size;
  return rowBottom + (height - (asc + desc)) / 2 + desc;
}

export async function buildWordListPdf(options: { word: string; sections: WordListSection[] }) {
  const faces = await loadFaces();
  const doc = await PDFDocument.create();
  doc.setTitle(`${options.word} - word list`);
  doc.setProducer("Granth library word search");
  const embedder = new FontEmbedder(doc);
  const text = new TextDrawer(faces, embedder);
  const [pageWidth, pageHeight] = A4;
  const tableWidth = pageWidth - 2 * MARGIN;
  const columns = [SERIAL_WIDTH, tableWidth - SERIAL_WIDTH - PAGE_WIDTH_COL, PAGE_WIDTH_COL];
  const columnX = [MARGIN, MARGIN + columns[0], MARGIN + columns[0] + columns[1]];
  const referenceFace = faces.devanagari.regular;

  const drawCell = (page: PDFPage, col: number, rowBottom: number, value: string, weight: Weight) => {
    // A word too long for its cell is set smaller rather than cut.
    const runs = text.layout(value, weight);
    const room = columns[col] - 2 * CELL_PAD_X;
    const width = text.width(runs, TEXT_SIZE);
    const size = width > room ? Math.max(MIN_WORD_SIZE, (TEXT_SIZE * room) / width) : TEXT_SIZE;
    text.draw(page, runs, columnX[col] + CELL_PAD_X, baselineIn(rowBottom, ROW_HEIGHT, referenceFace, size), size);
  };

  const drawRow = (page: PDFPage, rowTop: number, cells: [string, string, string], weight: Weight) => {
    const bottom = rowTop - ROW_HEIGHT;
    for (let col = 0; col < 3; col += 1) {
      strokeRect(page, columnX[col], bottom, columns[col], ROW_HEIGHT);
      drawCell(page, col, bottom, cells[col], weight);
    }
    return bottom;
  };

  const header: [string, string, string] = ["क्रमः", "शब्दः", "पृष्ठम्"];

  for (const section of options.sections) {
    let page = doc.addPage(A4);
    let y = pageHeight - MARGIN;

    const titleRuns = text.layout(`शब्दः : ${options.word}`, "bold");
    y -= TITLE_SIZE;
    text.draw(page, titleRuns, MARGIN, y, TITLE_SIZE);
    y -= TEXT_SIZE * 1.9;
    const infoRuns = text.layout(
      `ग्रन्थः : ${section.granthName}   |   कुल : ${toDevanagariDigits(section.rows.length)}`,
      "regular"
    );
    const infoSize = Math.min(TEXT_SIZE, TEXT_SIZE * (tableWidth / Math.max(1, text.width(infoRuns, TEXT_SIZE))));
    text.draw(page, infoRuns, MARGIN, y, infoSize);
    y -= 14;

    y = drawRow(page, y, header, "bold");
    section.rows.forEach((row, index) => {
      if (y - ROW_HEIGHT < MARGIN) {
        page = doc.addPage(A4);
        y = drawRow(page, pageHeight - MARGIN, header, "bold");
      }
      const printed = row.printedPage ? toDevanagariDigits(row.printedPage) : `PDF ${toDevanagariDigits(row.pdfPage)}`;
      y = drawRow(page, y, [toDevanagariDigits(index + 1), row.word, printed], "regular");
    });
  }

  embedder.finish();
  return doc.save({ useObjectStreams: false });
}

// ------------------------------------------------------------------ line list

export type LineListRow = {
  /** Printed page ("91", "73-74"), or null when unknown. */
  printedPage: string | null;
  pdfPage: number;
  /** The page's lines that hold a match (a passage can hold several close together). */
  hitLines: number[];
  /** The matched lines with two lines of text above and below, as one run; found words are hits. */
  pieces: Array<{ text: string; hit: boolean }>;
};

/** `heading` is the granth's 3-digit book number, printed in the footer of each of its pages. */
export type LineListSection = { heading: string; rows: LineListRow[] };

const LINE_TEXT_SIZE = 10.5;
// tight leading so as many lines as possible fit on a page
const LINE_LEADING = 1.25;
const LINE_HEADING_SIZE = 12;
// the line list: space above and below each record, and the rule between records
const LINE_RECORD_PAD = 4;
const LINE_RULE = 0.35;
const LINE_RULE_COLOR = rgb(0.55, 0.55, 0.55);
const UNDERLINE = 0.6;
/**
 * The vyutpatti table's old layout: the table in the left 60% of the page, the
 * right 40% blank for Maharaj Saheb's handwritten notes.
 */
export const NOTES_TABLE_RIGHT = A4[0] * 0.6;
/** Room kept at the bottom of each page for the footer. */
export const FOOTER_ROOM = 22;

/** The book number centred at the foot of the page. */
export function drawFooter(text: TextDrawer, page: PDFPage, value: string, left = MARGIN, right = NOTES_TABLE_RIGHT) {
  if (!value) return;
  const runs = text.layout(value, "bold");
  const width = text.width(runs, LINE_HEADING_SIZE);
  text.draw(page, runs, left + Math.max(0, (right - left - width) / 2), MARGIN - 6, LINE_HEADING_SIZE);
}

export type Piece = { text: string; weight: Weight; underline?: boolean };

export type Word = {
  parts: Array<{ runs: ShapedRun[]; underline?: boolean }>;
  width: number;
  space: number;
  /** a later piece of a word too wide for the cell: starts a new line, no space */
  continues?: boolean;
};

const AKSHARAS = new Intl.Segmenter("sa", { granularity: "grapheme" });

/** Splits a word wider than `room` between akṣaras (a conjunct stays whole) into pieces that fit. */
function breakWord(text: TextDrawer, pieces: Piece[], size: number, room: number, space: number): Word[] {
  const out: Word[] = [];
  let parts: Word["parts"] = [];
  let width = 0;
  const flush = () => { if (parts.length) out.push({ parts, width, space: out.length ? 0 : space, continues: out.length > 0 }); parts = []; width = 0; };
  for (const piece of pieces) {
    let chunk = "";
    for (const { segment } of AKSHARAS.segment(piece.text)) {
      const w = text.width(text.layout(chunk + segment, piece.weight), size);
      if (width + w > room && (chunk || parts.length)) {
        if (chunk) parts.push({ runs: text.layout(chunk, piece.weight), underline: piece.underline });
        flush();
        chunk = segment;
      } else chunk += segment;
    }
    if (chunk) {
      const runs = text.layout(chunk, piece.weight);
      parts.push({ runs, underline: piece.underline });
      width += text.width(runs, size);
    }
  }
  flush();
  return out;
}

/** Breaks pieces into lines no wider than `room` (at spaces; a word longer than a line gets its own). */
export function wrapPieces(text: TextDrawer, pieces: Piece[], size: number, room: number) {
  // words keep their pieces, so a word half-highlighted stays one word
  const words: Array<Array<Piece>> = [[]];
  for (const piece of pieces) {
    const bits = piece.text.split(/( +)/);
    for (const bit of bits) {
      if (!bit) continue;
      if (/^ +$/.test(bit)) { words.push([]); continue; }
      words[words.length - 1].push({ ...piece, text: bit });
    }
  }
  const spaceWidth = text.width(text.layout(" ", "regular"), size);
  const shaped: Word[] = words.filter((w) => w.length).flatMap((w) => {
    const parts = w.map((p) => ({ runs: text.layout(p.text, p.weight), underline: p.underline }));
    const width = parts.reduce((sum, p) => sum + text.width(p.runs, size), 0);
    return width > room ? breakWord(text, w, size, room, spaceWidth) : [{ parts, width, space: spaceWidth }];
  });
  const lines: Word[][] = [[]];
  let used = 0;
  for (const w of shaped) {
    const need = (lines[lines.length - 1].length ? w.space : 0) + w.width;
    if ((w.continues || used + need > room) && lines[lines.length - 1].length) { lines.push([w]); used = w.width; }
    else { lines[lines.length - 1].push(w); used += need; }
  }
  return lines;
}

/**
 * "[५, २८४]": the line, then the page, the page underlined (Sahebji, 9 Oct
 * 2026). In a file of several granths the page follows the granth's book
 * number, in English digits: "[५, 073/२८४]". A page with no printed number
 * says so in full: "[५, PDF page number १२]". No-break spaces keep a
 * reference on one line.
 */
function referencePieces(row: LineListRow, book: string | null): Piece[] {
  const page = row.printedPage ? toDevanagariDigits(row.printedPage) : toDevanagariDigits(row.pdfPage);
  const pageLabel = row.printedPage ? "" : "PDF\u00a0page\u00a0number\u00a0";
  return row.hitLines.flatMap((line, i): Piece[] => [
    { text: `${i ? " " : "\u00a0"}[${toDevanagariDigits(line)},\u00a0${book ? `${book}/` : ""}${pageLabel}`, weight: "regular" },
    { text: page, weight: "regular", underline: true },
    { text: "]", weight: "regular" },
  ]);
}

/**
 * The line list: for each granth, one record per matched line with two lines
 * of text above and below it (Sahebji, 9 Oct 2026), the found words bold and
 * the "[line, page]" reference right after the passage's last word. Matches
 * close enough for their passages to overlap or touch share one record, which
 * carries a reference for each. The text spans the full page width (for the
 * next month or so, so the longer records print on fewer pages); the old
 * left-40% column with the rest blank for notes is gone. The book number is
 * printed in the footer of each page, and in a file of several granths also in
 * every reference. No table and no serial numbers: a thin rule across the page
 * separates one record from the next.
 */
export async function buildLineListPdf(options: { word: string; sections: LineListSection[] }) {
  const faces = await loadFaces();
  const doc = await PDFDocument.create();
  doc.setTitle(`${options.word} - line list`);
  doc.setProducer("Granth library word search");
  const embedder = new FontEmbedder(doc);
  const text = new TextDrawer(faces, embedder);
  const [pageWidth, pageHeight] = A4;
  const bottom = MARGIN + FOOTER_ROOM;
  const room = pageWidth - 2 * MARGIN;
  const lineHeight = LINE_TEXT_SIZE * LINE_LEADING;
  const reference = faces.devanagari.regular;
  const ascent = (reference.ascender / reference.upem) * LINE_TEXT_SIZE;
  const descent = (-reference.descender / reference.upem) * LINE_TEXT_SIZE;
  const severalGranths = options.sections.length > 1;

  const drawLines = (page: PDFPage, lines: Word[][], x: number, top: number) =>
    lines.forEach((words, i) => {
      const baseline = top - ascent - i * lineHeight;
      let at = x;
      words.forEach((w, k) => {
        if (k) at += w.space;
        for (const part of w.parts) {
          const start = at;
          at = text.draw(page, part.runs, at, baseline, LINE_TEXT_SIZE);
          if (part.underline) {
            const y = baseline - descent * 0.45;
            page.drawLine({ start: { x: start, y }, end: { x: at, y }, thickness: UNDERLINE, color: rgb(0, 0, 0) });
          }
        }
      });
    });
  const rule = (page: PDFPage, y: number) =>
    page.drawLine({ start: { x: MARGIN, y }, end: { x: pageWidth - MARGIN, y }, thickness: LINE_RULE, color: LINE_RULE_COLOR });

  for (const section of options.sections) {
    let page = doc.addPage(A4);
    drawFooter(text, page, section.heading, MARGIN, pageWidth - MARGIN);
    let y = pageHeight - MARGIN;

    section.rows.forEach((row, index) => {
      const pieces: Piece[] = [
        ...row.pieces.map((piece): Piece => ({ text: piece.text, weight: piece.hit ? "bold" : "regular" })),
        ...referencePieces(row, severalGranths ? section.heading : null),
      ];
      const wrapped = wrapPieces(text, pieces, LINE_TEXT_SIZE, room);
      const height = wrapped.length * lineHeight + 2 * LINE_RECORD_PAD;
      if (y - height < bottom) {
        page = doc.addPage(A4);
        drawFooter(text, page, section.heading, MARGIN, pageWidth - MARGIN);
        y = pageHeight - MARGIN;
      }
      // a thin rule between two records, none above the first on a page
      if (index > 0 && y < pageHeight - MARGIN) rule(page, y);
      drawLines(page, wrapped, MARGIN, y - LINE_RECORD_PAD);
      y -= height;
    });
  }

  embedder.finish();
  return doc.save({ useObjectStreams: false });
}
