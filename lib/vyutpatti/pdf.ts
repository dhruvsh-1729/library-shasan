// The vyutpatti PDF, in the two layouts of Maharaj Saheb's sample (WhatsApp
// scan, 28 Sep 2026 17.19):
//
//   1. the internal table (the box sheet): Sr | V.T | Granth | ShastraPath |
//      Pub.Rem | In.Rem with one row per kosh entry, the kosh named by its
//      3-digit granth number, and under the table a key of those numbers to the
//      koshes' names. The sample's vishay heading, "BOX No - n" and the vishay
//      band across the table are left out (Sahebji, 9 Oct 2026). The
//      table spans the full page width (Dhruv, 6 Oct 2026: the file sent to
//      Maharaj Saheb); { fullWidth: false } gives the old left-60% layout with
//      the right 40% blank for notes;
//   2. the reader page: "1.1 | अचौर्य" large, then "व्युत्पत्तिअर्थ" and one line
//      per word, each starting with a corner mark.
//
// All the tables come first, then all the reader pages, so the reader part
// prints as one run. Text is shaped with HarfBuzz (lib/word-list-pdf).
//
// For now only the internal table is exported (Dhruv, 3 Oct 2026): the reader
// page, the layout as it will go in the book, is still being worked on. Its
// code below is kept and runs again with { readerPages: true }.

import { PDFDocument, type PDFPage, rgb } from "pdf-lib";
import {
  A4,
  FontEmbedder,
  MARGIN,
  NOTES_TABLE_RIGHT,
  type Piece,
  TextDrawer,
  type Weight,
  loadFaces,
  strokeRect,
  wrapPieces,
} from "@/lib/word-list-pdf";
import type { ReaderLine, TableRow } from "@/lib/vyutpatti/format";

export type VyutpattiPdfSection = {
  number: string;
  vishay: string;
  box: string;
  rows: TableRow[];
  lines: ReaderLine[];
};

const BLACK = rgb(0, 0, 0);

// ------------------------------------------------------------------ table

const TABLE_SIZE = 8.5;
const TABLE_LEADING = 1.3;
const CELL_PAD_X = 3;
const CELL_PAD_Y = 2.5;
const TABLE_BORDER = 0.6;
// Sr | V.T | Granth | ShastraPath | Pub.Rem | In.Rem, in the table's 60% of the page
const TABLE_COLUMNS = [18, 22, 38, 0, 46, 70];
// Full page width (the default): the extra room goes to ShastraPath and In.Rem, and the Granth
// column fits समासविग्रह on one line.
const FULL_WIDTH_COLUMNS = [20, 24, 62, 0, 46, 150];
const TABLE_HEADER = ["Sr.", "V.T", "Granth", "ShastraPath", "Pub.Rem", "In.Rem"];
// space between the table and the key to its Granth column
const LEGEND_GAP = 12;

// ------------------------------------------------------------------ reader

const NUMBER_SIZE = 40;
const TITLE_SIZE = 40;
const MIN_TITLE_SIZE = 24;
const LABEL_SIZE = 12;
const LINE_SIZE = 12.5;
const LINE_LEADING = 1.55;
const LABEL_WIDTH = 92;
const MARK_GAP = 16;

function drawWrapped(
  text: TextDrawer,
  page: PDFPage,
  lines: ReturnType<typeof wrapPieces>,
  x: number,
  firstBaseline: number,
  size: number,
  leading: number
) {
  lines.forEach((words, i) => {
    let pen = x;
    words.forEach((w, k) => {
      if (k) pen += w.space;
      for (const part of w.parts) pen = text.draw(page, part.runs, pen, firstBaseline - i * size * leading, size);
    });
  });
}

/** The corner mark the sample prints before each line: ⌜ drawn as two strokes. */
function cornerMark(page: PDFPage, x: number, baseline: number, size: number) {
  const top = baseline + size * 0.72;
  const len = size * 0.42;
  page.drawLine({ start: { x, y: top }, end: { x: x + len, y: top }, thickness: 1.1, color: BLACK });
  page.drawLine({ start: { x, y: top }, end: { x, y: top - len }, thickness: 1.1, color: BLACK });
}

/** The book-format reader pages are left out of the export until that layout is settled. */
export const EXPORT_READER_PAGES = false;

export async function buildVyutpattiPdf(sections: VyutpattiPdfSection[], options: { readerPages?: boolean; fullWidth?: boolean } = {}) {
  const readerPages = options.readerPages ?? EXPORT_READER_PAGES;
  const faces = await loadFaces();
  const doc = await PDFDocument.create();
  doc.setTitle(sections.length === 1 ? `${sections[0].vishay} - व्युत्पत्ति` : "व्युत्पत्ति");
  doc.setProducer("Granth library vyutpatti");
  const embedder = new FontEmbedder(doc);
  const text = new TextDrawer(faces, embedder);
  const [pageWidth, pageHeight] = A4;
  const width = pageWidth - 2 * MARGIN;
  const reference = faces.devanagari.regular;
  const ascentOf = (size: number) => (reference.ascender / reference.upem) * size;

  // ---------------------------------------------------------------- tables
  const fullWidth = options.fullWidth ?? true;
  const tableWidth = fullWidth ? width : NOTES_TABLE_RIGHT - MARGIN;
  const cols = [...(fullWidth ? FULL_WIDTH_COLUMNS : TABLE_COLUMNS)];
  cols[3] = tableWidth - cols.reduce((a, b) => a + b, 0);
  const colX = cols.map((_, i) => MARGIN + cols.slice(0, i).reduce((a, b) => a + b, 0));
  const lineHeight = TABLE_SIZE * TABLE_LEADING;
  const plain = (value: string, weight: Weight = "regular"): Piece[] => [{ text: value, weight }];

  const rowLayout = (cells: string[], weight: Weight) => {
    // A cell keeps its own line breaks.
    const wrapped = cells.map((cell, col) =>
      String(cell ?? "")
        .split("\n")
        .flatMap((para) => wrapPieces(text, plain(para, weight), TABLE_SIZE, cols[col] - 2 * CELL_PAD_X))
    );
    return { wrapped, height: Math.max(1, ...wrapped.map((l) => l.length)) * lineHeight + 2 * CELL_PAD_Y };
  };

  const drawRow = (page: PDFPage, top: number, layout: ReturnType<typeof rowLayout>) => {
    layout.wrapped.forEach((lines, col) => {
      strokeRect(page, colX[col], top - layout.height, cols[col], layout.height, TABLE_BORDER);
      drawWrapped(text, page, lines, colX[col] + CELL_PAD_X, top - CELL_PAD_Y - ascentOf(TABLE_SIZE), TABLE_SIZE, TABLE_LEADING);
    });
    return top - layout.height;
  };

  for (const section of sections) {
    // No heading, "BOX No" line or vishay band: the sheet is the table alone
    // (Sahebji, 9 Oct 2026). No organisation's name or mark either.
    let page = doc.addPage(A4);
    const header = rowLayout(TABLE_HEADER, "regular");
    let y = drawRow(page, pageHeight - MARGIN, header);

    section.rows.forEach((row, index) => {
      const cells = [String(index + 1), "व्यु.", row.granthNo || row.granth, row.shastraPath, row.pubRem, row.inRem];
      const layout = rowLayout(cells, "regular");
      if (y - layout.height < MARGIN) {
        page = doc.addPage(A4);
        y = drawRow(page, pageHeight - MARGIN, header);
      }
      y = drawRow(page, y, layout);
    });
    if (!section.rows.length) {
      text.draw(page, text.layout("No kosh entry was found.", "regular"), MARGIN, y - 18, 10);
      continue;
    }

    // The key to the Granth column: each kosh's 3-digit granth number and its
    // name, so the numbers need no explaining (Sahebji asked what they were).
    const legend = new Map<string, string>();
    for (const row of section.rows) if (row.granthNo && !legend.has(row.granthNo)) legend.set(row.granthNo, row.granth);
    if (!legend.size) continue;
    const legendLines = [...legend].sort(([a], [b]) => a.localeCompare(b)).flatMap(([no, name]) =>
      wrapPieces(text, plain(`${no} - ${name}`), TABLE_SIZE, tableWidth)
    );
    const needed = (legendLines.length + 1) * lineHeight + LEGEND_GAP;
    if (y - needed < MARGIN) {
      page = doc.addPage(A4);
      y = pageHeight - MARGIN;
    } else y -= LEGEND_GAP;
    drawWrapped(text, page, wrapPieces(text, plain("Granth no.", "bold"), TABLE_SIZE, tableWidth), MARGIN, y - ascentOf(TABLE_SIZE), TABLE_SIZE, TABLE_LEADING);
    drawWrapped(text, page, legendLines, MARGIN, y - ascentOf(TABLE_SIZE) - lineHeight, TABLE_SIZE, TABLE_LEADING);
  }

  // ---------------------------------------------------------------- reader pages
  for (const section of readerPages ? sections : []) {
    let page = doc.addPage(A4);
    const top = pageHeight - MARGIN - 40;
    // "1.1 | अचौर्य"
    const numberRuns = text.layout(section.number || "", "bold");
    const numberWidth = section.number ? text.width(numberRuns, NUMBER_SIZE) : 0;
    const titleBaseline = top - 30;
    const ruleX = MARGIN + (section.number ? numberWidth + 14 : 0);
    if (section.number) text.draw(page, numberRuns, MARGIN, titleBaseline + 26, NUMBER_SIZE);
    // A long vishay is set smaller, down to a size still read at a glance, then
    // broken between aksharas over as many lines as it needs.
    const titleRoom = MARGIN + width - (ruleX + 14);
    const titleRuns = text.layout(section.vishay, "bold");
    const titleSize = Math.max(MIN_TITLE_SIZE, Math.min(TITLE_SIZE, (TITLE_SIZE * titleRoom) / Math.max(1, text.width(titleRuns, TITLE_SIZE))));
    const titleLines = wrapPieces(text, [{ text: section.vishay, weight: "bold" }], titleSize, titleRoom);
    const titleLeading = titleSize * 1.3;
    const titleBottom = titleBaseline - 4 - (titleLines.length - 1) * titleLeading;
    page.drawLine({ start: { x: ruleX, y: top + 34 }, end: { x: ruleX, y: Math.min(titleBaseline - 30, titleBottom - 14) }, thickness: 1.2, color: BLACK });
    drawWrapped(text, page, titleLines, ruleX + 14, titleBaseline - 4, titleSize, 1.3);

    let y = Math.min(titleBaseline - 82, titleBottom - 52);
    const label = text.layout("व्युत्पत्तिअर्थ", "bold");
    text.draw(page, label, MARGIN, y, LABEL_SIZE);
    page.drawLine({
      start: { x: MARGIN, y: y - 5 },
      end: { x: MARGIN + text.width(label, LABEL_SIZE), y: y - 5 },
      thickness: 0.8,
      color: BLACK,
    });

    const lineX = MARGIN + LABEL_WIDTH + MARK_GAP;
    const room = width - LABEL_WIDTH - MARK_GAP;
    for (const line of section.lines) {
      const pieces: Piece[] = [
        { text: line.head, weight: "bold" },
        { text: line.body, weight: "regular" },
      ];
      const wrapped = wrapPieces(text, pieces, LINE_SIZE, room);
      const height = wrapped.length * LINE_SIZE * LINE_LEADING;
      if (y - height < MARGIN) {
        page = doc.addPage(A4);
        y = pageHeight - MARGIN - LINE_SIZE;
      }
      cornerMark(page, lineX - MARK_GAP + 3, y, LINE_SIZE);
      drawWrapped(text, page, wrapped, lineX, y, LINE_SIZE, LINE_LEADING);
      y -= height + LINE_SIZE * 0.45;
    }
  }

  embedder.finish();
  return doc.save({ useObjectStreams: false });
}
