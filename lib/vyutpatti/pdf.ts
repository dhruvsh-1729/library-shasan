// The vyutpatti PDF. Three layouts:
//
//   1. plain (the default; Maharaj Saheb, 2 and 6 Nov 2026: "we don't want
//      the box … a normal format", "remove the tables; we want line-to-line
//      data"): one entry after another as ordinary lines, no borders —
//
//          1.  देव - पुं. (दीव्यतीति देवः) (अभिधानव्युत्पत्तिप्रक्रियाकोश भाग-2, पृ. 349)
//              દેવતા અર્થમાં.
//              અનુવાદ: પુંલિંગ. 'दीव्यति इति देवः' — …
//
//      the Gujarati anuvaad of each entry directly under it, every vishay
//      running on from the last on the same pages;
//   2. table ({ layout: "table" }), the internal table of the 28 Sep 2026
//      sample: Sr | V.T | Granth | ShastraPath | Pub.Rem | In.Rem, one row
//      per kosh entry, the kosh named by its 3-digit granth number, and a key
//      of those numbers under it; the anuvaad on pages of its own after the
//      tables. { fullWidth: false } gives the old left-60% table;
//   3. reader pages ({ readerPages: true }): "1.1 | अचौर्य" large, then
//      "व्युत्पत्तिअर्थ" and one line per word — the layout for the book, not
//      exported while it is being worked on.
//
// Text is shaped with HarfBuzz (lib/word-list-pdf).

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
import { type ReaderLine, type TableRow, splitShastraPath } from "@/lib/vyutpatti/format";
import { drawAnuvaadPages } from "@/lib/anuvaad/pdf";
import type { AnuvaadBlock } from "@/lib/anuvaad/markup";

export type VyutpattiPdfSection = {
  number: string;
  vishay: string;
  box: string;
  /** One per kosh entry; each may carry its Gujarati anuvaad. */
  rows: TableRow[];
  lines: ReaderLine[];
  /** The closing Gujarati line on what the vishay means, after its entries. */
  closing?: string;
};

export type VyutpattiPdfLayout = "plain" | "table";

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

// ------------------------------------------------------------------ plain

const PLAIN_SIZE = 11.5;
const PLAIN_LEADING = 1.55;
const PLAIN_HEADING_SIZE = 15;
const PLAIN_INDENT = 24; // the entry's lines hang under its text, clear of the serial
const ENTRY_GAP = 9;
const SECTION_GAP = 22;
const PLAIN_FOOTER_SIZE = 9;

/** The Gujarati anuvaad of each entry as anuvaad blocks, for the table layout's anuvaad pages. */
function anuvaadBlocks(section: VyutpattiPdfSection): AnuvaadBlock[] {
  const points = section.rows.flatMap((row, i): AnuvaadBlock[] =>
    row.anuvaad?.trim() ? [{ kind: "point", label: `${i + 1}. ${splitShastraPath(row.shastraPath).head}`, text: row.anuvaad.trim(), level: 1 }] : []
  );
  if (!points.length && !section.closing?.trim()) return [];
  return [
    { kind: "title", text: `${section.vishay} — વ્યુત્પત્તિનો ગુજરાતી અનુવાદ` },
    ...points,
    ...(section.closing?.trim() ? [{ kind: "para", text: section.closing.trim() } as AnuvaadBlock] : []),
  ];
}

export async function buildVyutpattiPdf(
  sections: VyutpattiPdfSection[],
  options: { layout?: VyutpattiPdfLayout; readerPages?: boolean; fullWidth?: boolean } = {}
) {
  const layout = options.layout ?? "plain";
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

  // ---------------------------------------------------------------- plain
  if (layout === "plain") {
    const top = pageHeight - MARGIN;
    const bottom = MARGIN + PLAIN_FOOTER_SIZE * 2;
    const step = PLAIN_SIZE * PLAIN_LEADING;
    let pageNo = 0;
    let page: PDFPage | null = null;
    let y = top;
    const newPage = () => {
      page = doc.addPage(A4);
      pageNo += 1;
      const runs = text.layout(String(pageNo), "regular");
      text.draw(page, runs, (pageWidth - text.width(runs, PLAIN_FOOTER_SIZE)) / 2, MARGIN - 6, PLAIN_FOOTER_SIZE);
      y = top;
      return page;
    };
    /** Room for `height` more points, else a fresh page. */
    const room = (height: number): PDFPage => (!page || y - height < bottom ? newPage() : page);
    /** Wrapped lines at x, one at a time, so a long entry runs on over the page break. */
    const flow = (lines: ReturnType<typeof wrapPieces>, x: number, size: number, leading: number, keepWith = 1) => {
      const lineStep = size * leading;
      lines.forEach((line, i) => {
        const p = room(lineStep * (i === 0 ? Math.min(keepWith, lines.length) : 1));
        drawWrapped(text, p, [line], x, y - ascentOf(size), size, leading);
        y -= lineStep;
      });
    };

    sections.forEach((section, s) => {
      if (s && page) y -= SECTION_GAP;
      // the vishay on a line of its own: "1.1 देवपर्षदा"
      const heading = [section.number, section.vishay].filter(Boolean).join(" ");
      flow(wrapPieces(text, plain(heading, "bold"), PLAIN_HEADING_SIZE, width), MARGIN, PLAIN_HEADING_SIZE, 1.35, 3);
      y -= PLAIN_SIZE * 0.5;
      if (!section.rows.length) {
        flow(wrapPieces(text, plain("No kosh entry was found."), PLAIN_SIZE, width), MARGIN, PLAIN_SIZE, PLAIN_LEADING);
        return;
      }
      const room2 = width - PLAIN_INDENT;
      section.rows.forEach((row, index) => {
        if (index) y -= ENTRY_GAP;
        const { head, rest } = splitShastraPath(row.shastraPath);
        const first = wrapPieces(text, [{ text: head, weight: "bold" }, { text: rest ? ` - ${rest}` : "", weight: "regular" }], PLAIN_SIZE, room2);
        const extra: ReturnType<typeof wrapPieces> = [];
        if (row.inRem.trim()) extra.push(...wrapPieces(text, plain(row.inRem.trim()), PLAIN_SIZE, room2));
        if (row.pubRem.trim()) extra.push(...wrapPieces(text, [{ text: "Pub. Rem: ", weight: "bold" }, { text: row.pubRem.trim(), weight: "regular" }], PLAIN_SIZE, room2));
        const gu = row.anuvaad?.trim() ? wrapPieces(text, [{ text: "અનુવાદ: ", weight: "bold" }, { text: row.anuvaad.trim(), weight: "regular" }], PLAIN_SIZE, room2) : [];
        // the serial and the entry's first two lines stay together
        const p = room(step * Math.min(2, first.length + extra.length + gu.length));
        const serial = text.layout(`${index + 1}.`, "regular");
        text.draw(p, serial, MARGIN, y - ascentOf(PLAIN_SIZE), PLAIN_SIZE);
        flow(first, MARGIN + PLAIN_INDENT, PLAIN_SIZE, PLAIN_LEADING);
        flow(extra, MARGIN + PLAIN_INDENT, PLAIN_SIZE, PLAIN_LEADING);
        if (gu.length) {
          y -= PLAIN_SIZE * 0.2;
          flow(gu, MARGIN + PLAIN_INDENT, PLAIN_SIZE, PLAIN_LEADING);
        }
      });
      if (section.closing?.trim()) {
        y -= ENTRY_GAP;
        flow(wrapPieces(text, plain(section.closing.trim()), PLAIN_SIZE, width), MARGIN, PLAIN_SIZE, PLAIN_LEADING, 2);
      }
    });
  }

  // ---------------------------------------------------------------- tables
  for (const section of layout === "table" ? sections : []) {
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

  // ------------------------------------------------------------ anuvaad
  // Table layout: the Gujarati anuvaad of each vishay at the end, one vishay to a page run.
  const withAnuvaad = layout === "table" ? sections.map((s) => ({ s, blocks: anuvaadBlocks(s) })).filter((x) => x.blocks.length) : [];
  if (withAnuvaad.length) {
    const guj = faces.gujarati.regular;
    drawAnuvaadPages(
      doc,
      text,
      (size) => (guj.ascender / guj.upem) * size,
      withAnuvaad.map(({ s, blocks }) => ({ caption: [s.number, s.vishay].filter(Boolean).join(" "), blocks }))
    );
  }

  embedder.finish();
  return doc.save({ useObjectStreams: false });
}
