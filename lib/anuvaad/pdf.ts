// The Gujarati anuvaad pages, laid out like the pages Maharaj Saheb sent with
// the box files (WhatsApp scans, 29 Oct 2026): A4 portrait, the granth as a
// bold heading, then "વિષય:" in bold, numbered part headings in bold, the
// text in a large readable size with bullets (•) and sub-bullets (◦) whose
// labels are bold, the page number at the foot. Each passage starts on a new
// page. Text is shaped with HarfBuzz (lib/word-list-pdf).

import { PDFDocument, type PDFPage, rgb } from "pdf-lib";
import { A4, FontEmbedder, MARGIN, type Piece, TextDrawer, loadFaces, wrapPieces } from "@/lib/word-list-pdf";
import type { AnuvaadBlock } from "@/lib/anuvaad/markup";

export type AnuvaadPdfSection = {
  /** A small line over the heading: the row's Sr. and the box's vishay, so the page can be matched to its row. */
  caption?: string;
  blocks: AnuvaadBlock[];
  /**
   * A second anuvaad of the same passage, set beside the first in two columns
   * (22 Nov 2026: "one option showing the book's translation and AI side by
   * side"); `labels` head the two columns.
   */
  aside?: AnuvaadBlock[];
  labels?: [string, string];
};

const BLACK = rgb(0, 0, 0);

const CAPTION_SIZE = 10;
const TITLE_SIZE = 15;
const VISHAY_SIZE = 14;
const SECTION_SIZE = 14;
const BODY_SIZE = 13;
const LEADING = 1.75;
const SIDE = MARGIN + 14; // the scans have wide side margins
const BULLET_INDENT = 22;
const SUB_INDENT = 46;
const FOOTER_SIZE = 11;

const GUJARATI_DIGITS = "૦૧૨૩૪૫૬૭૮૯";
const gujaratiDigits = (n: number) => String(n).replace(/[0-9]/g, (d) => GUJARATI_DIGITS[Number(d)]);

type Ctx = { doc: PDFDocument; text: TextDrawer; ascent: (size: number) => number };

function drawLines(ctx: Ctx, page: PDFPage, lines: ReturnType<typeof wrapPieces>, x: number, baseline: number, size: number) {
  lines.forEach((words, i) => {
    let pen = x;
    words.forEach((w, k) => {
      if (k) pen += w.space;
      for (const part of w.parts) pen = ctx.text.draw(page, part.runs, pen, baseline - i * size * LEADING, size);
    });
  });
}

/**
 * Adds the anuvaad pages to `doc`, one passage per page run. Shared by the
 * anuvaad export and the vyutpatti PDF (whose Gujarati anuvaad goes at the end).
 */
export type AnuvaadPageOptions = {
  /** false: no page numbers at the foot (the pages are put between other pages). */
  numbers?: boolean;
  /** Filled with the index, in `doc`, of the first page of each section. */
  starts?: number[];
};

export function drawAnuvaadPages(
  doc: PDFDocument,
  text: TextDrawer,
  ascentOf: (size: number) => number,
  sections: AnuvaadPdfSection[],
  firstNumber = 1,
  options: AnuvaadPageOptions = {}
) {
  const ctx: Ctx = { doc, text, ascent: ascentOf };
  const [pageWidth, pageHeight] = A4;
  const top = pageHeight - MARGIN - 6;
  const bottom = MARGIN + 26;
  let number = firstNumber;
  // the pages of the section being drawn, in order: two columns each flow down them on their own
  let pages: PDFPage[] = [];

  const footer = (p: PDFPage) => {
    const runs = text.layout(gujaratiDigits(number), "regular");
    text.draw(p, runs, (pageWidth - text.width(runs, FOOTER_SIZE)) / 2, MARGIN - 4, FOOTER_SIZE);
    number += 1;
  };
  const pageAt = (index: number) => {
    while (pages.length <= index) {
      const p = doc.addPage(A4);
      if (options.numbers !== false) footer(p);
      pages.push(p);
    }
    return pages[index];
  };

  /** Draws blocks down a column from (page index, y), turning pages as needed; returns where it ended. */
  const flow = (blocks: AnuvaadBlock[], left: number, right: number, at: { index: number; y: number }, scale = 1) => {
    let { index: pageIndex, y } = at;
    const room = (height: number) => {
      if (y - height < bottom) {
        pageIndex += 1;
        y = top;
      }
      return pageAt(pageIndex);
    };
    for (const block of blocks) {
      let size = BODY_SIZE * scale;
      let x = left;
      let pieces: Piece[];
      let before = 0;
      let after = size * 0.5;
      let bullet: "dot" | "ring" | null = null;
      // the APC entry's śāstra text, framed (Maharaj Saheb's marks, 17 Nov 2026)
      const framed = block.kind === "boxed";
      if (framed) {
        x = left + 6;
        before = size * 0.3;
        after = size * 0.9;
      }
      switch (block.kind) {
        case "title":
          size = TITLE_SIZE * scale;
          pieces = [{ text: block.text, weight: "bold" }];
          after = size * 0.7;
          break;
        case "vishay":
          size = VISHAY_SIZE * scale;
          pieces = [{ text: `વિષય: ${block.text}`, weight: "bold" }];
          after = size * 0.8;
          break;
        case "section":
          size = SECTION_SIZE * scale;
          pieces = [{ text: block.text, weight: "bold" }];
          before = size * 0.5;
          after = size * 0.45;
          break;
        case "point":
          x = left + (block.level === 2 ? SUB_INDENT : BULLET_INDENT) * scale;
          bullet = block.level === 2 ? "ring" : "dot";
          pieces = block.label
            ? [
                { text: `${block.label}: `, weight: "bold" },
                { text: block.text, weight: "regular" },
              ]
            : [{ text: block.text, weight: "regular" }];
          break;
        case "main":
          pieces = [{ text: block.text, weight: "bold" }];
          break;
        default:
          pieces = [{ text: block.text, weight: "regular" }];
      }
      const lines = wrapPieces(text, pieces, size, right - x - (framed ? 6 : 0));
      const lineHeight = size * LEADING;
      y -= before;
      // A heading is kept with at least two lines of what follows it.
      const keep = block.kind === "section" || block.kind === "title" ? lineHeight * 2 : 0;
      // Long paragraphs flow over pages a few lines at a time.
      let index = 0;
      while (index < lines.length) {
        const p = room(lineHeight + keep);
        const fit = Math.max(1, Math.floor((y - bottom) / lineHeight));
        const chunk = lines.slice(index, index + fit);
        const baseline = y - ascentOf(size);
        if (bullet && index === 0) {
          const cx = x - 10;
          const cy = baseline + size * 0.32;
          if (bullet === "dot") p.drawCircle({ x: cx, y: cy, size: 2.1, color: BLACK });
          else p.drawCircle({ x: cx, y: cy, size: 2.1, borderColor: BLACK, borderWidth: 0.8 });
        }
        drawLines(ctx, p, chunk, x, baseline, size);
        if (framed) {
          const h = chunk.length * lineHeight + 6;
          p.drawRectangle({ x: left, y: y - h + 2, width: right - left, height: h + 2, borderWidth: 0.9, borderColor: BLACK });
        }
        y -= chunk.length * lineHeight;
        index += chunk.length;
      }
      y -= after;
    }
    return { index: pageIndex, y };
  };

  for (const section of sections) {
    pages = [];
    pageAt(0);
    options.starts?.push(doc.getPageCount() - 1);
    let y = top;
    if (section.caption) {
      const runs = text.layout(section.caption, "regular");
      text.draw(pages[0], runs, SIDE, y - ascentOf(CAPTION_SIZE), CAPTION_SIZE);
      y -= CAPTION_SIZE * 2.2;
    }
    if (!section.aside?.length) {
      flow(section.blocks, SIDE, pageWidth - SIDE, { index: 0, y });
      continue;
    }
    // two columns side by side, each headed by its label, a thin rule between them
    const left = MARGIN;
    const right = pageWidth - MARGIN;
    const gap = 16;
    const width = (right - left - gap) / 2;
    const columns: Array<[AnuvaadBlock[], number]> = [
      [section.blocks, left],
      [section.aside, left + width + gap],
    ];
    const ends = columns.map(([blocks, x], c) => {
      const label = section.labels?.[c];
      const heading: AnuvaadBlock[] = label ? [{ kind: "section", text: label }] : [];
      return flow([...heading, ...blocks], x, x + width, { index: 0, y }, 0.92);
    });
    const lastPage = Math.max(...ends.map((e) => e.index));
    for (let i = 0; i <= lastPage; i += 1) {
      const p = pageAt(i);
      const from = i === 0 ? y : top;
      const to = i === lastPage ? Math.min(...ends.filter((e) => e.index === i).map((e) => e.y), from) : bottom;
      const mid = left + width + gap / 2;
      p.drawLine({ start: { x: mid, y: from }, end: { x: mid, y: Math.max(bottom, to) }, thickness: 0.4, color: rgb(0.6, 0.6, 0.6) });
    }
  }
  return number;
}

export async function buildAnuvaadPdf(sections: AnuvaadPdfSection[], options: AnuvaadPageOptions = {}) {
  const faces = await loadFaces();
  const doc = await PDFDocument.create();
  doc.setTitle("ગુજરાતી અનુવાદ");
  doc.setProducer("Granth library anuvaad");
  const embedder = new FontEmbedder(doc);
  const text = new TextDrawer(faces, embedder);
  const reference = faces.gujarati.regular;
  drawAnuvaadPages(doc, text, (size) => (reference.ascender / reference.upem) * size, sections, 1, options);
  embedder.finish();
  return doc.save({ useObjectStreams: false });
}
