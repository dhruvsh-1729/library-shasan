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
};

const BLACK = rgb(0, 0, 0);

const CAPTION_SIZE = 9;
const TITLE_SIZE = 14;
const VISHAY_SIZE = 13;
const SECTION_SIZE = 13;
const BODY_SIZE = 12;
const LEADING = 1.75;
const SIDE = MARGIN + 14; // the scans have wide side margins
const BULLET_INDENT = 22;
const SUB_INDENT = 46;
const FOOTER_SIZE = 10;

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
  const right = pageWidth - SIDE;
  const top = pageHeight - MARGIN - 6;
  const bottom = MARGIN + 26;
  let number = firstNumber;
  let page: PDFPage | null = null;
  let y = top;

  const footer = (p: PDFPage) => {
    const runs = text.layout(gujaratiDigits(number), "regular");
    text.draw(p, runs, (pageWidth - text.width(runs, FOOTER_SIZE)) / 2, MARGIN - 4, FOOTER_SIZE);
    number += 1;
  };
  const newPage = () => {
    page = doc.addPage(A4);
    if (options.numbers !== false) footer(page);
    y = top;
    return page;
  };
  /** Room for `height` more points on this page, or a fresh page. */
  const room = (height: number) => {
    if (!page || y - height < bottom) newPage();
    return page!;
  };

  for (const section of sections) {
    newPage();
    options.starts?.push(doc.getPageCount() - 1);
    if (section.caption) {
      const runs = text.layout(section.caption, "regular");
      const p = room(CAPTION_SIZE * 2);
      text.draw(p, runs, SIDE, y - ascentOf(CAPTION_SIZE), CAPTION_SIZE);
      y -= CAPTION_SIZE * 2.2;
    }

    for (const block of section.blocks) {
      let size = BODY_SIZE;
      let x = SIDE;
      let pieces: Piece[];
      let before = 0;
      let after = BODY_SIZE * 0.5;
      let bullet: "dot" | "ring" | null = null;
      switch (block.kind) {
        case "title":
          size = TITLE_SIZE;
          pieces = [{ text: block.text, weight: "bold" }];
          after = TITLE_SIZE * 0.7;
          break;
        case "vishay":
          size = VISHAY_SIZE;
          pieces = [{ text: `વિષય: ${block.text}`, weight: "bold" }];
          after = VISHAY_SIZE * 0.8;
          break;
        case "section":
          size = SECTION_SIZE;
          pieces = [{ text: block.text, weight: "bold" }];
          before = SECTION_SIZE * 0.5;
          after = SECTION_SIZE * 0.45;
          break;
        case "point":
          x = SIDE + (block.level === 2 ? SUB_INDENT : BULLET_INDENT);
          bullet = block.level === 2 ? "ring" : "dot";
          pieces = block.label
            ? [
                { text: `${block.label}: `, weight: "bold" },
                { text: block.text, weight: "regular" },
              ]
            : [{ text: block.text, weight: "regular" }];
          break;
        default:
          pieces = [{ text: block.text, weight: "regular" }];
      }
      const lines = wrapPieces(text, pieces, size, right - x);
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
        y -= chunk.length * lineHeight;
        index += chunk.length;
      }
      y -= after;
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
