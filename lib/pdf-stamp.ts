// A small line at the foot of every page cut from a book: which book and
// which page it is (Maharaj Saheb, 20 Nov 2026: "whatever PDF result we get
// and print, the print should carry the book number and the page number").
//
// "Book 196 · Panchashak Prakaran · Part 1 · page 280 · PDF 295", in grey,
// in the bottom margin, so a loose printed sheet can be put back to its book.

import { type PDFDocument, type PDFPage, StandardFonts, rgb } from "pdf-lib";
import { describeGranth, getGranthCatalog } from "@/lib/granth-catalog";
import { getTursoClient } from "@/lib/turso";

const SIZE = 7;
const GREY = rgb(0.4, 0.4, 0.4);

/** The stamp's text for each PDF page of a granth. */
export async function stampLabels(granthKey: string, pdfPages: number[]): Promise<Map<number, string>> {
  const out = new Map<number, string>();
  if (!granthKey || !pdfPages.length) return out;
  const described = describeGranth(await getGranthCatalog(), granthKey);
  const book = described?.entry.book_number ? `Book ${described.entry.book_number}` : "";
  const name = described?.displayName ?? "";
  let printed = new Map<number, string>();
  try {
    const r = await getTursoClient().execute({
      sql: `SELECT page_number, printed_page FROM ocr_printed_pages WHERE granth_key = ? AND page_number IN (${pdfPages.map(() => "?").join(",")})`,
      args: [granthKey, ...pdfPages],
    });
    printed = new Map(r.rows.filter((row) => row.printed_page != null).map((row) => [Number(row.page_number), String(row.printed_page)]));
  } catch {
    // no printed numbers: the PDF page alone
  }
  for (const page of pdfPages) {
    const parts = [book, name, printed.get(page) ? `page ${printed.get(page)}` : "", `PDF ${page}`].filter(Boolean);
    out.set(page, parts.join(" · "));
  }
  return out;
}

/** Latin letters only reach the standard font; anything else is left out of the stamp. */
const ascii = (text: string) => text.replace(/[^\x20-\x7e·]/g, "").replace(/\s{2,}/g, " ").trim();

/** Writes each page's label at its foot, left, in the margin. */
export async function stampPages(doc: PDFDocument, pages: Array<{ page: PDFPage; label: string }>) {
  if (!pages.some((p) => p.label)) return;
  const font = await doc.embedFont(StandardFonts.Helvetica);
  for (const { page, label } of pages) {
    const text = ascii(label);
    if (!text) continue;
    const { width } = page.getSize();
    const box = page.getCropBox();
    const fits = font.widthOfTextAtSize(text, SIZE) < width - 24;
    page.drawText(fits ? text : text.slice(0, 120), { x: box.x + 12, y: box.y + 5, size: SIZE, font, color: GREY });
  }
}
