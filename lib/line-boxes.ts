// Server side of lib/line-box-rings: the printed-line boxes and page text for
// a PDF's pages, from Turso, and the rings they give. Used by the PDF export
// and the page viewer; a page without boxes (a book our OCR has not
// published, or text and boxes that no longer line up) gives null, and the
// caller falls back to the PDF's own text layer.

import { describeGranth, getGranthCatalog, type GranthCatalog } from "@/lib/granth-catalog";
import { type LineBox, type LineRuns, lineBoxRings } from "@/lib/line-box-rings";
import type { OCRSearchMode, OCRSearchScripts } from "@/lib/ocr-search";
import type { WordBox } from "@/lib/pdf-word-boxes";
import { getTursoClient } from "@/lib/turso";

const granthByPdf = new WeakMap<GranthCatalog, Map<string, string>>();

/** The granth a PDF address belongs to. */
async function granthKeyForPdf(pdfUrl: string) {
  const catalog = await getGranthCatalog();
  let map = granthByPdf.get(catalog);
  if (!map) {
    map = new Map();
    for (const entry of catalog.entries) {
      const url = describeGranth(catalog, entry.granth_key)?.pdfUrl;
      if (url) map.set(url, entry.granth_key);
    }
    granthByPdf.set(catalog, map);
  }
  return map.get(pdfUrl) ?? map.get(pdfUrl.split("?")[0]) ?? null;
}

type Mark = { queries: string[]; matchMode: OCRSearchMode; scripts?: OCRSearchScripts };

/**
 * Rings for each asked page of a PDF, in PDF user space (lower-left origin),
 * or null for a page the line boxes cannot place.
 */
export async function lineBoxRingsForPdf(
  pdfUrl: string,
  pages: Array<{ page: number; width: number; height: number }>,
  mark: Mark
): Promise<Map<number, WordBox[] | null>> {
  const out = new Map<number, WordBox[] | null>(pages.map((p) => [p.page, null]));
  if (!mark.queries.length || !pages.length) return out;
  const granthKey = await granthKeyForPdf(pdfUrl);
  if (!granthKey) return out;
  let rows;
  try {
    rows = (
      await getTursoClient().execute({
        sql: `SELECT b.page_number, b.boxes, b.words, p.content FROM ocr_line_boxes b
              JOIN ocr_pages p ON p.granth_key = b.granth_key AND p.page_number = b.page_number
              WHERE b.granth_key = ? AND b.page_number IN (${pages.map(() => "?").join(",")})`,
        args: [granthKey, ...pages.map((p) => p.page)],
      })
    ).rows;
  } catch {
    return out; // no table yet: every page falls back
  }
  const size = new Map(pages.map((p) => [p.page, p]));
  for (const row of rows) {
    const page = Number(row.page_number);
    const dims = size.get(page);
    if (!dims) continue;
    let boxes: LineBox[];
    try {
      boxes = JSON.parse(String(row.boxes));
    } catch {
      continue;
    }
    let runs: Array<LineRuns | null> | null = null;
    try {
      runs = row.words ? JSON.parse(String(row.words)) : null;
    } catch {
      runs = null;
    }
    out.set(page, lineBoxRings(String(row.content ?? ""), boxes, dims, mark.queries, mark.matchMode, mark.scripts ?? null, runs));
  }
  return out;
}

/** Drops boxes that are not on the page at all (a text layer that runs past the edge). */
export function onPage(boxes: WordBox[], page: { width: number; height: number }) {
  return boxes.filter((b) => b.x < page.width && b.x + b.width > 0 && b.y < page.height && b.y + b.height > 0);
}
