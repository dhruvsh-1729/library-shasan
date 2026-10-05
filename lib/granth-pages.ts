// Granth (printed) page numbers typed by a reader, matched to PDF pages.
// The PDF of a granth starts with covers and front matter, so its page 112 is
// often the book's page 86; ocr_printed_pages knows each page's printed number.

/** Devanagari (०-९) and Gujarati (૦-૯) digits as ASCII. */
function asciiDigits(text: string) {
  return text.replace(/[०-९૦-૯]/g, (ch) => String(ch.charCodeAt(0) - (ch <= "९" ? 0x0966 : 0x0ae6)));
}

/** Longest range a reader can type ("1-500"), so a typo cannot list a million pages. */
const MAX_RANGE = 2000;

/** "86, 88 89 301-303" (any digits, any separators) → [86, 88, 89, 301, 302, 303]. */
export function parseGranthPageInput(text: string): number[] {
  const pages = new Set<number>();
  for (const [, a, b] of asciiDigits(text).matchAll(/(\d+)(?:\s*[-–—]\s*(\d+))?/g)) {
    const from = Number(a);
    const to = b ? Number(b) : from;
    if (from <= 0 || to < from || to - from > MAX_RANGE) {
      if (from > 0) pages.add(from);
      continue;
    }
    for (let page = from; page <= to; page += 1) pages.add(page);
  }
  return [...pages].sort((x, y) => x - y);
}

/** The granth pages a printed label covers: "91" → [91], "73-74" (two-page scan) → [73, 74]. */
export function printedPageNumbers(label: string | null | undefined): number[] {
  const match = asciiDigits(String(label ?? "")).trim().match(/^(\d+)(?:\s*-\s*(\d+))?$/);
  if (!match) return [];
  const from = Number(match[1]);
  const to = match[2] ? Number(match[2]) : from;
  if (to < from || to - from > 4) return [from];
  return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

/**
 * The PDF pages holding the wanted granth pages, and the granth pages that
 * could not be placed. `pages` is the book's printed-page map; a granth page
 * missing from it (its number was not read) is placed by the offset its
 * nearest numbered neighbours on both sides agree on, as front matter and
 * covers shift the whole book by the same count.
 */
export function pdfPagesForGranthPages(
  pages: Array<{ page_number: number; printed_page: string | null }>,
  granthPages: number[],
  pageCount = 0
) {
  const byGranthPage = new Map<number, number>();
  for (const page of pages) {
    for (const printed of printedPageNumbers(page.printed_page)) {
      if (!byGranthPage.has(printed)) byGranthPage.set(printed, page.page_number);
    }
  }
  const known = [...byGranthPage.entries()].sort((a, b) => a[0] - b[0]);

  const offsetBetween = (granthPage: number) => {
    let below: [number, number] | undefined;
    let above: [number, number] | undefined;
    for (const entry of known) {
      if (entry[0] < granthPage) below = entry;
      else if (entry[0] > granthPage) {
        above = entry;
        break;
      }
    }
    if (!below || !above) return null;
    const offset = below[1] - below[0];
    return above[1] - above[0] === offset ? offset : null;
  };

  const pdfPages = new Set<number>();
  const missing: number[] = [];
  for (const granthPage of granthPages) {
    let pdfPage = byGranthPage.get(granthPage);
    if (!pdfPage) {
      const offset = offsetBetween(granthPage);
      if (offset !== null) pdfPage = granthPage + offset;
    }
    if (pdfPage && pdfPage > 0 && (!pageCount || pdfPage <= pageCount)) pdfPages.add(pdfPage);
    else missing.push(granthPage);
  }
  return { pdfPages: [...pdfPages].sort((a, b) => a - b), missing };
}
