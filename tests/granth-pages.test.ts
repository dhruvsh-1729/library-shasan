// Granth page numbers typed in the download sheet, matched to PDF pages.
import assert from "node:assert/strict";
import { test } from "node:test";
import { parseGranthPageInput, pdfPagesForGranthPages, printedPageNumbers } from "@/lib/granth-pages";

test("parses lists, ranges and Indic digits", () => {
  assert.deepEqual(parseGranthPageInput("86 88, 89; 301-303 407 443"), [86, 88, 89, 301, 302, 303, 407, 443]);
  assert.deepEqual(parseGranthPageInput("૮૬, ८८"), [86, 88]);
  assert.deepEqual(parseGranthPageInput("5–3 0 x"), [5]);
});

test("a two-page scan covers both printed pages", () => {
  assert.deepEqual(printedPageNumbers("73-74"), [73, 74]);
  assert.deepEqual(printedPageNumbers("91"), [91]);
  assert.deepEqual(printedPageNumbers("xii"), []);
  assert.deepEqual(printedPageNumbers(null), []);
});

test("maps granth pages to PDF pages and reports the rest", () => {
  const pages = [
    { page_number: 112, printed_page: "86" },
    { page_number: 114, printed_page: "88" },
    { page_number: 200, printed_page: null },
    { page_number: 300, printed_page: "73-74" },
  ];
  assert.deepEqual(pdfPagesForGranthPages(pages, [86, 87, 88, 74]), { pdfPages: [112, 114, 300], missing: [87] });
});
