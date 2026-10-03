// granth_gatha_map page spans: no network, no database.
import assert from "node:assert/strict";
import { test } from "node:test";
import { preferVerseRows, repairPageEnds } from "@/lib/granth-mapping";

test("a gatha number answers with the verse, not the sutra, chapter opening or page note of the same number", () => {
  const rows = [{ unit: "sutra", p: 1 }, { unit: "niryukti", p: 2 }, { unit: "chapter", p: 3 }, { unit: "other", p: 4 }];
  assert.deepEqual(preferVerseRows(rows).map((r) => r.p), [2]);
});

test("a sutra still answers when it is all the book has, and rows from before the unit column count as verses", () => {
  assert.deepEqual(preferVerseRows([{ unit: "sutra", p: 1 }, { unit: "chapter", p: 2 }]).map((r) => r.p), [1]);
  assert.deepEqual(preferVerseRows([{ unit: null, p: 1 }, { unit: "sutra", p: 2 }]).map((r) => r.p), [1]);
});

const row = (book: string, start: number, end: number | null, next: number | null = null) => ({
  book_code: book, pdf_url: `https://x/${book}.pdf`, page_start: start, page_end: end, next_page_start: next,
});

test("a chapter's last verse ends where the next verse starts, not at the end of the book", () => {
  const rows = [row("8", 30, 38, 39), row("8", 39, 280), row("8", 42, 75, 76), row("8", 76, 280)];
  assert.deepEqual(repairPageEnds(rows).map((r) => r.page_end), [38, 41, 75, 280]);
});

test("the book's last verse keeps its stored end, and the next anchor is looked up in the same PDF only", () => {
  const rows = [row("8", 39, 280), row("9", 41, 120)];
  assert.deepEqual(repairPageEnds(rows).map((r) => r.page_end), [280, 120]);
});

test("a verse on the same page as the next keeps one page, and source rows are not changed", () => {
  const rows = [row("8", 50, 300), row("8", 50, 300, 51), row("8", 51, 52)];
  const before = JSON.stringify(rows);
  const fixed = repairPageEnds(rows);
  assert.deepEqual(fixed.map((r) => r.page_end), [50, 50, 52]);
  assert.equal(JSON.stringify(rows), before);
});
