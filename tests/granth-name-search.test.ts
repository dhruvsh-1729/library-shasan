// The library's name search: spelling- and script-tolerant, a whole title first.
import assert from "node:assert/strict";
import { test } from "node:test";
import { prepareRow, rankRows } from "@/lib/granth-name-search";

const rows = [
  prepareRow({ source: "010_B0001_agam_bhagvati_sutra_2.pdf", extra: "Agam 10 Bhagvati Sutra 2 Satik Gujarati Anuvad", titles: ["Agam 10 Bhagvati Sutra 2 Satik Gujarati Anuvad"] }),
  prepareRow({ source: "212_bhagvati.pdf", extra: "Bhagvati भगवती", titles: ["Bhagvati", "भगवती"] }),
  prepareRow({ source: "361_klesh_hanopay_dwatrinshika.pdf", extra: "Klesh Hanopay Dwatrinshika", titles: ["Klesh Hanopay Dwatrinshika"] }),
  prepareRow({ source: "375_sanskrit_hindi_kosh_apte.pdf", extra: "Sanskrit Hindi Kosh", titles: ["Sanskrit Hindi Kosh"] }),
];

test("a query that is a whole title ranks that book first", () => {
  assert.equal(rankRows(rows, "bhagvati")[0], 1);
  assert.equal(rankRows(rows, "bhagwati")[0], 1);
  assert.equal(rankRows(rows, "भगवती")[0], 1);
});

test("a title typed as one word is found even where an h sits at the join", () => {
  assert.equal(rankRows(rows, "kleshhanopaydwatrinshika")[0], 2);
  assert.equal(rankRows(rows, "sanskrithindikosh")[0], 3);
});

test("a book number finds that book only", () => {
  assert.deepEqual(rankRows(rows, "375"), [3]);
});
