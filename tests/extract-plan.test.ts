// The extractor's page planning: no network, no database.
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildParts, pageTotal, parseAsked, passagePages, planGathas, planPages, splitForEmail, type GathaRow } from "@/lib/extract-plan";

const row = (gatha: number, pageStart: number, pageEnd: number, volumeKey = "268", gathaTo: number | null = null): GathaRow => ({
  gatha, gathaTo, volumeKey, pdfUrl: `https://x/${volumeKey}.pdf`, pageStart, pageEnd,
});

test("numbers can be typed loosely and in Devanagari or Gujarati digits", () => {
  assert.deepEqual(parseAsked("12-14, 20").numbers, [12, 13, 14, 20]);
  assert.deepEqual(parseAsked("12 to 14 20").numbers, [12, 13, 14, 20]);
  assert.deepEqual(parseAsked("१२–१४").numbers, [12, 13, 14]);
  assert.deepEqual(parseAsked("૫, 5, 3").numbers, [5, 3]);
  assert.match(parseAsked("12, abc").error ?? "", /abc/);
  assert.match(parseAsked("1-5000").error ?? "", /more than/);
});

test("consecutive gathas on running pages become one passage; a gap starts another", () => {
  const rows = [row(844, 23, 23), row(845, 23, 24), row(846, 24, 25), row(900, 60, 61)];
  const plan = planGathas("844-846, 900, 901", rows, new Map([["268", 352]]));
  assert.deepEqual(plan.passages.map((p) => [p.label, p.first, p.last]), [["Gathas 844–846", 23, 25], ["Gatha 900", 60, 61]]);
  assert.deepEqual(plan.missing, [901]);
});

test("a run that crosses into the next volume is split at the volume", () => {
  const rows = [row(836, 309, 314, "267"), row(837, 21, 21, "268")];
  const plan = planGathas("836-837", rows, new Map());
  assert.deepEqual(plan.passages.map((p) => [p.volumeKey, p.label]), [["267", "Gatha 836"], ["268", "Gatha 837"]]);
});

test("an anchor covering two gathas ('770, 771') answers both numbers", () => {
  const plan = planGathas("771", [row(770, 124, 124, "074", 771)], new Map());
  assert.equal(plan.missing.length, 0);
  assert.equal(plan.passages[0].first, 124);
});

test("pages added before and after stop at the start and end of the PDF, and each page is built once", () => {
  const [p] = planPages("2-3", "025", "https://x/025.pdf", 5).passages;
  assert.deepEqual(passagePages({ ...p, before: 4, after: 9 }), [1, 2, 3, 4, 5]);
  const parts = buildParts([{ ...p, after: 1 }, { ...p, id: "b", first: 4, last: 4 }]);
  assert.deepEqual(parts[0].pages, [2, 3, 4]);
  assert.equal(pageTotal([{ ...p, after: 1 }, { ...p, id: "b", first: 4, last: 4 }]), 3);
});

test("pages past the end of the PDF are reported, not built", () => {
  const plan = planPages("4-7", "025", "https://x/025.pdf", 5);
  assert.deepEqual(plan.missing, [6, 7]);
  assert.equal(plan.passages[0].last, 5);
});

test("email parts each stay under the limit, cutting a passage that is too big alone", () => {
  const per = new Map([["https://x/025.pdf", 1_000_000]]);
  const passages = planPages("1-5, 10-30", "025", "https://x/025.pdf", 400).passages;
  const groups = splitForEmail(passages, per, 8_000_000);
  for (const g of groups) assert.ok(pageTotal(g) <= 8, `group of ${pageTotal(g)} pages`);
  assert.equal(groups.reduce((n, g) => n + pageTotal(g), 0), 26);
});
