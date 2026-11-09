// Two Find options at once: a page is found when either finds the word, and
// its matches are the union of both (an overlap counted once).
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  effectiveSearchModes,
  findOCRSearchMatchesForQueries,
  joinSearchModes,
  parseOCRSearchMode,
  parseOCRSearchModes,
  searchModeList,
} from "@/lib/ocr-search";
import { buildOCRPrefilter, prefilterCount, prefilterFrom, prefilterIds, prefilterIdsAfter } from "@/lib/ocr-search-index";
import { buildSearchUrl, modeSearchesParts, parseSearchUrl } from "@/lib/search-url";

const words = (list: Array<{ text: string }>) => list.map((m) => m.text);

test("two Find options are read, ordered and capped at two", () => {
  assert.equal(parseOCRSearchModes("contains,sanskrit_forms"), "sanskrit_forms,contains");
  assert.equal(parseOCRSearchModes(["contains", "exact_word"]), "exact_word,contains");
  assert.equal(parseOCRSearchModes("sanskrit_forms+ends_with"), "sanskrit_forms,ends_with");
  assert.equal(parseOCRSearchModes("contains,contains"), "contains");
  assert.equal(parseOCRSearchModes("nonsense"), "exact_word");
  assert.equal(parseOCRSearchModes(undefined), "exact_word");
  assert.equal(searchModeList(joinSearchModes(["contains", "begins_with", "exact_word"])).length, 2);
  // one-option callers still get one option
  assert.equal(parseOCRSearchMode("sanskrit_forms,contains"), "sanskrit_forms");
  assert.equal(parseOCRSearchMode("begins_with"), "begins_with");
});

test("the matches of two options are the union of each one's", () => {
  const page = "देवः पूज्यते। देवेन्द्रः आगतः। महादेवस्य मन्दिरम्। देवम् नमामि।";
  const forms = words(findOCRSearchMatchesForQueries(page, "देव", "sanskrit_forms"));
  const begins = words(findOCRSearchMatchesForQueries(page, "देव", "begins_with"));
  const ends = words(findOCRSearchMatchesForQueries(page, "देवस्य", "ends_with"));
  assert.deepEqual(forms, ["देवः", "देवम्"]);
  assert.ok(begins.length >= 3);
  const both = findOCRSearchMatchesForQueries(page, "देव", "sanskrit_forms,begins_with");
  // every match of either option is covered, with no overlapping duplicates
  for (const m of [...findOCRSearchMatchesForQueries(page, "देव", "sanskrit_forms"), ...findOCRSearchMatchesForQueries(page, "देव", "begins_with")]) {
    assert.ok(both.some((b) => b.start <= m.start && b.end >= m.end), `missing ${m.text}`);
  }
  for (let i = 1; i < both.length; i += 1) assert.ok(both[i].start >= both[i - 1].end, "matches overlap");
  assert.ok(ends.length === 1);
  // a page only one option finds is still found with both ticked
  const onlyInside = "महादेवस्य मन्दिरम्।";
  assert.equal(findOCRSearchMatchesForQueries(onlyInside, "देव", "sanskrit_forms").length, 0);
  assert.equal(findOCRSearchMatchesForQueries(onlyInside, "देव", "sanskrit_forms,contains").length, 1);
  const onlyForm = "देवाः आगताः।";
  assert.equal(findOCRSearchMatchesForQueries(onlyForm, "देव", "exact_word").length, 0);
  assert.equal(findOCRSearchMatchesForQueries(onlyForm, "देव", "sanskrit_forms,exact_word").length, 1);
});

test("a short word drops 'anywhere inside' for its forms, keeping the other option", () => {
  assert.equal(effectiveSearchModes("exact_word,contains", ["नय"]), "sanskrit_forms,exact_word");
  assert.equal(effectiveSearchModes("sanskrit_forms,contains", ["नय"]), "sanskrit_forms");
  assert.equal(effectiveSearchModes("sanskrit_forms,contains", ["हिंसा"]), "sanskrit_forms,contains");
});

test("two options read both indexes and take the union of their pages", () => {
  const one = buildOCRPrefilter(["हिंसा"], "contains");
  assert.equal(one.parts.length, 1);
  assert.match(prefilterFrom(one).from, /JOIN ocr_pages p ON p\.id = \w+\.rowid/);
  const two = buildOCRPrefilter(["हिंसा"], "sanskrit_forms,contains");
  assert.equal(two.parts.length, 2);
  assert.notEqual(two.parts[0].table, two.parts[1].table);
  assert.match(prefilterIds(two).sql, / UNION /);
  assert.equal(prefilterIds(two).args.length, 2);
  assert.equal(prefilterIdsAfter(two, 10, 50).args.length, 5);
  assert.match(prefilterCount(two).sql, /count\(\*\)/);
  // two options on the same index are one MATCH
  const same = buildOCRPrefilter(["हिंसा"], "sanskrit_forms,exact_word");
  assert.equal(same.parts.length, 1);
});

test("a link keeps both options, and both search compound parts when one is 'all its forms'", () => {
  const parsed = parseSearchUrl({ q: "हिंसा", match: "contains,sanskrit_forms" });
  assert.equal(parsed.matchMode, "sanskrit_forms,contains");
  const url = buildSearchUrl({ ...parsed, scope: "all", granthIds: [] }, new Map());
  assert.match(decodeURIComponent(url), /match=sanskrit_forms,contains/);
  assert.equal(modeSearchesParts("sanskrit_forms,contains"), true);
  assert.equal(modeSearchesParts("exact_word,contains"), false);
});
