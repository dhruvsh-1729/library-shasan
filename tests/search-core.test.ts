// Fast checks on the search core: no network, no database.
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildOCRSearchOccurrences, findOCRSearchMatchesForQueries, isTooShortForContains, parseOCRSearchScripts, scriptsOfQueries } from "@/lib/ocr-search";
import { romanWordReadings } from "@/lib/roman-sanskrit";
import { composeQueries, formsFromList, isRomanQuery } from "@/lib/search-query";
import { buildSearchUrl, parseSearchUrl, resolveGranthValues } from "@/lib/search-url";

const texts = (list: Array<{ text: string }>) => list.map((m) => m.text);

test("romanised words are read by Sanskrit rules, not Hindi ones", () => {
  const read = (w: string) => romanWordReadings(w, 120).map((r) => r.text);
  for (const [word, form] of [
    ["hinsa", "हिंसा"],
    ["atma", "आत्मा"],
    ["jiva", "जीव"],
    ["tattvartha", "तत्त्वार्थ"],
    ["jnana", "ज्ञान"],
    ["gyan", "ज्ञान"],
    ["dharm", "धर्म"],
    ["karma", "कर्म"],
    ["acharanga", "आचारांग"],
    ["krishna", "कृष्ण"],
  ]) {
    assert.ok(read(word).includes(form), `${word} should offer ${form}`);
  }
  // IAST is read exactly.
  assert.equal(romanWordReadings("kṛṣṇa", 5)[0].text, "कृष्ण");
});

test("a Devanagari query finds the Gujarati spelling unless the script is filtered", () => {
  const page = "જ્યાં મિથ્યાજ્ઞાન હોય ત્યાં હિંસા, અસત્ય. पुनः हिंसा न कार्या।";
  assert.deepEqual(texts(findOCRSearchMatchesForQueries(page, ["हिंसा"], "sanskrit_forms")), ["હિંસા", "हिंसा"]);
  assert.deepEqual(texts(findOCRSearchMatchesForQueries(page, ["हिंसा"], "sanskrit_forms", ["devanagari"])), ["हिंसा"]);
  assert.deepEqual(texts(findOCRSearchMatchesForQueries(page, ["हिंसा"], "sanskrit_forms", ["gujarati"])), ["હિંસા"]);
});

test("script choices come from the ticked spellings, before folding merges them", () => {
  assert.deepEqual(scriptsOfQueries(["हिंसा"]), ["devanagari"]);
  assert.equal(scriptsOfQueries(["हिंसा", "હિંસા"]), null);
  assert.equal(parseOCRSearchScripts("devanagari,gujarati"), null);
  assert.deepEqual(parseOCRSearchScripts("gujarati"), ["gujarati"]);
});

test("exact word does not match inside a longer word", () => {
  assert.equal(findOCRSearchMatchesForQueries("अहिंसा परमो धर्मः", ["हिंसा"], "exact_word").length, 0);
  assert.equal(findOCRSearchMatchesForQueries("अहिंसा परमो धर्मः", ["हिंसा"], "contains").length, 1);
});

test("contains search refuses queries the fold shortens below three letters", () => {
  assert.equal(isTooShortForContains("न्द"), true);
  assert.equal(isTooShortForContains("हिंसा"), false);
});

test("occurrence snippets never start or end in the middle of a letter", () => {
  const text = "મિથ્યાત્વથી હણાયેલો કહેવાય. જ્યાં મિથ્યાત્વ હોય ત્યાં બોધ મલિન જ હોય. આથી મિથ્યાજ્ઞાન હોય ત્યાં હિંસા, અસત્ય, ચોરી";
  for (let pad = 40; pad < 140; pad += 3) {
    const [occ] = buildOCRSearchOccurrences(text, "हिंसा", "sanskrit_forms", pad);
    const first = [...occ.snippet.replace(/^…/, "")][0];
    assert.ok(!/\p{M}/u.test(first), `pad ${pad}: starts with a mark`);
    assert.equal(occ.snippet.slice(occ.matchStart, occ.matchEnd), "હિંસા");
  }
});

test("chosen spellings compose into phrase queries", () => {
  assert.equal(isRomanQuery("samyag darshan"), true);
  assert.equal(isRomanQuery("हिंसा"), false);
  const words = [
    { word: "samyag", forms: [{ form: "सम्यग्", pages: 10 }, { form: "सम्यग", pages: 2 }] },
    { word: "darshan", forms: [{ form: "दर्शन", pages: 9 }] },
  ];
  assert.deepEqual(composeQueries(formsFromList(words, null)), ["सम्यग् दर्शन"]);
  assert.deepEqual(composeQueries(formsFromList(words, ["सम्यग्", "सम्यग", "दर्शन"])), ["सम्यग् दर्शन", "सम्यग दर्शन"]);
});

test("search links round-trip, and old links still resolve", () => {
  const keys = new Map([["doc-1", "069"]]);
  const url = buildSearchUrl(
    { q: "hinsa", forms: ["हिंसा"], scripts: ["devanagari"], matchMode: "exact_word", scope: "selected", granthIds: ["doc-1"], page: 2 },
    keys
  );
  assert.equal(url.startsWith("/?"), true);
  const parsed = parseSearchUrl(Object.fromEntries(new URL(`http://x${url}`).searchParams));
  assert.deepEqual([parsed.q, parsed.forms, parsed.scripts, parsed.matchMode, parsed.page, parsed.granthValues], [
    "hinsa",
    ["हिंसा"],
    ["devanagari"],
    "exact_word",
    2,
    ["069"],
  ]);
  // ?langs=typed on a Devanagari query meant Devanagari only.
  assert.deepEqual(parseSearchUrl({ q: "हिंसा", langs: "typed" }).scripts, ["devanagari"]);
  // A duplicate's old key leads to the granth searched in its place.
  const options = [{ key: "g6f79", custom_id: "acharang-pdf" }];
  assert.deepEqual(resolveGranthValues(["398_B041343"], options, { "398_B041343": "acharang-pdf" }).ids, ["acharang-pdf"]);
});
