import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { koshRank } from "@/lib/koshes.mjs";
import { aksharaCount, compoundParts, createLexicon } from "@/lib/sanskrit-compound.mjs";
import { type CompoundWord, defaultParts, partQueries, partsFromList } from "@/lib/search-query";

const lex = createLexicon(JSON.parse(readFileSync(new URL("../data/compound-lexicon.json", import.meta.url), "utf8")));

/** The parts as "अ- + कटु + …": prefixes with a dash, words by their search term. */
function split(word: string) {
  const result = compoundParts(word, lex);
  return (result?.parts ?? [])
    .map((p) => (p.kind === "prefix" ? `${p.label}-` : p.kind === "word" ? p.term : p.kind === "ending" ? `(${p.text})` : `?${p.text}`))
    .join(" + ");
}

test("syllables count conjuncts once", () => {
  assert.equal(aksharaCount("द्रव्य"), 2);
  assert.equal(aksharaCount("तप"), 2);
  assert.equal(aksharaCount("अ"), 1);
});

test("compounds split into kosh words", () => {
  assert.equal(split("अकर्कशप्रशस्तवचनविनयअभ्यंतरतप"), "अकर्कश + प्रशस्त + वचन + विनय + अभ्यंतर + तप");
  assert.equal(split("अकटुप्रशस्तमनविनयअभ्यंतरतप"), "अ- + कटु + प्रशस्त + मन + विनय + अभ्यंतर + तप");
  assert.equal(split("कायोत्सर्गआगार"), "काय + उत्सर्ग + आगार");
  assert.equal(split("विरसरूक्षअभिग्रहरसत्यागबाह्यतप"), "विरस + रूक्ष + अभिग्रह + रस + त्याग + बाह्य + तप");
});

test("sandhi at the joins is undone", () => {
  assert.equal(split("जिनागम"), "जिन + आगम"); // a + ā
  assert.equal(split("देवेन्द्र"), "देव + इंद्र"); // a + i, and a headword itself still splits
  assert.equal(split("मनोयोग"), "मनस् + योग"); // -as before a voiced sound
  assert.equal(split("तपोधन"), "तपस् + धन");
  assert.equal(split("सम्यग्दर्शन"), "सम्यक् + दर्शन"); // voiced stop is the voiceless one
  assert.equal(split("चंद्रमहास्वप्न"), "चंद्र + महा + स्वप्न");
  assert.equal(split("रौद्रादृष्टि"), "रौद्र + दृष्टि"); // not रौद्र + अदृष्टि
});

test("stems are the kosh headword spellings", () => {
  // केवली is itself a headword (Shabda Ratna Mahodadhi); योगी only as योगिन्.
  assert.equal(split("केवलीपर्याय"), "केवली + पर्याय");
  assert.equal(split("सयोगीकेवली"), "स- + योगिन् + केवली");
});

test("common words are not taken apart, suffixes stay on", () => {
  for (const word of ["उपाध्याय", "न्याय", "द्रव्य", "स्वाध्याय"]) assert.equal(split(word), word);
  assert.equal(split("अकर्तृत्ववाद"), "अकर्तृत्व + वाद");
  assert.equal(split("गुणवानगुण"), "गुणवान + गुण");
  assert.equal(split("अप्रत्याख्यानीयलोभकषाय"), "अ- + प्रत्याख्यानीय + लोभ + कषाय");
});

test("Gujarati case endings and ळ are handled", () => {
  assert.equal(split("परवशतामात्रमां"), "परवशता + मात्र + (मां)");
  assert.equal(split("वचनबळ"), "वचन + बल");
});

test("chosen parts become queries: kosh spelling and the typed spelling", () => {
  const words: CompoundWord[] = [
    {
      word: "मनोयोगअज्झल्",
      inKosh: [],
      parts: [
        { text: "मनो", kind: "word", term: "मनस्", alias: "मन", koshes: ["Apte"] },
        { text: "योग", kind: "word", term: "योग", koshes: ["Apte"] },
        { text: "अज्झल्", kind: "unknown", term: "अज्झल्" },
      ],
    },
  ];
  assert.deepEqual(defaultParts(words), ["मनस्", "योग"]);
  assert.deepEqual(partsFromList(words, null), ["मनस्", "योग"]);
  assert.deepEqual(partsFromList(words, []), []);
  assert.deepEqual(partsFromList(words, ["योग", "नहीं"]), ["योग"]);
  assert.deepEqual(partQueries(words, ["मनस्", "अज्झल्"]), ["मनस्", "मन", "अज्झल्"]);
});

test("kosh export order: Abhidhan Vyutpatti, Shabda Ratna Mahodadhi, Apte, then the rest", () => {
  const keys = ["069", "375", "371", "380", "370", "382", "381"];
  assert.deepEqual([...keys].sort((a, b) => koshRank(a) - koshRank(b)), ["370", "371", "380", "381", "382", "375", "069"]);
});
