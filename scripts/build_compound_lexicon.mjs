// Builds data/compound-lexicon.json, the word list lib/sanskrit-compound.mjs
// splits compounds with, from:
//   1. the headwords of the six koshes (Shabda Ratna Mahodadhi 380-382,
//      Abhidhan Vyutpatti Prakriya Kosh 370-371, Apte 375) in Turso;
//   2. Devanagari words printed in the text of at least two of the three
//      koshes (catches headwords the OCR broke, keeps out Hindi/Gujarati glosses);
//   3. the numbered headwords of the Agamic vyutpatti kosh (395), which has
//      the Jain terms the three koshes lack (सामायिक, उपासक …).
//
// The vishay list is never read: the vishay names may not be stored or used
// electronically, so every word here comes from a kosh.
//
//   node --env-file=.env scripts/build_compound_lexicon.mjs
import { writeFileSync } from "node:fs";
import { createClient } from "@libsql/client";
import { foldSanskrit } from "../lib/sanskrit-fold.mjs";
import { KOSHES, koshBit } from "../lib/koshes.mjs";
import { SOURCE, UPASARGAS, aksharaCount, canStartWord, createLexicon, searchableParts, splitCompound } from "../lib/sanskrit-compound.mjs";

const OUT = new URL("../data/compound-lexicon.json", import.meta.url);

// Word tokens written in Devanagari (Gujarati-script glosses are not Sanskrit headwords).
const DEVANAGARI_WORD = /[ऀ-ॣ॰-ॿ]{2,}/gu;
// A headword line: the word, an optional dash or bracket, then a gender or
// part-of-speech label ("कापथ–न.-९८४-", "प्रबोधः (पुं.)", "उदसन न. (").
const LABEL = "(?:पुं|पु|नपुं|न|स्त्री|स्री|त्रि|वि|अ|अव्य|क्ली|उभ)";
const HEADWORD_LINE = new RegExp(`^[\\s\\-–—*☐]*([\\u0900-\\u0963\\u0970-\\u097f]{2,})\\s*[-–—]?\\s*[(\\[]?\\s*${LABEL}\\s*[.)\\-–—,:]`, "u");
// Apte's compound entries after "सम.": "-पद्मम् (नपुं.)".
const APTE_SUBENTRY = new RegExp(`[-–—]\\s*([\\u0900-\\u0963]{2,})\\s*\\(\\s*${LABEL}\\s*[.)]`, "gu");

const MIN_TEXT_COUNT = 3;
// The Agamic vyutpatti kosh numbers its entries: "११९. उपासकाः - साधूनुपासते…".
const AGAMIC_KOSH = "395";
const AGAMIC_BIT = 8;
// A word the three koshes already make well (cheap parts) keeps their parts, so
// each half is looked up there (कायोत्सर्ग = काय + उत्सर्ग); only what they make
// badly (सामायिक = सामन् + अयिकं, a cost per part this high) is taken from 395.
const WEAK_SPLIT_COST = 1.2;
const AGAMIC_HEADWORD_LINE = /^\s*[०-९0-9]+\s*\.\s*([\u0900-\u0963\u0970-\u097f]{2,})\s*(?:\([^)]*\)\s*)?[-–—]/u;

/** The stems a printed headword gives: देवः → देव, आत्मन् → आत्म, योगिन् → योगी, मनस् → मन. */
function headwordStems(head) {
  const stems = new Set();
  const base = /[ःं]$/.test(head) && aksharaCount(head) > 1 ? head.slice(0, -1) : head;
  stems.add(base);
  if (base.endsWith("न्")) {
    const stem = base.slice(0, -2);
    stems.add(stem);
    if (stem.endsWith("ि")) stems.add(`${stem.slice(0, -1)}ी`);
  }
  if (base.endsWith("स्")) stems.add(base.slice(0, -2));
  if (base.endsWith("ृ")) stems.add(`${base.slice(0, -1)}ा`);
  // महत् is महा at the head of a compound (महाव्रत, महास्वप्न).
  if (base === "महत्") stems.add("महा");
  // पर्षद् → पर्षदा, परिषद् → परिषदा: vishay names write consonant-stem feminines with -ā.
  if (/[\u0915-\u0939]्$/.test(base) && !base.endsWith("न्") && !base.endsWith("स्")) stems.add(`${base.slice(0, -1)}ा`);
  // One syllable only for a consonant stem (अप्, वाक्); अ, न … are not parts.
  return [...stems].filter((s) => canStartWord(s) && (aksharaCount(s) >= 2 || s.endsWith("्")));
}

const words = new Map(); // stem -> [head, source, koshMask, freq]
function addWord(stem, head, source, mask, freq = 0) {
  const current = words.get(stem);
  if (!current) {
    words.set(stem, [head, source, mask, freq]);
    return;
  }
  // A better source, or the headword spelled exactly as the stem (तपस्, not
  // an OCR-damaged "तपस्न्" that also reduces to it), names the entry.
  if (source < current[1] || (source === current[1] && head === stem && current[0] !== stem)) {
    current[0] = head;
    current[1] = source;
  }
  current[2] |= mask;
  current[3] = Math.max(current[3], freq);
}

// ------------------------------------------------------------------ koshes
const client = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const textCounts = new Map(); // folded token -> { n, mask }
for (const { key } of KOSHES) {
  const bit = koshBit(key);
  const result = await client.execute({ sql: "SELECT content FROM ocr_pages WHERE granth_key = ?", args: [key] });
  let heads = 0;
  for (const row of result.rows) {
    const content = String(row.content ?? "").normalize("NFC");
    for (const raw of content.match(DEVANAGARI_WORD) ?? []) {
      const token = foldSanskrit(raw);
      const entry = textCounts.get(token) ?? { n: 0, mask: 0 };
      entry.n += 1;
      entry.mask |= bit;
      textCounts.set(token, entry);
    }
    const folded = foldSanskrit(content);
    for (const line of folded.split("\n")) {
      const found = [];
      const m = line.match(HEADWORD_LINE);
      if (m) found.push(m[1]);
      if (key === "375") for (const s of line.matchAll(APTE_SUBENTRY)) found.push(s[1]);
      for (const head of found) {
        heads += 1;
        for (const stem of headwordStems(head)) addWord(stem, head, SOURCE.head, bit);
      }
    }
  }
  console.log(`kosh ${key}: ${result.rows.length} pages, ${heads} headword lines`);
}

// परा, अधि, प्रति … stand as words in vishays (परादृष्टि); the OCR misses some as headwords.
for (const upasarga of UPASARGAS) if (aksharaCount(upasarga) >= 2 && !upasarga.endsWith("्")) addWord(upasarga, upasarga, SOURCE.head, 0);

const frequency = (token) => textCounts.get(token)?.n ?? 0;
for (const [stem, entry] of words) entry[3] = Math.max(frequency(stem), frequency(entry[0]));

let textWords = 0;
for (const [token, { n, mask }] of textCounts) {
  if (n < MIN_TEXT_COUNT || (mask & (mask - 1)) === 0) continue; // in two koshes at least
  const stem = /[ःं]$/.test(token) ? token.slice(0, -1) : token;
  if (aksharaCount(stem) < 3 || !canStartWord(stem) || words.has(stem)) continue;
  addWord(stem, stem, SOURCE.text, mask, n);
  textWords += 1;
}
console.log(`headword stems: ${words.size - textWords}, text words: ${textWords}`);

// ------------------------------------------------------------------ Agamic kosh
{
  const lex = createLexicon({ words: Object.fromEntries(words) });
  const result = await client.execute({ sql: "SELECT content FROM ocr_pages WHERE granth_key = ?", args: [AGAMIC_KOSH] });
  let heads = 0;
  let learned = 0;
  for (const row of result.rows) {
    for (const line of foldSanskrit(String(row.content ?? "").normalize("NFC")).split("\n")) {
      const m = line.match(AGAMIC_HEADWORD_LINE);
      if (!m) continue;
      heads += 1;
      for (const stem of headwordStems(m[1])) {
        if (words.has(stem)) continue;
        const split = splitCompound(stem, lex);
        const parts = split ? searchableParts(split.parts).length : 0;
        const weak = !split || split.parts.some((p) => p.kind === "unknown") || split.cost / Math.max(1, parts) >= WEAK_SPLIT_COST;
        if (!weak) continue;
        addWord(stem, m[1], SOURCE.agamic, AGAMIC_BIT, frequency(stem));
        learned += 1;
      }
    }
  }
  console.log(`kosh ${AGAMIC_KOSH}: ${result.rows.length} pages, ${heads} headword lines, ${learned} words the three koshes lack`);
}

const sorted = Object.fromEntries([...words].sort((a, b) => (a[0] < b[0] ? -1 : 1)));
writeFileSync(OUT, `${JSON.stringify({ version: 1, built: new Date().toISOString().slice(0, 10), words: sorted })}\n`);
console.log(`wrote ${OUT.pathname}: ${words.size} words`);
