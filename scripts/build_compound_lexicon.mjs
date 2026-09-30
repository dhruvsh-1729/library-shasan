// Builds data/compound-lexicon.json, the word list lib/sanskrit-compound.mjs
// splits compounds with, from:
//   1. the headwords of the six koshes (Shabda Ratna Mahodadhi 380-382,
//      Abhidhan Vyutpatti Prakriya Kosh 370-371, Apte 375) in Turso;
//   2. Devanagari words printed in the text of at least two of the three
//      koshes (catches headwords the OCR broke, keeps out Hindi/Gujarati glosses);
//   3. the vishay list (abhishekbhai- final_subs_processed.xlsx, Kingston SSD):
//      stretches no kosh word covers but that recur across vishays (पच्चक्खाण,
//      अट्ठम, परिषह …) are learned as vishay terms.
//
//   node --env-file=.env scripts/build_compound_lexicon.mjs \
//     ["/media/dell/KINGSTON/abhishekbhai- final_subs_processed.xlsx"] [--report]
import { readFileSync, writeFileSync } from "node:fs";
import { createClient } from "@libsql/client";
import XLSX from "xlsx";
import { foldSanskrit } from "../lib/sanskrit-fold.mjs";
import { KOSHES, koshBit } from "../lib/koshes.mjs";
import { SOURCE, UPASARGAS, aksharaCount, canStartWord, compoundParts, createLexicon, searchableParts, splitCompound } from "../lib/sanskrit-compound.mjs";

const OUT = new URL("../data/compound-lexicon.json", import.meta.url);
const DEFAULT_VISHAY = "/media/dell/KINGSTON/abhishekbhai- final_subs_processed.xlsx";
const args = process.argv.slice(2);
const report = args.includes("--report");
const vishayPath = args.find((a) => !a.startsWith("--")) ?? DEFAULT_VISHAY;

// Word tokens written in Devanagari (Gujarati-script glosses are not Sanskrit headwords).
const DEVANAGARI_WORD = /[ऀ-ॣ॰-ॿ]{2,}/gu;
// A headword line: the word, an optional dash or bracket, then a gender or
// part-of-speech label ("कापथ–न.-९८४-", "प्रबोधः (पुं.)", "उदसन न. (").
const LABEL = "(?:पुं|पु|नपुं|न|स्त्री|स्री|त्रि|वि|अ|अव्य|क्ली|उभ)";
const HEADWORD_LINE = new RegExp(`^[\\s\\-–—*☐]*([\\u0900-\\u0963\\u0970-\\u097f]{2,})\\s*[-–—]?\\s*[(\\[]?\\s*${LABEL}\\s*[.)\\-–—,:]`, "u");
// Apte's compound entries after "सम.": "-पद्मम् (नपुं.)".
const APTE_SUBENTRY = new RegExp(`[-–—]\\s*([\\u0900-\\u0963]{2,})\\s*\\(\\s*${LABEL}\\s*[.)]`, "gu");

const MIN_TEXT_COUNT = 3;
const MIN_VISHAY_TOPICS = 3;
// Longer unknown stretches are several words the koshes lack, not one term.
const MAX_VISHAY_SYLLABLES = 6;
// A kosh split costing this much per part is built from weak or OCR-damaged headwords
// (सामायिक = सामन् + अयिकं); cheaper ones are real (जिनागम = जिन + आगम, 1.04).
const WEAK_SPLIT_COST = 1.2;

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
  // पर्षद् → पर्षदा, परिषद् → परिषदा: the vishay list writes consonant-stem feminines with -ā.
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

// ------------------------------------------------------------------ vishay
function vishayWords(topic) {
  return String(topic ?? "")
    .normalize("NFC")
    .split(/[\s{}()[\],।॥.\-–—/]+/)
    .filter((w) => /[ऀ-ॿ]/.test(w) && aksharaCount(w) >= 2)
    .map((w) => foldSanskrit(w));
}

let topics = [];
try {
  const workbook = XLSX.readFile(vishayPath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  topics = XLSX.utils.sheet_to_json(sheet, { header: 1 }).slice(1).map((row) => String(row[0] ?? "")).filter(Boolean);
  console.log(`vishay topics: ${topics.length} from ${vishayPath}`);
} catch (error) {
  console.warn(`vishay list not read (${error.message}); the lexicon has kosh words only`);
}

// Building blocks: a vishay word that begins or ends at least MIN_VISHAY_TOPICS
// other vishay words (सामायिक in सामायिकचारित्र, सामायिकव्रत …) is a term of
// its own, unless the koshes already make it from well-attested headwords
// (जिनागम = जिन + आगम stays split, so each half is looked up in the koshes).
if (topics.length) {
  const lex = createLexicon({ words: Object.fromEntries(words) });
  const vocabulary = new Set(topics.flatMap(vishayWords));
  const uses = new Map();
  for (const word of vocabulary) {
    for (let k = 1; k < word.length; k += 1) {
      for (const piece of [word.slice(0, k), word.slice(k)]) {
        if (piece !== word && vocabulary.has(piece)) uses.set(piece, (uses.get(piece) ?? 0) + 1);
      }
    }
  }
  let learned = 0;
  for (const [word, count] of uses) {
    const syllables = aksharaCount(word);
    if (count < MIN_VISHAY_TOPICS || words.has(word) || !canStartWord(word) || syllables < 2 || syllables > MAX_VISHAY_SYLLABLES) continue;
    // The koshes make it well (cheap parts, not a short word chopped in three): keep their parts.
    const split = splitCompound(word, lex);
    const parts = split ? searchableParts(split.parts).length : 0;
    const weak = !split || split.cost / Math.max(1, parts) >= WEAK_SPLIT_COST || (parts >= 3 && syllables <= 6);
    if (!weak) continue;
    addWord(word, word, SOURCE.vishay, 0, count);
    learned += 1;
  }
  console.log(`building blocks: learned ${learned} vishay terms`);
}

// Two rounds: terms learned in the first round split more vishays in the second.
for (let round = 1; round <= 2 && topics.length; round += 1) {
  const lex = createLexicon({ words: Object.fromEntries(words) });
  const seenIn = new Map(); // unknown stretch -> Set of topic indexes
  topics.forEach((topic, index) => {
    for (const word of vishayWords(topic)) {
      const split = splitCompound(word, lex);
      for (const part of split?.parts ?? []) {
        if (part.kind !== "unknown" || aksharaCount(part.text) < 2) continue;
        const set = seenIn.get(part.text) ?? new Set();
        set.add(index);
        seenIn.set(part.text, set);
      }
    }
  });
  let learned = 0;
  for (const [text, set] of seenIn) {
    const syllables = aksharaCount(text);
    if (set.size < MIN_VISHAY_TOPICS || syllables < 2 || syllables > MAX_VISHAY_SYLLABLES || !canStartWord(text) || words.has(text)) continue;
    addWord(text, text, SOURCE.vishay, 0, set.size);
    learned += 1;
  }
  console.log(`round ${round}: learned ${learned} vishay terms`);
}

const sorted = Object.fromEntries([...words].sort((a, b) => (a[0] < b[0] ? -1 : 1)));
writeFileSync(OUT, `${JSON.stringify({ version: 1, built: new Date().toISOString().slice(0, 10), words: sorted })}\n`);
console.log(`wrote ${OUT.pathname}: ${words.size} words`);

if (report && topics.length) {
  const lex = createLexicon(JSON.parse(readFileSync(OUT, "utf8")));
  let total = 0;
  let allKosh = 0;
  let someUnknown = 0;
  for (const topic of topics) {
    for (const word of vishayWords(topic)) {
      total += 1;
      const r = compoundParts(word, lex);
      const parts = r?.parts ?? [];
      if (parts.some((p) => p.kind === "unknown")) someUnknown += 1;
      else if (parts.every((p) => p.kind !== "word" || p.source !== SOURCE.vishay)) allKosh += 1;
    }
  }
  console.log({ vishayWords: total, allPartsInKosh: allKosh, withUnknownPart: someUnknown });
}
