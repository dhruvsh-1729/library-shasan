// A vishay's vyutpatti, by Maharaj Saheb's rules (handwritten list, 29 Sep 2026):
//
//   1. The user types the vishay in Devanagari; nothing is guessed from, or
//      stored in, any vishay list.
//   2. The vishay is split into its words (a compound: कायिकहिंसा = कायिक +
//      हिंसा; a negation: अकथा = अ + कथा) — the engine splits it, given the
//      split the kosh word list suggests.
//   3. Each word is looked up in the three koshes in order (Abhidhan Vyutpatti
//      Prakriya Kosh, Shabda Ratna Mahodadhi, Apte); only a word none of them
//      has goes to the other four; a compound none has is looked up under its
//      first word in Apte (देवेन्द्र under देव, "-इन्द्रः").
//   4. The entries are read from the page — Shabda Ratna Mahodadhi from its
//      scan, since our OCR garbled its Gujarati — with the printed page number.
//   5. The word an entry derives from is looked up too, so the reader sees
//      the chain (चोर → चौर → चौर्य → अचौर्य).
//   6. A word in no kosh gets a derivation from the engine, citing the
//      grammar granth it rests on, and is marked as AI.
//   7. A compound vishay ends with its samāsa vigraha.

import { readFileSync } from "node:fs";
import path from "node:path";
import { extractPdfPagesByRange } from "@/lib/pdf-range-subset.mjs";
import { headwordKeys } from "@/lib/kosh-headwords.mjs";
import { SOURCE, aksharaCount, compoundParts, createLexicon, searchableParts, splitCompound } from "@/lib/sanskrit-compound.mjs";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { type Engine, LlmError, type LlmFile, type Usage, askJson, newUsage } from "@/lib/vyutpatti/llm";
import { type Candidate, findApteSubentry, findCandidates, headwordTiers } from "@/lib/vyutpatti/lookup";
import {
  type AiDerivation,
  type ReaderLine,
  type TableRow,
  type VyutpattiEntry,
  agreement,
  normalizeDerivation,
  readerLineForAi,
  readerLineForEntry,
  readerLineForVigraha,
  remarkStamp,
  tableRowForAi,
  tableRowForEntry,
  tableRowForVigraha,
} from "@/lib/vyutpatti/format";

export type VishayInput = { number: string; vishay: string; box: string };

export type WordRole = "whole" | "part" | "base";

export type VyutpattiWord = {
  word: string;
  role: WordRole;
  /** For a base: the word it was found under; for a broken-up part: the word it came from. */
  of?: string;
  /** Not in the koshes, so broken up into these (rule 3: भवनपति → भवन + पति). */
  brokenInto?: string[];
  /** Kosh entries for the word in a sense the vishay does not use (kept for the internal table only). */
  otherSense?: VyutpattiEntry[];
  entries: VyutpattiEntry[];
  ai?: AiDerivation;
};

export type VyutpattiResult = {
  number: string;
  vishay: string;
  box: string;
  engine: Engine;
  parts: PlanPart[];
  vigraha: string;
  samasa: string;
  words: VyutpattiWord[];
  lines: ReaderLine[];
  rows: TableRow[];
  costUsd: number;
  notes: string[];
};

export class VishayInputError extends Error {}

// Letters must be Devanagari (rule: only Hindi-lipi words); digits, spaces,
// braces and punctuation are allowed, as the vishay names use them:
// "अवितथकथनऋजुव्यवहारगुण {भावश्रावकगुण}", "द्रव्यना 21 स्वभाव".
const OTHER_LETTER = /[^\P{L}ऀ-ॿ]/u;

/** The vishay as typed, checked, with single spaces. */
export function cleanVishay(raw: string) {
  const text = String(raw ?? "").normalize("NFC").replace(/[\s\u0085]+/g, " ").trim();
  if (!text) throw new VishayInputError("Type the vishay.");
  if (text.length > 300) throw new VishayInputError("The vishay is too long (300 letters at most).");
  if (/[઀-૿]/u.test(text)) throw new VishayInputError(`“${text}” is in Gujarati script. Type the vishay in Devanagari (Hindi lipi).`);
  if (OTHER_LETTER.test(text)) throw new VishayInputError(`“${text}” has letters that are not Devanagari. Type the vishay in Devanagari only.`);
  if (!/[ऀ-ॿ]/u.test(text)) throw new VishayInputError("Type the vishay in Devanagari.");
  return text;
}

/**
 * The vishay without the context in braces: "सकरणयोग{अयोगीकेवली}" is the
 * vishay सकरणयोग, under अयोगीकेवली. The context only guides the senses.
 */
export function splitContext(vishay: string) {
  const context = [...vishay.matchAll(/\{([^}]*)\}/gu)].map((m) => m[1].trim()).filter(Boolean).join(", ");
  const main = vishay.replace(/\{[^}]*\}/gu, " ").replace(/\s+/g, " ").trim();
  return { main: main || vishay, context };
}

/** "10.6.6 कायिकहिंसा" or "कायिकहिंसा", one per line. */
export function parseVishayLines(text: string, box: string): VishayInput[] {
  return String(text ?? "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const m = line.match(/^([0-9०-९]+(?:[.][0-9०-९]+)*)[.)]?\s+(.+)$/u);
      return { number: m ? m[1] : "", vishay: m ? m[2] : line, box: String(box ?? "").trim() };
    });
}

// ------------------------------------------------------------------ prompts

const SYSTEM = `You help Jain scholars prepare vyutpatti (etymological derivation) entries for a vishay kosh from printed Sanskrit koshes.
Accuracy matters more than completeness. Never invent kosh text: copy what is printed. Use the Paninian and Haima grammatical tradition.
Write Sanskrit in Devanagari and meanings in Gujarati script. Answer with JSON only, no commentary.`;

let lexicon: ReturnType<typeof createLexicon> | null = null;
function getLexicon() {
  lexicon ??= createLexicon(JSON.parse(readFileSync(path.join(process.cwd(), "data", "compound-lexicon.json"), "utf8")));
  return lexicon;
}

/** The split the kosh word list suggests, as "अ- + कथा". */
function suggestedSplit(vishay: string) {
  const split = compoundParts(vishay, getLexicon());
  if (!split) return vishay;
  return split.parts
    .map((p) => (p.kind === "prefix" ? `${p.label}-` : p.kind === "word" ? p.term : p.kind === "ending" ? `(${p.text})` : p.text))
    .join(" + ");
}

export type PlanPart = {
  word: string;
  /** The letters of the vishay this part covers, as written (with its sandhi). */
  text?: string;
  /** अ-, अन्-, an upasarga written apart: shown, not looked up. */
  prefix: boolean;
  /** A Gujarati word written in Devanagari (ना, मां, थी, करनार), or a number: shown, not looked up. */
  skip?: boolean;
};

export type Plan = { parts: PlanPart[]; vigraha: string; samasa: string };

/** A maxim or a long phrase is looked up by its key words only. */
const MAX_LOOKED_UP = 12;

/** The split the kosh word list suggests, word by word: "अ- + कथा", "द्रव्य + (ना) | स्वभाव". */
function suggestedSplitOf(main: string) {
  return main
    .split(/[\s,।॥-]+/u)
    .filter((w) => /[ऀ-ॿ]/u.test(w))
    .map((w) => suggestedSplit(w))
    .join(" | ");
}

/**
 * Maharaj Saheb's rule: a word is broken up only when the kosh does not have
 * it (भवनपति not found → भवन + पति). So adjacent parts whose letters together
 * are a kosh headword are joined back: महा + व्रत → महाव्रत, कु + शील → कुशील,
 * क + रावण → करावण. The longest join wins; one in the three koshes, or one in
 * the other four that replaces a piece the three koshes lack.
 */
export async function joinKoshWords(parts: PlanPart[], main: string): Promise<PlanPart[]> {
  const letters = main.replace(/[^\u0900-\u097f]/gu, "");
  const texts = parts.map((p) => p.text ?? "");
  if (parts.length < 2) return parts;
  // A join is only tried where the parts' letters, together, are written in the vishay.
  const written = (from: number, to: number) => {
    const span = texts.slice(from, to + 1);
    return span.every(Boolean) && letters.includes(span.join(""));
  };
  const MAX_SPAN = 4;
  const spans: string[] = [];
  for (let i = 0; i < parts.length; i += 1) {
    for (let j = i + 1; j < Math.min(parts.length, i + MAX_SPAN); j += 1) {
      if (parts.slice(i, j + 1).some((p) => p.skip)) break;
      if (written(i, j)) spans.push(texts.slice(i, j + 1).join(""));
    }
  }
  const pieceWords = parts.filter((p) => !p.prefix && !p.skip).map((p) => p.word);
  const tiers = await headwordTiers([...new Set([...spans, ...pieceWords])]).catch(() => new Map<string, { tier: number; head: string }>());
  const out: PlanPart[] = [];
  for (let i = 0; i < parts.length; ) {
    let joined: PlanPart | null = null;
    let next = i + 1;
    for (let j = Math.min(parts.length, i + MAX_SPAN) - 1; j > i; j -= 1) {
      const span = parts.slice(i, j + 1);
      if (span.some((p) => p.skip) || !written(i, j)) continue;
      const text = texts.slice(i, j + 1).join("");
      const hit = tiers.get(text);
      if (!hit) continue;
      const piecesInTier1 = span.filter((p) => !p.prefix).every((p) => tiers.get(p.word)?.tier === 1);
      if (hit.tier === 1 || !piecesInTier1) {
        joined = { word: text, text, prefix: false };
        next = j + 1;
        break;
      }
    }
    out.push(joined ?? parts[i]);
    i = next;
  }
  return out;
}

export async function planVishay(engine: Engine, vishay: string, usage: Usage): Promise<Plan> {
  const { main, context } = splitContext(vishay);
  const prompt = `Vishay: ${main}${context ? `\nContext (the topic it belongs to; not part of the vishay): ${context}` : ""}
Split suggested by our kosh word list (it may be wrong): ${suggestedSplitOf(main)}

Split the vishay into the words to look up in a Sanskrit/Prakrit kosh, in the order they occur.
- A compound (समास) is split into all its members, however long; a negation is split off (अकथा = अ + कथा, अचौर्य = अ + चौर्य).
- Undo sandhi, and give each member as a kosh headword (prātipadika) would be printed: कायिक, हिंसा, मनस्, कर्मन्.
- text: the exact letters of the vishay the part covers, as written there (महाव्रत → "महा" + "व्रत"; मनोयोग → "मनो" + "योग"); the texts of all parts, joined, spell the vishay.
- Never cut a word into meaningless pieces (करावण is one word, not क + रावण or कर + आवण).
- A word that is itself one ordinary kosh word (हिंसा, उपाध्याय, द्रव्य, सामायिक, प्रत्याख्यान) is not split further; a technical term a kosh prints as one headword (कायोत्सर्ग, उपसंपदा, प्रतिवासुदेव) is kept whole.
- Mark the negation particle (अ / अन्) and any upasarga written apart with "prefix": true; it is not looked up.
- The vishay may mix in Gujarati words written in Devanagari letters (ना, नी, नो, मां, थी, ने, करनार, छतां, संबंधी, बे, कितना) and numbers: list them with "skip": true; they are not looked up. Look up the Sanskrit/Prakrit words only.
- A sentence or maxim (a न्याय, a quoted line) is not split word by word: give at most ${MAX_LOOKED_UP} of its key technical nouns and adjectives, as stems; skip its verb forms (तारयति, अनुवर्तते) and particles (च, न, इति, तु, एव, अपि, हि) with "skip": true; vigraha "".
- Split off कु-, सु-, दुस्-, निस्- and the negation as prefixes (कुगुरु = कु- + गुरु) unless a kosh prints the whole as one word.
- vigraha: the samāsa vigraha of the whole vishay in Sanskrit (e.g. "न चौर्यम् इति अचौर्यम्", "कायिकी चासौ हिंसा च कायिकहिंसा"); "" when the vishay is a single plain word, a phrase of several separate words, or a sentence.
- samasa: its type in Sanskrit (नञ्तत्पुरुषः, कर्मधारयः, षष्ठीतत्पुरुषः, द्वन्द्वः, बहुव्रीहिः …), or "".

JSON: {"parts":[{"word":"","text":"","prefix":false,"skip":false}],"vigraha":"","samasa":""}`;
  const out = (await askJson(engine, { system: SYSTEM, prompt, maxTokens: 1600 }, usage)) as Partial<Plan>;
  const parts: PlanPart[] = (Array.isArray(out?.parts) ? out.parts : [])
    .map((p) => ({
      word: String(p?.word ?? "").normalize("NFC").trim(),
      text: String(p?.text ?? "").normalize("NFC").replace(/\s+/g, ""),
      prefix: Boolean(p?.prefix),
      skip: Boolean(p?.skip),
    }))
    .filter((p) => p.word && /^[\u0900-\u097f]+$/u.test(p.word));
  const merged = await joinKoshWords(parts, main);
  parts.splice(0, parts.length, ...merged);
  let looked = 0;
  for (const p of parts) {
    if (p.prefix || p.skip) continue;
    looked += 1;
    if (looked > MAX_LOOKED_UP) p.skip = true;
  }
  const single = main.replace(/[\s\d,।॥.()-]+/gu, "");
  return {
    parts: parts.length ? parts : [{ word: single, prefix: false }],
    vigraha: String(out?.vigraha ?? "").trim(),
    samasa: String(out?.samasa ?? "").trim(),
  };
}

// ------------------------------------------------------------------ reading entries

type Reading = {
  id: string;
  is_entry?: boolean;
  why?: string;
  fits_vishay?: boolean;
  derivation_fits?: boolean;
  relevant_gender?: string;
  head?: string;
  gender?: string;
  derivation?: string;
  meaning?: string;
  meaning_gu?: string;
  relevant_gu?: string;
  base?: string | null;
};

const SCAN_CACHE = new Map<string, Promise<Uint8Array>>();
const SCAN_CACHE_SIZE = 60;

function scanOf(url: string, page: number) {
  const key = `${url}#${page}`;
  let hit = SCAN_CACHE.get(key);
  if (!hit) {
    hit = extractPdfPagesByRange(url, [page]).then((r) => r.bytes);
    hit.catch(() => SCAN_CACHE.delete(key));
    SCAN_CACHE.set(key, hit);
    if (SCAN_CACHE.size > SCAN_CACHE_SIZE) SCAN_CACHE.delete(SCAN_CACHE.keys().next().value as string);
  }
  return hit;
}

const LANGUAGE = { gu: "Gujarati", hi: "Hindi", sa: "Sanskrit" } as const;

/** At most this many scans and candidates go in one call, to keep calls small. */
const MAX_SCANS_PER_CALL = 6;
const MAX_CANDIDATES_PER_CALL = 8;

async function readCandidates(
  engine: Engine,
  vishay: string,
  asks: Array<{ word: string; candidates: Candidate[] }>,
  usage: Usage,
  notes: string[]
): Promise<Map<string, Reading>> {
  const all = asks.flatMap((a) => a.candidates.map((c) => ({ word: a.word, c })));
  const out = new Map<string, Reading>();
  const batches: Array<typeof all> = [];
  for (let i = 0; i < all.length; i += MAX_CANDIDATES_PER_CALL) batches.push(all.slice(i, i + MAX_CANDIDATES_PER_CALL));
  // A long compound has many words: its batches are read side by side, and a
  // batch whose answer runs too long is read again in halves.
  const readOne = async (batch: typeof all): Promise<void> => {
    const files: LlmFile[] = [];
    const scanName = new Map<string, string>();
    if (engine === "claude") {
      const wanted = batch.filter(({ c }) => c.kosh.readFromScan && c.pdfUrl).slice(0, MAX_SCANS_PER_CALL);
      await Promise.all(
        wanted.flatMap(({ c }) =>
          [c.pdfPage, ...(c.continues ? [c.pdfPage + 1] : [])].map(async (page) => {
            const name = `${c.granthKey}-p${page}.pdf`;
            if (files.some((f) => f.name === name)) return;
            try {
              files.push({ name, pdf: await scanOf(c.pdfUrl as string, page) });
              scanName.set(c.id, [scanName.get(c.id), name].filter(Boolean).join(", "));
            } catch {
              notes.push(`The scan of ${c.citation}, PDF page ${page}, could not be opened; its entry was read from the OCR text.`);
            }
          })
        )
      );
    }
    const blocks = batch.map(({ word, c }) => {
      const scan = scanName.get(c.id);
      return `--- id: ${c.id}
word: ${word}
kosh: ${c.citation} (meanings in ${LANGUAGE[c.kosh.language as keyof typeof LANGUAGE] ?? "Sanskrit"})${c.kosh.family === "avpk" ? "; format: headword-gender-shloka no.-Gujarati meaning, then ☐ synonyms, then a * line with the derivation" : ""}${c.kosh.tier === 2 && c.kosh.family !== "agamic" ? "; a Prakrit kosh: the headword is Prakrit, and the Sanskrit form after it (in brackets, or after the dash) is NOT a derivation" : ""}
${scan ? `page scan attached: ${scan} — read the entry from the IMAGE; the OCR below misreads Gujarati as Devanagari letters` : "read from the OCR text below (it may have small OCR errors)"}
OCR text:
${c.text}`;
    });
    const prompt = `We are preparing the vyutpatti of the vishay "${vishay}" word by word. Each block below is a place in a kosh where the entry of the block's own "word" may begin. Judge every block on its own word, never on the vishay: a block whose word is कथा is the entry of कथा even when the vishay is अकथा.

${blocks.join("\n\n")}

For each id return:
- is_entry: true when this is the entry of the block's word: its printed headword is that word or that word's nominative/stem form (कथा, कायिकः for कायिक, चौर्यम् for चौर्य; for a Prakrit kosh, the Sanskrit form given for the headword is that word). False for a different word, a longer compound beginning with it, a synonym list, or a running head.
- why: when is_entry is false, the reason in a few English words; otherwise "".
- fits_vishay: whether the sense of this entry is the one the vishay "${vishay}" uses. The vishays are topics of Jain scripture, so a sense fits only when it is the meaning the word has in that Jain topic: काय as "body" fits कायिकहिंसा, काय as the root of the little finger does not; मूलगुण as the basic vows fits, मूलगुण as an arithmetical multiplier does not; अमुक्त as "not liberated" fits अमुक्तमां मुक्तसंज्ञा, अमुक्त as "a weapon held in the hand" does not. When unsure, false.
- derivation_fits: whether the printed derivation belongs to that same sense (Shabda Ratna Mahodadhi's काय prints "कः प्रजापतिर्देवताऽस्य" for the Prajāpati sense, not for "body": false for कायिकहिंसा).
- relevant_gender: the label printed for that sense, when the entry gives the senses different labels; else the same as gender.
- head: the headword as printed.
- gender: the grammar label as printed (पुं., स्त्री., न., त्रि., वि., अव्य. …).
- derivation: the derivation as printed — the text in the first round or square brackets after the label (Shabda Ratna Mahodadhi, Apte), or AVPK's * line — without the brackets or star; "" if the entry prints none. Never put a Prakrit kosh's Sanskrit form here.
- meaning: the meanings as printed, in the kosh's language, OCR errors corrected only where certain; leave out illustrative quotations.
- meaning_gu: the meaning in Gujarati script (ગુજરાતી લિપિ, never Devanagari letters) — copied when printed in Gujarati, otherwise a faithful translation.
- relevant_gu: only the sense(s) of meaning_gu that fit the vishay "${vishay}", in Gujarati, complete in itself (never "see above" or a reference to another entry).
- base: the word (a noun/adjective stem, in Devanagari) this word is derived from according to the derivation, e.g. चौर for चौर्य ("चौरस्य भावः"), चोर for चौर ("चोर एव अण्"); null when it comes straight from a dhātu (चुर्+अच्) or none is given.

JSON: [{"id":"","is_entry":true,"why":"","fits_vishay":true,"derivation_fits":true,"relevant_gender":"","head":"","gender":"","derivation":"","meaning":"","meaning_gu":"","relevant_gu":"","base":null}]`;
    let reply: unknown;
    try {
      reply = await askJson(engine, { system: SYSTEM, prompt, files, maxTokens: 1200 + 700 * batch.length }, usage);
    } catch (error) {
      if (!(error instanceof LlmError) || error.status === 503 || batch.length < 2) throw error;
      const half = Math.ceil(batch.length / 2);
      await Promise.all([readOne(batch.slice(0, half)), readOne(batch.slice(half))]);
      return;
    }
    for (const r of Array.isArray(reply) ? (reply as Reading[]) : []) if (r?.id) out.set(String(r.id), r);
  };
  await Promise.all(batches.map((batch) => readOne(batch)));
  return out;
}

function toEntry(word: string, c: Candidate, r: Reading, engine: Engine): VyutpattiEntry {
  let derivation = normalizeDerivation(String(r.derivation ?? ""));
  // "अधिक (अधिक)", or a Prakrit kosh's Sanskrit form: not a derivation.
  if ([word, c.head, c.sanskrit ?? "", String(r.head ?? "")].some((w) => w && foldSanskrit(w) === foldSanskrit(derivation))) derivation = "";
  const readFrom = engine === "claude" && c.kosh.readFromScan && c.pdfUrl ? "scan" : "ocr";
  return {
    id: c.id,
    word,
    granthKey: c.granthKey,
    citation: c.citation,
    tier: c.kosh.tier,
    pdfPage: c.pdfPage,
    printedPage: c.printedPage,
    pdfUrl: c.pdfUrl,
    head: String(r.head ?? c.head),
    gender: String(r.gender ?? ""),
    derivation,
    meaning: String(r.meaning ?? ""),
    meaningGu: String(r.meaning_gu ?? ""),
    relevantGu: String(r.relevant_gu ?? ""),
    fitsVishay: r.fits_vishay !== false,
    derivationFits: r.derivation_fits !== false,
    relevantGender: String(r.relevant_gender ?? ""),
    readFrom,
    // The derivation is Devanagari, which the OCR reads well even where it
    // garbled the Gujarati: a reading it does not support is flagged.
    checked: agreement(derivation, c.text) >= 0.6,
  };
}

// ------------------------------------------------------------------ AI fallback

async function deriveMissing(engine: Engine, vishay: string, words: string[], usage: Usage): Promise<Map<string, AiDerivation>> {
  const out = new Map<string, AiDerivation>();
  if (!words.length) return out;
  const prompt = `Vishay: ${vishay}
None of our koshes (Abhidhan Vyutpatti Prakriya Kosh, Shabda Ratna Mahodadhi, Apte, Agamic Vyutpatti Kosh, Abhidhan Rajendra Kosh, Paia Sadda Mahannavo, Alpaparichit Saiddhantik Shabdakosh) has these words: ${words.join(", ")}.
For each, give its vyutpatti as Shabda Ratna Mahodadhi would print it:
- gender: the abbreviated label (पुं., स्त्री., न., त्रि., अव्य.).
- derivation: short, as a kosh prints it ("चुर्+अच्", "कायस्येदं ठक्", "हिंस्+अ+टाप्"); no explanations.
- meaning_gu: the meaning, briefly, in Gujarati script (ગુજરાતી લિપિ, never Devanagari letters).
- source: the granth the derivation rests on, with the sūtra where you are sure of it (e.g. "सिद्धहेमशब्दानुशासन ६।४।१", "पाणिनीय अष्टाध्यायी ३।१।१३४", "उणादिसूत्र"); "" when you are not sure. Never invent a sūtra number.

JSON: [{"word":"","gender":"","derivation":"","meaning_gu":"","source":""}]`;
  const reply = await askJson(engine, { system: SYSTEM, prompt, maxTokens: 400 + 300 * words.length }, usage);
  for (const r of Array.isArray(reply) ? (reply as Array<Record<string, unknown>>) : []) {
    const word = String(r?.word ?? "").normalize("NFC").trim();
    if (!word) continue;
    out.set(foldSanskrit(word), {
      word,
      gender: String(r.gender ?? ""),
      derivation: String(r.derivation ?? ""),
      meaningGu: String(r.meaning_gu ?? ""),
      source: String(r.source ?? ""),
    });
  }
  return out;
}

/**
 * The base the engine names must be written in the entry's derivation
 * (चौर in "चौरस्य भावः"), and not be the word itself in another form
 * (कारी in "करोतीति … कारी" is कारिन्, not a base कार).
 */
export function baseIsInDerivation(base: string, word: string, derivation: string) {
  const b = foldSanskrit(base.normalize("NFC"));
  const own = new Set(headwordKeys(word));
  if (!b || own.has(b)) return false;
  const tokens = foldSanskrit(derivation.normalize("NFC")).split(/[^\u0900-\u0963\u0970-\u097f]+/u).filter(Boolean);
  return tokens.some((t) => !own.has(t) && !headwordKeys(t).some((k) => own.has(k)) && (t === b || t.startsWith(b)));
}

// ------------------------------------------------------------------ words no kosh has

// Derived words the koshes print only as their base: शिथिलता (शिथिल),
// एकादशत्व (एकादश), प्रत्याख्यानीय (प्रत्याख्यान), आत्मिक (आत्मन्).
const SUFFIXES: Array<[string, string[]]> = [
  ["त्वम्", [""]], ["त्व", [""]], ["ता", [""]], ["ीय", ["", "ा"]], ["ईय", [""]],
  ["िक", ["", "न्", "ा"]], ["इक", [""]], ["वत्", [""]], ["मान", [""]],
];

/** The bases a derived word may come from. */
export function baseCandidates(word: string) {
  const out: string[] = [];
  for (const [suffix, restore] of SUFFIXES) {
    if (!word.endsWith(suffix) || word.length - suffix.length < 2) continue;
    const stem = word.slice(0, -suffix.length);
    for (const r of restore) out.push(stem + r);
  }
  return [...new Set(out)].filter((w) => w !== word);
}

type Missing = { kind: "split"; parts: Array<{ word: string; prefix: boolean }> } | { kind: "base"; base: string } | null;

/**
 * A word the koshes do not have: broken into words they do have (rule 3:
 * कुगुरु → कु- + गुरु, प्रतिवासुदेव → प्रति + वासुदेव), or, for a derived
 * word, the base they have (शिथिलता → शिथिल).
 */
// A split costing more than this per part is made of weak or OCR-broken
// headwords (अपहर + णी, प्रयु + जन); a real one costs about 1 (ऊर्मि + मालिनी).
const MAX_SPLIT_COST_PER_PART = 1.2;

export async function resolveMissing(word: string): Promise<Missing> {
  const split = compoundParts(word, getLexicon());
  const scored = splitCompound(word, getLexicon(), { forbidWhole: true });
  const wordParts = (split?.parts ?? []).filter((p) => p.kind === "word" || p.kind === "unknown");
  // Every piece is a real kosh headword of two syllables or more, and the split is a cheap one.
  const clean =
    wordParts.length >= 1 &&
    wordParts.every((p) => p.kind === "word" && p.source === SOURCE.head && aksharaCount(p.stem) >= 2) &&
    Boolean(scored) &&
    scored!.cost / Math.max(1, scored!.parts.length) < MAX_SPLIT_COST_PER_PART;
  const parts = (clean ? split?.parts ?? [] : [])
    .filter((p) => p.kind === "word" || p.kind === "unknown" || p.kind === "prefix")
    .map((p) => ({ word: p.kind === "prefix" ? p.label : "term" in p && p.term ? p.term : p.text, prefix: p.kind === "prefix" }));
  const words = parts.filter((p) => !p.prefix).map((p) => p.word);
  const bases = baseCandidates(word);
  const tiers = await headwordTiers([...words, ...bases]).catch(() => new Map<string, { tier: number; head: string }>());
  if (parts.length >= 2 && words.length >= 1 && words.every((w) => tiers.get(w)?.tier === 1) && !(words.length === 1 && foldSanskrit(words[0]) === foldSanskrit(word))) {
    return { kind: "split", parts };
  }
  const base = bases.find((b) => tiers.has(b));
  return base ? { kind: "base", base } : null;
}

// ------------------------------------------------------------------ the run

/** Derivation chains are followed this far back (चौर्य → चौर → चोर). */
const MAX_BASE_DEPTH = 3;

export type Progress = (message: string) => void;

export async function buildVyutpatti(input: VishayInput, engine: Engine, progress: Progress = () => {}): Promise<VyutpattiResult> {
  const vishay = cleanVishay(input.vishay);
  const usage = newUsage();
  const notes: string[] = [];
  const stamp = remarkStamp();
  if (engine === "sarvam") notes.push("Read with Sarvam: Shabda Ratna Mahodadhi's Gujarati meanings come from OCR text, not the scan. Check them against the page.");

  progress("Splitting the vishay");
  const plan = await planVishay(engine, vishay, usage);
  const { main } = splitContext(vishay);
  // The whole is looked up as one word only when it is one word (not a phrase or a maxim).
  const whole = /^[ऀ-ॿ]+$/u.test(main) ? main : "";
  const looked = [...new Set(plan.parts.filter((p) => !p.prefix && !p.skip).map((p) => p.word))];
  const wholeIsPart = !whole || (looked.length === 1 && foldSanskrit(looked[0]) === foldSanskrit(whole));

  const words: VyutpattiWord[] = [];
  const seen = new Set<string>();
  const queue: Array<{ word: string; role: WordRole; of?: string; depth: number; broken?: boolean; tier2?: boolean; otherSense?: VyutpattiEntry[] }> = [];
  for (const word of looked) queue.push({ word, role: "part", depth: 0 });
  if (!wholeIsPart) queue.push({ word: whole, role: "whole", depth: 0 });

  while (queue.length) {
    const round = queue.splice(0).filter((q) => {
      const key = foldSanskrit(q.word);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (!round.length) break;
    progress(`Looking up ${round.map((q) => q.word).join(", ")} in the koshes`);
    const found = await Promise.all(
      round.map(async (q) => {
        let candidates = await findCandidates(q.word, q.tier2 ? { tier: 2 } : {});
        // देवेन्द्र: not a headword, but Apte has it under देव as "-इन्द्रः".
        if (!candidates.length && q.role !== "base" && !q.tier2) {
          const split = compoundParts(q.word, getLexicon());
          const parts = split ? searchableParts(split.parts) : [];
          if (parts.length === 2) {
            const term = (p: (typeof parts)[number]) => ("term" in p && p.term ? p.term : p.text);
            const sub = await findApteSubentry(term(parts[0]), term(parts[1]));
            if (sub) candidates = [sub];
          }
        }
        return { ...q, candidates };
      })
    );
    const withCandidates = found.filter((f) => f.candidates.length);
    if (withCandidates.length) progress(`Reading ${withCandidates.reduce((n, f) => n + f.candidates.length, 0)} kosh entries${engine === "claude" ? " from the page scans" : ""}`);
    const readings = withCandidates.length ? await readCandidates(engine, vishay, withCandidates, usage, notes) : new Map<string, Reading>();

    for (const f of found) {
      const entries = f.candidates
        .map((c) => ({ c, r: readings.get(c.id) }))
        .filter((x): x is { c: Candidate; r: Reading } => Boolean(x.r?.is_entry))
        .map(({ c, r }) => toEntry(f.word, c, r, engine));
      // Only entries in the vishay's sense count (AVPK's अमुक्त is "a knife held in
      // the hand", not "the unliberated"); the others stay in the internal table.
      const fitting = entries.filter((e) => e.fitsVishay);
      const item: VyutpattiWord = { word: f.word, role: f.role, of: f.of, entries: fitting };
      if (fitting.length < entries.length) item.otherSense = entries.filter((e) => !e.fitsVishay);
      words.push(item);
      if (f.role === "base" && !fitting.length && entries.length) notes.push(`${f.word} (the base of ${f.of}) is in the koshes only in another sense; left out.`);
      // The three koshes have it only in other senses: the other four come next.
      if (!fitting.length && entries.length && f.role !== "base" && !f.tier2 && f.candidates.every((c) => c.kosh.tier === 1)) {
        words.pop();
        seen.delete(foldSanskrit(f.word));
        queue.push({ ...f, tier2: true, otherSense: entries });
        continue;
      }
      if (f.otherSense) item.otherSense = [...(item.otherSense ?? []), ...f.otherSense];
      // Not in any kosh in this sense: break it up, or look up the base it is derived from.
      if (!fitting.length && f.role === "part" && !f.broken) {
        const missing = await resolveMissing(f.word);
        if (missing?.kind === "split") {
          item.brokenInto = missing.parts.map((p) => (p.prefix ? `${p.word}-` : p.word));
          const at = looked.indexOf(f.word);
          const inner = missing.parts.filter((p) => !p.prefix).map((p) => p.word);
          if (at >= 0) looked.splice(at, 1, ...inner);
          for (const w of inner) queue.push({ word: w, role: "part", of: f.word, depth: 0, broken: true });
          notes.push(`${f.word} is not in the koshes; it was broken into ${item.brokenInto.join(" + ")}.`);
        } else if (missing?.kind === "base") {
          queue.push({ word: missing.base, role: "base", of: f.word, depth: f.depth + 1 });
        }
      }
      // The chain back: the word the first (highest priority) entry derives from.
      const base = f.candidates
        .map((c) => ({ c, r: readings.get(c.id) }))
        .filter(({ c, r }) => r?.is_entry && r.fits_vishay !== false && r.base && baseIsInDerivation(String(r.base), f.word, String(r.derivation ?? c.text)))
        .map(({ r }) => r!.base)[0];
      if (base && f.depth < MAX_BASE_DEPTH && fitting.length) {
        const clean = String(base).normalize("NFC").trim();
        if (/^[ऀ-ॿ]{2,}$/u.test(clean)) queue.push({ word: clean, role: "base", of: f.word, depth: f.depth + 1 });
      }
    }
  }

  // Rule 6: a part no kosh has gets the engine's derivation. A base not found
  // is left out, and so is a whole compound: its samāsa vigraha explains it.
  // The whole gets one only when it is a single word with no vigraha, never a long compound.
  const missing = words.filter(
    (w) => !w.entries.length && !w.brokenInto && (w.role === "part" || (w.role === "whole" && !plan.vigraha && looked.length <= 1))
  );
  if (missing.length) {
    progress(`${missing.map((w) => w.word).join(", ")}: in no kosh; asking for its vyutpatti`);
    // A derived word whose base a kosh has is derived from that base (शिथिलता from शिथिल).
    const withBase = missing.map((w) => {
      const base = words.find((b) => b.role === "base" && b.of === w.word && b.entries.length);
      return base ? `${w.word} (its base ${base.word} is in ${base.entries[0].citation})` : w.word;
    });
    const derived = await deriveMissing(engine, vishay, withBase, usage);
    for (const w of missing) w.ai = derived.get(foldSanskrit(w.word));
  }

  const kept = words.filter((w) => w.entries.length || w.ai);
  const ordered = orderWords(kept, looked, whole);
  const lines: ReaderLine[] = [];
  const rows: TableRow[] = [];
  for (const w of ordered) {
    if (w.entries.length) {
      lines.push(readerLineForEntry(primaryEntry(w.entries)));
      for (const e of w.entries) rows.push(tableRowForEntry(e, stamp));
    } else if (w.ai) {
      lines.push(readerLineForAi(w.ai));
      rows.push(tableRowForAi(w.ai, stamp));
    }
    // The kosh's other senses, for the internal record only.
    for (const e of w.otherSense ?? []) {
      const row = tableRowForEntry(e, stamp);
      rows.push({ ...row, inRem: `${row.inRem} (આ વિષયમાં આ અર્થ નથી)` });
    }
  }
  if (plan.vigraha && whole && !wholeIsPart) {
    lines.push(readerLineForVigraha(whole, plan.vigraha));
    rows.push(tableRowForVigraha(whole, plan.vigraha, plan.samasa, stamp));
  }
  for (const w of words) if (w.role === "base" && !w.entries.length && !w.otherSense) notes.push(`${w.word} (the base of ${w.of}) is not in the koshes and is left out.`);
  for (const line of lines) {
    if (/[\u0900-\u097f]/u.test(line.body) && !/[\u0a80-\u0aff]/u.test(line.body) && line.source !== "vigraha") {
      notes.push(`The meaning for ${line.head} is not in Gujarati script; check it.`);
    }
  }

  return {
    number: input.number,
    vishay,
    box: input.box,
    engine,
    parts: plan.parts,
    vigraha: plan.vigraha,
    samasa: plan.samasa,
    words: ordered,
    lines,
    rows,
    costUsd: Math.round(usage.costUsd * 10000) / 10000,
    notes,
  };
}

/**
 * The entry the reader line is made from: the first kosh in priority whose
 * sense fits the vishay and that prints a derivation for that sense (AVPK's
 * चौर has none, Shabda Ratna Mahodadhi's has चोर एव अण्; its काय derives the
 * Prajāpati sense, Apte's चि+घञ् the body), else the closest to that.
 */
export function primaryEntry(entries: VyutpattiEntry[]) {
  return (
    entries.find((e) => e.fitsVishay && e.derivation && e.derivationFits) ??
    entries.find((e) => e.fitsVishay && e.derivation) ??
    entries.find((e) => e.fitsVishay) ??
    entries.find((e) => e.derivation) ??
    entries[0]
  );
}

/**
 * Reader order: each part with its chain, the oldest base first (चोर, चौर,
 * चौर्य), parts in the vishay's order, the whole vishay last.
 */
function orderWords(words: VyutpattiWord[], parts: string[], vishay: string) {
  const byKey = new Map(words.map((w) => [foldSanskrit(w.word), w]));
  const out: VyutpattiWord[] = [];
  const placed = new Set<string>();
  const chainOf = (word: VyutpattiWord) => {
    const chain = [word];
    let current = word;
    for (;;) {
      const base = words.find((w) => w.role === "base" && w.of && foldSanskrit(w.of) === foldSanskrit(current.word));
      if (!base || chain.includes(base)) break;
      chain.unshift(base);
      current = base;
    }
    return chain;
  };
  const place = (w: VyutpattiWord | undefined) => {
    if (!w) return;
    for (const c of chainOf(w)) {
      const key = foldSanskrit(c.word);
      if (placed.has(key)) continue;
      placed.add(key);
      out.push(c);
    }
  };
  for (const part of parts) place(byKey.get(foldSanskrit(part)));
  place(byKey.get(foldSanskrit(vishay)));
  for (const w of words) place(w);
  return out;
}
