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
import { headwordKeys, vyutpattiKosh } from "@/lib/kosh-headwords.mjs";
import { SOURCE, aksharaCount, compoundParts, createLexicon, searchableParts, splitCompound } from "@/lib/sanskrit-compound.mjs";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { type EntryReading, cacheKey, loadReadings, saveReadings } from "@/lib/vyutpatti/entry-cache";
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
- A Gujarati word with its Gujarati ending stays whole and is skipped: खूणामां (Gujarati "in the corner"), बेठेली, घरमां, साधुओनी — never cut it into a stem to look up (not खूणा + मां).
- A sentence or maxim (a न्याय, a quoted line) is not split word by word: give at most ${MAX_LOOKED_UP} of its key technical nouns and adjectives, as stems; skip its verb forms (तारयति, अनुवर्तते) and particles (च, न, इति, तु, एव, अपि, हि) with "skip": true; vigraha "".
- Split off कु-, सु-, दुस्-, निस्- and the negation as prefixes (कुगुरु = कु- + गुरु) unless a kosh prints the whole as one word.
- vigraha: the samāsa vigraha of the whole vishay in Sanskrit (e.g. "न चौर्यम् इति अचौर्यम्", "कायिकी चासौ हिंसा च कायिकहिंसा"); "" when the vishay is a single plain word, a phrase of several separate words, or a sentence. It uses the members as the vishay writes them and ends with the vishay exactly as written ("श्रमणानां पर्षदा श्रमणपर्षदा", not "… श्रमणपर्षद्"). A member written in the vishay that is itself a compound gets its own vigraha first, as its own clause, before the whole's (only such a member: never add a word or a compound the vishay does not have; ज्योतिष्कदेवीपर्षदा has no ज्योतिष्कदेव, so "ज्योतिष्काणां देवीनां पर्षदा ज्योतिष्कदेवीपर्षदा"): "भवनानां पतिः इति भवनपतिः, भवनपतीनां देवीनां पर्षदा भवनपतिदेवीपर्षदा" (Maharaj Saheb's correction). A member made of a noun and a verbal noun (उपपद) is explained with the finite verb: "कुम्भं करोति इति कुम्भकारः", never "कुम्भं कारः".
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

type Reading = EntryReading & {
  id: string;
  fits_vishay?: boolean;
  derivation_fits?: boolean;
  relevant_gender?: string;
  relevant_gu?: string;
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

/** At most this many candidates (each one or two page scans) go in one call, to keep calls small. */
const MAX_CANDIDATES_PER_CALL = 5;
const MAX_SENSES_PER_CALL = 16;

/** Readings being made right now, so two vishays run side by side share one reading of an entry. */
const PENDING = new Map<string, Promise<EntryReading | undefined>>();

/**
 * What each candidate entry prints, read once and kept (lib/vyutpatti/entry-cache):
 * the same entry reads the same in every vishay. Nothing here depends on the vishay.
 */
async function readEntries(
  engine: Engine,
  asks: Array<{ word: string; candidates: Candidate[] }>,
  usage: Usage,
  notes: string[],
  readAgain: boolean
): Promise<Map<string, EntryReading>> {
  const all = asks.flatMap((a) => a.candidates.map((c) => ({ word: a.word, c, key: cacheKey(c.id, foldSanskrit(a.word)) })));
  const out = new Map<string, EntryReading>();
  // readAgain: every entry is read again and the kept reading replaced (an entry found misread).
  const kept = readAgain ? new Map<string, EntryReading>() : await loadReadings(all.map((x) => ({ entryId: x.c.id, wordKey: foldSanskrit(x.word) })), engine);
  const toRead: typeof all = [];
  const waits: Array<Promise<void>> = [];
  for (const x of all) {
    if (out.has(x.key)) continue;
    const hit = kept.get(x.key);
    if (hit) {
      out.set(x.key, hit);
      continue;
    }
    const pending = PENDING.get(x.key);
    if (pending) {
      waits.push(pending.then((r) => void (r && out.set(x.key, r))));
      continue;
    }
    if (toRead.some((y) => y.key === x.key)) continue;
    toRead.push(x);
  }
  const resolvers = new Map<string, (r: EntryReading | undefined) => void>();
  for (const x of toRead) PENDING.set(x.key, new Promise((resolve) => resolvers.set(x.key, resolve)));

  const batches: Array<typeof toRead> = [];
  for (let i = 0; i < toRead.length; i += MAX_CANDIDATES_PER_CALL) batches.push(toRead.slice(i, i + MAX_CANDIDATES_PER_CALL));
  const fresh: Array<{ entryId: string; wordKey: string; reading: EntryReading }> = [];
  // A long compound has many words: its batches are read side by side, and a
  // batch whose answer runs too long is read again in halves.
  const readOne = async (batch: typeof toRead): Promise<void> => {
    const files: LlmFile[] = [];
    const scanName = new Map<string, string>();
    if (engine === "claude") {
      await Promise.all(
        batch
          .filter(({ c }) => c.kosh.readFromScan && c.pdfUrl)
          .flatMap(({ c }) =>
            [c.pdfPage, ...(c.continues ? [c.pdfPage + 1] : [])].map(async (page) => {
              const name = `${c.granthKey}-p${page}.pdf`;
              try {
                const pdf = await scanOf(c.pdfUrl as string, page);
                if (!files.some((f) => f.name === name)) files.push({ name, pdf });
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
kosh: ${c.citation} (meanings in ${LANGUAGE[c.kosh.language as keyof typeof LANGUAGE] ?? "Sanskrit"})${c.kosh.family === "avpk" ? "; format: headword-gender-shloka no.-Gujarati meaning, then ☐ synonyms, then a * line with the derivation, which often runs on over two or more printed lines" : ""}${c.kosh.tier === 2 && c.kosh.family !== "agamic" ? "; a Prakrit kosh: the headword is Prakrit, and the Sanskrit form after it (in brackets, or after the dash) is NOT a derivation" : ""}
${scan ? `page scan attached: ${scan} — read the entry from the IMAGE, all of it to its last line; the OCR below may misread Gujarati as Devanagari letters, mix the page's two columns, or stop early` : "read from the OCR text below (it may have small OCR errors)"}
OCR text:
${c.text}`;
    });
    const prompt = `Each block below is a place in a kosh where the entry of the block's own "word" may begin. Read each entry exactly as the kosh prints it. What you return is kept and printed for every topic that cites this entry, so it must be the whole entry, word for word, never a summary.

${blocks.join("\n\n")}

For each id return:
- is_entry: true when this is the entry of the block's word: its printed headword is that word or that word's nominative/stem form (कथा, कायिकः for कायिक, चौर्यम् for चौर्य; for a Prakrit kosh, the Sanskrit form given for the headword is that word). False for a different word, a longer compound beginning with it, a synonym list, or a running head.
- why: when is_entry is false, the reason in a few English words; otherwise "".
- head: the headword as printed.
- gender: the grammar label as printed (पुं., स्त्री., न., त्रि., वि., अव्य. …).
- derivation: the derivation as printed, complete to its last word — the text in the first round or square brackets after the label (Shabda Ratna Mahodadhi, Apte), or AVPK's * line with every line it runs on to (a line ending in "-" continues on the next) — without the brackets or star; "" if the entry prints none. Never put a Prakrit kosh's Sanskrit form here.
- meaning: every meaning as printed, in the kosh's language, OCR errors corrected only where certain; leave out illustrative quotations.
- meaning_gu: every meaning of the entry in Gujarati script (ગુજરાતી લિપિ, never Devanagari letters), in the printed order, all senses and labels, complete: never shortened, summarised, or cut with "વગેરે" where the kosh lists more — copied when printed in Gujarati, otherwise a faithful full translation.
- base: the word (a noun/adjective stem, in Devanagari) this word is derived from according to the derivation, e.g. चौर for चौर्य ("चौरस्य भावः"), चोर for चौर ("चोर एव अण्"); null when it comes straight from a dhātu (चुर्+अच्) or none is given.

JSON: [{"id":"","is_entry":true,"why":"","head":"","gender":"","derivation":"","meaning":"","meaning_gu":"","base":null}]`;
    let reply: unknown;
    try {
      reply = await askJson(engine, { system: SYSTEM, prompt, files, maxTokens: 1500 + 1200 * batch.length }, usage);
    } catch (error) {
      if (!(error instanceof LlmError) || error.status === 503 || batch.length < 2) throw error;
      const half = Math.ceil(batch.length / 2);
      await Promise.all([readOne(batch.slice(0, half)), readOne(batch.slice(half))]);
      return;
    }
    const byId = new Map((Array.isArray(reply) ? (reply as Reading[]) : []).filter((r) => r?.id).map((r) => [String(r.id), r]));
    for (const x of batch) {
      const r = byId.get(x.c.id);
      if (!r) continue;
      const reading: EntryReading = {
        is_entry: r.is_entry,
        why: r.why,
        head: r.head,
        gender: r.gender,
        derivation: r.derivation,
        meaning: r.meaning,
        meaning_gu: r.meaning_gu,
        base: r.base ?? null,
      };
      out.set(x.key, reading);
      // Kept only when read as intended: a scan that failed to open gave an OCR reading.
      const meant = engine !== "claude" || !x.c.kosh.readFromScan || !x.c.pdfUrl || scanName.has(x.c.id);
      if (meant) fresh.push({ entryId: x.c.id, wordKey: foldSanskrit(x.word), reading });
    }
  };
  try {
    await Promise.all(batches.map((batch) => readOne(batch)));
    await saveReadings(fresh, engine);
  } finally {
    for (const x of toRead) {
      resolvers.get(x.key)?.(out.get(x.key));
      PENDING.delete(x.key);
    }
  }
  await Promise.all(waits);
  return out;
}

/** Which sense of each entry the vishay uses: judged on every run, from the kept readings. */
async function judgeSenses(
  engine: Engine,
  vishay: string,
  items: Array<{ id: string; word: string; citation: string; reading: EntryReading }>,
  usage: Usage
): Promise<Map<string, Reading>> {
  const out = new Map<string, Reading>();
  const batches: Array<typeof items> = [];
  for (let i = 0; i < items.length; i += MAX_SENSES_PER_CALL) batches.push(items.slice(i, i + MAX_SENSES_PER_CALL));
  await Promise.all(
    batches.map(async (batch) => {
      const blocks = batch.map(
        ({ id, word, citation, reading: r }) => `--- id: ${id}
word: ${word}
kosh: ${citation}
entry: ${[r.head, r.gender, r.derivation ? `(${r.derivation})` : ""].filter(Boolean).join(" ")}
meanings: ${r.meaning_gu || r.meaning || ""}`
      );
      const prompt = `We are preparing the vyutpatti of the vishay "${vishay}" word by word. Below are kosh entries of its words. Judge every block on its own word's sense in the vishay.

${blocks.join("\n\n")}

For each id return:
- fits_vishay: whether the entry has the meaning the word has in the vishay "${vishay}". The vishays are Jain topics. Answer false only for a clearly different meaning: काय as the root of the little finger does not fit कायिकहिंसा (it means body there); मूलगुण as an arithmetical multiplier does not fit a vishay on vows; अमुक्त as "a weapon held in the hand" does not fit अमुक्तमां मुक्तसंज्ञा. An entry whose only meaning is one particular person (the mother of a tīrthaṅkara, a named queen) or a form of address used in plays does not fit a general term (देवी in a vishay on goddesses). The ordinary meaning of a plain word fits (कथा as talk or story fits अकथा). An entry that only notes how the word is used, not what it means (AVPK's पति: "added to a word, it makes a word for master"), does not fit. An entry that has the vishay's sense among others fits. When unsure, true.
- derivation_fits: whether the printed derivation belongs to that same sense (Shabda Ratna Mahodadhi's काय prints "कः प्रजापतिर्देवताऽस्य" for the Prajāpati sense, not for "body": false for कायिकहिंसा).
- relevant_gender: the label printed for that sense, when the entry gives the senses different labels; else the entry's label.
- relevant_gu: only the sense(s) that fit the vishay, in Gujarati, complete in itself (never "see above" or a reference to another entry).

JSON: [{"id":"","fits_vishay":true,"derivation_fits":true,"relevant_gender":"","relevant_gu":""}]`;
      const reply = await askJson(engine, { system: SYSTEM, prompt, maxTokens: 400 + 250 * batch.length }, usage);
      for (const r of Array.isArray(reply) ? (reply as Reading[]) : []) if (r?.id) out.set(String(r.id), r);
    })
  );
  return out;
}

async function readCandidates(
  engine: Engine,
  vishay: string,
  asks: Array<{ word: string; candidates: Candidate[] }>,
  usage: Usage,
  notes: string[],
  readAgain: boolean
): Promise<Map<string, Reading>> {
  const read = await readEntries(engine, asks, usage, notes, readAgain);
  const entries = asks.flatMap((a) =>
    a.candidates
      .map((c) => ({ id: c.id, word: a.word, citation: c.citation, reading: read.get(cacheKey(c.id, foldSanskrit(a.word))) }))
      .filter((x): x is typeof x & { reading: EntryReading } => Boolean(x.reading?.is_entry))
  );
  const senses = entries.length ? await judgeSenses(engine, vishay, entries, usage) : new Map<string, Reading>();
  const out = new Map<string, Reading>();
  for (const a of asks) {
    for (const c of a.candidates) {
      const reading = read.get(cacheKey(c.id, foldSanskrit(a.word)));
      if (!reading) continue;
      const sense = senses.get(c.id);
      out.set(c.id, {
        ...reading,
        id: c.id,
        fits_vishay: sense?.fits_vishay,
        derivation_fits: sense?.derivation_fits,
        relevant_gender: sense?.relevant_gender,
        relevant_gu: sense?.relevant_gu,
      });
    }
  }
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
    // garbled the Gujarati: a reading it does not support is flagged. It is
    // checked against the whole page, since on a page whose columns the OCR
    // mixed, the end of a long derivation is not in the entry's own lines.
    checked: agreement(derivation, c.pageText || c.text) >= 0.6,
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
  let split = compoundParts(word, getLexicon());
  const scored = splitCompound(word, getLexicon(), { forbidWhole: true });
  // The word list may know the whole as one word (भवनपति, from the Agamic kosh's word list) although
  // no kosh prints an entry for it: then the best cut into kosh words is used (भवन + पति).
  const wholeOnly = (split?.parts ?? []).filter((p) => p.kind === "word" || p.kind === "unknown").length === 1;
  if (split && wholeOnly && scored && scored.parts.length >= 2) split = { ...split, parts: scored.parts };
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

export type BuildOptions = {
  /** Read every kosh entry again instead of using the kept readings, and keep the new ones. */
  fresh?: boolean;
};

export async function buildVyutpatti(
  input: VishayInput,
  engine: Engine,
  progress: Progress = () => {},
  options: BuildOptions = {}
): Promise<VyutpattiResult> {
  const vishay = cleanVishay(input.vishay);
  const usage = newUsage();
  const notes: string[] = [];
  if (engine === "sarvam") notes.push("Read with Sarvam: Shabda Ratna Mahodadhi's Gujarati meanings come from OCR text, not the scan. Check them against the page.");

  progress("Splitting");
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
    progress(`Looking up ${round.length} word${round.length === 1 ? "" : "s"}`);
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
    if (withCandidates.length) progress(`Reading ${withCandidates.reduce((n, f) => n + f.candidates.length, 0)} kosh entries`);
    const readings = withCandidates.length ? await readCandidates(engine, vishay, withCandidates, usage, notes, Boolean(options.fresh)) : new Map<string, Reading>();

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
    progress(`${missing.length} word${missing.length === 1 ? "" : "s"} in no kosh: writing with AI`);
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
  // The kosh's other senses (w.otherSense) are not printed: Sahebji struck every
  // "આ વિષયમાં આ અર્થ નથી" row out on the parṣadā sheets (7 Oct 2026).
  for (const w of ordered) {
    if (w.entries.length) {
      const shown = byKoshPriority(w.entries);
      lines.push(readerLineForEntry(primaryEntry(shown)));
      for (const e of shown) rows.push(tableRowForEntry(e));
    } else if (w.ai) {
      lines.push(readerLineForAi(w.ai));
      rows.push(tableRowForAi(w.ai));
    }
  }
  if (plan.vigraha && whole && !wholeIsPart) {
    lines.push(readerLineForVigraha(whole, plan.vigraha));
    rows.push(tableRowForVigraha(whole, plan.vigraha, plan.samasa));
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
 * Maharaj Saheb's order is AVPK, then Shabda Ratna Mahodadhi, then Apte: Apte
 * is cited only when one of the first two lacks the word in the vishay's sense.
 * On the parṣadā sheets (7 Oct 2026) he struck out Apte's देव, पर्षद्, भवन
 * where both had them, and kept Apte's व्यंतर (no Shabda Ratna Mahodadhi entry)
 * and देवी (AVPK's देवी is only Aranath's mother or a queen).
 */
export function byKoshPriority(entries: VyutpattiEntry[]) {
  const family = (e: VyutpattiEntry) => vyutpattiKosh(e.granthKey)?.family;
  const has = (f: string) => entries.some((e) => e.fitsVishay && family(e) === f);
  return has("avpk") && has("srm") ? entries.filter((e) => family(e) !== "apte") : entries;
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
