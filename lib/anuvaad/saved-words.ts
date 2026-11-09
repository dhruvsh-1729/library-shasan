// Maharaj Saheb's own Gujarati for śāstra words, learnt from his anuvaad pages
// and kept for everyone (9 Nov 2026: "identify the vyutpatti words in the box
// file, understand and store the Gujarati translation given below, so it can
// be used later by someone else").
//
// A box file and its Gujarati anuvaad pages are read together. The words the
// anuvaad explains ("સંજ્ઞા: …", "પ્રભુ એટલે સમર્થ") are listed with the
// explanation copied from it; a word is kept only when it is in the box's
// own text and its Gujarati is word for word on the anuvaad page, so nothing
// is invented. They are saved after a person checks them, and are then used
// first: in Vyutpatti (the entry's anuvaad) and in Anuvaad (the box glossary).

import { getTursoClient } from "@/lib/turso";
import { foldSanskrit, sanskritQueryForms } from "@/lib/sanskrit-fold.mjs";
import { headwordKeys } from "@/lib/kosh-headwords.mjs";
import { gujaratiToDevanagari } from "@/lib/to-devanagari";
import { CHAT_MODELS, chat } from "@/lib/sarvam-chat";

export type SavedWord = {
  id: number;
  /** The word as the box prints it, in Devanagari. */
  word: string;
  /** Maharaj Saheb's Gujarati for it, as his anuvaad has it. */
  gujarati: string;
  /** Where it was learnt: the box and granth, as given when saved. */
  source: string;
  addedBy: string;
  addedAt: string;
};

export type WordProposal = { word: string; gujarati: string; inBox: boolean; onPage: number | null };

let ready: Promise<unknown> | null = null;
function ensureTable() {
  ready ??= getTursoClient()
    .batch(
      [
        `CREATE TABLE IF NOT EXISTS anuvaad_saved_words (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          word TEXT NOT NULL,
          word_key TEXT NOT NULL,
          gujarati TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT '',
          added_by TEXT NOT NULL DEFAULT '',
          added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE INDEX IF NOT EXISTS anuvaad_saved_words_key ON anuvaad_saved_words (word_key)`,
      ],
      "write"
    )
    .catch((error) => {
      ready = null;
      throw error;
    });
  return ready;
}

/** The key a word is saved and looked up by: its folded stem (देवः, देव, દેવ → देव). */
export function wordKey(word: string) {
  const devanagari = gujaratiToDevanagari(String(word ?? "").normalize("NFC")).replace(/[\s'"‘’“”:;,.।॥()[\]-]+/g, "");
  const keys = headwordKeys(devanagari) as string[];
  return keys.length ? keys.reduce((a, b) => (b.length < a.length ? b : a)) : foldSanskrit(devanagari);
}

const rowOf = (r: Record<string, unknown>): SavedWord => ({
  id: Number(r.id),
  word: String(r.word ?? ""),
  gujarati: String(r.gujarati ?? ""),
  source: String(r.source ?? ""),
  addedBy: String(r.added_by ?? ""),
  addedAt: String(r.added_at ?? ""),
});

export async function listSavedWords(search = ""): Promise<SavedWord[]> {
  await ensureTable();
  const q = String(search ?? "").trim();
  const result = q
    ? await getTursoClient().execute({
        sql: `SELECT * FROM anuvaad_saved_words WHERE word_key = ? OR word LIKE ? OR gujarati LIKE ? ORDER BY word LIMIT 500`,
        args: [wordKey(q), `%${q}%`, `%${q}%`],
      })
    : await getTursoClient().execute(`SELECT * FROM anuvaad_saved_words ORDER BY added_at DESC, id DESC LIMIT 500`);
  return result.rows.map((r) => rowOf(r as Record<string, unknown>));
}

/** Saves words (a word already saved with the same Gujarati is not saved twice). */
export async function saveWords(words: Array<{ word: string; gujarati: string }>, source: string, addedBy: string) {
  await ensureTable();
  const clean = words
    .map((w) => ({ word: String(w.word ?? "").normalize("NFC").trim(), gujarati: String(w.gujarati ?? "").normalize("NFC").replace(/\s+/g, " ").trim() }))
    .filter((w) => w.word && w.gujarati && wordKey(w.word));
  if (!clean.length) return 0;
  const client = getTursoClient();
  let saved = 0;
  for (const w of clean) {
    const key = wordKey(w.word);
    const same = await client.execute({ sql: `SELECT 1 FROM anuvaad_saved_words WHERE word_key = ? AND gujarati = ? LIMIT 1`, args: [key, w.gujarati] });
    if (same.rows.length) continue;
    await client.execute({
      sql: `INSERT INTO anuvaad_saved_words (word, word_key, gujarati, source, added_by) VALUES (?, ?, ?, ?, ?)`,
      args: [w.word, key, w.gujarati, String(source ?? "").slice(0, 300), String(addedBy ?? "").slice(0, 200)],
    });
    saved += 1;
  }
  return saved;
}

export async function updateSavedWord(id: number, change: { word?: string; gujarati?: string }) {
  await ensureTable();
  const word = change.word != null ? String(change.word).normalize("NFC").trim() : null;
  const gujarati = change.gujarati != null ? String(change.gujarati).normalize("NFC").replace(/\s+/g, " ").trim() : null;
  if (word === "" || gujarati === "") throw new Error("A saved word needs both the word and its Gujarati.");
  await getTursoClient().execute({
    sql: `UPDATE anuvaad_saved_words SET word = COALESCE(?, word), word_key = COALESCE(?, word_key), gujarati = COALESCE(?, gujarati) WHERE id = ?`,
    args: [word, word ? wordKey(word) : null, gujarati, id],
  });
}

export async function deleteSavedWord(id: number) {
  await ensureTable();
  await getTursoClient().execute({ sql: `DELETE FROM anuvaad_saved_words WHERE id = ?`, args: [id] });
}

/** For each vyutpatti entry whose word is saved, Maharaj Saheb's Gujarati for it (index → text; the latest when saved more than once). */
export async function savedGujaratiFor(entries: Array<{ head: string }>) {
  const out = new Map<number, string>();
  const keys = entries.map((e) => (e.head ? wordKey(e.head) : ""));
  const wanted = [...new Set(keys.filter(Boolean))];
  if (!wanted.length) return out;
  await ensureTable();
  const result = await getTursoClient().execute({
    sql: `SELECT word_key, gujarati FROM anuvaad_saved_words WHERE word_key IN (${wanted.map(() => "?").join(",")}) ORDER BY id`,
    args: wanted,
  });
  const byKey = new Map(result.rows.map((r) => [String(r.word_key), String(r.gujarati)]));
  keys.forEach((key, i) => {
    const text = key ? byKey.get(key) : undefined;
    if (text) out.set(i, text);
  });
  return out;
}

/** Folded words of a text, with their Sanskrit forms' stems, for "is this word in the box?". */
function foldedWords(text: string) {
  return new Set(
    foldSanskrit(gujaratiToDevanagari(String(text ?? "").normalize("NFC")))
      .split(/[^\p{L}\p{M}]+/u)
      .filter((w) => w.length >= 2)
  );
}

/** Whether the word, in any of its Sanskrit forms, is printed in the text. */
export function wordInText(word: string, text: string, words = foldedWords(text)) {
  const devanagari = gujaratiToDevanagari(String(word ?? "").normalize("NFC")).trim();
  if (!devanagari) return false;
  if (/\s/.test(devanagari)) return foldSanskrit(gujaratiToDevanagari(text)).includes(foldSanskrit(devanagari));
  const forms = new Set([foldSanskrit(devanagari), ...(sanskritQueryForms(devanagari) as string[])]);
  for (const w of words) if (forms.has(w)) return true;
  // a compound in the box holding the word whole ("प्राणातिपातनिवृत्ति" for प्राणातिपात)
  const stem = wordKey(devanagari);
  return stem.length >= 4 && [...words].some((w) => w.includes(stem));
}

/** Saved words printed in a passage, as glossary lines for its anuvaad. */
export async function savedWordsInText(text: string, limit = 40): Promise<Array<{ word: string; gujarati: string }>> {
  await ensureTable();
  const words = foldedWords(text);
  if (!words.size) return [];
  const all = await getTursoClient().execute(`SELECT word, gujarati FROM anuvaad_saved_words ORDER BY id DESC LIMIT 5000`);
  const seen = new Set<string>();
  const out: Array<{ word: string; gujarati: string }> = [];
  for (const r of all.rows) {
    const word = String(r.word);
    const key = wordKey(word);
    if (seen.has(key) || !wordInText(word, text, words)) continue;
    seen.add(key);
    out.push({ word, gujarati: String(r.gujarati) });
    if (out.length >= limit) break;
  }
  return out;
}

// ------------------------------------------------------------ learning from a box and its anuvaad

const GUJ = /[઀-૿]/g;
const DEV = /[ऀ-ॿ]/g;

/** A page is anuvaad when it is mostly Gujarati letters; a box page is mostly Devanagari. */
export function isAnuvaadPage(text: string) {
  const g = (String(text).match(GUJ) ?? []).length;
  const d = (String(text).match(DEV) ?? []).length;
  return g > 200 && g > d;
}

const squash = (text: string) => String(text ?? "").normalize("NFC").replace(/[*_`#>|]/g, "").replace(/\s+/g, " ").trim();

const LEARN_RULES = `You read a Gujarati anuvaad that a Jain ācārya wrote of a Sanskrit/Prakrit śāstra passage. List only the śāstra terms it defines: a term followed by its meaning in other words — a bold label and its explanation ("પરિચ્છિન્ન: મર્યાદિત"), or a term followed by એટલે, અર્થાત્, "=" or a meaning in brackets ("અસ્તેય (ચોરી ન કરવી)").
Never list a term whose "explanation" is only the same term again, a number, a heading, or part of a sentence about something else. Never list ordinary words (અને, તથા, છે, કારણ). Fewer, certain terms are better than many.
For each, give:
- word: the śāstra word itself in Devanagari, as Sanskrit (write the Gujarati label back in Devanagari letters: "સંજ્ઞા" → "संज्ञा");
- gujarati: its explanation, copied word for word from the anuvaad — never reworded, shortened in the middle, or added to. One sentence or clause, as written.
Leave out headings, numbering, the vishay line and anything that is not one word's explanation.
Answer with JSON lines only, one per word: {"word": "...", "gujarati": "..."}`;

/** Ordinary words that are never a vyutpatti word of a box. */
const NOT_TERMS = new Set(
  ["तथा", "अर्थात्", "कारण", "इति", "एव", "च", "वा", "छे", "ज", "अने", "पण", "जीवो", "सदा", "सर्वथा", "कदाचित्", "करे", "विषय", "स्वरूप"].map((w) => foldSanskrit(w))
);

/** Whether a proposed pair is a real explanation: a term of three letters or more, explained in other words. */
export function isExplanation(word: string, gujarati: string) {
  const key = foldSanskrit(word).replace(/[^\p{L}\p{M}]/gu, "");
  if (Array.from(key).length < 3 || NOT_TERMS.has(key)) return false;
  const text = gujarati.trim();
  // a fragment: starts with a joining word, a bracket or a number, or ends mid-sentence
  if (/^(?:અને|તથા|કે|પણ|તેથી|જ્યારે|\(|[૦-૯0-9])/u.test(text) || /[,:–—-]$/u.test(text)) return false;
  // only the word itself again ("વિરત" for विरत, "સાચું (સુપ્રત્યાખ્યાન)" keeps its gloss)
  const rest = foldSanskrit(gujaratiToDevanagari(text)).replace(/[^\p{L}\p{M}]/gu, "");
  if (!rest || rest === key || (rest.includes(key) && rest.length - key.length < 3)) return false;
  return /[\u0a80-\u0aff]{2,}/u.test(text);
}

/** Asks for the words one anuvaad page explains, keeping only those whose Gujarati is word for word on the page. */
async function wordsOfAnuvaadPage(page: string): Promise<Array<{ word: string; gujarati: string }>> {
  const text = squash(page).slice(0, 9000);
  if (!text) return [];
  const { content } = await chat({
    model: CHAT_MODELS.fast,
    temperature: 0,
    maxTokens: 4000,
    messages: [
      { role: "system", content: LEARN_RULES },
      { role: "user", content: `Anuvaad:\n${String(page).slice(0, 9000)}` },
    ],
  });
  const flat = text.replace(/[“”"]/g, "");
  const out: Array<{ word: string; gujarati: string }> = [];
  for (const line of content.replace(/<think>[\s\S]*?<\/think>/g, "").split("\n")) {
    const m = line.match(/\{[\s\S]*\}/);
    if (!m) continue;
    try {
      const item = JSON.parse(m[0]) as { word?: unknown; gujarati?: unknown };
      const word = gujaratiToDevanagari(String(item.word ?? "").normalize("NFC")).replace(/[:：]+$/, "").trim();
      const gujarati = squash(String(item.gujarati ?? "")).replace(/^[:–—-]\s*/, "");
      // the Gujarati must be the anuvaad's own words
      if (word && gujarati.length >= 3 && flat.includes(gujarati.replace(/[“”"]/g, "")) && isExplanation(word, gujarati)) out.push({ word, gujarati });
    } catch {
      // a line that is not JSON is skipped
    }
  }
  return out;
}

/**
 * The words the anuvaad pages explain, each with its Gujarati, marked whether
 * the box's own text prints the word. `pages` are the OCR texts of a box file
 * and its anuvaad pages in any order; each page is sorted by its script.
 */
export async function learnWords(pages: string[]): Promise<{ proposals: WordProposal[]; boxPages: number; anuvaadPages: number }> {
  const boxText = pages.filter((p) => !isAnuvaadPage(p)).join("\n");
  const anuvaad = pages.map((text, i) => ({ text, page: i + 1 })).filter((p) => isAnuvaadPage(p.text));
  const words = foldedWords(boxText);
  const seen = new Set<string>();
  const proposals: WordProposal[] = [];
  for (const page of anuvaad) {
    for (const w of await wordsOfAnuvaadPage(page.text)) {
      const id = `${wordKey(w.word)}\u0000${w.gujarati}`;
      if (seen.has(id)) continue;
      seen.add(id);
      proposals.push({ ...w, inBox: wordInText(w.word, boxText, words), onPage: page.page });
    }
  }
  // words the box prints first: those are the box's own vyutpatti words
  proposals.sort((a, b) => Number(b.inBox) - Number(a.inBox));
  return { proposals, boxPages: pages.length - anuvaad.length, anuvaadPages: anuvaad.length };
}
