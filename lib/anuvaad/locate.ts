// Finds a box passage in the library's own granths (Maharaj Saheb, 9 Nov
// 2026: "the software should find that address in our PDF file in the system,
// and the AI should get that PDF page, not the box file"; 13 Nov: "give the
// same place in the PDF with four pages before and after"; "if the book has
// its translation, give that page first; if not, translate").
//
// The passage's longest words are looked up in the folded word index; the
// pages that hold them all (or most) are scored by how many of the passage's
// words they print, and the best page is the passage's place. Its neighbours
// (±4 pages) come with their text, and any of them that is mostly Gujarati is
// marked as the book's own Gujarati.

import { getTursoClient } from "@/lib/turso";
import { describeGranth, getGranthCatalog, isSearchable } from "@/lib/granth-catalog";
import { WORD_TOKEN_PATTERN, foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { bookNumbers, foldName } from "@/lib/granth-name-search";
import type { GranthCatalog } from "@/lib/granth-catalog";
import { gathaMarks, gathaRange, passageWords } from "@/lib/anuvaad/book-text";

export type LocatedPage = {
  page: number;
  printedPage: string | null;
  text: string;
  /** Most of the page is Gujarati: the book's own translation. */
  gujarati: boolean;
  /** For a Gujarati page: share of the passage's words it quotes (in Gujarati letters), 0-1. */
  quotes?: number;
};

export type LocatedPassage = {
  granthKey: string;
  granthName: string;
  pdfUrl: string;
  /** The page the passage is printed on. */
  page: number;
  printedPage: string | null;
  /** Share of the passage's words found on that page (0-1). */
  score: number;
  /** The box's Granth column names this book. */
  named?: boolean;
  /** The library's book number ("196"), for the printed anuvaad. */
  bookNumber?: string;
  /** The page and its neighbours, in order. */
  pages: LocatedPage[];
};

export const NEARBY_PAGES = 4;

export { passageWords } from "@/lib/anuvaad/book-text";

const ftsWord = (w: string) => `"${w.replace(/"/g, '""')}"`;

/** Devanagari letters written in Gujarati script (the blocks run in parallel). */
export const toGujaratiScript = (text: string) => text.replace(/[ऀ-ॿ]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) + 0x180));

/** Share of the passage's words a Gujarati page quotes: their first 4 letters, in Gujarati script. */
export function quotedShare(words: string[], gujaratiText: string) {
  const stems = words.slice(0, 20).map((w) => [...toGujaratiScript(w)].slice(0, 4).join(""));
  if (!stems.length) return 0;
  return stems.filter((stem) => gujaratiText.includes(stem)).length / stems.length;
}

/** Mostly Gujarati letters: a translation page. */
export function isGujaratiPage(text: string) {
  const gu = text.match(/[઀-૿]/g)?.length ?? 0;
  const dev = text.match(/[ऀ-ॿ]/g)?.length ?? 0;
  return gu > 200 && gu > dev * 1.5;
}

type Candidate = { id: number; granthKey: string; page: number };

// ---------------------------------------------------------------- the box's Granth column
// Maharaj Saheb, 18 Nov 2026: the box named Panchashak and the anuvaad came
// from another book. The Granth column ("पंचाशक अभयदेवसूरि टीका, 04 - पूजाविधि,
// गाथा 42") names the book; a granth of ours with that name is searched first.

/** Consonants only, m and n alike: "पंचाशक" and "Panchashak" both give "pncsk". */
const skeleton = (text: string) =>
  foldName(text)
    .replace(/[aeiouh\s]/g, "")
    .replace(/m/g, "n")
    .replace(/([a-z])\1+/g, "$1");

/** Words of the Granth column that are not names: commentary, part, gatha… */
const NOT_A_NAME = /^(?:मूल|टीका|स्वोपज्ञ|भाग|गाथा|श्लोक|सूत्र|अध्ययन|उद्देशक|प्रकाश|ढाळ|अधिकार|प्रकरण|प्रकरणम्|उपरि|वृत्ति|भाष्य|निर्युक्ति|चूर्णि|उ|श्रुतस्कंध|शतक|प्रस्ताव|महाग्रन्थ|ग्रंथ|ग्रन्थ)$/;

/** Name skeletons the Granth column gives: the first word of each part, and the first two together. */
export function granthHints(granth: string) {
  const out = new Set<string>();
  for (const part of granth.split(/[;,>]/)) {
    const words = part
      .replace(/[0-9०-९]+/g, " ")
      .replace(/[()\-–—.:]/g, " ")
      .split(/\s+/)
      .filter((w) => /[ऀ-ॿ઀-૿]/.test(w) && !NOT_A_NAME.test(w));
    if (!words.length) continue;
    for (const k of [skeleton(words[0]), words[1] ? skeleton(words[0] + words[1]) : ""]) if (k.length >= 4) out.add(k);
  }
  return [...out];
}

/** Our granths the Granth column names. */
export function hintedGranths(catalog: GranthCatalog, granth: string) {
  const hints = granthHints(granth);
  if (!hints.length) return [];
  const keys: string[] = [];
  for (const [key, entry] of catalog.byGranthKey) {
    if (!isSearchable(catalog, entry)) continue;
    const name = (describeGranth(catalog, key)?.displayName ?? "").replace(/·.*$/, "");
    const words = name.split(/\s+/).filter(Boolean);
    // the name from each of its words on ("Agam 42 Dashvaikalik Sutra …" → "Dashvaikalik Sutra …")
    const tails = words.map((_, i) => skeleton(words.slice(i).join(" "))).filter((t) => t.length >= 5);
    const named = hints.some((h) => tails.some((t) => t.startsWith(h) || (h.startsWith(t) && t.length >= h.length * 0.6)));
    if (named) keys.push(key);
  }
  return keys;
}

/**
 * Pages of the named granths that print the box's first gatha number as a
 * verse ends it ("॥२९॥"), when the words cannot find it: a box may hold only
 * the Sanskrit tika ("यतना … धर्मसारो") of a book that prints the Prakrit mula
 * ("जयणा उ धम्मसारो ॥२९॥"). A book restarts its numbers in each chapter, so
 * the page whose Gujarati quotes the passage, or whose heads name the box's
 * chapter ("07 – जिनभवन विधिप्रकरण"), is taken.
 */
async function byGathaNumber(granth: string, words: string[], keys: string[]): Promise<Candidate & { score: number } | null> {
  const range = gathaRange(granth);
  if (!range || !keys.length) return null;
  const [first] = range;
  const digits = [String(first), String(first).replace(/\d/g, (d) => "०१२३४५६७८९"[Number(d)]), String(first).replace(/\d/g, (d) => "૦૧૨૩૪૫૬૭૮૯"[Number(d)])];
  const client = getTursoClient();
  const r = await client.execute({
    sql: `SELECT id, granth_key, page_number, content FROM ocr_pages
          WHERE granth_key IN (${keys.map(() => "?").join(",")}) AND (${digits.map(() => "content LIKE ?").join(" OR ")})`,
    args: [...keys, ...digits.map((d) => `%${d}%`)],
  });
  const pages = r.rows.map((row) => ({ id: Number(row.id), granthKey: String(row.granth_key), page: Number(row.page_number), text: String(row.content ?? "") }));
  const verse = pages.filter((p) => p.text.split("\n").some((line) => /[ऀ-ॿ]{3}/.test(line) && gathaMarks(line).some((k) => k.style === "close" && k.from <= first && first <= k.to)));
  if (!verse.length) return null;
  // the chapter the box names, beyond the book's own name
  const chapter = granthHints(granth);
  const scored = verse.map((p) => {
    const quoted = quotedShare(words, p.text);
    const skel = skeleton(p.text.slice(0, 400));
    const named = chapter.some((h) => h.length >= 4 && skel.includes(h)) ? 0.3 : 0;
    return { ...p, score: quoted + named };
  });
  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];
  // one candidate and no evidence is still the only such verse; several need something to choose by
  if (scored.length > 1 && best.score === scored[1].score) return null;
  return { id: best.id, granthKey: best.granthKey, page: best.page, score: 0 };
}

/** Pages of the named granths that print any of the passage's words, best first. */
async function hintedCandidates(words: string[], keys: string[]): Promise<Candidate[]> {
  if (!keys.length) return [];
  const client = getTursoClient();
  const r = await client.execute({
    sql: `SELECT s.page_id AS id, s.granth_key AS granth_key, s.page_number AS page
          FROM ocr_pages_folded_fts f JOIN ocr_pages_folded_state s ON s.page_id = f.rowid
          WHERE ocr_pages_folded_fts MATCH ? AND s.granth_key IN (${keys.map(() => "?").join(",")}) LIMIT 400`,
    args: [words.slice(0, 40).map(ftsWord).join(" OR "), ...keys],
  });
  return r.rows.map((row) => ({ id: Number(row.id), granthKey: String(row.granth_key), page: Number(row.page) }));
}

async function candidates(words: string[], excluded: Set<string>): Promise<Candidate[]> {
  const client = getTursoClient();
  // all of the 6 longest words, then 4 of them, then any 3
  const tries = [words.slice(0, 6), words.slice(0, 4), words.slice(0, 3)].filter((t, i, all) => t.length >= 2 && (i === 0 || t.length < all[i - 1].length));
  for (const set of tries) {
    const r = await client.execute({
      sql: `SELECT s.page_id AS id, s.granth_key AS granth_key, s.page_number AS page
            FROM ocr_pages_folded_fts f JOIN ocr_pages_folded_state s ON s.page_id = f.rowid
            WHERE ocr_pages_folded_fts MATCH ? LIMIT 60`,
      args: [set.map(ftsWord).join(" AND ")],
    });
    const rows = r.rows.map((row) => ({ id: Number(row.id), granthKey: String(row.granth_key), page: Number(row.page) })).filter((c) => !excluded.has(c.granthKey));
    if (rows.length) return rows;
  }
  // last: pages with any two of the eight longest words, scored below
  const loose = words.slice(0, 8);
  const pairs: string[] = [];
  for (let i = 0; i < loose.length; i += 1) for (let j = i + 1; j < loose.length; j += 1) pairs.push(`(${ftsWord(loose[i])} AND ${ftsWord(loose[j])})`);
  if (pairs.length) {
    const r = await client.execute({
      sql: `SELECT s.page_id AS id, s.granth_key AS granth_key, s.page_number AS page
            FROM ocr_pages_folded_fts f JOIN ocr_pages_folded_state s ON s.page_id = f.rowid
            WHERE ocr_pages_folded_fts MATCH ? LIMIT 150`,
      args: [pairs.join(" OR ")],
    });
    return r.rows.map((row) => ({ id: Number(row.id), granthKey: String(row.granth_key), page: Number(row.page) })).filter((c) => !excluded.has(c.granthKey));
  }
  return [];
}

/**
 * Where the passage is printed in the library, best first (at most `limit`
 * granths). `granth`, the box's Granth column, puts the books it names first.
 */
export async function locatePassage(passage: string, limit = 3, granth = ""): Promise<LocatedPassage[]> {
  const words = passageWords(passage);
  if (words.length < 3) return [];
  const catalog = await getGranthCatalog();
  const excluded = new Set(catalog.excludedKeys);
  const hinted = new Set(granth ? hintedGranths(catalog, granth).filter((k) => !excluded.has(k)) : []);
  const [general, named] = await Promise.all([candidates(words, excluded), hintedCandidates(words, [...hinted])]);
  const found = [...new Map([...general, ...named].map((c) => [c.id, c])).values()];
  if (!found.length) return [];

  const client = getTursoClient();
  const contents = await client.execute({
    sql: `SELECT id, content FROM ocr_pages WHERE id IN (${found.map(() => "?").join(",")})`,
    args: found.map((c) => c.id),
  });
  const textById = new Map(contents.rows.map((row) => [Number(row.id), String(row.content ?? "")]));
  const scored = found
    .map((c) => {
      const page = new Set(foldSanskrit(textById.get(c.id) ?? "").match(WORD_TOKEN_PATTERN) ?? []);
      const hits = words.filter((w) => page.has(w)).length;
      return { ...c, score: hits / words.length };
    })
    .sort((a, b) => b.score - a.score);

  // one place per granth: its best page
  const best = new Map<string, (typeof scored)[number]>();
  for (const c of scored) if (!best.has(c.granthKey) && isSearchable(catalog, catalog.byGranthKey.get(c.granthKey))) best.set(c.granthKey, c);
  // a short passage (a kosh line, a phrase) shares a few words with many pages: it needs a closer match
  const needed = words.length < 8 ? 0.75 : 0.35;
  // in a book the box names, a few words are enough: its Gujarati edition may print only the mula, or the tika in pieces
  const namedWords = Math.min(6, Math.max(3, Math.round(words.length * 0.05)));
  const ranked = [...best.values()]
    .filter((c) => (hinted.has(c.granthKey) ? Math.round(c.score * words.length) >= namedWords || c.score >= needed : c.score >= needed))
    .sort((a, b) => Number(hinted.has(b.granthKey)) - Number(hinted.has(a.granthKey)) || b.score - a.score);
  // nothing in the named books by words: by the gatha number the box gives
  if (hinted.size && !ranked.some((c) => hinted.has(c.granthKey))) {
    const byNumber = await byGathaNumber(granth, words, [...hinted]);
    if (byNumber) ranked.unshift(byNumber);
  }
  const top = ranked.slice(0, limit);

  const out: LocatedPassage[] = [];
  for (const c of top) {
    const described = describeGranth(catalog, c.granthKey);
    const around = await client.execute({
      sql: `SELECT p.page_number AS page, p.content AS content, pp.printed_page AS printed
            FROM ocr_pages p LEFT JOIN ocr_printed_pages pp ON pp.granth_key = p.granth_key AND pp.page_number = p.page_number
            WHERE p.granth_key = ? AND p.page_number BETWEEN ? AND ? ORDER BY p.page_number`,
      args: [c.granthKey, c.page - NEARBY_PAGES, c.page + NEARBY_PAGES],
    });
    const pages = around.rows.map((row) => {
      const text = String(row.content ?? "");
      const gujarati = isGujaratiPage(text);
      return { page: Number(row.page), printedPage: row.printed ? String(row.printed) : null, text, gujarati, ...(gujarati ? { quotes: Math.round(quotedShare(words, text) * 100) / 100 } : {}) };
    });
    out.push({
      granthKey: c.granthKey,
      granthName: described?.displayName ?? c.granthKey,
      pdfUrl: described?.pdfUrl ?? "",
      page: c.page,
      printedPage: pages.find((p) => p.page === c.page)?.printedPage ?? null,
      score: Math.round(c.score * 100) / 100,
      named: hinted.has(c.granthKey),
      bookNumber: described?.entry.book_number || String(bookNumbers(described?.entry.source_rel_path ?? "")[0] ?? ""),
      pages,
    });
  }
  return out;
}
