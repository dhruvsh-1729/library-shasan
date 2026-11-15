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
  /** The page and its neighbours, in order. */
  pages: LocatedPage[];
};

export const NEARBY_PAGES = 4;

const MIN_WORD = 4;

/** The passage's distinct folded words of 4+ letters, longest first. */
export function passageWords(passage: string) {
  const words = (foldSanskrit(passage).match(WORD_TOKEN_PATTERN) ?? []).filter((w) => /[ऀ-ॿ]/.test(w) && [...w].length >= MIN_WORD);
  return [...new Set(words)].sort((a, b) => [...b].length - [...a].length);
}

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

/** Where the passage is printed in the library, best first (at most `limit` granths). */
export async function locatePassage(passage: string, limit = 3): Promise<LocatedPassage[]> {
  const words = passageWords(passage);
  if (words.length < 3) return [];
  const catalog = await getGranthCatalog();
  const excluded = new Set(catalog.excludedKeys);
  const found = await candidates(words, excluded);
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
  const top = [...best.values()].filter((c) => c.score >= needed).slice(0, limit);

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
      pages,
    });
  }
  return out;
}
