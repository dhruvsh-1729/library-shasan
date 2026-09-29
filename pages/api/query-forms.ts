import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { hasRomanLetters, romanWordReadings } from "@/lib/roman-sanskrit";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { getTursoClient } from "@/lib/turso";

// Which Devanagari spellings a romanised query stands for, and how many pages
// hold each one, so the search page can show "हिंसा · 3,851 pages" and let the
// reader pick instead of silently searching one guess.

export type QueryForm = { form: string; pages: number };
export type QueryFormsResponse = {
  query: string;
  words: Array<{ word: string; forms: QueryForm[] }>;
};

// fts5vocab over the folded word index: one row per folded term with the
// number of pages (doc) it is on. A virtual table, so it stores nothing.
const VOCAB_TABLE = "ocr_pages_folded_vocab";
let vocabReady: Promise<unknown> | null = null;
function ensureVocab() {
  vocabReady ??= getTursoClient()
    .execute(`CREATE VIRTUAL TABLE IF NOT EXISTS ${VOCAB_TABLE} USING fts5vocab(ocr_pages_folded_fts, 'row')`)
    .catch((error) => {
      vocabReady = null;
      throw error;
    });
  return vocabReady;
}

const MAX_WORDS = 4;
const READINGS_PER_WORD = 120;
const FORMS_PER_WORD = 6;

async function formsFor(words: string[]) {
  const readings = words.map((word) => romanWordReadings(word, READINGS_PER_WORD));
  // Readings that fold alike are one indexed term; keep the cheapest spelling.
  const byFolded = new Map<string, { form: string; cost: number }>();
  for (const list of readings) {
    for (const reading of list) {
      const key = foldSanskrit(reading.text);
      const current = byFolded.get(key);
      if (key && (!current || reading.cost < current.cost)) byFolded.set(key, { form: reading.text, cost: reading.cost });
    }
  }
  const keys = [...byFolded.keys()];
  const pages = new Map<string, number>();
  if (keys.length) {
    await ensureVocab();
    const result = await getTursoClient().execute({
      sql: `SELECT term, doc FROM ${VOCAB_TABLE} WHERE term IN (${keys.map(() => "?").join(",")})`,
      args: keys,
    });
    for (const row of result.rows) pages.set(String(row.term), Number(row.doc ?? 0));
  }

  return words.map((word, index) => {
    const seen = new Set<string>();
    const forms: QueryForm[] = [];
    for (const reading of readings[index]) {
      const key = foldSanskrit(reading.text);
      if (seen.has(key)) continue;
      seen.add(key);
      forms.push({ form: byFolded.get(key)?.form ?? reading.text, pages: pages.get(key) ?? 0 });
    }
    const found = forms.filter((form) => form.pages > 0).sort((a, b) => b.pages - a.pages);
    // Nothing in the library: still offer the plain reading, marked 0 pages.
    return { word, forms: (found.length ? found : forms.slice(0, 1)).slice(0, FORMS_PER_WORD) };
  });
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const query = String(req.query.q ?? "").normalize("NFC").replace(/\s+/g, " ").trim().slice(0, 120);
  if (!query || !hasRomanLetters(query)) {
    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 3600 });
    return res.status(200).json({ query, words: [] } satisfies QueryFormsResponse);
  }
  try {
    const words = query.split(" ").filter(Boolean).slice(0, MAX_WORDS);
    const { value, status } = await getCachedJson(buildCacheKey(req, "query-forms"), 600, async () => ({
      query,
      words: await formsFor(words),
    }));
    setPublicCacheHeaders(res, { maxAgeSeconds: 600, staleWhileRevalidateSeconds: 3600 }, status);
    return res.status(200).json(value);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
