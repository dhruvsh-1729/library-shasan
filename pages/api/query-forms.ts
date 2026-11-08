import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { hasRomanLetters } from "@/lib/roman-sanskrit";
import { rankedReadings } from "@/lib/roman-forms";

// Which Devanagari spellings a romanised query stands for, and how many pages
// hold each one, so the search page can show "हिंसा · 3,851 pages" and let the
// reader pick instead of silently searching one guess.

export type QueryForm = { form: string; pages: number };
export type QueryFormsResponse = {
  query: string;
  words: Array<{ word: string; forms: QueryForm[] }>;
};

const MAX_WORDS = 4;
const FORMS_PER_WORD = 6;

async function formsFor(words: string[]) {
  const ranked = await rankedReadings(words);
  return words.map((word, index) => {
    const found = ranked[index].filter((reading) => reading.pages > 0);
    // Nothing in the library: still offer the likeliest reading, marked 0 pages.
    const list = found.length ? found : [...ranked[index]].sort((x, y) => x.cost - y.cost).slice(0, 1);
    return { word, forms: list.slice(0, FORMS_PER_WORD).map((reading) => ({ form: reading.form, pages: reading.pages })) };
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
