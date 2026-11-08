import { romanWordReadings } from "@/lib/roman-sanskrit";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { getTursoClient } from "@/lib/turso";

// The Devanagari spellings a romanised word may stand for, each with the
// number of library pages it is on, ranked by both: a reading that needs fewer
// guesses wins, but a word the granths use thousands of times beats a rare one
// that is only a cheaper guess (akash → आकाश, not आकष). Shared by the search
// page's spelling choices and the vyutpatti page.

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

/** Pages each folded term is on (0 when the library never has it). */
export async function foldedTermPages(keys: string[]) {
  const pages = new Map<string, number>();
  if (!keys.length) return pages;
  await ensureVocab();
  for (let i = 0; i < keys.length; i += 400) {
    const chunk = keys.slice(i, i + 400);
    const result = await getTursoClient().execute({
      sql: `SELECT term, doc FROM ${VOCAB_TABLE} WHERE term IN (${chunk.map(() => "?").join(",")})`,
      args: chunk,
    });
    for (const row of result.rows) pages.set(String(row.term), Number(row.doc ?? 0));
  }
  return pages;
}

export type RankedReading = { form: string; folded: string; cost: number; pages: number; score: number };

/**
 * How much one extra guess weighs against ten times as many pages. Tuned on
 * the 448 shastriya typings of tests/fixtures/shastriya-words.ts against the
 * live library (8 Nov 2026): 0.4 puts the meant word first for 436, against
 * 432 by pages alone and 429 at 1.0.
 */
const COST_WEIGHT = 0.4;

export function readingScore(cost: number, pages: number) {
  return Math.log10(pages + 1) - COST_WEIGHT * cost;
}

/** Readings that fold alike are one indexed term: the cheapest spelling of each, with its pages and score. */
export async function rankedReadings(words: string[], readingsPerWord = 600): Promise<RankedReading[][]> {
  const lists = words.map((word) => {
    const byFolded = new Map<string, { form: string; cost: number }>();
    for (const reading of romanWordReadings(word, readingsPerWord)) {
      const folded = foldSanskrit(reading.text);
      if (folded && !byFolded.has(folded)) byFolded.set(folded, { form: reading.text, cost: reading.cost });
    }
    return byFolded;
  });
  const pages = await foldedTermPages([...new Set(lists.flatMap((list) => [...list.keys()]))]);
  return lists.map((list) =>
    [...list]
      .map(([folded, { form, cost }]) => {
        const count = pages.get(folded) ?? 0;
        return { form, folded, cost, pages: count, score: readingScore(cost, count) };
      })
      .sort((a, b) => b.score - a.score)
  );
}
