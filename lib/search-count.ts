// Exact totals for a search: every page the index offers is checked against
// its text with the same rules as the result tiles.
//
// /api/search checks only the first 4,000 hit pages so results come back at
// once; past that its totals carry a "+". This counts all of them (a broad
// word like स्त्री has ~15,000 index hits, ~10,500 real pages, ~24 s), so the
// page can ask for it in the background and replace the "+" with the number.
// Results are kept for an hour; concurrent requests for one search share one
// count.

import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { excludeDuplicatesSql, getGranthCatalog } from "@/lib/granth-catalog";
import { type OCRSearchMode, type OCRSearchScripts, findOCRSearchMatchesForQueries } from "@/lib/ocr-search";
import { buildOCRPrefilter } from "@/lib/ocr-search-index";
import { getTursoClient } from "@/lib/turso";

export type SearchCount = {
  pages: number;
  occurrences: number;
  formCounts: Array<{ form: string; count: number }>;
  indexPages: number;
  ms: number;
};

const BATCH = 500;
const PARALLEL = 6;
const TTL_MS = 60 * 60 * 1000;
const cache = new Map<string, { at: number; value: Promise<SearchCount> }>();

export function countSearch(opts: { queries: string[]; matchMode: OCRSearchMode; scripts: OCRSearchScripts; relPaths: string[] }) {
  const key = JSON.stringify([opts.queries, opts.matchMode, opts.scripts, [...opts.relPaths].sort()]);
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < TTL_MS) return hit.value;
  const value = runCount(opts).catch((error) => {
    cache.delete(key);
    throw error;
  });
  cache.set(key, { at: Date.now(), value });
  if (cache.size > 300) cache.delete(cache.keys().next().value!);
  return value;
}

async function runCount({ queries, matchMode, scripts, relPaths }: { queries: string[]; matchMode: OCRSearchMode; scripts: OCRSearchScripts; relPaths: string[] }): Promise<SearchCount> {
  const started = Date.now();
  const client = getTursoClient();
  const catalog = await getGranthCatalog();
  const prefilter = buildOCRPrefilter(queries, matchMode);
  const table = prefilter.table;
  const dup = excludeDuplicatesSql(catalog, "p.granth_key");
  const relSql = relPaths.length ? ` AND g.source_rel_path IN (${relPaths.map(() => "?").join(",")})` : "";
  const ids = (
    await client.execute({
      sql: `SELECT DISTINCT p.id AS id
            FROM ${table}
            JOIN ocr_pages p ON p.id = ${table}.rowid
            JOIN ocr_granths g ON g.granth_key = p.granth_key
            WHERE ${table} MATCH ?${relSql}${dup.sql}
            ORDER BY p.id`,
      args: [prefilter.match, ...relPaths, ...dup.args],
    })
  ).rows.map((row) => Number(row.id));

  const chunks: number[][] = [];
  for (let i = 0; i < ids.length; i += BATCH) chunks.push(ids.slice(i, i + BATCH));
  let pages = 0;
  let occurrences = 0;
  const forms = new Map<string, { form: string; count: number }>();
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(PARALLEL, chunks.length) }, async () => {
      while (next < chunks.length) {
        const chunk = chunks[next++];
        const rows = (
          await client.execute({ sql: `SELECT content FROM ocr_pages WHERE id IN (${chunk.map(() => "?").join(",")})`, args: chunk })
        ).rows;
        for (const row of rows) {
          const matches = findOCRSearchMatchesForQueries(String(row.content ?? ""), queries, matchMode, scripts);
          if (!matches.length) continue;
          pages += 1;
          occurrences += matches.length;
          for (const match of matches) {
            const key = foldSanskrit(match.text);
            const entry = forms.get(key);
            if (entry) entry.count += 1;
            else forms.set(key, { form: match.text.replace(/[​-‍﻿]/g, ""), count: 1 });
          }
        }
      }
    })
  );

  return {
    pages,
    occurrences,
    formCounts: [...forms.values()].sort((a, b) => b.count - a.count).slice(0, 60),
    indexPages: ids.length,
    ms: Date.now() - started,
  };
}
