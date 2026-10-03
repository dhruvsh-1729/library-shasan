import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import {
  buildOCRSearchOccurrences,
  buildOCRSearchExcerptForQueries,
  findOCRSearchMatchesForQueries,
  isTooShortForContains,
  MAX_SEARCH_QUERIES,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
  parseOCRSearchScripts,
} from "@/lib/ocr-search";
import { buildOCRPrefilter } from "@/lib/ocr-search-index";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { excludeDuplicatesSql, getGranthCatalog, relPathsForIds } from "@/lib/granth-catalog";
import { resolveGranthPdfTargets } from "@/lib/search-pdf-resolver";
import { getTursoClient } from "@/lib/turso";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";

type TursoSearchRow = {
  granth_key: string;
  source_rel_path: string;
  pdf_name: string;
  page_number: number;
  content: string;
  rank: number;
};

function parseLimit(raw: unknown) {
  const value = Number(raw ?? 20);
  if (!Number.isFinite(value) || value <= 0) return 20;
  return Math.min(Math.floor(value), 100);
}

function parsePage(raw: unknown) {
  const value = Number(raw ?? 1);
  if (!Number.isFinite(value) || value <= 0) return 1;
  return Math.floor(value);
}

function parseGranthIds(raw: string | string[] | undefined) {
  if (!raw) return [];
  const values = Array.isArray(raw) ? raw : String(raw).split(",");
  return values
    .map((v) => String(v).trim())
    .filter(Boolean);
}

function parseQueryVariants(raw: string | string[] | undefined) {
  const values = Array.isArray(raw) ? raw : raw ? [raw] : [];
  return values.flatMap((value) => {
    const text = String(value || "").trim();
    if (!text) return [];
    if (text.startsWith("[")) {
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) return parsed.map((item) => String(item || ""));
      } catch {
        return [text];
      }
    }
    return text.split(/\r?\n|\|/g);
  });
}

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toStr(value: unknown, fallback = "") {
  if (value == null) return fallback;
  return String(value);
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const q = String(req.query.q ?? "").trim();
    const limit = parseLimit(req.query.limit);
    const page = parsePage(req.query.page);
    const offset = Math.max(0, (page - 1) * limit);
    const selectedGranths = parseGranthIds(req.query.granths).slice(0, 250);
    let matchMode = parseOCRSearchMode(req.query.matchMode);
    const scripts = parseOCRSearchScripts(req.query.scripts);
    const queries = normalizeOCRSearchQueries(q, parseQueryVariants(req.query.queryVariant ?? req.query.queryVariants))
      .filter((query) => Array.from(query).length >= 2)
      .slice(0, MAX_SEARCH_QUERIES);

    if (queries.length === 0) {
      setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 });
      return res.status(200).json({
        results: [],
        total: 0,
        page,
        per_page: limit,
        total_pages: 1,
        total_is_exact: true,
        match_mode: matchMode,
        queries: [],
      });
    }

    // Anywhere-inside-a-word needs 3 letters after folding (its index is of
    // letter triples); a shorter word, like नय, is searched as the word and
    // all its forms instead, and the answer says so (match_mode).
    if (matchMode === "contains" && queries.some(isTooShortForContains)) matchMode = "sanskrit_forms";

    const cacheKey = buildCacheKey(req, "pdf-search-turso");
    // Where a search's time goes, as a Server-Timing header (DevTools shows it per request).
    const timings: string[] = [];
    let mark = performance.now();
    const lap = (name: string) => {
      const now = performance.now();
      timings.push(`${name};dur=${Math.round(now - mark)}`);
      mark = now;
    };
    const { value: payload, status } = await getCachedJson(cacheKey, 60, async () => {
      const catalog = await getGranthCatalog();
      lap("catalog");
      const { relPaths: selectedRelPaths, missing: missingGranths } = relPathsForIds(catalog, selectedGranths);

      if (selectedGranths.length > 0 && selectedRelPaths.length === 0) {
        return {
          results: [],
          total: 0,
          selected_granth_count: selectedGranths.length,
          missing_granths: missingGranths,
          page,
          per_page: limit,
          total_pages: 1,
          total_is_exact: true,
          search_backend: "turso",
          match_mode: matchMode,
          queries,
        };
      }

      const client = getTursoClient();
      const relFilterSql = selectedRelPaths.length
        ? ` AND g.source_rel_path IN (${selectedRelPaths.map(() => "?").join(",")})`
        : "";
      const prefilter = buildOCRPrefilter(queries, matchMode);
      const ftsTable = prefilter.table;
      const dup = excludeDuplicatesSql(catalog, "p.granth_key");
      const hitSql = `SELECT p.id AS page_id
                FROM ${ftsTable}
                JOIN ocr_pages p ON p.id = ${ftsTable}.rowid
                JOIN ocr_granths g ON g.granth_key = p.granth_key
                WHERE ${ftsTable} MATCH ?${relFilterSql}${dup.sql}`;
      const hitArgs = [prefilter.match, ...selectedRelPaths, ...dup.args];

      // The FTS tables are only a prefilter; the boundary rules for each match
      // mode (and the script filter) live in findOCRSearchMatches, so every
      // tile is a page whose text is checked.
      //
      // Reading text is what costs: checking the first 4,000 hit pages meant
      // pulling 11–18 MB of page text from Turso for 20 tiles, 7–35 s from the
      // server. So the hit page ids come first (cheap, and their number is the
      // index total), then text is read in batches, in page order, only until
      // the asked page of tiles is filled. A search with few hits is still
      // checked in full and its totals are exact; otherwise the totals carry a
      // "+" and the page fetches the exact ones from /api/search-count.
      const FULL_SCAN_LIMIT = 800;
      const TEXT_BATCH = 150;
      const PARALLEL_BATCHES = 3;
      const pageColumns = `p.id, p.granth_key, g.source_rel_path, g.granth_name AS pdf_name, p.page_number, p.content`;
      // Page ids, in page order. Across the whole library they come straight
      // from the index (its rowid is the page id), a few thousand at a time
      // from a cursor: ~60 ms, where grouping every hit with its granth took
      // up to 8 s for a common word. Duplicate copies of a granth are dropped
      // when the text is read. A scoped search is small, so it lists its hits
      // whole.
      const ID_CHUNK = 1500;
      const scoped = selectedRelPaths.length > 0;
      let indexTotal: number;
      const ids: number[] = [];
      let idsDone = false;
      if (scoped) {
        const rows = (
          await client.execute({
            sql: `WITH hits AS (${hitSql}) SELECT page_id FROM hits GROUP BY page_id ORDER BY page_id ASC`,
            args: hitArgs,
          })
        ).rows;
        for (const row of rows) ids.push(Number(row.page_id));
        indexTotal = ids.length;
        idsDone = true;
      } else {
        const counted = await client.execute({ sql: `SELECT count(*) AS n FROM ${ftsTable} WHERE ${ftsTable} MATCH ?`, args: [prefilter.match] });
        indexTotal = toInt(counted.rows[0]?.n);
      }
      const moreIds = async () => {
        if (idsDone) return;
        const after = ids.length ? ids[ids.length - 1] : 0;
        const rows = (
          await client.execute({
            sql: `SELECT rowid AS id FROM ${ftsTable} WHERE ${ftsTable} MATCH ? AND rowid > ? ORDER BY rowid LIMIT ?`,
            args: [prefilter.match, after, ID_CHUNK],
          })
        ).rows;
        for (const row of rows) ids.push(Number(row.id));
        if (rows.length < ID_CHUNK) idsDone = true;
      };
      lap("ids");
      const countsExact = indexTotal <= FULL_SCAN_LIMIT;
      const wanted = countsExact ? Infinity : offset + limit;

      const readBatch = async (batch: number[]) => {
        const result = await client.execute({
          sql: `SELECT ${pageColumns}
                FROM ocr_pages p
                JOIN ocr_granths g ON g.granth_key = p.granth_key
                WHERE p.id IN (${batch.map(() => "?").join(",")})${relFilterSql}${dup.sql}`,
          args: [...batch, ...selectedRelPaths, ...dup.args],
        });
        const byId = new Map(result.rows.map((row) => [Number(row.id), row]));
        return batch.map((id) => byId.get(id)).filter((row): row is NonNullable<typeof row> => Boolean(row));
      };

      let totalOccurrences = 0;
      // How often each written form was matched, so a count can be checked
      // (पर्षद् 2, परिषद् 17 ...). Keyed by folded form, shown as first seen.
      const formCounts = new Map<string, { form: string; count: number }>();
      const verifiedRows: Awaited<ReturnType<typeof readBatch>> = [];
      let scanned = 0;
      for (;;) {
        if (verifiedRows.length >= wanted) break;
        if (scanned + PARALLEL_BATCHES * TEXT_BATCH > ids.length && !idsDone) await moreIds();
        if (scanned >= ids.length) break;
        // A few batches at once, kept in page order.
        const group: number[][] = [];
        for (let i = 0; i < PARALLEL_BATCHES && scanned < ids.length; i += 1) {
          group.push(ids.slice(scanned, scanned + TEXT_BATCH));
          scanned += TEXT_BATCH;
        }
        for (const rowsOfBatch of await Promise.all(group.map(readBatch))) {
          for (const row of rowsOfBatch) {
            const matches = findOCRSearchMatchesForQueries(toStr(row.content), queries, matchMode, scripts);
            if (!matches.length) continue;
            verifiedRows.push(row);
            totalOccurrences += matches.length;
            for (const match of matches) {
              const key = foldSanskrit(match.text);
              const entry = formCounts.get(key);
              if (entry) entry.count += 1;
              else formCounts.set(key, { form: match.text.replace(/[\u200b-\u200d\ufeff]/g, ""), count: 1 });
            }
          }
        }
      }
      scanned = Math.min(scanned, ids.length);
      const verifiedPages = verifiedRows.length;
      const occurrencesExact = countsExact;
      const OCCURRENCE_SCAN_CAP = FULL_SCAN_LIMIT;
      const listRows = verifiedRows.slice(offset, offset + limit);
      lap("text");

      const rows = listRows.map((row) => ({
        granth_key: toStr(row.granth_key),
        source_rel_path: toStr(row.source_rel_path),
        pdf_name: toStr(row.pdf_name),
        page_number: toInt(row.page_number),
        content: toStr(row.content),
        rank: 0,
      })) as TursoSearchRow[];

      const targets = await resolveGranthPdfTargets(
        rows.map((row) => ({
          granth_key: row.granth_key,
          source_rel_path: row.source_rel_path,
          pdf_name: row.pdf_name,
          page_number: row.page_number,
        }))
      );

      lap("pdfs");
      const results = rows.map((row, index) => {
        const target = targets[index];
        const viewerUrl = target.pdf_url
          ? `/pdf-viewer?pdf=${encodeURIComponent(target.pdf_url)}&page=${encodeURIComponent(String(target.page_number))}`
          : "";
        return {
          granth_key: row.granth_key,
          custom_id: target.custom_id,
          pdf_name: target.pdf_name,
          pdf_url: target.pdf_url,
          page_number: target.page_number,
          source_page_number: row.page_number,
          source_rel_path: row.source_rel_path,
          snippet: buildOCRSearchExcerptForQueries(row.content, queries, matchMode, 520, scripts),
          occurrences: buildOCRSearchOccurrences(row.content, queries, matchMode, 320, scripts),
          score: row.rank,
          occurrence_count: findOCRSearchMatchesForQueries(row.content, queries, matchMode, scripts).length,
          // False only past the scan cap, for an index hit the text does not confirm.
          verified: findOCRSearchMatchesForQueries(row.content, queries, matchMode, scripts).length > 0,
          csv_url: target.csv_url,
          open_pdf_url: viewerUrl,
          matched_queries: queries,
        };
      });

      // Fall back to the raw index count only when the scan hit its cap.
      // Past the full scan the index total stands in (it can include pages with no real match) until the exact count arrives.
      const total = countsExact ? verifiedPages : Math.max(indexTotal, verifiedPages);
      return {
        results,
        total,
        total_pages_with_matches: total,
        total_occurrences: totalOccurrences,
        form_counts: [...formCounts.values()].sort((a, b) => b.count - a.count).slice(0, 60),
        total_occurrences_exact: occurrencesExact,
        occurrence_scan_cap: OCCURRENCE_SCAN_CAP,
        occurrence_scanned_pages: scanned,
        selected_granth_count: selectedGranths.length,
        missing_granths: missingGranths,
        page,
        per_page: limit,
        total_pages: Math.max(1, Math.ceil(total / limit)),
        // Past the scan cap the total is the index's count, which can include
        // pages with no real match.
        total_is_exact: countsExact,
        search_backend: "turso",
        search_table: ftsTable,
        match_mode: matchMode,
        scripts,
        queries,
      };
    });

    lap("build");
    res.setHeader("Server-Timing", timings.join(", "));
    setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
