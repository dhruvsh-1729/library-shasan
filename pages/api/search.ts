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
    const matchMode = parseOCRSearchMode(req.query.matchMode);
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

    if (matchMode === "contains" && queries.some(isTooShortForContains)) {
      setNoStore(res);
      return res.status(400).json({
        error: "Contains search needs at least 3 letters once conjuncts are folded (न्द counts as 2). Use a longer query or another match mode.",
        results: [],
        total: 0,
        page,
        per_page: limit,
        total_pages: 1,
        total_is_exact: true,
        match_mode: matchMode,
        queries,
      });
    }

    const cacheKey = buildCacheKey(req, "pdf-search-turso");
    const { value: payload, status } = await getCachedJson(cacheKey, 60, async () => {
      const catalog = await getGranthCatalog();
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
      // mode live in findOCRSearchMatches. Counting straight off the index
      // therefore reports pages that hold no real match for the chosen mode.
      // Both totals are taken from the verified scan instead, so the numbers on
      // screen are the same ones the result tiles are built from.
      // Capped so a broad query cannot pull the whole corpus into memory.
      const OCCURRENCE_SCAN_CAP = 4000;

      // The count and the scan read the same hit set and do not depend on each
      // other, so they go out together.
      const pageColumns = `p.granth_key, g.source_rel_path, g.granth_name AS pdf_name, p.page_number, p.content`;
      const [countResult, scanResult] = await Promise.all([
        client.execute({
          sql: `WITH hits AS (${hitSql})
                SELECT COUNT(DISTINCT page_id) AS total
                FROM hits`,
          args: hitArgs,
        }),
        client.execute({
          sql: `WITH hits AS (${hitSql}),
                unique_hits AS (SELECT page_id FROM hits GROUP BY page_id)
                SELECT ${pageColumns}
                FROM unique_hits
                JOIN ocr_pages p ON p.id = unique_hits.page_id
                JOIN ocr_granths g ON g.granth_key = p.granth_key
                ORDER BY unique_hits.page_id ASC
                LIMIT ?`,
          args: [...hitArgs, OCCURRENCE_SCAN_CAP + 1],
        }),
      ]);
      const countsExact = scanResult.rows.length <= OCCURRENCE_SCAN_CAP;
      let totalOccurrences = 0;
      // How often each written form was matched, so a count can be checked
      // (पर्षद् 2, परिषद् 17 ...). Keyed by folded form, shown as first seen.
      const formCounts = new Map<string, { form: string; count: number }>();
      const verifiedRows: typeof scanResult.rows = [];
      for (const row of scanResult.rows.slice(0, OCCURRENCE_SCAN_CAP)) {
        const matches = findOCRSearchMatchesForQueries(toStr(row.content), queries, matchMode, scripts);
        if (matches.length > 0) {
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
      const verifiedPages = verifiedRows.length;
      const occurrencesExact = countsExact;

      // Tiles are always verified pages, so none is a page whose only hit is
      // in a script the search left out (with Devanagari only, the default,
      // the raw index puts Gujarati pages first). Past the cap the scan goes
      // on in batches until the asked page of results is filled.
      const SCAN_BATCH = 1000;
      const MAX_EXTRA_BATCHES = 8;
      let scanned = Math.min(scanResult.rows.length, OCCURRENCE_SCAN_CAP);
      for (let batch = 0; !countsExact && verifiedRows.length < offset + limit && batch < MAX_EXTRA_BATCHES; batch += 1) {
        const more = (
          await client.execute({
            sql: `WITH hits AS (${hitSql}),
                  unique_hits AS (SELECT page_id FROM hits GROUP BY page_id)
                  SELECT ${pageColumns}
                  FROM unique_hits
                  JOIN ocr_pages p ON p.id = unique_hits.page_id
                  JOIN ocr_granths g ON g.granth_key = p.granth_key
                  ORDER BY unique_hits.page_id ASC
                  LIMIT ? OFFSET ?`,
            args: [...hitArgs, SCAN_BATCH, scanned],
          })
        ).rows;
        scanned += more.length;
        for (const row of more) {
          if (findOCRSearchMatchesForQueries(toStr(row.content), queries, matchMode, scripts).length > 0) verifiedRows.push(row);
        }
        if (more.length < SCAN_BATCH) break;
      }
      const listRows = verifiedRows.slice(offset, offset + limit);

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
      const total = countsExact ? verifiedPages : toInt(countResult.rows[0]?.total);
      return {
        results,
        total,
        total_pages_with_matches: total,
        total_occurrences: totalOccurrences,
        form_counts: [...formCounts.values()].sort((a, b) => b.count - a.count).slice(0, 60),
        total_occurrences_exact: occurrencesExact,
        occurrence_scan_cap: OCCURRENCE_SCAN_CAP,
        occurrence_scanned_pages: Math.min(scanResult.rows.length, OCCURRENCE_SCAN_CAP),
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

    setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
