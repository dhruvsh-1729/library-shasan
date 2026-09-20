import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import {
  buildOCRSearchOccurrences,
  buildOCRSearchExcerptForQueries,
  findOCRSearchMatchesForQueries,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
  type OCRSearchMode,
} from "@/lib/ocr-search";
import { buildOCRSuffixQuery, escapeFtsPhrase, escapeFtsToken } from "@/lib/ocr-search-index";
import {
  type DocumentMeta,
  fetchDocumentMetaByCustomIds,
  resolveGranthPdfTargets,
} from "@/lib/search-pdf-resolver";
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

function ftsMatchQueryFor(query: string, matchMode: OCRSearchMode) {
  if (matchMode === "begins_with") return `${escapeFtsToken(query)}*`;
  if (matchMode === "ends_with") return buildOCRSuffixQuery(query);
  return escapeFtsPhrase(query);
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
    const queries = normalizeOCRSearchQueries(q, parseQueryVariants(req.query.queryVariant ?? req.query.queryVariants))
      .filter((query) => Array.from(query).length >= 2)
      .slice(0, 8);

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

    if (matchMode === "contains" && queries.some((query) => Array.from(query).length < 3)) {
      setNoStore(res);
      return res.status(400).json({
        error: "Contains search requires at least 3 characters so it can use the trigram index.",
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
      const selectedDocs = await fetchDocumentMetaByCustomIds(selectedGranths);
      const selectedRelPaths = selectedDocs
        .map((row) => String(row.original_relative_path ?? "").trim())
        .filter(Boolean);

      if (selectedGranths.length > 0 && selectedRelPaths.length === 0) {
        return {
          results: [],
          total: 0,
          selected_granth_count: selectedGranths.length,
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
      const ftsTable =
        matchMode === "contains"
          ? "ocr_pages_trigram_fts"
          : matchMode === "ends_with"
            ? "ocr_pages_suffix_fts"
            : "ocr_pages_search_fts";
      const hitSql = queries
        .map(
          () => `SELECT p.id AS page_id
                FROM ${ftsTable}
                JOIN ocr_pages p ON p.id = ${ftsTable}.rowid
                JOIN ocr_granths g ON g.granth_key = p.granth_key
                WHERE ${ftsTable} MATCH ?${relFilterSql}`
        )
        .join(" UNION ALL ");
      const hitArgs = queries.flatMap((query) => [ftsMatchQueryFor(query, matchMode), ...selectedRelPaths]);

      const countResult = await client.execute({
        sql: `WITH hits AS (${hitSql})
              SELECT COUNT(DISTINCT page_id) AS total
              FROM hits`,
        args: hitArgs,
      });

      // The FTS tables are only a prefilter; the boundary rules for each match
      // mode live in findOCRSearchMatches. Counting straight off the index
      // therefore reports pages that hold no real match for the chosen mode.
      // Both totals are taken from the verified scan instead, so the numbers on
      // screen are the same ones the result tiles are built from.
      // Capped so a broad query cannot pull the whole corpus into memory.
      const OCCURRENCE_SCAN_CAP = 4000;
      const occurrenceResult = await client.execute({
        sql: `WITH hits AS (${hitSql}),
              unique_hits AS (SELECT page_id FROM hits GROUP BY page_id)
              SELECT p.content
              FROM unique_hits
              JOIN ocr_pages p ON p.id = unique_hits.page_id
              LIMIT ?`,
        args: [...hitArgs, OCCURRENCE_SCAN_CAP + 1],
      });
      const countsExact = occurrenceResult.rows.length <= OCCURRENCE_SCAN_CAP;
      let totalOccurrences = 0;
      let verifiedPages = 0;
      for (const row of occurrenceResult.rows.slice(0, OCCURRENCE_SCAN_CAP)) {
        const hits = findOCRSearchMatchesForQueries(toStr(row.content), queries, matchMode).length;
        if (hits > 0) {
          verifiedPages += 1;
          totalOccurrences += hits;
        }
      }
      const occurrencesExact = countsExact;

      const listResult = await client.execute({
        sql: `WITH hits AS (${hitSql}),
              unique_hits AS (
                SELECT page_id, MIN(page_id) AS sort_id
                FROM hits
                GROUP BY page_id
              )
              SELECT
                p.granth_key,
                g.source_rel_path,
                g.granth_name AS pdf_name,
                p.page_number,
                p.content,
                0 AS rank
              FROM unique_hits
              JOIN ocr_pages p ON p.id = unique_hits.page_id
              JOIN ocr_granths g ON g.granth_key = p.granth_key
              ORDER BY unique_hits.sort_id ASC
              LIMIT ? OFFSET ?`,
        args: [...hitArgs, limit, offset],
      });

      const rows = listResult.rows.map((row) => ({
        granth_key: toStr(row.granth_key),
        source_rel_path: toStr(row.source_rel_path),
        pdf_name: toStr(row.pdf_name),
        page_number: toInt(row.page_number),
        content: toStr(row.content),
        rank: Number(row.rank ?? 0),
      })) as TursoSearchRow[];

      const selectedByRelPath = new Map<string, DocumentMeta>(
        selectedDocs
          .filter((row) => row.original_relative_path)
          .map((row) => [String(row.original_relative_path), row])
      );
      const targets = await resolveGranthPdfTargets(
        rows.map((row) => ({
          granth_key: row.granth_key,
          source_rel_path: row.source_rel_path,
          pdf_name: row.pdf_name,
          page_number: row.page_number,
        })),
        { extraDocsByRelPath: selectedByRelPath }
      );

      const results = rows.map((row, index) => {
        const target = targets[index];
        const viewerUrl = target.pdf_url
          ? `/pdf-viewer?pdf=${encodeURIComponent(target.pdf_url)}&page=${encodeURIComponent(String(target.page_number))}`
          : "";
        return {
          custom_id: target.custom_id,
          pdf_name: target.pdf_name,
          pdf_url: target.pdf_url,
          page_number: target.page_number,
          source_page_number: row.page_number,
          source_rel_path: row.source_rel_path,
          snippet: buildOCRSearchExcerptForQueries(row.content, queries, matchMode, 520),
          occurrences: buildOCRSearchOccurrences(row.content, queries, matchMode, 320),
          score: row.rank,
          occurrence_count: findOCRSearchMatchesForQueries(row.content, queries, matchMode).length,
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
        total_occurrences_exact: occurrencesExact,
        occurrence_scan_cap: OCCURRENCE_SCAN_CAP,
        occurrence_scanned_pages: Math.min(occurrenceResult.rows.length, OCCURRENCE_SCAN_CAP),
        selected_granth_count: selectedGranths.length,
        page,
        per_page: limit,
        total_pages: Math.max(1, Math.ceil(total / limit)),
        total_is_exact: true,
        search_backend: "turso",
        search_table: ftsTable,
        match_mode: matchMode,
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
