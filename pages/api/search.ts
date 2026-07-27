import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { buildOCRSearchExcerpt, findOCRSearchMatches, parseOCRSearchMode } from "@/lib/ocr-search";
import { buildOCRSuffixQuery, escapeFtsPhrase, escapeFtsToken } from "@/lib/ocr-search-index";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import { getTursoClient } from "@/lib/turso";

type TursoSearchRow = {
  granth_key: string;
  source_rel_path: string;
  pdf_name: string;
  page_number: number;
  content: string;
  rank: number;
};

type DocumentMeta = {
  custom_id: string;
  original_relative_path: string | null;
  pdf_name: string | null;
  pdf_url: string | null;
  csv_url: string | null;
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

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toStr(value: unknown, fallback = "") {
  if (value == null) return fallback;
  return String(value);
}

async function fetchDocumentMetaByCustomIds(customIds: string[]) {
  if (customIds.length === 0) return [] as DocumentMeta[];
  const { data, error } = await getSupabaseAdmin()
    .from("documents")
    .select("custom_id,original_relative_path,pdf_name,pdf_url,csv_url")
    .in("custom_id", customIds);

  if (error) throw new Error(error.message);
  return (data ?? []) as DocumentMeta[];
}

async function fetchDocumentMetaByRelPaths(relPaths: string[]) {
  if (relPaths.length === 0) return [] as DocumentMeta[];
  const { data, error } = await getSupabaseAdmin()
    .from("documents")
    .select("custom_id,original_relative_path,pdf_name,pdf_url,csv_url")
    .in("original_relative_path", relPaths);

  if (error) throw new Error(error.message);
  return (data ?? []) as DocumentMeta[];
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
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

    if (!q || q.length < 2) {
      setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 });
      return res.status(200).json({
        results: [],
        total: 0,
        page,
        per_page: limit,
        total_pages: 1,
        total_is_exact: true,
        match_mode: matchMode,
      });
    }

    if (matchMode === "contains" && Array.from(q).length < 3) {
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
      const matchQuery =
        matchMode === "begins_with"
          ? `${escapeFtsToken(q)}*`
          : matchMode === "ends_with"
            ? buildOCRSuffixQuery(q)
            : escapeFtsPhrase(q);
      const baseArgs = [matchQuery, ...selectedRelPaths];

      const countResult = await client.execute({
        sql: `SELECT COUNT(*) AS total
              FROM ${ftsTable}
              JOIN ocr_pages p ON p.id = ${ftsTable}.rowid
              JOIN ocr_granths g ON g.granth_key = p.granth_key
              WHERE ${ftsTable} MATCH ?${relFilterSql}`,
        args: baseArgs,
      });

      const listResult = await client.execute({
        sql: `SELECT
                p.granth_key,
                g.source_rel_path,
                g.granth_name AS pdf_name,
                p.page_number,
                p.content,
                0 AS rank
              FROM ${ftsTable}
              JOIN ocr_pages p ON p.id = ${ftsTable}.rowid
              JOIN ocr_granths g ON g.granth_key = p.granth_key
              WHERE ${ftsTable} MATCH ?${relFilterSql}
              ORDER BY ${ftsTable}.rowid ASC
              LIMIT ? OFFSET ?`,
        args: [...baseArgs, limit, offset],
      });

      const rows = listResult.rows.map((row) => ({
        granth_key: toStr(row.granth_key),
        source_rel_path: toStr(row.source_rel_path),
        pdf_name: toStr(row.pdf_name),
        page_number: toInt(row.page_number),
        content: toStr(row.content),
        rank: Number(row.rank ?? 0),
      })) as TursoSearchRow[];

      const resultRelPaths = Array.from(new Set(rows.map((row) => row.source_rel_path).filter(Boolean)));
      const resultDocs = await fetchDocumentMetaByRelPaths(resultRelPaths);
      const selectedByRelPath = new Map(
        selectedDocs
          .filter((row) => row.original_relative_path)
          .map((row) => [String(row.original_relative_path), row])
      );
      const resultByRelPath = new Map(
        resultDocs
          .filter((row) => row.original_relative_path)
          .map((row) => [String(row.original_relative_path), row])
      );

      const results = rows.map((row) => {
        const meta = resultByRelPath.get(row.source_rel_path) ?? selectedByRelPath.get(row.source_rel_path);
        const customId = meta?.custom_id ?? row.granth_key;
        const pdfUrl = meta?.pdf_url ?? "";
          return {
            custom_id: customId,
            pdf_name: meta?.pdf_name ?? row.pdf_name,
            pdf_url: pdfUrl,
            page_number: row.page_number,
            snippet: buildOCRSearchExcerpt(row.content, q, matchMode, 520),
            score: row.rank,
            occurrence_count: findOCRSearchMatches(row.content, q, matchMode).length,
            csv_url: meta?.csv_url ?? null,
            open_pdf_url: pdfUrl ? `${pdfUrl}#page=${encodeURIComponent(String(row.page_number))}` : "",
          };
      });

      const total = toInt(countResult.rows[0]?.total);
      return {
        results,
        total,
        selected_granth_count: selectedGranths.length,
        page,
        per_page: limit,
        total_pages: Math.max(1, Math.ceil(total / limit)),
        total_is_exact: true,
        search_backend: "turso",
        search_table: ftsTable,
        match_mode: matchMode,
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
