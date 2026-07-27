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

type SourceMeta = {
  custom_id: string | null;
  original_rel_path: string | null;
  file_name: string | null;
  file_type: string | null;
  ufs_url: string | null;
};

type PdfCatalogMeta = {
  custom_id: string | null;
  original_rel_path: string | null;
  file_name: string | null;
  file_type: string | null;
  ufs_url: string | null;
};

type PdfFallback = {
  meta: PdfCatalogMeta;
  score: number;
  partNumber: number | null;
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

function normalizeHttpUrl(value: string | null | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    const url = new URL(raw);
    if (url.protocol !== "http:" && url.protocol !== "https:") return "";
    return url.toString();
  } catch {
    return "";
  }
}

function looksLikePdfSource(source: Pick<SourceMeta, "file_name" | "file_type" | "ufs_url"> | undefined) {
  if (!source) return "";
  const fileName = String(source.file_name || "").trim().toLowerCase();
  const fileType = String(source.file_type || "").trim().toLowerCase();
  const rawUrl = String(source.ufs_url || "").trim();
  const url = normalizeHttpUrl(rawUrl);
  if (!url) return "";
  if (fileName.endsWith(".pdf") || fileType.includes("pdf") || /\.pdf(?:[?#]|$)/i.test(rawUrl)) return url;
  return "";
}

function sourcePdfUrl(source: SourceMeta | undefined) {
  return looksLikePdfSource(source);
}

function baseName(value: string) {
  return value.split(/[\\/]/).pop() || value;
}

function stripExtension(value: string) {
  return value.replace(/\.[^.]+$/i, "");
}

function normalizeSearchText(value: string) {
  return String(value || "")
    .toLowerCase()
    .replace(/[_\-.]+/g, " ")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizedFileStem(value: string | null | undefined) {
  return normalizeSearchText(stripExtension(baseName(String(value || ""))));
}

function partNumberFromText(value: string) {
  const match = normalizeSearchText(value).match(/\bpart\s*0*(\d+)\b/);
  if (!match) return null;
  const part = Number(match[1]);
  return Number.isFinite(part) && part > 0 ? part : null;
}

function legacySearchTokens(row: TursoSearchRow) {
  const genericTokens = new Set(["part", "prakaran", "prakarana", "ocr"]);
  const sourceStem = stripExtension(baseName(row.source_rel_path));
  const withoutCodes = sourceStem
    .replace(/^\d+[_\s-]*/g, "")
    .replace(/\bB\d+\b/gi, "")
    .replace(/\bOCR\b/gi, "");
  const normalized = normalizeSearchText(`${row.pdf_name} ${withoutCodes}`);
  const tokens = new Set(
    normalized
      .split(/\s+/)
      .filter((token) => token.length >= 4 && !/^\d+$/.test(token) && !genericTokens.has(token))
  );

  if (normalized.includes("pratimashatak")) {
    tokens.add("pratima");
    tokens.add("shatak");
  }
  if (normalized.includes("dwatrishika")) {
    tokens.add("dwatrinshika");
  }
  if (normalized.includes("sammatitark")) {
    tokens.add("sammati");
    tokens.add("tark");
  }

  return [...tokens];
}

function scorePdfFallback(row: TursoSearchRow, meta: PdfCatalogMeta) {
  const rowSourceStem = normalizedFileStem(row.source_rel_path);
  const metaFileStem = normalizedFileStem(meta.file_name);
  const metaRelStem = normalizedFileStem(meta.original_rel_path);
  if (rowSourceStem && (rowSourceStem === metaFileStem || rowSourceStem === metaRelStem)) return 100;

  const haystack = normalizeSearchText(`${meta.file_name || ""} ${meta.original_rel_path || ""} ${meta.custom_id || ""}`);
  const tokens = legacySearchTokens(row);
  if (tokens.length === 0) return 0;

  let score = 0;
  let matchedTokenCount = 0;
  let longestMatchedToken = 0;
  for (const token of tokens) {
    if (haystack.includes(token)) {
      score += token.length >= 7 ? 3 : 2;
      matchedTokenCount += 1;
      longestMatchedToken = Math.max(longestMatchedToken, token.length);
    }
  }

  if (score === 0) return 0;
  if (matchedTokenCount < 2 && longestMatchedToken < 8) return 0;

  const rowPart = partNumberFromText(`${row.pdf_name} ${row.source_rel_path}`);
  const metaPart = partNumberFromText(`${meta.file_name || ""} ${meta.original_rel_path || ""}`);
  if (rowPart && metaPart) score += rowPart === metaPart ? 5 : -2;

  if (haystack.includes("pratima") && haystack.includes("shatak") && tokens.includes("pratima")) score += 4;
  if (haystack.includes("mitra") && haystack.includes("dwatrinshika") && tokens.includes("mitra")) score += 6;

  return score;
}

function findPdfFallbacks(row: TursoSearchRow, catalog: PdfCatalogMeta[]) {
  return catalog
    .map((meta) => ({
      meta,
      score: scorePdfFallback(row, meta),
      partNumber: partNumberFromText(`${meta.file_name || ""} ${meta.original_rel_path || ""}`),
    }))
    .filter((candidate) => candidate.score >= 6)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return (a.partNumber ?? 9999) - (b.partNumber ?? 9999);
    })
    .slice(0, 12);
}

async function fetchPdfCatalog() {
  const { data, error } = await getSupabaseAdmin()
    .from("granth_ocr_files")
    .select("custom_id,original_rel_path,file_name,file_type,ufs_url")
    .not("ufs_url", "is", null)
    .limit(1500);

  if (error) throw new Error(error.message);
  return ((data ?? []) as PdfCatalogMeta[]).filter((row) => looksLikePdfSource(row));
}

async function fetchTursoPageCounts(relPaths: string[]) {
  const unique = Array.from(new Set(relPaths.map((path) => path.trim()).filter(Boolean)));
  if (unique.length === 0) return new Map<string, number>();

  const client = getTursoClient();
  const result = await client.execute({
    sql: `SELECT g.source_rel_path, COUNT(p.id) AS page_count
          FROM ocr_granths g
          JOIN ocr_pages p ON p.granth_key = g.granth_key
          WHERE g.source_rel_path IN (${unique.map(() => "?").join(",")})
          GROUP BY g.source_rel_path`,
    args: unique,
  });

  return new Map(result.rows.map((row) => [String(row.source_rel_path || ""), toInt(row.page_count)]));
}

function choosePdfFallback(row: TursoSearchRow, fallbacks: PdfFallback[], pageCounts: Map<string, number>) {
  if (fallbacks.length === 0) return null;
  const partCandidates = fallbacks
    .filter((candidate) => candidate.partNumber != null)
    .sort((a, b) => (a.partNumber ?? 9999) - (b.partNumber ?? 9999));

  if (partCandidates.length > 1) {
    let remainingPage = row.page_number;
    for (const candidate of partCandidates) {
      const relPath = String(candidate.meta.original_rel_path || "");
      const pageCount = pageCounts.get(relPath) || 0;
      if (pageCount > 0 && remainingPage > pageCount) {
        remainingPage -= pageCount;
        continue;
      }
      return { meta: candidate.meta, pageNumber: Math.max(1, remainingPage) };
    }
  }

  return { meta: fallbacks[0].meta, pageNumber: row.page_number };
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

async function fetchSourceMetaByRelPaths(relPaths: string[]) {
  if (relPaths.length === 0) return [] as SourceMeta[];
  const { data, error } = await getSupabaseAdmin()
    .from("granth_ocr_files")
    .select("custom_id,original_rel_path,file_name,file_type,ufs_url")
    .in("original_rel_path", relPaths);

  if (error) throw new Error(error.message);
  return (data ?? []) as SourceMeta[];
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
      const [resultDocs, resultSources] = await Promise.all([
        fetchDocumentMetaByRelPaths(resultRelPaths),
        fetchSourceMetaByRelPaths(resultRelPaths),
      ]);
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
      const sourceByRelPath = new Map(
        resultSources
          .filter((row) => row.original_rel_path)
          .map((row) => [String(row.original_rel_path), row])
      );
      const rowsNeedingFallback = rows.filter((row) => {
        const meta = resultByRelPath.get(row.source_rel_path) ?? selectedByRelPath.get(row.source_rel_path);
        const sourceMeta = sourceByRelPath.get(row.source_rel_path);
        return !normalizeHttpUrl(meta?.pdf_url) && !sourcePdfUrl(sourceMeta);
      });
      const pdfCatalog = rowsNeedingFallback.length > 0 ? await fetchPdfCatalog() : [];
      const fallbackCandidatesByRelPath = new Map<string, PdfFallback[]>();
      for (const row of rowsNeedingFallback) {
        fallbackCandidatesByRelPath.set(row.source_rel_path, findPdfFallbacks(row, pdfCatalog));
      }
      const fallbackRelPaths = Array.from(
        new Set(
          [...fallbackCandidatesByRelPath.values()]
            .flat()
            .map((candidate) => String(candidate.meta.original_rel_path || ""))
            .filter(Boolean)
        )
      );
      const fallbackPageCounts = await fetchTursoPageCounts(fallbackRelPaths);

      const results = rows.map((row) => {
        const meta = resultByRelPath.get(row.source_rel_path) ?? selectedByRelPath.get(row.source_rel_path);
        const sourceMeta = sourceByRelPath.get(row.source_rel_path);
        const fallback = choosePdfFallback(
          row,
          fallbackCandidatesByRelPath.get(row.source_rel_path) ?? [],
          fallbackPageCounts
        );
        const fallbackPdfUrl = normalizeHttpUrl(fallback?.meta.ufs_url);
        const pageNumber = fallback?.pageNumber ?? row.page_number;
        const customId = meta?.custom_id ?? sourceMeta?.custom_id ?? fallback?.meta.custom_id ?? row.granth_key;
        const pdfUrl = normalizeHttpUrl(meta?.pdf_url) || sourcePdfUrl(sourceMeta) || fallbackPdfUrl;
        const viewerUrl = pdfUrl
          ? `/pdf-viewer?pdf=${encodeURIComponent(pdfUrl)}&page=${encodeURIComponent(String(pageNumber))}`
          : "";
        return {
          custom_id: customId,
          pdf_name: meta?.pdf_name ?? sourceMeta?.file_name ?? fallback?.meta.file_name ?? row.pdf_name,
          pdf_url: pdfUrl,
          page_number: pageNumber,
          source_page_number: row.page_number,
          source_rel_path: row.source_rel_path,
          snippet: buildOCRSearchExcerpt(row.content, q, matchMode, 520),
          score: row.rank,
          occurrence_count: findOCRSearchMatches(row.content, q, matchMode).length,
          csv_url: meta?.csv_url ?? null,
          open_pdf_url: viewerUrl,
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
