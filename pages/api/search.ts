import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import {
  buildOCRSearchExcerptForQueries,
  findOCRSearchMatchesForQueries,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
  type OCRSearchMode,
} from "@/lib/ocr-search";
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
  catalog_source?: "ocr" | "library";
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

function normalizeDigits(value: string) {
  return Array.from(String(value || ""))
    .map((char) => {
      const code = char.codePointAt(0) || 0;
      if (code >= 0x0966 && code <= 0x096f) return String(code - 0x0966);
      if (code >= 0x0ae6 && code <= 0x0aef) return String(code - 0x0ae6);
      return char;
    })
    .join("");
}

function normalizeSearchText(value: string) {
  return normalizeDigits(value)
    .toLowerCase()
    .replace(/[_\-.]+/g, " ")
    .replace(/[^\p{L}\p{N}\p{M}]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizedFileStem(value: string | null | undefined) {
  return normalizeSearchText(stripExtension(baseName(String(value || ""))));
}

function partNumberFromText(value: string) {
  const match = normalizeSearchText(value).match(/(?:^|\s)(?:part|bhag|भाग|ભાગ)\s*0*(\d+)(?:\s|$)/u);
  if (!match) return null;
  const part = Number(match[1]);
  return Number.isFinite(part) && part > 0 ? part : null;
}

function legacySearchTokens(row: TursoSearchRow) {
  const genericTokens = new Set([
    "part",
    "bhag",
    "prakaran",
    "prakarana",
    "ocr",
    "पर्व",
    "पर्वम्",
    "सर्ग",
    "भाग",
    "ભાગ",
  ]);
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
  if (/आचारांग|आचाराङ्ग/.test(normalized)) {
    tokens.add("acharang");
    tokens.add("acharanga");
  }
  if (normalized.includes("सूत्र")) {
    tokens.add("sutra");
  }
  if (normalized.includes("भावानुवाद")) {
    tokens.add("bhavanuvad");
  }
  if (/त्रिषष्टि|त्रिषष्टिशलाका/.test(normalized)) {
    tokens.add("trishashti");
    tokens.add("shalaka");
    tokens.add("purush");
    tokens.add("charitra");
  }

  return [...tokens];
}

const PARV_ORDINALS: Array<[RegExp, number]> = [
  [/प्रथम/u, 1],
  [/द्वितीय/u, 2],
  [/तृतीय/u, 3],
  [/चतुर्थ/u, 4],
  [/पञ्चम|पंचम/u, 5],
  [/षष्ठ/u, 6],
  [/सप्तम/u, 7],
  [/अष्टम/u, 8],
  [/नवम/u, 9],
  [/दशम/u, 10],
];

function extractNumbersAfterLabels(value: string, labels: string[]) {
  const tokens = normalizeSearchText(value).split(/\s+/).filter(Boolean);
  const numbers = new Set<number>();

  for (let index = 0; index < tokens.length; index += 1) {
    if (!labels.some((label) => tokens[index].startsWith(label))) continue;

    for (let offset = index + 1; offset < tokens.length && offset <= index + 6; offset += 1) {
      const token = tokens[offset];
      if (!/^\d+$/.test(token)) break;
      const number = Number(token);
      if (Number.isFinite(number) && number > 0) numbers.add(number);
    }
  }

  return numbers;
}

function extractParvNumbers(value: string) {
  const numbers = extractNumbersAfterLabels(value, ["पर्व", "parv", "parva"]);
  const normalized = normalizeSearchText(value);
  for (const [pattern, number] of PARV_ORDINALS) {
    if (pattern.test(normalized)) numbers.add(number);
  }
  return numbers;
}

function setOverlapSize(a: Set<number>, b: Set<number>) {
  let count = 0;
  for (const value of a) {
    if (b.has(value)) count += 1;
  }
  return count;
}

function isTrishashtiFamily(value: string) {
  const normalized = normalizeSearchText(value);
  return (
    normalized.includes("त्रिषष्टि") ||
    normalized.includes("trishashti") ||
    normalized.includes("shalakapurushcharitra")
  );
}

function ftsMatchQueryFor(query: string, matchMode: OCRSearchMode) {
  if (matchMode === "begins_with") return `${escapeFtsToken(query)}*`;
  if (matchMode === "ends_with") return buildOCRSuffixQuery(query);
  return escapeFtsPhrase(query);
}

function scorePdfFallback(row: TursoSearchRow, meta: PdfCatalogMeta) {
  const rowSourceStem = normalizedFileStem(row.source_rel_path);
  const metaFileStem = normalizedFileStem(meta.file_name);
  const metaRelStem = normalizedFileStem(meta.original_rel_path);
  if (rowSourceStem && (rowSourceStem === metaFileStem || rowSourceStem === metaRelStem)) return 100;

  const haystack = normalizeSearchText(`${meta.file_name || ""} ${meta.original_rel_path || ""} ${meta.custom_id || ""}`);
  const rowText = `${row.pdf_name} ${row.source_rel_path} ${row.granth_key}`;
  const metaText = `${meta.file_name || ""} ${meta.original_rel_path || ""} ${meta.custom_id || ""}`;
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

  const rowParvs = extractParvNumbers(rowText);
  const metaParvs = extractParvNumbers(metaText);
  const trishashtiFamilyMatch = isTrishashtiFamily(rowText) && isTrishashtiFamily(metaText);
  const rowPart = partNumberFromText(rowText);
  const metaPart = partNumberFromText(metaText);
  if (rowPart && metaPart) {
    if (rowPart === metaPart) score += 12;
    else if (!trishashtiFamilyMatch) return 0;
    else score -= 1;
  }

  if (rowParvs.size > 0 && metaParvs.size > 0) {
    const overlap = setOverlapSize(rowParvs, metaParvs);
    const coverage = overlap / rowParvs.size;
    if (trishashtiFamilyMatch && overlap === 0) return 0;
    if (trishashtiFamilyMatch && rowParvs.size >= 3 && coverage < 0.5) return 0;
    if (overlap === 0) score -= trishashtiFamilyMatch ? 2 : 5;
    else score += Math.round(overlap * 10 + coverage * 10);
  }

  if (haystack.includes("pratima") && haystack.includes("shatak") && tokens.includes("pratima")) score += 4;
  if (haystack.includes("mitra") && haystack.includes("dwatrinshika") && tokens.includes("mitra")) score += 6;
  if (trishashtiFamilyMatch) score += 4;

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
  const supabase = getSupabaseAdmin();
  const pageSize = 1000;
  const ocrRowsRaw: PdfCatalogMeta[] = [];
  const libraryRowsRaw: Array<{
    custom_id: string | null;
    pdf_rel_path: string | null;
    pdf_file_name: string | null;
    pdf_url: string | null;
  }> = [];

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await supabase
      .from("granth_ocr_files")
      .select("custom_id,original_rel_path,file_name,file_type,ufs_url")
      .not("ufs_url", "is", null)
      .range(from, from + pageSize - 1);
    if (error) throw new Error(error.message);
    ocrRowsRaw.push(...((data ?? []) as PdfCatalogMeta[]));
    if (!data || data.length < pageSize) break;
  }

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await supabase
      .from("granth_library_files")
      .select("custom_id,pdf_rel_path,pdf_file_name,pdf_url")
      .not("pdf_url", "is", null)
      .range(from, from + pageSize - 1);
    if (error) throw new Error(error.message);
    libraryRowsRaw.push(
      ...((data ?? []) as Array<{
        custom_id: string | null;
        pdf_rel_path: string | null;
        pdf_file_name: string | null;
        pdf_url: string | null;
      }>)
    );
    if (!data || data.length < pageSize) break;
  }

  const ocrRows = ocrRowsRaw.map((row) => ({
    ...row,
    catalog_source: "ocr" as const,
  }));
  const libraryRows = libraryRowsRaw.map((row) => ({
    custom_id: row.custom_id,
    original_rel_path: row.pdf_rel_path,
    file_name: row.pdf_file_name,
    file_type: "application/pdf",
    ufs_url: row.pdf_url,
    catalog_source: "library" as const,
  }));

  return [...ocrRows, ...libraryRows].filter((row) => looksLikePdfSource(row));
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
          snippet: buildOCRSearchExcerptForQueries(row.content, queries, matchMode, 520),
          score: row.rank,
          occurrence_count: findOCRSearchMatchesForQueries(row.content, queries, matchMode).length,
          csv_url: meta?.csv_url ?? null,
          open_pdf_url: viewerUrl,
          matched_queries: queries,
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
