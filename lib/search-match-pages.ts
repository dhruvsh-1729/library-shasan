import { buildOCRSearchExcerpt, findOCRSearchMatches, type OCRSearchMode } from "@/lib/ocr-search";
import { buildOCRSuffixQuery, escapeFtsPhrase, escapeFtsToken } from "@/lib/ocr-search-index";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import { getTursoClient } from "@/lib/turso";

type DocumentRow = {
  custom_id: string | null;
  original_relative_path: string | null;
  pdf_name: string | null;
  pdf_url: string | null;
  csv_url: string | null;
};

type SourceRow = {
  custom_id: string | null;
  original_rel_path: string | null;
  file_name: string | null;
  file_type: string | null;
  ufs_url: string | null;
  cover_image_url: string | null;
};

export type SearchPdfSource = {
  customId: string;
  sourceRelPath: string;
  pdfName: string;
  pdfUrl: string;
  coverImageUrl: string | null;
};

export type SearchMatchPage = {
  page_number: number;
  occurrence_count: number;
  snippet: string;
};

export const MAX_MATCH_PAGE_PREVIEW = 2000;
export const MAX_MATCH_PAGE_DOWNLOAD = 900;

export class SearchMatchError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

function toInt(value: unknown, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.floor(parsed);
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

function looksLikePdf(row: Pick<SourceRow, "file_name" | "file_type" | "ufs_url"> | null | undefined) {
  if (!row) return false;
  const fileName = String(row.file_name || "").trim().toLowerCase();
  const fileType = String(row.file_type || "").trim().toLowerCase();
  const url = String(row.ufs_url || "").trim().toLowerCase();
  return fileName.endsWith(".pdf") || fileType.includes("pdf") || /\.pdf(?:[?#]|$)/i.test(url);
}

function firstRow<T>(rows: T[] | null | undefined) {
  return Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
}

async function fetchDocumentByCustomId(customId: string) {
  const { data, error } = await getSupabaseAdmin()
    .from("documents")
    .select("custom_id,original_relative_path,pdf_name,pdf_url,csv_url")
    .eq("custom_id", customId)
    .limit(1);

  if (error) throw new SearchMatchError(500, error.message);
  return firstRow((data ?? []) as DocumentRow[]);
}

async function fetchSourceByCustomId(customId: string) {
  const { data, error } = await getSupabaseAdmin()
    .from("granth_ocr_files")
    .select("custom_id,original_rel_path,file_name,file_type,ufs_url,cover_image_url")
    .eq("custom_id", customId)
    .limit(1);

  if (error) throw new SearchMatchError(500, error.message);
  return firstRow((data ?? []) as SourceRow[]);
}

async function fetchSourceByRelPath(relPath: string) {
  if (!relPath) return null;

  const { data, error } = await getSupabaseAdmin()
    .from("granth_ocr_files")
    .select("custom_id,original_rel_path,file_name,file_type,ufs_url,cover_image_url")
    .eq("original_rel_path", relPath)
    .limit(1);

  if (error) throw new SearchMatchError(500, error.message);
  return firstRow((data ?? []) as SourceRow[]);
}

export async function resolveSearchPdfSource(customId: string): Promise<SearchPdfSource> {
  const normalizedCustomId = String(customId || "").trim();
  if (!normalizedCustomId) throw new SearchMatchError(400, "Missing granth identifier.");

  const [doc, sourceByCustomId] = await Promise.all([
    fetchDocumentByCustomId(normalizedCustomId),
    fetchSourceByCustomId(normalizedCustomId),
  ]);

  const sourceRelPath = String(doc?.original_relative_path || sourceByCustomId?.original_rel_path || "").trim();
  const sourceByRelPath =
    sourceRelPath && sourceByCustomId?.original_rel_path !== sourceRelPath
      ? await fetchSourceByRelPath(sourceRelPath)
      : null;
  const pdfSource = looksLikePdf(sourceByCustomId)
    ? sourceByCustomId
    : looksLikePdf(sourceByRelPath)
      ? sourceByRelPath
      : null;
  const source = pdfSource ?? sourceByCustomId ?? sourceByRelPath;
  const sourcePdfUrl = normalizeHttpUrl(pdfSource?.ufs_url);
  const pdfUrl = normalizeHttpUrl(doc?.pdf_url) || sourcePdfUrl;
  const pdfName = String(doc?.pdf_name || source?.file_name || normalizedCustomId || "granth.pdf").trim();

  if (!sourceRelPath) {
    throw new SearchMatchError(404, "This result is not linked to searchable granth page metadata.");
  }
  if (!pdfUrl) {
    throw new SearchMatchError(404, "This result is not linked to an uploaded PDF.");
  }

  return {
    customId: normalizedCustomId,
    sourceRelPath,
    pdfName,
    pdfUrl,
    coverImageUrl: source?.cover_image_url ?? null,
  };
}

export function validateSearchDownloadQuery(query: string, matchMode: OCRSearchMode) {
  const q = String(query || "").trim();
  if (q.length < 2) throw new SearchMatchError(400, "Enter at least 2 characters before building a PDF.");
  if (matchMode === "contains" && Array.from(q).length < 3) {
    throw new SearchMatchError(400, "Contains search requires at least 3 characters.");
  }
  return q;
}

function ftsConfig(query: string, matchMode: OCRSearchMode) {
  if (matchMode === "contains") {
    return { table: "ocr_pages_trigram_fts", matchQuery: escapeFtsPhrase(query) };
  }
  if (matchMode === "ends_with") {
    return { table: "ocr_pages_suffix_fts", matchQuery: buildOCRSuffixQuery(query) };
  }
  return {
    table: "ocr_pages_search_fts",
    matchQuery: matchMode === "begins_with" ? `${escapeFtsToken(query)}*` : escapeFtsPhrase(query),
  };
}

export async function loadSearchMatchPages(
  sourceRelPath: string,
  query: string,
  matchMode: OCRSearchMode,
  limit = MAX_MATCH_PAGE_PREVIEW
) {
  const q = validateSearchDownloadQuery(query, matchMode);
  const boundedLimit = Math.max(1, Math.min(Math.floor(limit), MAX_MATCH_PAGE_PREVIEW));
  const { table, matchQuery } = ftsConfig(q, matchMode);
  const client = getTursoClient();

  const result = await client.execute({
    sql: `SELECT
            p.page_number,
            p.content
          FROM ${table}
          JOIN ocr_pages p ON p.id = ${table}.rowid
          JOIN ocr_granths g ON g.granth_key = p.granth_key
          WHERE ${table} MATCH ? AND g.source_rel_path = ?
          ORDER BY p.page_number ASC
          LIMIT ?`,
    args: [matchQuery, sourceRelPath, boundedLimit + 1],
  });

  const byPage = new Map<number, SearchMatchPage>();
  let rowCount = 0;

  for (const row of result.rows) {
    rowCount += 1;
    if (rowCount > boundedLimit) break;

    const pageNumber = toInt(row.page_number);
    const content = String(row.content ?? "");
    const matches = findOCRSearchMatches(content, q, matchMode);
    if (pageNumber <= 0 || matches.length === 0) continue;

    const existing = byPage.get(pageNumber);
    if (existing) {
      existing.occurrence_count += matches.length;
      continue;
    }

    byPage.set(pageNumber, {
      page_number: pageNumber,
      occurrence_count: matches.length,
      snippet: buildOCRSearchExcerpt(content, q, matchMode, 260),
    });
  }

  return {
    pages: [...byPage.values()].sort((a, b) => a.page_number - b.page_number),
    truncated: result.rows.length > boundedLimit,
  };
}
