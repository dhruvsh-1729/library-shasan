import { getSupabaseAdmin } from "@/lib/supabase-server";
import { describeGranth, getGranthCatalog, relPathsForIds } from "@/lib/granth-catalog";

export type GranthPdfRow = {
  granth_key: string;
  source_rel_path: string;
  pdf_name: string;
  page_number: number;
};

export type DocumentMeta = {
  custom_id: string;
  original_relative_path: string | null;
  pdf_name: string | null;
  pdf_url: string | null;
  csv_url: string | null;
};

export type SourceMeta = {
  custom_id: string | null;
  original_rel_path: string | null;
  file_name: string | null;
  file_type: string | null;
  ufs_url: string | null;
  cover_image_url?: string | null;
};

export type LibraryFileMeta = {
  custom_id: string | null;
  pdf_rel_path: string | null;
  pdf_file_name: string | null;
  pdf_url: string | null;
};

export type ResolvedGranthPdf = {
  granth_key: string;
  source_rel_path: string;
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  csv_url: string | null;
  page_number: number;
  source_page_number: number;
};

/**
 * Metadata tables change only when documents are scanned or uploaded, so a long
 * TTL keeps bulk exports and busy search sessions off Supabase egress. Targeted
 * single-granth lookups below stay uncached and therefore always fresh.
 */
const PDF_CATALOG_TTL_MS = 15 * 60 * 1000;
/**
 * Supabase filters travel in the request URL, and granth rel paths are long, so
 * `in.(...)` lookups break once a few dozen paths are batched together. Small
 * lookups stay targeted; larger ones read the (small) tables in full instead.
 */
const TARGETED_REL_PATH_LIMIT = 20;
const REL_PATH_CHUNK_SIZE = 10;

declare global {
  // eslint-disable-next-line no-var
  var __libraryDocumentCatalogCache: { expiresAt: number; value: DocumentMeta[] } | undefined;
  // eslint-disable-next-line no-var
  var __librarySourceCatalogCache: { expiresAt: number; value: SourceMeta[] } | undefined;
  // eslint-disable-next-line no-var
  var __libraryFileCatalogCache: { expiresAt: number; value: LibraryFileMeta[] } | undefined;
}


export function normalizeHttpUrl(value: string | null | undefined) {
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

export function looksLikePdfSource(source: Pick<SourceMeta, "file_name" | "file_type" | "ufs_url"> | undefined) {
  if (!source) return "";
  const fileName = String(source.file_name || "").trim().toLowerCase();
  const fileType = String(source.file_type || "").trim().toLowerCase();
  const rawUrl = String(source.ufs_url || "").trim();
  const url = normalizeHttpUrl(rawUrl);
  if (!url) return "";
  if (fileName.endsWith(".pdf") || fileType.includes("pdf") || /\.pdf(?:[?#]|$)/i.test(rawUrl)) return url;
  return "";
}

export function sourcePdfUrl(source: SourceMeta | undefined) {
  return looksLikePdfSource(source);
}

export async function fetchDocumentMetaByCustomIds(customIds: string[]) {
  const unique = Array.from(new Set(customIds.filter(Boolean)));
  if (unique.length === 0) return [] as DocumentMeta[];
  if (unique.length > TARGETED_REL_PATH_LIMIT) {
    const wanted = new Set(unique);
    return (await fetchDocumentCatalog()).filter((row) => wanted.has(String(row.custom_id || "")));
  }

  const rows: DocumentMeta[] = [];
  for (let index = 0; index < unique.length; index += REL_PATH_CHUNK_SIZE) {
    const { data, error } = await getSupabaseAdmin()
      .from("documents")
      .select("custom_id,original_relative_path,pdf_name,pdf_url,csv_url")
      .in("custom_id", unique.slice(index, index + REL_PATH_CHUNK_SIZE));

    if (error) throw new Error(error.message);
    rows.push(...((data ?? []) as DocumentMeta[]));
  }
  return rows;
}

/**
 * Maps picker ids (document custom ids, "text:<key>" ids) to the indexed rel
 * paths they cover, through the granth catalog. A granth indexed under another
 * folder than its PDF's path (re-indexed from a spreadsheet) still resolves, and
 * a duplicate resolves to the granth it duplicates.
 */
export async function resolveRelPathsForCustomIds(customIds: string[]) {
  const catalog = await getGranthCatalog();
  const { entries, relPaths, missing } = relPathsForIds(catalog, customIds);
  return { entries, relPaths, missing };
}

async function fetchAllRows<T>(table: string, columns: string) {
  const pageSize = 1000;
  const rows: T[] = [];

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await getSupabaseAdmin()
      .from(table)
      .select(columns)
      .range(from, from + pageSize - 1);
    if (error) throw new Error(error.message);
    rows.push(...((data ?? []) as T[]));
    if (!data || data.length < pageSize) break;
  }

  return rows;
}

export async function fetchDocumentCatalog() {
  const cached = globalThis.__libraryDocumentCatalogCache;
  if (cached && cached.expiresAt > Date.now()) return cached.value;

  const value = await fetchAllRows<DocumentMeta>("documents", "custom_id,original_relative_path,pdf_name,pdf_url,csv_url");
  globalThis.__libraryDocumentCatalogCache = { expiresAt: Date.now() + PDF_CATALOG_TTL_MS, value };
  return value;
}

export async function fetchSourceCatalog() {
  const cached = globalThis.__librarySourceCatalogCache;
  if (cached && cached.expiresAt > Date.now()) return cached.value;

  const value = await fetchAllRows<SourceMeta>(
    "granth_ocr_files",
    "custom_id,original_rel_path,file_name,file_type,ufs_url,cover_image_url"
  );
  globalThis.__librarySourceCatalogCache = { expiresAt: Date.now() + PDF_CATALOG_TTL_MS, value };
  return value;
}

export async function fetchLibraryFileCatalog() {
  const cached = globalThis.__libraryFileCatalogCache;
  if (cached && cached.expiresAt > Date.now()) return cached.value;

  const value = await fetchAllRows<LibraryFileMeta>(
    "granth_library_files",
    "custom_id,pdf_rel_path,pdf_file_name,pdf_url"
  );
  globalThis.__libraryFileCatalogCache = { expiresAt: Date.now() + PDF_CATALOG_TTL_MS, value };
  return value;
}

export async function fetchDocumentMetaByRelPaths(relPaths: string[]) {
  const unique = Array.from(new Set(relPaths.filter(Boolean)));
  if (unique.length === 0) return [] as DocumentMeta[];
  if (unique.length > TARGETED_REL_PATH_LIMIT) {
    const wanted = new Set(unique);
    return (await fetchDocumentCatalog()).filter((row) => wanted.has(String(row.original_relative_path || "")));
  }

  const rows: DocumentMeta[] = [];
  for (let index = 0; index < unique.length; index += REL_PATH_CHUNK_SIZE) {
    const { data, error } = await getSupabaseAdmin()
      .from("documents")
      .select("custom_id,original_relative_path,pdf_name,pdf_url,csv_url")
      .in("original_relative_path", unique.slice(index, index + REL_PATH_CHUNK_SIZE));

    if (error) throw new Error(error.message);
    rows.push(...((data ?? []) as DocumentMeta[]));
  }
  return rows;
}

export async function fetchSourceMetaByCustomIds(customIds: string[]) {
  const unique = Array.from(new Set(customIds.filter(Boolean)));
  if (unique.length === 0) return [] as SourceMeta[];
  if (unique.length > TARGETED_REL_PATH_LIMIT) {
    const wanted = new Set(unique);
    return (await fetchSourceCatalog()).filter((row) => wanted.has(String(row.custom_id || "")));
  }

  const rows: SourceMeta[] = [];
  for (let index = 0; index < unique.length; index += REL_PATH_CHUNK_SIZE) {
    const { data, error } = await getSupabaseAdmin()
      .from("granth_ocr_files")
      .select("custom_id,original_rel_path,file_name,file_type,ufs_url,cover_image_url")
      .in("custom_id", unique.slice(index, index + REL_PATH_CHUNK_SIZE));

    if (error) throw new Error(error.message);
    rows.push(...((data ?? []) as SourceMeta[]));
  }
  return rows;
}

export async function fetchLibraryFileMetaByCustomIds(customIds: string[]) {
  const unique = Array.from(new Set(customIds.filter(Boolean)));
  if (unique.length === 0) return [] as LibraryFileMeta[];
  if (unique.length > TARGETED_REL_PATH_LIMIT) {
    const wanted = new Set(unique);
    return (await fetchLibraryFileCatalog()).filter((row) => wanted.has(String(row.custom_id || "")));
  }

  const rows: LibraryFileMeta[] = [];
  for (let index = 0; index < unique.length; index += REL_PATH_CHUNK_SIZE) {
    const { data, error } = await getSupabaseAdmin()
      .from("granth_library_files")
      .select("custom_id,pdf_rel_path,pdf_file_name,pdf_url")
      .in("custom_id", unique.slice(index, index + REL_PATH_CHUNK_SIZE));

    if (error) throw new Error(error.message);
    rows.push(...((data ?? []) as LibraryFileMeta[]));
  }
  return rows;
}

export async function fetchSourceMetaByRelPaths(relPaths: string[]) {
  const unique = Array.from(new Set(relPaths.filter(Boolean)));
  if (unique.length === 0) return [] as SourceMeta[];
  if (unique.length > TARGETED_REL_PATH_LIMIT) {
    const wanted = new Set(unique);
    return (await fetchSourceCatalog()).filter((row) => wanted.has(String(row.original_rel_path || "")));
  }

  const rows: SourceMeta[] = [];
  for (let index = 0; index < unique.length; index += REL_PATH_CHUNK_SIZE) {
    const { data, error } = await getSupabaseAdmin()
      .from("granth_ocr_files")
      .select("custom_id,original_rel_path,file_name,file_type,ufs_url")
      .in("original_rel_path", unique.slice(index, index + REL_PATH_CHUNK_SIZE));

    if (error) throw new Error(error.message);
    rows.push(...((data ?? []) as SourceMeta[]));
  }
  return rows;
}

/**
 * Resolves granth pages (from the OCR index) to the name and uploaded PDF shown
 * for them, from the granth catalog, so the search results, exports and
 * previews all name a page the same way. A granth with no linked PDF (the
 * text-only ones) gets an empty pdf_url and opens as OCR text: a PDF is never
 * guessed from shared title words.
 */
export async function resolveGranthPdfTargets(rows: GranthPdfRow[]): Promise<ResolvedGranthPdf[]> {
  if (rows.length === 0) return [];
  const catalog = await getGranthCatalog();

  return rows.map((row) => {
    const described = describeGranth(catalog, row.granth_key);
    return {
      granth_key: row.granth_key,
      source_rel_path: row.source_rel_path,
      custom_id: described?.entry.custom_id ?? row.granth_key,
      pdf_name: described?.displayName ?? row.pdf_name,
      pdf_url: described?.pdfUrl ?? "",
      csv_url: described?.csvUrl ?? null,
      page_number: row.page_number,
      source_page_number: row.page_number,
    };
  });
}
