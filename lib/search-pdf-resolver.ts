import { getSupabaseAdmin } from "@/lib/supabase-server";
import { getTursoClient } from "@/lib/turso";

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

export type PdfCatalogMeta = {
  custom_id: string | null;
  original_rel_path: string | null;
  file_name: string | null;
  file_type: string | null;
  ufs_url: string | null;
  catalog_source?: "ocr" | "library";
};

export type PdfFallback = {
  meta: PdfCatalogMeta;
  score: number;
  partNumber: number | null;
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

const PDF_CATALOG_TTL_MS = 5 * 60 * 1000;
/**
 * Supabase filters travel in the request URL, and granth rel paths are long, so
 * `in.(...)` lookups break once a few dozen paths are batched together. Small
 * lookups stay targeted; larger ones read the (small) tables in full instead.
 */
const TARGETED_REL_PATH_LIMIT = 20;
const REL_PATH_CHUNK_SIZE = 10;

declare global {
  // eslint-disable-next-line no-var
  var __libraryPdfCatalogCache: { expiresAt: number; value: PdfCatalogMeta[] } | undefined;
  // eslint-disable-next-line no-var
  var __libraryDocumentCatalogCache: { expiresAt: number; value: DocumentMeta[] } | undefined;
  // eslint-disable-next-line no-var
  var __librarySourceCatalogCache: { expiresAt: number; value: SourceMeta[] } | undefined;
  // eslint-disable-next-line no-var
  var __libraryFileCatalogCache: { expiresAt: number; value: LibraryFileMeta[] } | undefined;
}

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
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

function legacySearchTokens(row: GranthPdfRow) {
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

function scorePdfFallback(row: GranthPdfRow, meta: PdfCatalogMeta) {
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

export function findPdfFallbacks(row: GranthPdfRow, catalog: PdfCatalogMeta[]) {
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

export async function fetchPdfCatalog() {
  const cached = globalThis.__libraryPdfCatalogCache;
  if (cached && cached.expiresAt > Date.now()) return cached.value;

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

  const catalog = [...ocrRows, ...libraryRows].filter((row) => looksLikePdfSource(row));
  globalThis.__libraryPdfCatalogCache = { expiresAt: Date.now() + PDF_CATALOG_TTL_MS, value: catalog };
  return catalog;
}

export async function fetchTursoPageCounts(relPaths: string[]) {
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

export function choosePdfFallback(row: GranthPdfRow, fallbacks: PdfFallback[], pageCounts: Map<string, number>) {
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

/** Maps selected granth custom ids to the OCR index rel paths they cover. */
export async function resolveRelPathsForCustomIds(customIds: string[]) {
  const docs = await fetchDocumentMetaByCustomIds(customIds);
  return {
    docs,
    relPaths: Array.from(
      new Set(docs.map((row) => String(row.original_relative_path ?? "").trim()).filter(Boolean))
    ),
  };
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
 * Resolves granth pages (from the OCR index) to the uploaded PDF that should be
 * opened or exported, using the same document/source/fuzzy-catalog chain the
 * search results use so exports stay consistent with what users see on screen.
 */
export async function resolveGranthPdfTargets(
  rows: GranthPdfRow[],
  options: { extraDocsByRelPath?: Map<string, DocumentMeta> } = {}
): Promise<ResolvedGranthPdf[]> {
  if (rows.length === 0) return [];

  const relPaths = Array.from(new Set(rows.map((row) => row.source_rel_path).filter(Boolean)));
  const [docs, sources] = await Promise.all([
    fetchDocumentMetaByRelPaths(relPaths),
    fetchSourceMetaByRelPaths(relPaths),
  ]);

  const docByRelPath = new Map(
    docs.filter((row) => row.original_relative_path).map((row) => [String(row.original_relative_path), row])
  );
  const sourceByRelPath = new Map(
    sources.filter((row) => row.original_rel_path).map((row) => [String(row.original_rel_path), row])
  );
  const extraDocs = options.extraDocsByRelPath ?? new Map<string, DocumentMeta>();

  const rowsNeedingFallback = rows.filter((row) => {
    const meta = docByRelPath.get(row.source_rel_path) ?? extraDocs.get(row.source_rel_path);
    const sourceMeta = sourceByRelPath.get(row.source_rel_path);
    return !normalizeHttpUrl(meta?.pdf_url) && !sourcePdfUrl(sourceMeta);
  });

  const catalog = rowsNeedingFallback.length > 0 ? await fetchPdfCatalog() : [];
  const fallbackCandidatesByRelPath = new Map<string, PdfFallback[]>();
  for (const row of rowsNeedingFallback) {
    if (fallbackCandidatesByRelPath.has(row.source_rel_path)) continue;
    fallbackCandidatesByRelPath.set(row.source_rel_path, findPdfFallbacks(row, catalog));
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

  return rows.map((row) => {
    const meta = docByRelPath.get(row.source_rel_path) ?? extraDocs.get(row.source_rel_path);
    const sourceMeta = sourceByRelPath.get(row.source_rel_path);
    const fallback = choosePdfFallback(
      row,
      fallbackCandidatesByRelPath.get(row.source_rel_path) ?? [],
      fallbackPageCounts
    );
    const fallbackPdfUrl = normalizeHttpUrl(fallback?.meta.ufs_url);

    return {
      granth_key: row.granth_key,
      source_rel_path: row.source_rel_path,
      custom_id: meta?.custom_id ?? sourceMeta?.custom_id ?? fallback?.meta.custom_id ?? row.granth_key,
      pdf_name: meta?.pdf_name ?? sourceMeta?.file_name ?? fallback?.meta.file_name ?? row.pdf_name,
      pdf_url: normalizeHttpUrl(meta?.pdf_url) || sourcePdfUrl(sourceMeta) || fallbackPdfUrl,
      csv_url: meta?.csv_url ?? null,
      page_number: fallback?.pageNumber ?? row.page_number,
      source_page_number: row.page_number,
    };
  });
}
