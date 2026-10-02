// The granth catalog as the app uses it: one entry per indexed granth, with a
// clean title, the uploaded PDF it belongs to (decided from exact evidence),
// and whether it is kept out of search as a duplicate. Built by
// lib/granth-catalog-build.mjs from the metadata tables, which are never
// rewritten, plus the checked corrections in data/granth-catalog-overrides.json.

import overrides from "@/data/granth-catalog-overrides.json";
import {
  type GranthCatalogEntry,
  type GranthCatalogIssue,
  buildGranthCatalog,
  granthDisplayName,
} from "@/lib/granth-catalog-build.mjs";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import { getTursoClient } from "@/lib/turso";

export type { GranthCatalogEntry } from "@/lib/granth-catalog-build.mjs";
export { granthDisplayName } from "@/lib/granth-catalog-build.mjs";

export type DocumentRow = {
  custom_id: string | null;
  original_relative_path: string | null;
  pdf_name: string | null;
  pdf_url: string | null;
  csv_url: string | null;
  status: string | null;
};

export type GranthCatalog = {
  entries: GranthCatalogEntry[];
  issues: GranthCatalogIssue[];
  byGranthKey: Map<string, GranthCatalogEntry>;
  byRelPath: Map<string, GranthCatalogEntry>;
  /** Picker ids (document custom ids, "text:<key>") and legacy ids. */
  byCustomId: Map<string, GranthCatalogEntry>;
  documentsByCustomId: Map<string, DocumentRow>;
  /** Granths kept out of search: duplicates of another granth, or hidden. */
  excludedKeys: string[];
  builtAt: number;
};

// Metadata changes only when granths are scanned or uploaded.
const CATALOG_TTL_MS = 10 * 60 * 1000;
// A build takes about half a second; one still running after this is stuck.
const CATALOG_BUILD_TIMEOUT_MS = 45 * 1000;

declare global {
  // eslint-disable-next-line no-var
  var __granthCatalog: { value: GranthCatalog; expiresAt: number } | undefined;
  // eslint-disable-next-line no-var
  var __granthCatalogLoading: Promise<GranthCatalog> | undefined;
}

async function fetchAll<T>(table: string, columns: string) {
  const rows: T[] = [];
  for (let from = 0; ; from += 1000) {
    const { data, error } = await getSupabaseAdmin().from(table).select(columns).range(from, from + 999);
    if (error) throw new Error(`${table}: ${error.message}`);
    rows.push(...((data ?? []) as T[]));
    if (!data || data.length < 1000) break;
  }
  return rows;
}

async function buildCatalog(): Promise<GranthCatalog> {
  const [turso, documents, libraryFiles, books] = await Promise.all([
    getTursoClient()
      .execute("SELECT granth_key, book_number, library_code, granth_name, source_rel_path, page_count FROM ocr_granths")
      .then((result) =>
        result.rows.map((row) => ({
          granth_key: String(row.granth_key ?? ""),
          book_number: row.book_number == null ? null : String(row.book_number),
          library_code: row.library_code == null ? null : String(row.library_code),
          granth_name: String(row.granth_name ?? ""),
          source_rel_path: String(row.source_rel_path ?? ""),
          page_count: Number(row.page_count ?? 0),
        }))
      ),
    fetchAll<DocumentRow>("documents", "custom_id,original_relative_path,pdf_name,pdf_url,csv_url,status"),
    fetchAll<{ custom_id: string | null; book_id: number | null }>("granth_library_files", "custom_id,book_id"),
    fetchAll<{
      id: number;
      title_english: string | null;
      title_display: string | null;
      author_text: string | null;
      book_codes: string[];
    }>("granth_library_books", "id,title_english,title_display,author_text,book_codes"),
  ]);

  const { entries, issues } = buildGranthCatalog({ turso, documents, libraryFiles, books, overrides });
  const byGranthKey = new Map(entries.map((entry) => [entry.granth_key, entry]));
  const byRelPath = new Map(entries.map((entry) => [entry.source_rel_path, entry]));
  const byCustomId = new Map<string, GranthCatalogEntry>();
  for (const entry of entries) {
    byCustomId.set(entry.custom_id, entry);
    // Links from before a granth's PDF was linked used its text-only id.
    byCustomId.set(`text:${entry.granth_key}`, entry);
  }
  const excludedKeys = entries
    .filter((entry) => entry.hidden || (entry.duplicate_of && byGranthKey.has(entry.duplicate_of)))
    .map((entry) => entry.granth_key);

  return {
    entries,
    issues,
    byGranthKey,
    byRelPath,
    byCustomId,
    documentsByCustomId: new Map(
      documents.filter((doc) => doc.custom_id).map((doc) => [String(doc.custom_id), doc])
    ),
    excludedKeys,
    builtAt: Date.now(),
  };
}

/** The catalog, rebuilt at most every few minutes; concurrent callers share one build. */
export async function getGranthCatalog(): Promise<GranthCatalog> {
  const cached = globalThis.__granthCatalog;
  if (cached && cached.expiresAt > Date.now()) return cached.value;
  if (!globalThis.__granthCatalogLoading) {
    // A metadata request that never answers must not hold every later caller
    // forever: the shared build gives up, and the next call starts a new one.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("The granth catalog took too long to load.")), CATALOG_BUILD_TIMEOUT_MS);
    });
    globalThis.__granthCatalogLoading = Promise.race([buildCatalog(), timeout])
      .then((value) => {
        globalThis.__granthCatalog = { value, expiresAt: Date.now() + CATALOG_TTL_MS };
        return value;
      })
      .finally(() => {
        clearTimeout(timer);
        globalThis.__granthCatalogLoading = undefined;
      });
  }
  try {
    return await globalThis.__granthCatalogLoading;
  } catch (error) {
    // A stale catalog beats a failed search when Supabase blips.
    if (cached) return cached.value;
    throw error;
  }
}

/** The granths a search may return: every indexed granth that is not a duplicate. */
export function isSearchable(catalog: GranthCatalog, entry: GranthCatalogEntry | undefined) {
  return Boolean(entry) && !catalog.excludedKeys.includes(entry!.granth_key);
}

/** SQL that keeps duplicate granths out of a page search. `column` is e.g. "p.granth_key". */
export function excludeDuplicatesSql(catalog: GranthCatalog, column: string) {
  if (catalog.excludedKeys.length === 0) return { sql: "", args: [] as string[] };
  return {
    sql: ` AND ${column} NOT IN (${catalog.excludedKeys.map(() => "?").join(",")})`,
    args: [...catalog.excludedKeys],
  };
}

/** Indexed rel paths for picker ids; a duplicate resolves to the granth it duplicates. */
export function relPathsForIds(catalog: GranthCatalog, ids: string[]) {
  const entries: GranthCatalogEntry[] = [];
  const missing: string[] = [];
  for (const id of ids) {
    let entry = catalog.byCustomId.get(id) ?? catalog.byGranthKey.get(id);
    if (entry?.duplicate_of) entry = catalog.byGranthKey.get(entry.duplicate_of) ?? entry;
    if (entry) entries.push(entry);
    else missing.push(id);
  }
  const unique = [...new Map(entries.map((entry) => [entry.granth_key, entry])).values()];
  return { entries: unique, relPaths: unique.map((entry) => entry.source_rel_path), missing };
}

/** Everything a result needs to name and open the granth a page came from. */
export function describeGranth(catalog: GranthCatalog, granthKey: string) {
  const entry = catalog.byGranthKey.get(granthKey);
  if (!entry) return null;
  const doc = entry.document_custom_id ? catalog.documentsByCustomId.get(entry.document_custom_id) : undefined;
  return {
    entry,
    displayName: granthDisplayName(entry),
    pdfUrl: normalizePdfUrl(doc?.pdf_url),
    csvUrl: doc?.csv_url ?? null,
  };
}

function normalizePdfUrl(value: string | null | undefined) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  try {
    const url = new URL(raw);
    return url.protocol === "https:" || url.protocol === "http:" ? url.toString() : "";
  } catch {
    return "";
  }
}
