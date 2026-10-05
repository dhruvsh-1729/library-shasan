import {
  buildOCRSearchExcerptForQueries,
  findOCRSearchMatchesForQueries,
  isTooShortForContains,
  normalizeOCRSearchQueries,
  type OCRSearchMode,
  type OCRSearchScripts,
} from "@/lib/ocr-search";
import { buildOCRPrefilter } from "@/lib/ocr-search-index";
import {
  type LibraryFileMeta,
  type SourceMeta,
  fetchDocumentCatalog,
  fetchDocumentMetaByCustomIds,
  fetchLibraryFileCatalog,
  fetchLibraryFileMetaByCustomIds,
  fetchSourceCatalog,
  fetchSourceMetaByCustomIds,
  fetchSourceMetaByRelPaths,
} from "@/lib/search-pdf-resolver";
import { getTursoClient } from "@/lib/turso";
import { excludeDuplicatesSql, getGranthCatalog } from "@/lib/granth-catalog";

export type SearchPdfSource = {
  customId: string;
  sourceRelPath: string;
  pdfName: string;
  pdfUrl: string;
  coverImageUrl: string | null;
};

export type SearchMatchPage = {
  page_number: number;
  /** The number printed on the page ("91", "73-74"), or null when unknown or unverified. */
  printed_page: string | null;
  occurrence_count: number;
  snippet: string;
};

export type SearchMatchGranthSummary = {
  granth_key: string;
  source_rel_path: string;
  granth_name: string;
  matched_pages: number;
  first_page: number;
};

export type SearchMatchLine = {
  page_number: number;
  /** The number printed on the page ("91", "73-74"), or null when unknown or unverified. */
  printed_page: string | null;
  line_number: number;
  occurrence_count: number;
  matched_words: string[];
  line_text: string;
};

export const MAX_MATCH_PAGE_PREVIEW = 2000;
export const MAX_MATCH_PAGE_DOWNLOAD = 900;
/** Granths listed in the search-wide export dialog. */
export const MAX_EXPORT_GRANTH_PREVIEW = 500;
/** Granths merged into a single combined PDF. */
export const MAX_COMBINED_PDF_GRANTHS = 60;
/** Granths scanned for one CSV export. */
export const MAX_CSV_GRANTHS = 500;
/** Rows written to one CSV export. */
export const MAX_CSV_ROWS = 100000;
const MAX_CSV_LINE_TEXT_CHARS = 400;

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

function looksLikePdf(row: Pick<SourceMeta, "file_name" | "file_type" | "ufs_url"> | null | undefined) {
  if (!row) return false;
  const fileName = String(row.file_name || "").trim().toLowerCase();
  const fileType = String(row.file_type || "").trim().toLowerCase();
  const url = String(row.ufs_url || "").trim().toLowerCase();
  return fileName.endsWith(".pdf") || fileType.includes("pdf") || /\.pdf(?:[?#]|$)/i.test(url);
}

function indexBy<T>(rows: T[], key: (row: T) => string) {
  const map = new Map<string, T>();
  for (const row of rows) {
    const value = key(row).trim();
    if (!value || map.has(value)) continue;
    map.set(value, row);
  }
  return map;
}

export type SearchPdfSourceRequest = {
  customId: string;
  sourceRelPath?: string;
};

export type SearchPdfSourceResult =
  | { ok: true; source: SearchPdfSource }
  | { ok: false; status: number; error: string };

/** Above this many granths, one full (cached) table read beats a query per granth. */
const CATALOG_LOOKUP_THRESHOLD = 8;

function deriveSourceRelPath(
  request: SearchPdfSourceRequest,
  doc: { original_relative_path: string | null } | null,
  sourceByCustomId: SourceMeta | null,
  libraryFile: LibraryFileMeta | null
) {
  return String(
    request.sourceRelPath ||
      doc?.original_relative_path ||
      sourceByCustomId?.original_rel_path ||
      libraryFile?.pdf_rel_path ||
      ""
  ).trim();
}

/**
 * Resolves granths to their uploaded PDF in one pass. Small requests (a single
 * granth download) stay on targeted lookups; bulk exports read the small
 * document/OCR/library tables in full instead of issuing a query per granth.
 */
export async function resolveSearchPdfSources(
  requests: SearchPdfSourceRequest[]
): Promise<SearchPdfSourceResult[]> {
  if (requests.length === 0) return [];

  const customIds = requests.map((request) => String(request.customId || "").trim()).filter(Boolean);
  const useCatalogs = requests.length > CATALOG_LOOKUP_THRESHOLD;

  const [documents, sourcesByIdRows, libraryFiles] = useCatalogs
    ? await Promise.all([fetchDocumentCatalog(), fetchSourceCatalog(), fetchLibraryFileCatalog()])
    : await Promise.all([
        fetchDocumentMetaByCustomIds(customIds),
        fetchSourceMetaByCustomIds(customIds),
        fetchLibraryFileMetaByCustomIds(customIds),
      ]);

  const docsByCustomId = indexBy(documents, (row) => String(row.custom_id || ""));
  const sourcesByCustomId = indexBy(sourcesByIdRows, (row) => String(row.custom_id || ""));
  const libraryByCustomId = indexBy(libraryFiles as LibraryFileMeta[], (row) => String(row.custom_id || ""));

  // The rel path can come from the request or from the rows just fetched, so
  // the targeted branch resolves it before looking OCR files up by rel path.
  let sourcesByRelPath = indexBy(sourcesByIdRows, (row) => String(row.original_rel_path || ""));
  if (!useCatalogs) {
    const relPaths = requests.map((request) => {
      const customId = String(request.customId || "").trim();
      return deriveSourceRelPath(
        request,
        docsByCustomId.get(customId) ?? null,
        sourcesByCustomId.get(customId) ?? null,
        libraryByCustomId.get(customId) ?? null
      );
    });
    const missing = relPaths.filter((relPath) => relPath && !sourcesByRelPath.has(relPath));
    if (missing.length > 0) {
      sourcesByRelPath = indexBy(
        [...sourcesByIdRows, ...(await fetchSourceMetaByRelPaths(missing))],
        (row) => String(row.original_rel_path || "")
      );
    }
  }

  return requests.map((request) => {
    const normalizedCustomId = String(request.customId || "").trim();
    if (!normalizedCustomId) {
      return { ok: false as const, status: 400, error: "Missing granth identifier." };
    }

    const doc = docsByCustomId.get(normalizedCustomId) ?? null;
    const sourceByCustomId = sourcesByCustomId.get(normalizedCustomId) ?? null;
    const libraryFile = libraryByCustomId.get(normalizedCustomId) ?? null;

    const sourceRelPath = deriveSourceRelPath(request, doc, sourceByCustomId, libraryFile);
    const sourceByRelPath =
      sourceRelPath && sourceByCustomId?.original_rel_path !== sourceRelPath
        ? sourcesByRelPath.get(sourceRelPath) ?? null
        : null;
    const pdfSource = looksLikePdf(sourceByCustomId)
      ? sourceByCustomId
      : looksLikePdf(sourceByRelPath)
        ? sourceByRelPath
        : null;
    const source = pdfSource ?? sourceByCustomId ?? sourceByRelPath;
    const sourcePdfUrl = normalizeHttpUrl(pdfSource?.ufs_url);
    const pdfUrl = normalizeHttpUrl(doc?.pdf_url) || sourcePdfUrl || normalizeHttpUrl(libraryFile?.pdf_url);
    const pdfName = String(
      doc?.pdf_name || source?.file_name || libraryFile?.pdf_file_name || normalizedCustomId || "granth.pdf"
    ).trim();

    if (!sourceRelPath) {
      return {
        ok: false as const,
        status: 404,
        error: "This result is not linked to searchable granth page metadata.",
      };
    }
    if (!pdfUrl) {
      return { ok: false as const, status: 404, error: "This result is not linked to an uploaded PDF." };
    }

    return {
      ok: true as const,
      source: {
        customId: normalizedCustomId,
        sourceRelPath,
        pdfName,
        pdfUrl,
        coverImageUrl: source?.cover_image_url ?? null,
      },
    };
  });
}

export async function resolveSearchPdfSource(
  customId: string,
  preferredSourceRelPath = ""
): Promise<SearchPdfSource> {
  const [result] = await resolveSearchPdfSources([{ customId, sourceRelPath: preferredSourceRelPath }]);
  if (!result.ok) throw new SearchMatchError(result.status, result.error);
  return result.source;
}

export function validateSearchDownloadQuery(query: string, matchMode: OCRSearchMode) {
  return validateSearchDownloadQueries(query, [], matchMode)[0];
}

export function validateSearchDownloadQueries(
  query: string | string[],
  variants: string | string[] | null | undefined,
  matchMode: OCRSearchMode
) {
  const queries = normalizeOCRSearchQueries(query, variants).filter((value) => Array.from(value).length >= 2);
  if (queries.length === 0) throw new SearchMatchError(400, "Enter at least 2 characters before building a PDF.");
  if (matchMode === "contains" && queries.some(isTooShortForContains)) {
    throw new SearchMatchError(400, "Contains search needs at least 3 letters once conjuncts are folded (न्द counts as 2).");
  }
  return queries;
}

/**
 * Builds the FTS hit sub-query. `relPaths` scopes it to specific granths;
 * `null` searches every indexed granth (the "All granths" filter mode).
 */
async function buildHitQuery(queries: string[], matchMode: OCRSearchMode, relPaths: string[] | null) {
  const scopedPaths = relPaths ?? [];
  const relFilterSql = scopedPaths.length
    ? ` AND g.source_rel_path IN (${scopedPaths.map(() => "?").join(",")})`
    : "";
  const { table, match } = buildOCRPrefilter(queries, matchMode);
  const dup = excludeDuplicatesSql(await getGranthCatalog(), "p.granth_key");
  const sql = `SELECT p.id AS page_id
             FROM ${table}
             JOIN ocr_pages p ON p.id = ${table}.rowid
             JOIN ocr_granths g ON g.granth_key = p.granth_key
             WHERE ${table} MATCH ?${relFilterSql}${dup.sql}`;

  return { sql, args: [match, ...scopedPaths, ...dup.args] };
}

function normalizeLineBreaks(value: string) {
  return String(value ?? "").replace(/\r\n?/g, "\n");
}

function collapseLineText(value: string, maxChars = MAX_CSV_LINE_TEXT_CHARS) {
  const cleaned = String(value ?? "").replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxChars) return cleaned;
  return `${cleaned.slice(0, maxChars - 1).trimEnd()}…`;
}

/**
 * Maps every match on a page to the OCR line it sits on, so exports can point
 * at "page 42, line 7" instead of just the page.
 */
export function findMatchLinesInPageContent(
  content: string,
  queries: string[],
  matchMode: OCRSearchMode,
  scripts: OCRSearchScripts = null,
  maxLineChars = MAX_CSV_LINE_TEXT_CHARS
): Array<Omit<SearchMatchLine, "page_number" | "printed_page">> {
  const normalized = normalizeLineBreaks(content);
  const matches = findOCRSearchMatchesForQueries(normalized, queries, matchMode, scripts);
  if (matches.length === 0) return [];

  const lines = normalized.split("\n");
  const lineStarts: number[] = [];
  let offset = 0;
  for (const line of lines) {
    lineStarts.push(offset);
    offset += line.length + 1;
  }

  const byLine = new Map<number, { occurrences: number; words: Set<string> }>();
  let lineIndex = 0;

  for (const match of matches) {
    while (lineIndex + 1 < lineStarts.length && lineStarts[lineIndex + 1] <= match.start) lineIndex += 1;
    const entry = byLine.get(lineIndex) ?? { occurrences: 0, words: new Set<string>() };
    entry.occurrences += 1;
    // the whole word the match sits in, as printed (हिंसायां, not just हिंसा)
    const matchedText = wholeWordAt(normalized, match.start, match.end).replace(/\s+/g, " ").trim();
    if (matchedText) entry.words.add(matchedText);
    byLine.set(lineIndex, entry);
  }

  return [...byLine.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([index, entry]) => ({
      line_number: index + 1,
      occurrence_count: entry.occurrences,
      matched_words: [...entry.words],
      line_text: collapseLineText(lines[index] ?? "", maxLineChars),
    }));
}

/**
 * Lists every granth that has at least one matching page for the search, so a
 * multi-granth or all-granth search can be exported in one file.
 */
export async function loadSearchMatchGranths(
  relPaths: string[] | null,
  query: string | string[],
  matchMode: OCRSearchMode,
  limit = MAX_EXPORT_GRANTH_PREVIEW,
  queryVariants?: string | string[] | null,
  scripts: OCRSearchScripts = null
) {
  const queries = validateSearchDownloadQueries(query, queryVariants, matchMode);
  const boundedLimit = Math.max(1, Math.min(Math.floor(limit), MAX_EXPORT_GRANTH_PREVIEW));
  const hits = await buildHitQuery(queries, matchMode, relPaths);

  return loadVerifiedGranths(hits, queries, matchMode, scripts, boundedLimit);
}

// Every hit page is read and verified, in batches, a few at a time: capping the
// scan (it used to stop at 4,000 pages, like the first screen of /api/search)
// left a common word's later granths and pages out of the export.
const VERIFY_BATCH = 500;
const VERIFY_PARALLEL = 6;

/**
 * The index is only a prefilter, so every hit page is checked against its text
 * with the same rules as the result tiles (match mode, and scripts: the index
 * folds Gujarati into Devanagari). Counting raw index hits listed granths with
 * no real match and matched-page totals the screen never showed.
 */
async function loadVerifiedGranths(
  hits: Awaited<ReturnType<typeof buildHitQuery>>,
  queries: string[],
  matchMode: OCRSearchMode,
  scripts: OCRSearchScripts,
  limit: number
) {
  const client = getTursoClient();
  const ids = (
    await client.execute({
      sql: `WITH hits AS (${hits.sql}) SELECT DISTINCT page_id FROM hits ORDER BY page_id`,
      args: hits.args,
    })
  ).rows.map((row) => Number(row.page_id));
  const chunks: number[][] = [];
  for (let i = 0; i < ids.length; i += VERIFY_BATCH) chunks.push(ids.slice(i, i + VERIFY_BATCH));
  const rows: Array<Record<string, unknown>> = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(VERIFY_PARALLEL, chunks.length) }, async () => {
      while (next < chunks.length) {
        const chunk = chunks[next++];
        const result = await client.execute({
          sql: `SELECT g.granth_key, g.source_rel_path, g.granth_name, p.page_number, p.content
                FROM ocr_pages p JOIN ocr_granths g ON g.granth_key = p.granth_key
                WHERE p.id IN (${chunk.map(() => "?").join(",")})`,
          args: chunk,
        });
        rows.push(...result.rows);
      }
    })
  );

  const byGranth = new Map<string, SearchMatchGranthSummary>();
  for (const row of rows) {
    if (findOCRSearchMatchesForQueries(String(row.content ?? ""), queries, matchMode, scripts).length === 0) continue;
    const key = String(row.granth_key ?? "");
    const page = Math.max(1, toInt(row.page_number, 1));
    const entry = byGranth.get(key);
    if (entry) {
      entry.matched_pages += 1;
      entry.first_page = Math.min(entry.first_page, page);
      continue;
    }
    byGranth.set(key, {
      granth_key: key,
      source_rel_path: String(row.source_rel_path ?? ""),
      granth_name: String(row.granth_name ?? ""),
      matched_pages: 1,
      first_page: page,
    });
  }

  const sorted = [...byGranth.values()].sort(
    (a, b) => b.matched_pages - a.matched_pages || a.granth_name.localeCompare(b.granth_name)
  );
  return {
    granths: sorted.slice(0, limit),
    truncated: sorted.length > limit,
    queries,
  };
}

/**
 * Page-and-line level matches for one granth, used by the CSV export.
 */
export async function loadSearchMatchLines(
  sourceRelPath: string,
  query: string | string[],
  matchMode: OCRSearchMode,
  options: {
    pages?: number[] | null;
    maxRows?: number;
    queryVariants?: string | string[] | null;
    scripts?: OCRSearchScripts;
    /** Longest line text kept (the CSV keeps 400 characters). */
    maxLineChars?: number;
  } = {}
) {
  const queries = validateSearchDownloadQueries(query, options.queryVariants, matchMode);
  const pageFilter = options.pages && options.pages.length > 0 ? new Set(options.pages) : null;
  const maxRows = Math.max(1, Math.min(Math.floor(options.maxRows ?? MAX_CSV_ROWS), MAX_CSV_ROWS));
  const hits = await buildHitQuery(queries, matchMode, [sourceRelPath]);

  const result = await getTursoClient().execute({
    sql: `WITH hits AS (${hits.sql}),
            unique_hits AS (
              SELECT page_id
              FROM hits
              GROUP BY page_id
            )
          SELECT
            p.page_number,
            p.content,
            pp.printed_page
          FROM unique_hits
          JOIN ocr_pages p ON p.id = unique_hits.page_id
          LEFT JOIN ocr_printed_pages pp
            ON pp.granth_key = p.granth_key AND pp.page_number = p.page_number
            -- a page whose text and image readings disagree shows its PDF page instead
            AND (pp.verified IS NULL OR pp.verified <> 'conflict')
          ORDER BY p.page_number ASC
          LIMIT ?`,
    args: [...hits.args, MAX_MATCH_PAGE_PREVIEW + 1],
  });

  const lines: SearchMatchLine[] = [];
  const matchedPages = new Set<number>();
  let truncated = result.rows.length > MAX_MATCH_PAGE_PREVIEW;

  for (const row of result.rows.slice(0, MAX_MATCH_PAGE_PREVIEW)) {
    const pageNumber = toInt(row.page_number);
    if (pageNumber <= 0) continue;
    if (pageFilter && !pageFilter.has(pageNumber)) continue;

    const pageLines = findMatchLinesInPageContent(
      String(row.content ?? ""),
      queries,
      matchMode,
      options.scripts ?? null,
      options.maxLineChars
    );
    if (pageLines.length === 0) continue;
    matchedPages.add(pageNumber);
    const printedPage = String(row.printed_page ?? "").trim() || null;

    for (const line of pageLines) {
      if (lines.length >= maxRows) {
        truncated = true;
        break;
      }
      lines.push({ page_number: pageNumber, printed_page: printedPage, ...line });
    }
    if (truncated) break;
  }

  return { lines, matched_pages: [...matchedPages].sort((a, b) => a - b), truncated, queries };
}

export type SearchMatchOccurrence = {
  page_number: number;
  /** The number printed on the page ("91", "73-74" for a two-page scan), or null when unknown. */
  printed_page: string | null;
  /** The whole word the match sits in, as printed (a compound stays whole). */
  word: string;
};

const WORD_CHAR = /[\p{L}\p{M}‌‍]/u;
// A word printed across a line break: "हिंसाद्यष्ट-\nदशपापस्थान".
const BREAK_AFTER = /^-[ \t]*\n[ \t]*/;
const BREAK_BEFORE = /-[ \t]*\n[ \t]*$/;

/** Widens a match to the whole word around it, joining a word hyphenated across lines. */
function wholeWordAt(text: string, start: number, end: number) {
  const pieces: Array<[number, number]> = [[start, end]];
  for (;;) {
    const first = pieces[0];
    while (first[0] > 0 && WORD_CHAR.test(text[first[0] - 1])) first[0] -= 1;
    const brk = text.slice(Math.max(0, first[0] - 8), first[0]).match(BREAK_BEFORE);
    const prevEnd = brk ? first[0] - brk[0].length : 0;
    if (!brk || prevEnd <= 0 || !WORD_CHAR.test(text[prevEnd - 1])) break;
    pieces.unshift([prevEnd, prevEnd]);
  }
  for (;;) {
    const last = pieces[pieces.length - 1];
    while (last[1] < text.length && WORD_CHAR.test(text[last[1]])) last[1] += 1;
    const brk = text.slice(last[1], last[1] + 8).match(BREAK_AFTER);
    const nextStart = brk ? last[1] + brk[0].length : 0;
    if (!brk || !WORD_CHAR.test(text[nextStart] ?? "")) break;
    pieces.push([nextStart, nextStart]);
  }
  return pieces.map(([a, b]) => text.slice(a, b)).join("");
}

/**
 * One row per match, in page order, with the granth's printed page number
 * (ocr_printed_pages, from scripts/backfill_printed_pages.mjs) for the word-list PDF.
 */
export async function loadSearchMatchOccurrences(
  sourceRelPath: string,
  query: string | string[],
  matchMode: OCRSearchMode,
  options: {
    pages?: number[] | null;
    maxRows?: number;
    queryVariants?: string | string[] | null;
    scripts?: OCRSearchScripts;
  } = {}
) {
  const queries = validateSearchDownloadQueries(query, options.queryVariants, matchMode);
  const pageFilter = options.pages && options.pages.length > 0 ? new Set(options.pages) : null;
  const maxRows = Math.max(1, Math.min(Math.floor(options.maxRows ?? MAX_CSV_ROWS), MAX_CSV_ROWS));
  const hits = await buildHitQuery(queries, matchMode, [sourceRelPath]);

  const result = await getTursoClient().execute({
    sql: `WITH hits AS (${hits.sql}),
            unique_hits AS (
              SELECT page_id
              FROM hits
              GROUP BY page_id
            )
          SELECT
            p.page_number,
            p.content,
            pp.printed_page
          FROM unique_hits
          JOIN ocr_pages p ON p.id = unique_hits.page_id
          LEFT JOIN ocr_printed_pages pp
            ON pp.granth_key = p.granth_key AND pp.page_number = p.page_number
            -- a page whose text and image readings disagree shows its PDF page instead
            AND (pp.verified IS NULL OR pp.verified <> 'conflict')
          ORDER BY p.page_number ASC
          LIMIT ?`,
    args: [...hits.args, MAX_MATCH_PAGE_PREVIEW + 1],
  });

  const occurrences: SearchMatchOccurrence[] = [];
  let truncated = result.rows.length > MAX_MATCH_PAGE_PREVIEW;

  for (const row of result.rows.slice(0, MAX_MATCH_PAGE_PREVIEW)) {
    const pageNumber = toInt(row.page_number);
    if (pageNumber <= 0) continue;
    if (pageFilter && !pageFilter.has(pageNumber)) continue;

    const content = normalizeLineBreaks(String(row.content ?? ""));
    const printed = String(row.printed_page ?? "").trim() || null;
    for (const match of findOCRSearchMatchesForQueries(content, queries, matchMode, options.scripts ?? null)) {
      if (occurrences.length >= maxRows) {
        truncated = true;
        break;
      }
      const word = wholeWordAt(content, match.start, match.end).trim();
      if (word) occurrences.push({ page_number: pageNumber, printed_page: printed, word });
    }
    if (truncated) break;
  }

  return { occurrences, truncated, queries };
}

export async function loadSearchMatchPages(
  sourceRelPath: string,
  query: string | string[],
  matchMode: OCRSearchMode,
  limit = MAX_MATCH_PAGE_PREVIEW,
  queryVariants?: string | string[] | null,
  scripts: OCRSearchScripts = null
) {
  const queries = validateSearchDownloadQueries(query, queryVariants, matchMode);
  const boundedLimit = Math.max(1, Math.min(Math.floor(limit), MAX_MATCH_PAGE_PREVIEW));
  const client = getTursoClient();
  const hits = await buildHitQuery(queries, matchMode, [sourceRelPath]);

  const result = await client.execute({
    sql: `WITH hits AS (${hits.sql}),
            unique_hits AS (
              SELECT page_id
              FROM hits
              GROUP BY page_id
            )
          SELECT
            p.page_number,
            p.content,
            pp.printed_page
          FROM unique_hits
          JOIN ocr_pages p ON p.id = unique_hits.page_id
          LEFT JOIN ocr_printed_pages pp
            ON pp.granth_key = p.granth_key AND pp.page_number = p.page_number
            -- a page whose text and image readings disagree shows its PDF page instead
            AND (pp.verified IS NULL OR pp.verified <> 'conflict')
          ORDER BY p.page_number ASC
          LIMIT ?`,
    args: [...hits.args, boundedLimit + 1],
  });

  const byPage = new Map<number, SearchMatchPage>();
  let rowCount = 0;

  for (const row of result.rows) {
    rowCount += 1;
    if (rowCount > boundedLimit) break;

    const pageNumber = toInt(row.page_number);
    const content = String(row.content ?? "");
    const matches = findOCRSearchMatchesForQueries(content, queries, matchMode, scripts);
    if (pageNumber <= 0 || matches.length === 0) continue;

    const existing = byPage.get(pageNumber);
    if (existing) {
      existing.occurrence_count += matches.length;
      continue;
    }

    byPage.set(pageNumber, {
      page_number: pageNumber,
      printed_page: String(row.printed_page ?? "").trim() || null,
      occurrence_count: matches.length,
      snippet: buildOCRSearchExcerptForQueries(content, queries, matchMode, 260, scripts),
    });
  }

  return {
    pages: [...byPage.values()].sort((a, b) => a.page_number - b.page_number),
    truncated: result.rows.length > boundedLimit,
    queries,
  };
}
