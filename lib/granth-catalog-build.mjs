// Builds the granth catalog: one entry per indexed granth (a Turso
// ocr_granths row, the unit search works on), with its clean title and the
// uploaded PDF it belongs to.
//
// Plain JS so the app (lib/granth-catalog.ts) and the review script
// (scripts/granth_catalog_report.mjs) build exactly the same catalog. The
// source tables are never rewritten: titles are read out of the file names and
// the library's own book list, and data/granth-catalog-overrides.json holds the
// corrections a person has checked.
//
// The PDF link is decided here, once, from exact evidence (same path, or same
// book number and file name); a granth with no such evidence has no PDF and
// opens as OCR text. Guessing a PDF from shared title words is what sent the
// Acharang bhavanuvad hits to the Gujarati anuwad.

import { granthFileStem, parseGranthFileName, stripPartSuffix, titleCase, titleKey } from "./granth-title.mjs";

/** Turso paths of the text-only (spreadsheet) granths. */
export const TEXT_ONLY_PATH_SEGMENT = "GG 76 Prat OCR_ed/";
export const TEXT_ONLY_ID_PREFIX = "text:";

const clean = (value) => String(value ?? "").trim();

function stemKey(value) {
  return granthFileStem(value).toLowerCase().replace(/[^\p{L}\p{N}]+/gu, "");
}

/** "080-081" -> ["080", "081"]; numbers are compared without leading zeros. */
function numberSet(bookNumbers) {
  const m = clean(bookNumbers).match(/^(\d+)(?:-(\d+))?$/);
  if (!m) return new Set();
  const a = Number(m[1]);
  const b = m[2] ? Number(m[2]) : a;
  const out = new Set();
  for (let n = a; n <= b && n - a <= 20; n += 1) out.add(n);
  return out;
}

function sharesNumber(a, b) {
  for (const n of a) if (b.has(n)) return true;
  return false;
}

/** The native-script book title, without the per-part list some rows carry. */
function bookNativeTitle(book) {
  const display = clean(book?.title_display);
  if (!display) return "";
  // "उपमितिभवप्रपञ्चा कथा (…) ભાગ 01 उपमितिभवप्रपञ्चा कथा (…) ભાગ 02" lists every part.
  return display
    .split(/\s*(?:ભાગ|भाग)[\s-]*[0-9૦-૯०-९]+/u)[0]
    .replace(/\s*\([BC]\d{6}\)\s*$/i, "")
    .trim();
}

/**
 * @param {{
 *   turso: Array<{ granth_key: string, book_number?: string, library_code?: string | null, granth_name: string, source_rel_path: string, page_count?: number }>,
 *   documents: Array<{ custom_id: string, original_relative_path: string | null, pdf_name: string | null, pdf_url?: string | null, status?: string | null }>,
 *   libraryFiles: Array<{ custom_id: string | null, book_id: number | null }>,
 *   books: Array<{ id: number, title_english: string | null, title_display: string | null, author_text: string | null, book_codes?: string[] }>,
 *   overrides?: { granths?: Record<string, Record<string, unknown>>, books?: Record<string, Record<string, unknown>> },
 * }} sources
 */
export function buildGranthCatalog({ turso, documents, libraryFiles, books: rawBooks, overrides = {} }) {
  const granthOverrides = overrides.granths ?? {};
  // Corrections to the library's book list (a title copied from the row above…).
  const books = rawBooks.map((book) => ({ ...book, ...(overrides.books?.[String(book.id)] ?? {}) }));
  const docs = documents.filter((doc) => clean(doc.custom_id));
  const docByPath = new Map(docs.map((doc) => [clean(doc.original_relative_path), doc]));
  const docByCustomId = new Map(docs.map((doc) => [clean(doc.custom_id), doc]));
  const docParsed = new Map(docs.map((doc) => [doc, parseGranthFileName(doc.pdf_name || doc.original_relative_path)]));
  const bookById = new Map(books.map((book) => [book.id, book]));
  const issues = [];
  const bookByCustomId = new Map();
  const filesPerBook = new Map();
  for (const file of libraryFiles) {
    if (file.book_id == null) continue;
    filesPerBook.set(file.book_id, (filesPerBook.get(file.book_id) ?? 0) + 1);
    const book = bookById.get(file.book_id);
    const doc = docByCustomId.get(clean(file.custom_id));
    if (!book || !doc) continue;
    // Trust the link only when the book lists this file's number.
    const numbers = numberSet(docParsed.get(doc).bookNumbers);
    const codes = new Set((book.book_codes ?? []).map((code) => Number(code)));
    if (numbers.size && codes.size && !sharesNumber(numbers, codes)) {
      issues.push({ granth_key: clean(doc.pdf_name), problem: "library book list links a file its codes do not include", candidates: [String(book.title_english)] });
      continue;
    }
    bookByCustomId.set(clean(file.custom_id), book);
  }

  const exactPaths = new Set(turso.map((row) => clean(row.source_rel_path)));

  /** The uploaded PDF an indexed granth is, from exact evidence only. */
  function matchDocument(row, parsed) {
    const relPath = clean(row.source_rel_path);
    const exact = docByPath.get(relPath);
    if (exact) return { doc: exact, how: "path" };
    if (relPath.includes(TEXT_ONLY_PATH_SEGMENT)) return { doc: null, how: "text-only" };

    // Re-indexed from a spreadsheet under another folder: the same book number
    // and the same file name (codes and all), and no granth of its own.
    const numbers = numberSet(parsed.bookNumbers || row.book_number);
    const key = stemKey(relPath);
    const candidates = docs.filter((doc) => {
      if (exactPaths.has(clean(doc.original_relative_path))) return false;
      const other = docParsed.get(doc);
      if (numbers.size && !sharesNumber(numbers, numberSet(other.bookNumbers))) return false;
      if (!numbers.size && other.bookNumbers) return false;
      return stemKey(doc.pdf_name || doc.original_relative_path) === key;
    });
    if (candidates.length === 1) return { doc: candidates[0], how: "file-name" };
    if (candidates.length > 1) {
      issues.push({ granth_key: row.granth_key, problem: "several PDFs share this file name", candidates: candidates.map((d) => d.pdf_name) });
    }
    return { doc: null, how: "none" };
  }

  const entries = turso.map((row) => {
    const granthKey = clean(row.granth_key);
    const override = granthOverrides[granthKey] ?? {};
    const relPath = clean(row.source_rel_path);
    const textOnly = relPath.includes(TEXT_ONLY_PATH_SEGMENT);
    const fromPath = parseGranthFileName(relPath);

    let { doc, how } = matchDocument(row, fromPath);
    if (Object.prototype.hasOwnProperty.call(override, "document_custom_id")) {
      doc = override.document_custom_id ? docByCustomId.get(clean(override.document_custom_id)) ?? null : null;
      how = "override";
      if (override.document_custom_id && !doc) {
        issues.push({ granth_key: granthKey, problem: "override names a document that does not exist", candidates: [override.document_custom_id] });
      }
    }

    const book = doc ? bookByCustomId.get(clean(doc.custom_id)) : undefined;
    const parsed = doc ? docParsed.get(doc) : fromPath;
    const fromName = parseGranthFileName(row.granth_name);

    const bookEnglish = stripPartSuffix(book?.title_english);
    const bookSingleFile = book ? (filesPerBook.get(book.id) ?? 0) <= 1 : false;
    let title = parsed.romanTitle || fromName.romanTitle;
    const sameWorkAsBook = Boolean(bookEnglish) && (!title || titleKey(title) === titleKey(bookEnglish));
    // The library list's own spelling reads better than a file slug.
    if (sameWorkAsBook || (bookSingleFile && !title)) title = bookEnglish;

    let nativeTitle = parsed.nativeTitle || fromName.nativeTitle || fromPath.nativeTitle;
    if (!nativeTitle && book && (sameWorkAsBook || bookSingleFile)) nativeTitle = bookNativeTitle(book);

    const part = parsed.part ?? fromName.part ?? fromPath.part ?? null;
    const volumeSeries = parsed.series ?? fromName.series ?? null;
    const volume = parsed.volume ?? fromName.volume ?? null;
    const entry = {
      granth_key: granthKey,
      source_rel_path: relPath,
      /** The id the search picker and links use for this granth. */
      custom_id: doc ? clean(doc.custom_id) : `${TEXT_ONLY_ID_PREFIX}${granthKey}`,
      kind: doc ? "pdf" : "text",
      document_custom_id: doc ? clean(doc.custom_id) : null,
      document_rel_path: doc ? clean(doc.original_relative_path) : null,
      pdf_link: how,
      book_number: clean(parsed.bookNumbers || row.book_number) || null,
      library_code: parsed.libraryCode || fromPath.libraryCode || clean(row.library_code) || null,
      archive_ids: [...new Set([...parsed.archiveIds, ...fromPath.archiveIds])],
      title: clean(override.title) || title || "",
      native_title: clean(override.native_title) || nativeTitle || "",
      part: override.part === undefined ? part : override.part == null ? null : String(override.part),
      /** "Agam Satik, Vol. 5", or the library's book the file is one work of. */
      series: clean(override.series) || (volumeSeries ? `${volumeSeries}, Vol. ${volume}` : bookEnglish && !sameWorkAsBook ? bookEnglish : null),
      author: clean(override.author) || clean(book?.author_text) || null,
      page_count: Number(row.page_count ?? 0) || null,
      duplicate_of: clean(override.duplicate_of) || null,
      /** The name as stored, kept so nothing about the source is lost. */
      source_name: clean(doc?.pdf_name) || clean(row.granth_name),
      tags: [...new Set([...parsed.tags, ...fromPath.tags])],
      hidden: override.hidden === true,
      text_only: textOnly,
    };
    if (!entry.title && !entry.native_title) entry.title = titleCase(fromPath.stem.replace(/_/g, " "));
    return entry;
  });

  // A PDF with two indexed granths would be searched twice for one book.
  const byDocument = new Map();
  for (const entry of entries) {
    if (!entry.document_custom_id) continue;
    const list = byDocument.get(entry.document_custom_id) ?? [];
    list.push(entry.granth_key);
    byDocument.set(entry.document_custom_id, list);
  }
  for (const [customId, keys] of byDocument) {
    if (keys.length > 1) issues.push({ granth_key: keys.join(", "), problem: "one PDF has several indexed granths", candidates: [customId] });
  }

  return { entries, issues };
}

/** "Adhyatmasar Shabdasha Vivechan · Part 3" */
export function granthDisplayName(entry) {
  const name = entry.title || entry.native_title || entry.source_name || entry.granth_key;
  return entry.part != null ? `${name} · Part ${entry.part}` : name;
}
