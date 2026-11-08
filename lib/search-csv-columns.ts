// The search spreadsheet's columns, shared by the server (lib/search-csv) and
// the download dialog's column picker, so nothing here may use Node modules.

/**
 * Every column a search spreadsheet can have. The reader picks which go in
 * (Sahebji, 5 Nov 2026: "can the Excel sheet have only the columns we
 * need?"); by default the book number first and no granth name, PDF file or
 * matched-words columns (her marks on the sheet the same day).
 */
export const SEARCH_CSV_COLUMNS = [
  { id: "book", label: "Book number", default: true },
  { id: "granth", label: "Granth name", default: false },
  { id: "page", label: "Granth page number", default: true },
  { id: "line", label: "Line number on page", default: true },
  { id: "count", label: "Matches on line", default: true },
  { id: "words", label: "Matched words", default: false },
  { id: "text", label: "Line text", default: true },
  { id: "pdfPage", label: "PDF page number", default: true },
  { id: "pdfFile", label: "PDF file", default: false },
] as const;

export type SearchCsvColumn = (typeof SEARCH_CSV_COLUMNS)[number]["id"];

export const DEFAULT_SEARCH_CSV_COLUMNS: SearchCsvColumn[] = SEARCH_CSV_COLUMNS.filter((c) => c.default).map((c) => c.id);

/** The asked columns in the sheet's own order; the defaults when none valid are asked. */
export function parseSearchCsvColumns(value: unknown): SearchCsvColumn[] {
  const asked = new Set(Array.isArray(value) ? value.map(String) : []);
  const picked = SEARCH_CSV_COLUMNS.filter((c) => asked.has(c.id)).map((c) => c.id);
  return picked.length ? picked : DEFAULT_SEARCH_CSV_COLUMNS;
}

export const SEARCH_CSV_HEADERS = DEFAULT_SEARCH_CSV_COLUMNS.map((id) => SEARCH_CSV_COLUMNS.find((c) => c.id === id)!.label);

export function searchCsvHeaders(columns: SearchCsvColumn[]) {
  return columns.map((id) => SEARCH_CSV_COLUMNS.find((c) => c.id === id)!.label);
}

