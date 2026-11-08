import { once } from "node:events";
import { createWriteStream, type WriteStream } from "node:fs";
import path from "node:path";

/** Excel needs the byte order mark to read Gujarati and Devanagari columns. */
const UTF8_BOM = "\uFEFF";

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

/**
 * The granth's 3-digit book number from the start of its file name
 * ("069_B037418_…" → "069", "226_prashamrati_…" → "226"), never the accession
 * or library number. A file with no serial falls back to the granth's name.
 */
export function bookNumber(relPath: string, granthName: string) {
  const serial = path.basename(relPath).match(/^(\d+)(?=[-_ .])/);
  return serial ? serial[1].padStart(3, "0") : granthName;
}

/** The page as printed in the granth; "PDF 123" where the printed number is unknown. */
export function granthPageLabel(printedPage: string | null | undefined, pdfPage: number) {
  return printedPage ? printedPage : `PDF ${pdfPage}`;
}

export function csvCell(value: unknown) {
  const text = value == null ? "" : String(value);
  const flattened = text.replace(/\r\n?|\n/g, " ").replace(/\t/g, " ");
  // Stop spreadsheets from evaluating OCR text that happens to start with an operator.
  const safe = /^[=+@]/.test(flattened) ? `'${flattened}` : flattened;
  if (/[",;]/.test(safe) || safe !== safe.trim()) return `"${safe.replace(/"/g, '""')}"`;
  return safe;
}

export function csvRow(cells: unknown[]) {
  return `${cells.map(csvCell).join(",")}\r\n`;
}

/** Streams CSV rows to disk so large exports never sit in memory as one string. */
export class CsvFileWriter {
  private stream: WriteStream;
  private rows = 0;

  constructor(filePath: string, headers: string[] = SEARCH_CSV_HEADERS) {
    this.stream = createWriteStream(filePath, { encoding: "utf8" });
    this.stream.write(`${UTF8_BOM}${csvRow(headers)}`);
  }

  get rowCount() {
    return this.rows;
  }

  async writeRow(cells: unknown[]) {
    this.rows += 1;
    if (!this.stream.write(csvRow(cells))) {
      await once(this.stream, "drain");
    }
  }

  async close() {
    await new Promise<void>((resolve, reject) => {
      this.stream.once("error", reject);
      this.stream.end(() => resolve());
    });
    return this.rows;
  }
}
