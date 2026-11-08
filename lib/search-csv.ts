import { once } from "node:events";
import { createWriteStream, type WriteStream } from "node:fs";
import path from "node:path";

/** Excel needs the byte order mark to read Gujarati and Devanagari columns. */
const UTF8_BOM = "\uFEFF";

import { SEARCH_CSV_HEADERS } from "@/lib/search-csv-columns";

export {
  DEFAULT_SEARCH_CSV_COLUMNS,
  SEARCH_CSV_COLUMNS,
  parseSearchCsvColumns,
  searchCsvHeaders,
  type SearchCsvColumn,
} from "@/lib/search-csv-columns";
export { SEARCH_CSV_HEADERS };

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
