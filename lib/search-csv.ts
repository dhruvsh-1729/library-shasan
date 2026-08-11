import { once } from "node:events";
import { createWriteStream, type WriteStream } from "node:fs";

/** Excel needs the byte order mark to read Gujarati and Devanagari columns. */
const UTF8_BOM = "\uFEFF";

export const SEARCH_CSV_HEADERS = [
  "Granth name",
  "PDF file",
  "PDF page number",
  "Line number on page",
  "Matches on line",
  "Matched words",
  "Line text",
];

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
