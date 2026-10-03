export type PageRange = {
  start: number;
  end: number;
};

export type MappingRange = {
  adhikar: number | null;
  gatha: number | null;
  pageStart: number;
  pageEnd: number;
  anchorText?: string | null;
};

export type MappingSegment = {
  pdfUrl: string;
  pdfFileName: string;
  customId: string | null;
  bookCode: string | null;
  ranges: MappingRange[];
  pages: number[];
};

/** Units that are verses; a chapter opening or a page note is not. */
const VERSE_UNITS = new Set(["gatha", "shlok", "karika", "niryukti", "bhashya", "mool", "kalash"]);

/**
 * The rows a gatha lookup should use. A row's `unit` says what the index
 * pointed at: the same number can be both a sutra and a niryukti gatha
 * (Acharang), and chapter openings (gatha 0) and page notes are not verses at
 * all. Verse rows win when there are any; sutras are used when they are all a
 * book has; rows with no unit (before the column existed) are kept as verses.
 */
export function preferVerseRows<T extends { unit?: string | null }>(rows: T[]): T[] {
  const usable = rows.filter((row) => row.unit !== "chapter" && row.unit !== "other");
  const verses = usable.filter((row) => row.unit == null || VERSE_UNITS.has(row.unit));
  return verses.length ? verses : usable;
}

type SpanRow = {
  book_code?: string | null;
  pdf_url?: string | null;
  page_start: number;
  page_end?: number | null;
  next_page_start?: number | null;
};

/**
 * Repairs `page_end` of granth_gatha_map rows.
 *
 * The stored `page_end` is `next_page_start - 1`, but the last verse of every
 * adhikar has no `next_page_start` and was given the last page of the whole
 * book, so one verse could span hundreds of pages. Its real end is the page
 * before the next verse that starts anywhere later in the same PDF. Only the
 * book's very last verse keeps the stored end. Pass every row of the book (not
 * just the requested verses) so the following anchors are known; the source
 * rows are not changed.
 */
export function repairPageEnds<T extends SpanRow>(rows: T[]): T[] {
  const startsByFile = new Map<string, number[]>();
  const fileOf = (row: SpanRow) => `${row.book_code ?? ""}\u0000${row.pdf_url ?? ""}`;
  for (const row of rows) {
    const start = Number(row.page_start);
    if (!Number.isFinite(start)) continue;
    const list = startsByFile.get(fileOf(row)) ?? [];
    list.push(start);
    startsByFile.set(fileOf(row), list);
  }
  for (const list of startsByFile.values()) list.sort((a, b) => a - b);

  return rows.map((row) => {
    const start = Number(row.page_start);
    if (!Number.isFinite(start)) return row;
    const next = Number(row.next_page_start);
    let end: number;
    if (row.next_page_start != null && Number.isFinite(next)) {
      end = next - 1;
    } else {
      const following = startsByFile.get(fileOf(row))?.find((p) => p > start);
      end = following != null ? following - 1 : Number(row.page_end ?? start);
    }
    if (!Number.isFinite(end) || end < start) end = start;
    return end === row.page_end ? row : { ...row, page_end: end };
  });
}

export function parseNumberListSpec(spec: string) {
  const trimmed = String(spec || "").trim();
  if (!trimmed) return [];

  const out: number[] = [];
  for (const rawPart of trimmed.split(",")) {
    const part = rawPart.trim();
    if (!part) continue;

    if (part.includes("-")) {
      const bits = part.split("-").map((x) => x.trim()).filter(Boolean);
      if (bits.length !== 2) throw new Error(`Bad range: ${part}`);
      const a = Number.parseInt(bits[0], 10);
      const b = Number.parseInt(bits[1], 10);
      if (!Number.isFinite(a) || !Number.isFinite(b)) throw new Error(`Bad range: ${part}`);
      const start = Math.min(a, b);
      const end = Math.max(a, b);
      for (let n = start; n <= end; n += 1) out.push(n);
    } else {
      const n = Number.parseInt(part, 10);
      if (!Number.isFinite(n)) throw new Error(`Bad number: ${part}`);
      out.push(n);
    }
  }

  const seen = new Set<number>();
  return out.filter((n) => n > 0 && !seen.has(n) && seen.add(n));
}

export function pagesFromRanges(ranges: PageRange[], includeCover = false) {
  const pages = new Set<number>();
  if (includeCover) pages.add(1);

  for (const range of ranges) {
    const start = Math.max(1, Math.floor(range.start));
    const end = Math.max(start, Math.floor(range.end));
    for (let page = start; page <= end; page += 1) {
      pages.add(page);
    }
  }

  return [...pages].sort((a, b) => a - b);
}

export function groupSegments(
  rows: Array<{
    pdf_url: string | null;
    pdf_file_name: string;
    custom_id: string | null;
    book_code: string | null;
    adhikar?: number | null;
    gatha?: number | null;
    page_start: number;
    page_end?: number | null;
    anchor_text?: string | null;
  }>,
  includeCover = false
): MappingSegment[] {
  const byPdf = new Map<string, MappingSegment>();

  for (const row of rows) {
    if (!row.pdf_url) continue;
    const key = row.pdf_url;
    const existing =
      byPdf.get(key) ||
      ({
        pdfUrl: row.pdf_url,
        pdfFileName: row.pdf_file_name,
        customId: row.custom_id ?? null,
        bookCode: row.book_code ?? null,
        ranges: [],
        pages: [],
      } satisfies MappingSegment);

    const start = Number(row.page_start);
    const end = Math.max(start, Number(row.page_end || row.page_start));
    existing.ranges.push({
      adhikar: row.adhikar ?? null,
      gatha: row.gatha ?? null,
      pageStart: start,
      pageEnd: end,
      anchorText: row.anchor_text ?? null,
    });

    byPdf.set(key, existing);
  }

  for (const segment of byPdf.values()) {
    segment.ranges.sort((a, b) => a.pageStart - b.pageStart || (a.gatha || 0) - (b.gatha || 0));
    segment.pages = pagesFromRanges(
      segment.ranges.map((range) => ({ start: range.pageStart, end: range.pageEnd })),
      includeCover
    );
  }

  return [...byPdf.values()].sort((a, b) => a.pdfFileName.localeCompare(b.pdfFileName, "en"));
}
