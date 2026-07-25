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
