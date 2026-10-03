// Data for the step-by-step extractor: works (a granth and its volumes), the
// chapters of a work in its own words, and the gathas of one chapter with the
// pages each one is on. Pages are worked out in the browser from these rows,
// so the preview, the page count and the exported file all come from one list.
import { getGranthCatalog, granthDisplayName, type GranthCatalogEntry } from "@/lib/granth-catalog";
import { preferVerseRows, repairPageEnds } from "@/lib/granth-mapping";
import { getSupabaseAdmin } from "@/lib/supabase-server";

export type ExtractVolume = {
  key: string;
  part: string | null;
  pageCount: number | null;
  pdfUrl: string;
  sizeBytes: number | null;
  gathas: number;
};

export type ExtractWork = {
  id: string;
  title: string;
  nativeTitle: string | null;
  author: string | null;
  cover: string | null;
  bookNumbers: string[];
  volumes: ExtractVolume[];
  gathas: number;
};

export type ExtractChapter = {
  id: string;
  /** The chapter as the index names it: "सर्ग 5", "कप्पसुत्तं › उदेश 3"; null for a book numbered straight through. */
  label: string | null;
  adhikar: number | null;
  parentPath: string | null;
  /** What a verse is called here: ગાથા, श्लोक, सूत्र … */
  unitLabel: string | null;
  first: number;
  last: number;
  count: number;
  /** Numbers that are present, compressed: [[1, 40], [42, 60]]. */
  ranges: Array<[number, number]>;
  volumeKeys: string[];
  pageStart: number;
  pageEnd: number;
};

export type ExtractGatha = {
  gatha: number;
  gathaTo: number | null;
  volumeKey: string;
  pdfUrl: string;
  pageStart: number;
  pageEnd: number;
  unit: string | null;
};

type MapRow = {
  book_code: string | null;
  pdf_url: string | null;
  adhikar: number | null;
  gatha: number;
  gatha_to: number | null;
  unit: string | null;
  unit_label: string | null;
  parent_path: string | null;
  page_start: number;
  page_end: number | null;
  next_page_start: number | null;
  sequence_index: number;
};

declare global {
  var __extractWorks: { value: ExtractWork[]; expiresAt: number } | undefined;
}

const WORKS_TTL_MS = 5 * 60 * 1000;
const MAP_COLUMNS =
  "book_code,pdf_url,adhikar,gatha,gatha_to,unit,unit_label,parent_path,page_start,page_end,next_page_start,sequence_index";

/** Every matching row; the pages after the count is known are fetched eight at a time. */
async function fetchAll<T>(table: string, columns: string, filter?: (q: any) => any): Promise<T[]> {
  const sb = getSupabaseAdmin();
  const page = (from: number) => {
    let query = sb.from(table).select(columns, from === 0 ? { count: "exact" } : undefined).range(from, from + 999);
    if (filter) query = filter(query);
    return query;
  };
  const first = await page(0);
  if (first.error) throw new Error(`${table}: ${first.error.message}`);
  const rows: T[] = [...((first.data ?? []) as T[])];
  const total = first.count ?? rows.length;
  const starts: number[] = [];
  for (let from = 1000; from < total; from += 1000) starts.push(from);
  for (let i = 0; i < starts.length; i += 8) {
    const batch = await Promise.all(starts.slice(i, i + 8).map((from) => page(from)));
    for (const res of batch) {
      if (res.error) throw new Error(`${table}: ${res.error.message}`);
      rows.push(...((res.data ?? []) as T[]));
    }
  }
  return rows;
}

/** One work per title; its volumes are the catalog's granths with a served PDF. */
export async function listWorks(): Promise<ExtractWork[]> {
  const cached = globalThis.__extractWorks;
  if (cached && cached.expiresAt > Date.now()) return cached.value;

  const catalog = await getGranthCatalog();
  const [files, counts] = await Promise.all([
    fetchAll<{ custom_id: string | null; ufs_url: string | null; cover_image_url: string | null; file_size: number | null }>(
      "granth_ocr_files",
      "custom_id,ufs_url,cover_image_url,file_size"
    ),
    fetchAll<{ book_code: string | null }>("granth_gatha_map", "book_code", (q) => q.not("book_code", "is", null).not("unit", "in", "(chapter,other)")),
  ]);
  // The same upload is keyed differently in the two tables; its URL is what they share.
  const fileByUrl = new Map(files.filter((f) => f.ufs_url).map((f) => [String(f.ufs_url), f]));
  const fileById = new Map(files.filter((f) => f.custom_id).map((f) => [String(f.custom_id), f]));
  const fileFor = (customId: string | null, pdfUrl: string | null) => (pdfUrl && fileByUrl.get(pdfUrl)) || (customId ? fileById.get(customId) : undefined);
  const gathaCount = new Map<string, number>();
  for (const row of counts) gathaCount.set(String(row.book_code), (gathaCount.get(String(row.book_code)) ?? 0) + 1);

  const groups = new Map<string, { entries: GranthCatalogEntry[] }>();
  for (const entry of catalog.entries) {
    if (entry.hidden || entry.text_only || entry.kind !== "pdf" || !entry.document_custom_id) continue;
    if (entry.duplicate_of && catalog.byGranthKey.has(entry.duplicate_of)) continue;
    const doc = catalog.documentsByCustomId.get(entry.document_custom_id);
    if (!doc?.pdf_url) continue;
    const id = (entry.series || entry.title || granthDisplayName(entry)).trim().toLowerCase();
    const group = groups.get(id) ?? { entries: [] };
    group.entries.push(entry);
    groups.set(id, group);
  }

  const works: ExtractWork[] = [];
  for (const [id, { entries }] of groups) {
    entries.sort((a, b) => volumeOrder(a) - volumeOrder(b) || a.granth_key.localeCompare(b.granth_key));
    const first = entries[0];
    const volumes = entries.map((entry) => {
      const doc = catalog.documentsByCustomId.get(String(entry.document_custom_id))!;
      const file = fileFor(entry.document_custom_id, doc.pdf_url);
      return {
        key: entry.granth_key,
        part: entry.part,
        pageCount: entry.page_count,
        pdfUrl: String(doc.pdf_url),
        sizeBytes: file?.file_size ?? null,
        gathas: gathaCount.get(entry.granth_key) ?? 0,
      };
    });
    const cover =
      entries
        .map((e) => fileFor(e.document_custom_id, catalog.documentsByCustomId.get(String(e.document_custom_id))?.pdf_url ?? null)?.cover_image_url)
        .find(Boolean) ?? null;
    works.push({
      id,
      title: first.series || first.title || granthDisplayName(first),
      nativeTitle: first.native_title || null,
      author: first.author,
      cover,
      bookNumbers: [...new Set(entries.map((e) => e.book_number).filter((n): n is string => Boolean(n) && !/^0+$/.test(String(n))))],
      volumes,
      gathas: volumes.reduce((n, v) => n + v.gathas, 0),
    });
  }
  works.sort((a, b) => a.title.localeCompare(b.title));
  globalThis.__extractWorks = { value: works, expiresAt: Date.now() + WORKS_TTL_MS };
  return works;
}

function volumeOrder(entry: GranthCatalogEntry) {
  const n = Number(String(entry.part ?? "").match(/\d+/)?.[0] ?? entry.book_number ?? 0);
  return Number.isFinite(n) ? n : 0;
}

export async function getWork(id: string) {
  return (await listWorks()).find((work) => work.id === id) ?? null;
}

async function mapRows(keys: string[]) {
  const rows = await fetchAll<MapRow>("granth_gatha_map", MAP_COLUMNS, (q) => q.in("book_code", keys).order("sequence_index"));
  // Page ends need every anchor of the PDF, chapter openings included.
  return repairPageEnds(rows) as Array<MapRow & { page_end: number }>;
}

function chapterId(row: Pick<MapRow, "parent_path" | "adhikar">) {
  return `${row.parent_path ?? ""}|${row.adhikar ?? ""}`;
}

function compress(numbers: number[]): Array<[number, number]> {
  const sorted = [...new Set(numbers)].sort((a, b) => a - b);
  const out: Array<[number, number]> = [];
  for (const n of sorted) {
    const last = out[out.length - 1];
    if (last && n === last[1] + 1) last[1] = n;
    else out.push([n, n]);
  }
  return out;
}

/** The chapters of a work, in reading order, each with the gatha numbers it has. */
export async function listChapters(work: ExtractWork): Promise<ExtractChapter[]> {
  const keys = work.volumes.filter((v) => v.gathas > 0).map((v) => v.key);
  if (!keys.length) return [];
  const order = new Map(work.volumes.map((v, i) => [v.key, i]));
  const rows = (await mapRows(keys))
    .filter((r) => r.unit !== "chapter" && r.unit !== "other")
    .sort((a, b) => (order.get(String(a.book_code)) ?? 0) - (order.get(String(b.book_code)) ?? 0) || a.sequence_index - b.sequence_index);

  const chapters = new Map<string, ExtractChapter & { numbers: number[] }>();
  for (const row of rows) {
    const id = chapterId(row);
    let ch = chapters.get(id);
    if (!ch) {
      ch = {
        id,
        label: chapterLabel(row),
        adhikar: row.adhikar,
        parentPath: row.parent_path,
        unitLabel: cleanUnitLabel(row.unit_label),
        first: row.gatha,
        last: row.gatha,
        count: 0,
        ranges: [],
        volumeKeys: [],
        pageStart: row.page_start,
        pageEnd: row.page_end,
        numbers: [],
      };
      chapters.set(id, ch);
    }
    const to = row.gatha_to && row.gatha_to >= row.gatha && row.gatha_to - row.gatha < 50 ? row.gatha_to : row.gatha;
    for (let n = row.gatha; n <= to; n += 1) ch.numbers.push(n);
    if (!ch.volumeKeys.includes(String(row.book_code))) ch.volumeKeys.push(String(row.book_code));
    ch.pageEnd = Math.max(ch.pageEnd, row.page_end);
  }
  return [...chapters.values()].map(({ numbers, ...ch }) => {
    const ranges = compress(numbers);
    return { ...ch, ranges, count: new Set(numbers).size, first: ranges[0]?.[0] ?? ch.first, last: ranges[ranges.length - 1]?.[1] ?? ch.last };
  });
}

function cleanUnitLabel(label: string | null) {
  if (!label) return null;
  return label.replace(/\s*\(part \d+\)$/, "").replace(/^continue\s+/, "").trim() || null;
}

/** "सर्ग 5", or the whole path where the levels above matter ("कप्पसुत्तं › उदेश 3"). */
function chapterLabel(row: MapRow) {
  if (!row.parent_path) return row.adhikar != null ? String(row.adhikar) : null;
  return row.parent_path
    .split(" › ")
    .map((level) =>
      level
        .replace(/\([^)]*\)/g, "")
        .replace(/અનુક્રમણિકા|अनुक्रमणिका/g, "")
        .replace(/\s*·\s*क्रम\s+(\d+)$/, " (repeat $1)")
        .replace(/\s+/g, " ")
        .trim()
    )
    .filter(Boolean)
    .join(" › ");
}

/** The gathas of one chapter with the PDF pages each is on; verse rows only. */
export async function chapterGathas(work: ExtractWork, chapter: string): Promise<ExtractGatha[]> {
  const keys = work.volumes.filter((v) => v.gathas > 0).map((v) => v.key);
  if (!keys.length) return [];
  const pdfByKey = new Map(work.volumes.map((v) => [v.key, v.pdfUrl]));
  const rows = (await mapRows(keys)).filter((r) => chapterId(r) === chapter);
  const byNumber = new Map<number, MapRow[]>();
  for (const r of rows) byNumber.set(r.gatha, [...(byNumber.get(r.gatha) ?? []), r]);
  const out: ExtractGatha[] = [];
  for (const list of byNumber.values()) {
    for (const r of preferVerseRows(list)) {
      const pdfUrl = pdfByKey.get(String(r.book_code)) || r.pdf_url;
      if (!pdfUrl) continue;
      out.push({
        gatha: r.gatha,
        gathaTo: r.gatha_to,
        volumeKey: String(r.book_code),
        pdfUrl,
        pageStart: r.page_start,
        pageEnd: Math.max(r.page_start, Math.min(r.page_end ?? r.page_start, r.page_start + 40)),
        unit: r.unit,
      });
    }
  }
  return out.sort((a, b) => a.gatha - b.gatha || a.pageStart - b.pageStart);
}

/** Every PDF the extractor may build from: only library files, never an arbitrary URL. */
export async function libraryPdfUrls(): Promise<Map<string, { pageCount: number | null }>> {
  const works = await listWorks();
  const out = new Map<string, { pageCount: number | null }>();
  for (const w of works) for (const v of w.volumes) out.set(v.pdfUrl, { pageCount: v.pageCount });
  return out;
}
