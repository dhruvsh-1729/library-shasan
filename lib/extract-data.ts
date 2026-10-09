// Data for the step-by-step extractor: works (a granth and its volumes), the
// chapters of a work in its own words, and the gathas of one chapter with the
// pages each one is on. Pages are worked out in the browser from these rows,
// so the preview, the page count and the exported file all come from one list.
import { getGranthCatalog, granthDisplayName, type GranthCatalogEntry } from "@/lib/granth-catalog";
import { parseGranthFileName, stripPartSuffix, titleCase, titleKey } from "@/lib/granth-title.mjs";
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

type VolumeSource = {
  key: string;
  part: string | null;
  pageCount: number | null;
  pdfUrl: string;
  customId: string | null;
  /** Another file of a volume the work already has (a "- Copy", or an upload not yet searchable). */
  copy: boolean;
  order: number;
};

type WorkGroup = {
  title: string;
  nativeTitle: string | null;
  author: string | null;
  bookNumbers: string[];
  volumes: VolumeSource[];
};

/**
 * One work per title. Its volumes are every granth PDF in the library: the
 * catalog's granths (text-only ones and duplicates too, since their PDFs are
 * real files), plus uploads not searchable yet, which granth_library_files
 * ties to a library book. Gathas only come from the mapping, so the gatha
 * picker still lists mapped works alone; the page picker gets every PDF.
 */
export async function listWorks(): Promise<ExtractWork[]> {
  const cached = globalThis.__extractWorks;
  if (cached && cached.expiresAt > Date.now()) return cached.value;

  const catalog = await getGranthCatalog();
  const [files, counts, libraryFiles, books] = await Promise.all([
    fetchAll<{ custom_id: string | null; ufs_url: string | null; cover_image_url: string | null; file_size: number | null }>(
      "granth_ocr_files",
      "custom_id,ufs_url,cover_image_url,file_size"
    ),
    fetchAll<{ book_code: string | null }>("granth_gatha_map", "book_code", (q) => q.not("book_code", "is", null).not("unit", "in", "(chapter,other)")),
    fetchAll<{ custom_id: string | null; book_id: number | null; pdf_file_name: string | null; page_count: number | null }>(
      "granth_library_files",
      "custom_id,book_id,pdf_file_name,page_count"
    ),
    fetchAll<{ id: number; title_english: string | null; title_display: string | null; author_text: string | null }>(
      "granth_library_books",
      "id,title_english,title_display,author_text"
    ),
  ]);
  // The same upload is keyed differently in the two tables; its URL is what they share.
  const fileByUrl = new Map(files.filter((f) => f.ufs_url).map((f) => [String(f.ufs_url), f]));
  const fileById = new Map(files.filter((f) => f.custom_id).map((f) => [String(f.custom_id), f]));
  const fileFor = (customId: string | null, pdfUrl: string | null) => (pdfUrl && fileByUrl.get(pdfUrl)) || (customId ? fileById.get(customId) : undefined);
  const libraryFileById = new Map(libraryFiles.filter((f) => f.custom_id).map((f) => [String(f.custom_id), f]));
  const bookById = new Map(books.map((b) => [b.id, b]));
  const gathaCount = new Map<string, number>();
  for (const row of counts) gathaCount.set(String(row.book_code), (gathaCount.get(String(row.book_code)) ?? 0) + 1);

  const groups = new Map<string, WorkGroup>();
  const usedUrls = new Set<string>();
  const groupFor = (id: string, make: () => Omit<WorkGroup, "volumes">) => {
    let group = groups.get(id);
    if (!group) {
      group = { ...make(), volumes: [] };
      groups.set(id, group);
    }
    return group;
  };

  // Originals first, so a duplicate joins its original's work as a copy.
  const entries = catalog.entries
    .filter((entry) => !entry.hidden && entry.kind === "pdf" && entry.document_custom_id)
    .sort((a, b) => Number(Boolean(a.duplicate_of)) - Number(Boolean(b.duplicate_of)));
  const workOfKey = new Map<string, string>();
  // The library's archive number in a file name ("…_034258_hr3.pdf") names the volume across uploads.
  const volumeOfArchiveId = new Map<string, { group: WorkGroup; volume: VolumeSource }>();
  for (const entry of entries) {
    const doc = catalog.documentsByCustomId.get(String(entry.document_custom_id));
    if (!doc?.pdf_url) continue;
    const original = entry.duplicate_of ? catalog.byGranthKey.get(entry.duplicate_of) : undefined;
    if (original && usedUrls.has(doc.pdf_url)) continue;
    const id = (original && workOfKey.get(original.granth_key)) || (entry.series || entry.title || granthDisplayName(entry)).trim().toLowerCase();
    workOfKey.set(entry.granth_key, id);
    const group = groupFor(id, () => ({
      title: entry.series || entry.title || granthDisplayName(entry),
      nativeTitle: entry.native_title || null,
      author: entry.author,
      bookNumbers: [],
    }));
    const libraryPages = libraryFileById.get(String(entry.document_custom_id))?.page_count ?? null;
    const volume: VolumeSource = {
      key: entry.granth_key,
      part: entry.part,
      // A text-only granth's page count is its text's; the PDF's own count is the library's.
      pageCount: entry.text_only ? libraryPages ?? entry.page_count : entry.page_count ?? libraryPages,
      pdfUrl: String(doc.pdf_url),
      customId: entry.document_custom_id,
      copy: Boolean(original),
      order: original ? group.volumes.find((v) => v.key === original.granth_key)?.order ?? volumeOrder(entry) : volumeOrder(entry),
    };
    group.volumes.push(volume);
    if (!original) for (const archiveId of entry.archive_ids) if (!volumeOfArchiveId.has(archiveId)) volumeOfArchiveId.set(archiveId, { group, volume });
    if (entry.book_number && !/^0+$/.test(entry.book_number) && !group.bookNumbers.includes(entry.book_number)) group.bookNumbers.push(entry.book_number);
    usedUrls.add(String(doc.pdf_url));
  }

  // Uploads the catalog does not know yet: another file of a catalogued
  // volume when the archive number says so, else a volume of the library
  // book granth_library_files names. A PDF with neither, such as the
  // library's own book lists, is not a granth and is left out.
  for (const doc of catalog.documentsByCustomId.values()) {
    if (!doc.pdf_url || usedUrls.has(doc.pdf_url) || !doc.custom_id) continue;
    const libraryFile = libraryFileById.get(String(doc.custom_id));
    const parsed = parseGranthFileName(libraryFile?.pdf_file_name || doc.pdf_name || doc.original_relative_path);
    const pdfUrl = String(doc.pdf_url);
    const customId = String(doc.custom_id);
    const key = `file:${customId}`;

    const same = parsed.archiveIds.map((id) => volumeOfArchiveId.get(id)).find(Boolean);
    if (same) {
      same.group.volumes.push({
        key,
        part: same.volume.part,
        pageCount: libraryFile?.page_count ?? null,
        pdfUrl,
        customId,
        copy: true,
        order: same.volume.order,
      });
      usedUrls.add(pdfUrl);
      continue;
    }

    const book = libraryFile?.book_id != null ? bookById.get(libraryFile.book_id) : undefined;
    let group: WorkGroup | undefined;
    if (book) {
      const title = stripPartSuffix(book.title_english) || parsed.romanTitle || titleCase(parsed.stem.replace(/_/g, " "));
      group = groupFor(title.trim().toLowerCase(), () => ({
        title,
        nativeTitle: book.title_display || parsed.nativeTitle || null,
        author: book.author_text,
        bookNumbers: [],
      }));
    } else if (parsed.archiveIds.length && parsed.romanTitle) {
      // Another scan of a work, named only by its file: join the work its title spells.
      const wanted = looseTitleKey(parsed.romanTitle);
      group =
        [...groups.values()].find((g) => looseTitleKey(g.title) === wanted) ??
        groupFor(parsed.romanTitle.trim().toLowerCase(), () => ({
          title: parsed.romanTitle,
          nativeTitle: parsed.nativeTitle || null,
          author: null,
          bookNumbers: [],
        }));
    }
    if (!group) continue;
    const twin = group.volumes.find((v) => !v.copy && samePart(v.part, parsed.part));
    group.volumes.push({
      key,
      part: twin ? twin.part : parsed.part,
      pageCount: libraryFile?.page_count ?? null,
      pdfUrl,
      customId,
      copy: Boolean(twin),
      order: twin ? twin.order : Number(String(parsed.part ?? "").match(/\d+/)?.[0] ?? 0) || 0,
    });
    usedUrls.add(pdfUrl);
  }

  const works: ExtractWork[] = [];
  for (const [id, group] of groups) {
    group.volumes.sort((a, b) => a.order - b.order || Number(a.copy) - Number(b.copy) || a.key.localeCompare(b.key));
    const volumes = group.volumes.map((v) => ({
      key: v.key,
      part: v.copy ? copyLabel(v.part) : v.part,
      pageCount: v.pageCount,
      pdfUrl: v.pdfUrl,
      sizeBytes: fileFor(v.customId, v.pdfUrl)?.file_size ?? null,
      gathas: v.copy ? 0 : gathaCount.get(v.key) ?? 0,
    }));
    works.push({
      id,
      title: group.title,
      nativeTitle: group.nativeTitle,
      author: group.author,
      cover: group.volumes.map((v) => fileFor(v.customId, v.pdfUrl)?.cover_image_url).find(Boolean) ?? null,
      bookNumbers: group.bookNumbers,
      volumes,
      gathas: volumes.reduce((n, v) => n + v.gathas, 0),
    });
  }
  works.sort((a, b) => a.title.localeCompare(b.title));
  globalThis.__extractWorks = { value: works, expiresAt: Date.now() + WORKS_TTL_MS };
  return works;
}

/** titleKey without its a's, so "Sutrakritanga Sutra" finds "Sutrakritang Sutra". */
function looseTitleKey(title: string) {
  return titleKey(title).replace(/a/g, "");
}

function samePart(a: string | null, b: string | null) {
  const n = (p: string | null) => (p == null ? "" : String(Number(p)) === "NaN" ? p.trim() : String(Number(p)));
  return n(a) === n(b);
}

/** "Part 1 · other copy", so a second file of a volume is not mistaken for the next one. */
function copyLabel(part: string | null) {
  if (!part) return "Other copy";
  return /^\d+$/.test(part.trim()) ? `Part ${Number(part)} · other copy` : `${part} · other copy`;
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
