import { getTursoClient } from "@/lib/turso";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import {
  findOCRSearchMatchesForQueries,
  buildOCRSearchOccurrences,
  parseOCRSearchMode,
  type OCRSearchMode,
} from "@/lib/ocr-search";
import { buildOCRPrefilter } from "@/lib/ocr-search-index";

/**
 * Everything the assistant is allowed to talk about is fetched here, verbatim
 * from the OCR text. Nothing is paraphrased or re-fetched later, so an answer
 * can always be traced back to the exact pages listed in `passages`.
 */

export type ScopeKind = "gatha" | "pages" | "search";

export type ContextScope =
  | { kind: "gatha"; granthKey: string; adhikar?: number | null; gathaFrom: number; gathaTo?: number | null }
  | { kind: "pages"; granthKey: string; pageFrom: number; pageTo?: number | null }
  | { kind: "search"; query: string; matchMode?: OCRSearchMode; granthKeys?: string[] };

export type Passage = {
  granthKey: string;
  granthName: string;
  pageNumber: number;
  label: string;
  text: string;
};

/** One verse and the pages it actually occupies, after the spans are repaired. */
export type VerseSpan = {
  adhikar: number | null;
  gatha: number;
  pageStart: number;
  pageEnd: number;
};

export type AdhikarSummary = {
  adhikar: number;
  gathaFrom: number;
  gathaTo: number;
  pageStart: number;
  pageEnd: number;
};

export type ResolvedContext = {
  passages: Passage[];
  totalChars: number;
  truncated: boolean;
  summaryLine: string;
  /** The verses the scope resolved to, in reading order. Empty for page/word scopes. */
  verses: VerseSpan[];
  /** Verses that were dropped because the scope did not fit. */
  droppedVerses: VerseSpan[];
  /**
   * Set when a gatha number exists in several adhikars and none was chosen.
   * The caller must ask the reader which chapter they meant instead of
   * answering, because the pages would otherwise come from the wrong one.
   */
  needsAdhikar: AdhikarSummary[] | null;
};

/**
 * Sarvam's chat models expose a 32,000-token window. Measured on this corpus,
 * Gujarati and Sanskrit OCR runs about 2.4 characters per token, so 46,000
 * characters of scripture is roughly 19,000 tokens. That leaves room for the
 * system prompt, a couple of earlier turns and the answer itself. Anything
 * larger is split across several calls rather than silently cut short.
 */
export const MAX_CONTEXT_CHARS = 46_000;
export const CHARS_PER_TOKEN = 2.4;
/** How many model calls a single question may be spread over. */
export const MAX_CHUNKS = 4;
const MAX_PAGES_PER_REQUEST = 60;
const MAX_SEARCH_PASSAGES = 40;

/** A verse whose repaired span is longer than this means the mapping is wrong. */
const MAX_PLAUSIBLE_VERSE_PAGES = 30;

function clampPages(from: number, to: number) {
  const lo = Math.max(1, Math.floor(from));
  const hi = Math.max(lo, Math.floor(to));
  return { lo, hi: Math.min(hi, lo + MAX_PAGES_PER_REQUEST - 1) };
}

async function granthNames(keys: string[]) {
  if (!keys.length) return new Map<string, string>();
  const client = getTursoClient();
  const res = await client.execute({
    sql: `SELECT granth_key, granth_name FROM ocr_granths
          WHERE granth_key IN (${keys.map(() => "?").join(",")})`,
    args: keys,
  });
  return new Map(res.rows.map((r) => [String(r.granth_key), String(r.granth_name ?? r.granth_key)]));
}

async function fetchPages(granthKey: string, pages: number[]) {
  if (!pages.length) return [];
  const client = getTursoClient();
  const res = await client.execute({
    sql: `SELECT page_number, content FROM ocr_pages
          WHERE granth_key = ? AND page_number IN (${pages.map(() => "?").join(",")})
          ORDER BY page_number`,
    args: [granthKey, ...pages],
  });
  return res.rows.map((r) => ({ pageNumber: Number(r.page_number), text: String(r.content ?? "") }));
}

async function fetchPageRange(granthKey: string, lo: number, hi: number) {
  const client = getTursoClient();
  const res = await client.execute({
    sql: `SELECT page_number, content FROM ocr_pages
          WHERE granth_key = ? AND page_number BETWEEN ? AND ?
          ORDER BY page_number`,
    args: [granthKey, lo, hi],
  });
  return res.rows.map((r) => ({ pageNumber: Number(r.page_number), text: String(r.content ?? "") }));
}

type MapRow = {
  adhikar: number | null;
  gatha: number | null;
  page_start: number;
  page_end: number | null;
  next_page_start: number | null;
  page_count: number | null;
};

/**
 * Repairs the page span of each verse.
 *
 * `page_end` in granth_gatha_map is `next_page_start - 1`, which is right for
 * every row that has a next anchor. The last verse of each adhikar has no
 * `next_page_start`, and its `page_end` was filled in with the last page of the
 * whole book — so gatha 3/8 of Ashtak Prakaran claimed pages 68 to 351. The
 * real end is the first anchor that starts after it, anywhere in the book.
 */
function repairSpans(rows: MapRow[], followingStarts: number[]): VerseSpan[] {
  const spans: VerseSpan[] = [];

  for (const row of rows) {
    const start = Number(row.page_start);
    if (!Number.isFinite(start)) continue;
    const gatha = Number(row.gatha);
    const pageCount = Number(row.page_count);

    let end: number;
    if (row.next_page_start != null && Number.isFinite(Number(row.next_page_start))) {
      end = Number(row.next_page_start) - 1;
    } else {
      const nextAnchor = followingStarts.find((p) => p > start);
      // No later anchor means this really is the last verse of the book.
      end = nextAnchor != null ? nextAnchor - 1 : Number.isFinite(pageCount) ? pageCount : start;
    }

    if (!Number.isFinite(end) || end < start) end = start;
    // One broken row must not be able to drag a whole chapter into the answer.
    end = Math.min(end, start + MAX_PLAUSIBLE_VERSE_PAGES - 1);

    spans.push({
      adhikar: row.adhikar ?? null,
      gatha: Number.isFinite(gatha) ? gatha : 0,
      pageStart: start,
      pageEnd: end,
    });
  }

  return spans;
}

async function resolveGathaSpans(scope: Extract<ContextScope, { kind: "gatha" }>) {
  const sb = getSupabaseAdmin();
  let q = sb
    .from("granth_gatha_map")
    .select("adhikar,gatha,page_start,page_end,next_page_start,page_count")
    .eq("book_code", scope.granthKey)
    .gte("gatha", scope.gathaFrom)
    .lte("gatha", scope.gathaTo ?? scope.gathaFrom)
    .order("sequence_index");
  if (scope.adhikar != null) q = q.eq("adhikar", scope.adhikar);

  const { data, error } = await q;
  if (error) throw new Error(`gatha lookup failed: ${error.message}`);
  if (!data?.length) return null;

  const rows = data as MapRow[];

  // Only the rows with no `next_page_start` need to look further down the book.
  const openStarts = rows.filter((r) => r.next_page_start == null).map((r) => Number(r.page_start));
  let followingStarts: number[] = [];
  if (openStarts.length) {
    const { data: after, error: afterError } = await sb
      .from("granth_gatha_map")
      .select("page_start")
      .eq("book_code", scope.granthKey)
      .gt("page_start", Math.min(...openStarts))
      .order("page_start", { ascending: true })
      .limit(1000);
    if (afterError) throw new Error(`gatha boundary lookup failed: ${afterError.message}`);
    followingStarts = (after ?? []).map((r) => Number(r.page_start)).filter(Number.isFinite);
  }

  const spans = repairSpans(rows, followingStarts);
  const adhikars = [...new Set(rows.map((r) => r.adhikar).filter((a): a is number => a != null))];
  return { spans, adhikars };
}

/** The chapters of a granth, for the picker and for the "which one did you mean" reply. */
export async function listAdhikars(granthKey: string): Promise<AdhikarSummary[]> {
  const sb = getSupabaseAdmin();
  const rows: Array<{ adhikar: number | null; gatha: number | null; page_start: number }> = [];
  const pageSize = 1000;

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await sb
      .from("granth_gatha_map")
      .select("adhikar,gatha,page_start")
      .eq("book_code", granthKey)
      .order("sequence_index")
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`adhikar lookup failed: ${error.message}`);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < pageSize) break;
  }

  const byAdhikar = new Map<number, AdhikarSummary>();
  for (const row of rows) {
    if (row.adhikar == null) continue;
    const gatha = Number(row.gatha);
    const page = Number(row.page_start);
    const existing = byAdhikar.get(row.adhikar);
    if (!existing) {
      byAdhikar.set(row.adhikar, {
        adhikar: row.adhikar,
        gathaFrom: gatha,
        gathaTo: gatha,
        pageStart: page,
        pageEnd: page,
      });
      continue;
    }
    if (Number.isFinite(gatha)) {
      existing.gathaFrom = Math.min(existing.gathaFrom, gatha);
      existing.gathaTo = Math.max(existing.gathaTo, gatha);
    }
    if (Number.isFinite(page)) {
      existing.pageStart = Math.min(existing.pageStart, page);
      existing.pageEnd = Math.max(existing.pageEnd, page);
    }
  }

  return [...byAdhikar.values()].sort((a, b) => a.adhikar - b.adhikar);
}

function versesLabel(verses: VerseSpan[]) {
  if (!verses.length) return "";
  const gathas = verses.map((v) => v.gatha).filter((g) => g > 0);
  if (!gathas.length) return "";
  const lo = Math.min(...gathas);
  const hi = Math.max(...gathas);
  return lo === hi ? `gatha ${lo}` : `gathas ${lo}–${hi}`;
}

/** Which verses sit on a given page, for the passage label the model reads. */
function pageVerseIndex(verses: VerseSpan[]) {
  const byPage = new Map<number, VerseSpan[]>();
  for (const v of verses) {
    for (let p = v.pageStart; p <= v.pageEnd; p += 1) {
      const list = byPage.get(p) ?? [];
      list.push(v);
      byPage.set(p, list);
    }
  }
  return byPage;
}

function emptyContext(summaryLine: string): ResolvedContext {
  return {
    passages: [],
    totalChars: 0,
    truncated: false,
    summaryLine,
    verses: [],
    droppedVerses: [],
    needsAdhikar: null,
  };
}

function toInt(value: unknown, fallback: number | null = null) {
  const n = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(n) ? n : fallback;
}

/** Validates what the page sent. Shared by the ask and scope endpoints. */
export function parseScope(raw: Record<string, unknown> | undefined): ContextScope | { error: string } {
  const kind = String(raw?.kind ?? "");
  if (kind === "gatha") {
    const granthKey = String(raw?.granthKey ?? "").trim();
    const gathaFrom = toInt(raw?.gathaFrom);
    if (!granthKey) return { error: "Choose a granth." };
    if (gathaFrom == null || gathaFrom < 1) return { error: "Enter a starting gatha number." };
    return {
      kind: "gatha",
      granthKey,
      adhikar: raw?.adhikar == null || raw.adhikar === "" ? null : toInt(raw.adhikar),
      gathaFrom,
      gathaTo: raw?.gathaTo == null || raw.gathaTo === "" ? null : toInt(raw.gathaTo),
    };
  }
  if (kind === "pages") {
    const granthKey = String(raw?.granthKey ?? "").trim();
    const pageFrom = toInt(raw?.pageFrom);
    if (!granthKey) return { error: "Choose a granth." };
    if (pageFrom == null || pageFrom < 1) return { error: "Enter a starting page number." };
    return { kind: "pages", granthKey, pageFrom, pageTo: raw?.pageTo == null || raw.pageTo === "" ? null : toInt(raw.pageTo) };
  }
  if (kind === "search") {
    const query = String(raw?.query ?? "").trim();
    if (query.length < 2) return { error: "Enter at least two characters to search for." };
    return {
      kind: "search",
      query,
      matchMode: parseOCRSearchMode(raw?.matchMode),
      granthKeys: Array.isArray(raw?.granthKeys) ? raw.granthKeys.map(String) : [],
    };
  }
  return { error: "Pick what to look at: a gatha, a page range, or a word." };
}

export type ScopePreview = {
  pages: number[];
  verses: VerseSpan[];
  needsAdhikar: AdhikarSummary[] | null;
  summaryLine: string;
};

/**
 * Which pages the assistant would read, without pulling their text.
 *
 * The page calls this on every change to the form, so it must stay cheap: it
 * touches the mapping only, never the OCR store.
 */
export async function previewScope(scope: ContextScope): Promise<ScopePreview | null> {
  if (scope.kind === "pages") {
    const { lo, hi } = clampPages(scope.pageFrom, scope.pageTo ?? scope.pageFrom);
    const pages: number[] = [];
    for (let p = lo; p <= hi; p += 1) pages.push(p);
    return { pages, verses: [], needsAdhikar: null, summaryLine: `pages ${lo}–${hi}` };
  }
  if (scope.kind !== "gatha") return null;

  const hit = await resolveGathaSpans(scope);
  if (!hit) return { pages: [], verses: [], needsAdhikar: null, summaryLine: "No such gatha in the mapping." };

  const range = scope.gathaTo && scope.gathaTo !== scope.gathaFrom
    ? `${scope.gathaFrom}–${scope.gathaTo}`
    : `${scope.gathaFrom}`;

  if (scope.adhikar == null && hit.adhikars.length > 1) {
    return {
      pages: [],
      verses: [],
      needsAdhikar: await listAdhikars(scope.granthKey),
      summaryLine: `gatha ${range} exists in ${hit.adhikars.length} different adhikars of this granth.`,
    };
  }

  const pages = [...pageVerseIndex(hit.spans).keys()].sort((a, b) => a - b).slice(0, MAX_PAGES_PER_REQUEST);
  const where = scope.adhikar != null ? `adhikar ${scope.adhikar}, ` : "";
  const span = pages.length
    ? pages.length === 1 ? `page ${pages[0]}` : `pages ${pages[0]}–${pages[pages.length - 1]} (${pages.length})`
    : "no pages";
  return { pages, verses: hit.spans, needsAdhikar: null, summaryLine: `${where}gatha ${range} — ${span}` };
}

export async function resolveContext(scope: ContextScope): Promise<ResolvedContext> {
  if (scope.kind === "gatha") return resolveGathaContext(scope);
  if (scope.kind === "pages") return resolvePagesContext(scope);
  return resolveSearchContext(scope);
}

async function resolveGathaContext(scope: Extract<ContextScope, { kind: "gatha" }>): Promise<ResolvedContext> {
  const hit = await resolveGathaSpans(scope);
  if (!hit) return emptyContext("No such gatha in the mapping.");

  // Every granth numbers its gathas from 1 inside each adhikar, so an
  // unqualified "gatha 1–8" exists once per chapter. Reading the first pages
  // that come back means answering about chapter 1 when chapter 3 was meant,
  // which is worse than asking. So ask.
  if (scope.adhikar == null && hit.adhikars.length > 1) {
    const chapters = await listAdhikars(scope.granthKey);
    const range = scope.gathaTo && scope.gathaTo !== scope.gathaFrom
      ? `${scope.gathaFrom}–${scope.gathaTo}`
      : `${scope.gathaFrom}`;
    return {
      ...emptyContext(`gatha ${range} exists in ${hit.adhikars.length} different adhikars of this granth.`),
      needsAdhikar: chapters.length ? chapters : hit.adhikars.map((a) => ({
        adhikar: a, gathaFrom: 0, gathaTo: 0, pageStart: 0, pageEnd: 0,
      })),
    };
  }

  const names = await granthNames([scope.granthKey]);
  const name = names.get(scope.granthKey) ?? scope.granthKey;

  const verses = hit.spans;
  const byPage = pageVerseIndex(verses);
  const allPages = [...byPage.keys()].sort((a, b) => a - b);
  const wanted = allPages.slice(0, MAX_PAGES_PER_REQUEST);

  const rows = await fetchPages(scope.granthKey, wanted);
  const passages: Passage[] = rows.map((page) => {
    const on = byPage.get(page.pageNumber) ?? [];
    const label = on.length
      ? `page ${page.pageNumber} — ${on[0].adhikar != null ? `adhikar ${on[0].adhikar}, ` : ""}${versesLabel(on)}`
      : `page ${page.pageNumber}`;
    return { granthKey: scope.granthKey, granthName: name, pageNumber: page.pageNumber, label, text: page.text };
  });

  const { kept, truncated, lastPage } = trimToBudget(passages, wanted.length < allPages.length);
  const droppedVerses = truncated
    ? verses.filter((v) => lastPage != null && v.pageEnd > lastPage)
    : [];

  const range = scope.gathaTo && scope.gathaTo !== scope.gathaFrom
    ? `${scope.gathaFrom}–${scope.gathaTo}`
    : `${scope.gathaFrom}`;
  const where = scope.adhikar != null ? `adhikar ${scope.adhikar}, ` : "";
  const readPages = kept.map((p) => p.pageNumber);
  const pageSpan = readPages.length
    ? readPages.length === 1
      ? `page ${readPages[0]}`
      : `pages ${readPages[0]}–${readPages[readPages.length - 1]} (${readPages.length})`
    : "no pages";

  return {
    passages: kept,
    totalChars: kept.reduce((sum, p) => sum + p.text.length, 0),
    truncated,
    summaryLine: `${name} — ${where}gatha ${range} — ${pageSpan}`,
    verses,
    droppedVerses,
    needsAdhikar: null,
  };
}

async function resolvePagesContext(scope: Extract<ContextScope, { kind: "pages" }>): Promise<ResolvedContext> {
  const { lo, hi } = clampPages(scope.pageFrom, scope.pageTo ?? scope.pageFrom);
  const names = await granthNames([scope.granthKey]);
  const name = names.get(scope.granthKey) ?? scope.granthKey;

  const passages: Passage[] = (await fetchPageRange(scope.granthKey, lo, hi)).map((p) => ({
    granthKey: scope.granthKey,
    granthName: name,
    pageNumber: p.pageNumber,
    label: `page ${p.pageNumber}`,
    text: p.text,
  }));

  const { kept, truncated } = trimToBudget(passages, (scope.pageTo ?? scope.pageFrom) > hi);
  return {
    passages: kept,
    totalChars: kept.reduce((sum, p) => sum + p.text.length, 0),
    truncated,
    summaryLine: `${name} — pages ${lo}–${hi}`,
    verses: [],
    droppedVerses: [],
    needsAdhikar: null,
  };
}

async function resolveSearchContext(scope: Extract<ContextScope, { kind: "search" }>): Promise<ResolvedContext> {
  const client = getTursoClient();
  const mode: OCRSearchMode = scope.matchMode ?? "sanskrit_forms";
  const keys = (scope.granthKeys ?? []).filter(Boolean).slice(0, 25);
  const filter = keys.length ? ` AND p.granth_key IN (${keys.map(() => "?").join(",")})` : "";
  const prefilter = buildOCRPrefilter([scope.query], mode);
  const res = await client.execute({
    sql: `SELECT p.granth_key, p.page_number, p.content, g.granth_name
          FROM ${prefilter.table} f
          JOIN ocr_pages p ON p.id = f.rowid
          JOIN ocr_granths g ON g.granth_key = p.granth_key
          WHERE ${prefilter.table} MATCH ?${filter}
          LIMIT ?`,
    args: [prefilter.match, ...keys, MAX_SEARCH_PASSAGES * 4],
  });

  const passages: Passage[] = [];
  let truncated = false;
  for (const row of res.rows) {
    const content = String(row.content ?? "");
    const hits = findOCRSearchMatchesForQueries(content, scope.query, mode);
    if (!hits.length) continue;
    const occ = buildOCRSearchOccurrences(content, scope.query, mode, 700);
    passages.push({
      granthKey: String(row.granth_key),
      granthName: String(row.granth_name ?? row.granth_key),
      pageNumber: Number(row.page_number),
      label: `page ${row.page_number} (${hits.length} match${hits.length === 1 ? "" : "es"})`,
      text: occ.slice(0, 3).map((o) => o.snippet).join("\n…\n"),
    });
    if (passages.length >= MAX_SEARCH_PASSAGES) { truncated = true; break; }
  }

  const trimmed = trimToBudget(passages, truncated);
  return {
    passages: trimmed.kept,
    totalChars: trimmed.kept.reduce((sum, p) => sum + p.text.length, 0),
    truncated: trimmed.truncated,
    summaryLine: `“${scope.query}” — ${trimmed.kept.length} page${trimmed.kept.length === 1 ? "" : "s"}${keys.length ? ` in ${keys.length} selected granth(s)` : " across the library"}`,
    verses: [],
    droppedVerses: [],
    needsAdhikar: null,
  };
}

/**
 * Trims from the end so the earliest passages survive. The budget is what one
 * call can hold; a scope larger than this is answered in several passes by the
 * caller rather than thrown away here, so the cap is generous.
 */
function trimToBudget(passages: Passage[], alreadyTruncated: boolean) {
  const ceiling = MAX_CONTEXT_CHARS * MAX_CHUNKS;
  let total = 0;
  let truncated = alreadyTruncated;
  const kept: Passage[] = [];

  for (const p of passages) {
    if (total + p.text.length > ceiling && kept.length) { truncated = true; break; }
    total += p.text.length;
    kept.push(p);
  }

  return { kept, truncated, lastPage: kept.length ? kept[kept.length - 1].pageNumber : null };
}

/**
 * Splits the passages into groups that each fit in one call.
 *
 * The split follows verse boundaries wherever it can. A chunk that ends
 * mid-verse leaves both halves with a fragment of an argument, and the answers
 * written from them contradict each other in the merge.
 */
export function chunkPassages(passages: Passage[], verses: VerseSpan[] = []) {
  const byPage = pageVerseIndex(verses);
  const keyOf = (page: number) => (byPage.get(page) ?? []).map((v) => `${v.adhikar}/${v.gatha}`).join("+");

  // Pages covering the same verse (or set of verses) must stay together.
  const groups: Passage[][] = [];
  for (const p of passages) {
    const key = keyOf(p.pageNumber);
    const last = groups[groups.length - 1];
    if (key && last && keyOf(last[0].pageNumber) === key) last.push(p);
    else groups.push([p]);
  }

  const chunks: Passage[][] = [];
  let current: Passage[] = [];
  let size = 0;

  for (const group of groups) {
    const groupSize = group.reduce((sum, p) => sum + p.text.length, 0);
    if (current.length && size + groupSize > MAX_CONTEXT_CHARS) {
      chunks.push(current);
      current = [];
      size = 0;
    }
    for (const p of group) {
      // A single verse longer than one call still has to be cut somewhere.
      if (current.length && size + p.text.length > MAX_CONTEXT_CHARS) {
        chunks.push(current);
        current = [];
        size = 0;
      }
      current.push(p);
      size += p.text.length;
    }
  }
  if (current.length) chunks.push(current);

  return chunks.slice(0, MAX_CHUNKS);
}

export const LANGUAGES = [
  { id: "gujarati", label: "ગુજરાતી (Gujarati)" },
  { id: "hindi", label: "हिन्दी (Hindi)" },
  { id: "english", label: "English" },
  { id: "sanskrit", label: "संस्कृत (Sanskrit)" },
] as const;

export type LanguageId = (typeof LANGUAGES)[number]["id"];

function scopeSentence(context: ResolvedContext) {
  const verses = versesLabel(context.verses);
  if (!verses) return context.summaryLine;
  const adhikar = context.verses[0]?.adhikar;
  const chapter = adhikar != null ? `adhikar ${adhikar}, ` : "";
  return `${context.summaryLine}. Only ${chapter}${verses} are in scope.`;
}

export function buildPrompt(opts: {
  question: string;
  language: LanguageId;
  context: ResolvedContext;
  /** Set when this call sees only part of the scope. */
  part?: { index: number; total: number };
  passages?: Passage[];
}) {
  const langName = LANGUAGES.find((l) => l.id === opts.language)?.label ?? "English";
  const passages = opts.passages ?? opts.context.passages;
  const sources = passages
    .map((p, i) => `[${i + 1}] ${p.granthName} — ${p.label}\n${p.text}`)
    .join("\n\n---\n\n");

  const system = [
    "You are a careful assistant for a library of Jain scriptures (granths).",
    "The passages below are OCR text taken verbatim from the scanned books; each passage is one whole printed page.",
    "Answer ONLY from those passages. If they do not contain the answer, say so plainly.",
    "A page can begin or end mid-sentence and can carry text belonging to a neighbouring verse or chapter. Each passage is labelled with the chapter and verse it covers, and these books print a running header naming the chapter. Ignore anything on a page that falls outside the SCOPE.",
    "Stay inside the verses named in SCOPE. Never discuss, number or summarise a verse outside that range, and never invent a verse number that is not there.",
    "Never invent a verse, a citation, a page number or a translation.",
    "When you quote, keep the original script exactly as written.",
    "Cite the passages you used as [1], [2] and so on.",
    `Write your whole answer in ${langName}.`,
    "OCR text can contain mistakes; if a passage looks garbled, say so rather than guessing.",
  ].join(" ");

  const partVerses = opts.part
    ? versesLabel(opts.context.verses.filter((v) => passages.some((p) => p.pageNumber >= v.pageStart && p.pageNumber <= v.pageEnd)))
    : "";

  const user = [
    `SCOPE: ${scopeSentence(opts.context)}`,
    opts.part && opts.part.total > 1
      ? `NOTE: this is part ${opts.part.index} of ${opts.part.total} of that scope and covers ${partVerses || "the pages below"}. Answer only for those, in verse order. Another pass covers the rest, so do not say the text is incomplete and do not mention that this is a part.`
      : "",
    opts.context.truncated && !opts.part
      ? "NOTE: the scope was larger than the limit, so only part of it is shown. Say which verses you could read."
      : "",
    "",
    "PASSAGES:",
    sources || "(no text found for this scope)",
    "",
    `QUESTION: ${opts.question}`,
  ].filter(Boolean).join("\n");

  return { system, user };
}

/** The final pass that stitches the per-part answers into one reply. */
export function buildMergePrompt(opts: {
  question: string;
  language: LanguageId;
  context: ResolvedContext;
  parts: string[];
}) {
  const langName = LANGUAGES.find((l) => l.id === opts.language)?.label ?? "English";
  const system = [
    "You are a careful assistant for a library of Jain scriptures (granths).",
    "Below are answers written from consecutive parts of one passage of scripture, each already grounded in the text.",
    "Rewrite them as ONE continuous answer to the question, ordered by verse number from lowest to highest.",
    "Do not concatenate the parts and do not answer the question twice. Where two parts say the same thing, say it once. Where they disagree, keep what the verse order supports.",
    "Never write that something comes from a part, a passage list or a summary; write about the scripture itself.",
    "Add nothing that is not in the parts, and drop nothing that answers the question.",
    `Write your whole answer in ${langName}.`,
  ].join(" ");

  const user = [
    `SCOPE: ${scopeSentence(opts.context)}`,
    "",
    ...opts.parts.map((text, i) => `PART ${i + 1}:\n${text}`),
    "",
    `QUESTION: ${opts.question}`,
    "Answer it once, covering every verse in the scope in order.",
  ].join("\n");

  return { system, user };
}
