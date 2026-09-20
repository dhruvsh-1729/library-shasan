import { getTursoClient } from "@/lib/turso";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import {
  findOCRSearchMatchesForQueries,
  buildOCRSearchOccurrences,
  type OCRSearchMode,
} from "@/lib/ocr-search";

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

export type ResolvedContext = {
  passages: Passage[];
  totalChars: number;
  truncated: boolean;
  summaryLine: string;
};

/** Roughly 3 chars per token for Indic scripts; keeps the prompt inside the window. */
const MAX_CONTEXT_CHARS = 60_000;
const MAX_PAGES_PER_REQUEST = 40;
const MAX_SEARCH_PASSAGES = 40;

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

async function fetchPages(granthKey: string, lo: number, hi: number) {
  const client = getTursoClient();
  const res = await client.execute({
    sql: `SELECT page_number, content FROM ocr_pages
          WHERE granth_key = ? AND page_number BETWEEN ? AND ?
          ORDER BY page_number`,
    args: [granthKey, lo, hi],
  });
  return res.rows.map((r) => ({ pageNumber: Number(r.page_number), text: String(r.content ?? "") }));
}

/** Gatha ranges come from granth_gatha_map, which stores the page span per verse. */
async function resolveGathaPages(scope: Extract<ContextScope, { kind: "gatha" }>) {
  const sb = getSupabaseAdmin();
  let q = sb
    .from("granth_gatha_map")
    .select("adhikar,gatha,page_start,page_end,anchor_text")
    .eq("book_code", scope.granthKey)
    .gte("gatha", scope.gathaFrom)
    .lte("gatha", scope.gathaTo ?? scope.gathaFrom)
    .order("sequence_index");
  if (scope.adhikar != null) q = q.eq("adhikar", scope.adhikar);

  const { data, error } = await q;
  if (error) throw new Error(`gatha lookup failed: ${error.message}`);
  if (!data?.length) return null;

  const from = Math.min(...data.map((d) => Number(d.page_start)));
  const to = Math.max(...data.map((d) => Number(d.page_end ?? d.page_start)));
  return { from, to, entries: data };
}

export async function resolveContext(scope: ContextScope): Promise<ResolvedContext> {
  const passages: Passage[] = [];
  let truncated = false;
  let summaryLine = "";

  if (scope.kind === "gatha") {
    const hit = await resolveGathaPages(scope);
    if (!hit) {
      return { passages: [], totalChars: 0, truncated: false, summaryLine: "No such gatha in the mapping." };
    }
    const { lo, hi } = clampPages(hit.from, hit.to);
    if (hi < hit.to) truncated = true;
    const names = await granthNames([scope.granthKey]);
    const name = names.get(scope.granthKey) ?? scope.granthKey;
    for (const p of await fetchPages(scope.granthKey, lo, hi)) {
      passages.push({ granthKey: scope.granthKey, granthName: name, pageNumber: p.pageNumber, label: `page ${p.pageNumber}`, text: p.text });
    }
    const range = scope.gathaTo && scope.gathaTo !== scope.gathaFrom ? `${scope.gathaFrom}–${scope.gathaTo}` : `${scope.gathaFrom}`;
    summaryLine = `${name}, gatha ${range}${scope.adhikar != null ? ` (adhikar ${scope.adhikar})` : ""} — pages ${lo}–${hi}`;
  }

  if (scope.kind === "pages") {
    const { lo, hi } = clampPages(scope.pageFrom, scope.pageTo ?? scope.pageFrom);
    if ((scope.pageTo ?? scope.pageFrom) > hi) truncated = true;
    const names = await granthNames([scope.granthKey]);
    const name = names.get(scope.granthKey) ?? scope.granthKey;
    for (const p of await fetchPages(scope.granthKey, lo, hi)) {
      passages.push({ granthKey: scope.granthKey, granthName: name, pageNumber: p.pageNumber, label: `page ${p.pageNumber}`, text: p.text });
    }
    summaryLine = `${name} — pages ${lo}–${hi}`;
  }

  if (scope.kind === "search") {
    const client = getTursoClient();
    const mode: OCRSearchMode = scope.matchMode ?? "exact_word";
    const keys = (scope.granthKeys ?? []).filter(Boolean).slice(0, 25);
    const filter = keys.length ? ` AND p.granth_key IN (${keys.map(() => "?").join(",")})` : "";
    const res = await client.execute({
      sql: `SELECT p.granth_key, p.page_number, p.content, g.granth_name
            FROM ocr_pages_trigram_fts f
            JOIN ocr_pages p ON p.id = f.rowid
            JOIN ocr_granths g ON g.granth_key = p.granth_key
            WHERE ocr_pages_trigram_fts MATCH ?${filter}
            LIMIT ?`,
      args: [`"${scope.query.replace(/"/g, '""')}"`, ...keys, MAX_SEARCH_PASSAGES * 4],
    });

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
    summaryLine = `“${scope.query}” — ${passages.length} page${passages.length === 1 ? "" : "s"}${keys.length ? ` in ${keys.length} selected granth(s)` : " across the library"}`;
  }

  // Trim from the end so the earliest (most relevant) passages survive.
  let total = 0;
  const kept: Passage[] = [];
  for (const p of passages) {
    if (total + p.text.length > MAX_CONTEXT_CHARS) { truncated = true; break; }
    total += p.text.length;
    kept.push(p);
  }

  return { passages: kept, totalChars: total, truncated, summaryLine };
}

export const LANGUAGES = [
  { id: "gujarati", label: "ગુજરાતી (Gujarati)" },
  { id: "hindi", label: "हिन्दी (Hindi)" },
  { id: "english", label: "English" },
  { id: "sanskrit", label: "संस्कृत (Sanskrit)" },
] as const;

export type LanguageId = (typeof LANGUAGES)[number]["id"];

export function buildPrompt(opts: {
  question: string;
  language: LanguageId;
  context: ResolvedContext;
}) {
  const langName = LANGUAGES.find((l) => l.id === opts.language)?.label ?? "English";
  const sources = opts.context.passages
    .map((p, i) => `[${i + 1}] ${p.granthName} — ${p.label}\n${p.text}`)
    .join("\n\n---\n\n");

  const system = [
    "You are a careful assistant for a library of Jain scriptures (granths).",
    "The passages below are OCR text taken verbatim from the scanned books.",
    "Answer ONLY from those passages. If they do not contain the answer, say so plainly.",
    "Never invent a verse, a citation, a page number or a translation.",
    "When you quote, keep the original script exactly as written.",
    "Cite the passages you used as [1], [2] and so on.",
    `Write your whole answer in ${langName}.`,
    "OCR text can contain mistakes; if a passage looks garbled, say so rather than guessing.",
  ].join(" ");

  const user = [
    `SCOPE: ${opts.context.summaryLine}`,
    opts.context.truncated ? "NOTE: the scope was larger than the limit, so only part of it is shown." : "",
    "",
    "PASSAGES:",
    sources || "(no text found for this scope)",
    "",
    `QUESTION: ${opts.question}`,
  ].filter(Boolean).join("\n");

  return { system, user };
}
