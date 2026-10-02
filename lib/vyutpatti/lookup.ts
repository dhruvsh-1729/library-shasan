// Finding a word's entries in the vyutpatti koshes: the headword index
// (kosh_headwords, built by scripts/build_kosh_headwords.mjs) names the page
// and line, and the entry's text is cut from the page in reading order.
//
// Google's OCR of a two-column kosh often interleaves the columns, so a page's
// lines are put back in reading order from their printed-line boxes
// (ocr_line_boxes): the left column top to bottom, then the right one.

import { describeGranth, getGranthCatalog } from "@/lib/granth-catalog";
import { headwordKeys, headwordLines, koshCitationName, vyutpattiKosh } from "@/lib/kosh-headwords.mjs";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { getTursoClient } from "@/lib/turso";

export type KoshInfo = NonNullable<ReturnType<typeof vyutpattiKosh>>;

export type Candidate = {
  id: string;
  granthKey: string;
  kosh: KoshInfo;
  citation: string;
  pdfPage: number;
  printedPage: string | null;
  head: string;
  /** The Sanskrit form a Prakrit kosh gives for its headword. */
  sanskrit: string | null;
  /** The entry as the OCR has it, in reading order. */
  text: string;
  /** The entry runs on to the next PDF page (its first lines are in `text`). */
  continues: boolean;
  pdfUrl: string | null;
};

/** Candidates kept per kosh family: the headword index also holds look-alike lines. */
const PER_FAMILY = 3;
/** An entry longer than this is cut: the gender, derivation and first meanings come first. */
const MAX_ENTRY_LINES = 24;
const NEXT_PAGE_LINES = 12;

type Box = [number, number, number, number];

type PageText = { lines: string[]; order: number[] };

/** The page's non-empty line indexes in reading order (OCR order when it has no usable boxes). */
export function readingOrder(lines: string[], boxes: Box[] | null): number[] {
  const nonEmpty: number[] = [];
  lines.forEach((line, index) => {
    if (line.trim()) nonEmpty.push(index);
  });
  if (!boxes || boxes.length !== nonEmpty.length) return nonEmpty;
  const placed = nonEmpty.map((index, i) => {
    const [left, top, right] = boxes[i];
    const width = right - left;
    const centre = (left + right) / 2;
    const column = width > 0.6 ? -1 : centre < 0.5 ? 0 : 1;
    return { index, top, column };
  });
  const inColumns = placed.filter((p) => p.column >= 0);
  if (!inColumns.length) return nonEmpty;
  const columnsTop = Math.min(...inColumns.map((p) => p.top));
  const rank = (p: (typeof placed)[number]) => (p.column >= 0 ? 1 + p.column : p.top < columnsTop ? 0 : 3);
  return [...placed].sort((a, b) => rank(a) - rank(b) || a.top - b.top).map((p) => p.index);
}

async function loadPages(granthKey: string, pages: number[]) {
  const unique = [...new Set(pages)];
  const out = new Map<number, PageText>();
  if (!unique.length) return out;
  const placeholders = unique.map(() => "?").join(",");
  const [text, boxes] = await Promise.all([
    getTursoClient().execute({
      sql: `SELECT page_number, content FROM ocr_pages WHERE granth_key = ? AND page_number IN (${placeholders})`,
      args: [granthKey, ...unique],
    }),
    getTursoClient()
      .execute({
        sql: `SELECT page_number, boxes FROM ocr_line_boxes WHERE granth_key = ? AND page_number IN (${placeholders})`,
        args: [granthKey, ...unique],
      })
      .catch(() => ({ rows: [] as Array<Record<string, unknown>> })),
  ]);
  const boxesByPage = new Map<number, Box[]>();
  for (const row of boxes.rows) {
    try {
      boxesByPage.set(Number(row.page_number), JSON.parse(String(row.boxes)));
    } catch {
      // unreadable boxes: OCR order
    }
  }
  for (const row of text.rows) {
    const page = Number(row.page_number);
    const lines = String(row.content ?? "").normalize("NFC").split("\n");
    out.set(page, { lines, order: readingOrder(lines, boxesByPage.get(page) ?? null) });
  }
  return out;
}

/**
 * The entry that starts on `line`: it and the lines after it (in reading
 * order) up to the next entry's first line. Returns whether it ran off the page.
 */
export function cutEntry(format: string, page: PageText, line: number) {
  const starts = new Set(headwordLines(format, page.lines.join("\n")).map((h) => h.line));
  const at = page.order.indexOf(line);
  if (at < 0) return { lines: [page.lines[line] ?? ""], continues: false };
  const out: string[] = [];
  let i = at;
  for (; i < page.order.length && out.length < MAX_ENTRY_LINES; i += 1) {
    const index = page.order[i];
    if (i > at && starts.has(index)) break;
    out.push(page.lines[index]);
  }
  return { lines: out, continues: i >= page.order.length && out.length < MAX_ENTRY_LINES };
}

/** The first lines of a page, up to its first entry: the end of an entry from the page before. */
function pageOpening(format: string, page: PageText) {
  const starts = new Set(headwordLines(format, page.lines.join("\n")).map((h) => h.line));
  const out: string[] = [];
  for (const index of page.order) {
    if (starts.has(index) || out.length >= NEXT_PAGE_LINES) break;
    out.push(page.lines[index]);
  }
  // A running head ("चौर-चोलपट्ट]", "शब्दरत्नमहोदधिः ।", the page number) is not entry text.
  return out.slice(Math.min(out.length, 3));
}

async function printedPages(granthKey: string, pages: number[]) {
  const map = new Map<number, string>();
  if (!pages.length) return map;
  const result = await getTursoClient()
    .execute({
      sql: `SELECT page_number, printed_page FROM ocr_printed_pages WHERE granth_key = ? AND page_number IN (${pages.map(() => "?").join(",")})`,
      args: [granthKey, ...pages],
    })
    .catch(() => ({ rows: [] as Array<Record<string, unknown>> }));
  for (const row of result.rows) if (row.printed_page != null) map.set(Number(row.page_number), String(row.printed_page));
  return map;
}

/**
 * A word's entries: in the three koshes when any of them has it, otherwise in
 * the other four (Maharaj Saheb's rule). Each candidate is a headword line the
 * index found; the model then decides which really are the word's entry.
 */
export async function findCandidates(word: string, options: { tier?: 1 | 2 } = {}): Promise<Candidate[]> {
  // Only Devanagari (Hindi-lipi) words are looked up.
  if (!/^[\u0900-\u097f]+$/u.test(String(word ?? "").normalize("NFC"))) return [];
  const keys = headwordKeys(word);
  if (!keys.length) return [];
  const result = await getTursoClient().execute({
    sql: `SELECT key, granth_key, page_number, line_no, head, sanskrit FROM kosh_headwords WHERE key IN (${keys.map(() => "?").join(",")})`,
    args: keys,
  });
  const folded = foldSanskrit(word.normalize("NFC"));
  const rows = result.rows
    .map((row) => ({
      granthKey: String(row.granth_key),
      page: Number(row.page_number),
      line: Number(row.line_no),
      head: String(row.head),
      sanskrit: row.sanskrit == null ? null : String(row.sanskrit),
      kosh: vyutpattiKosh(String(row.granth_key)),
    }))
    .filter((row): row is typeof row & { kosh: KoshInfo } => Boolean(row.kosh));

  const tiers = options.tier ? [options.tier] : [1, 2];
  for (const tier of tiers) {
    const inTier = rows.filter((row) => row.kosh.tier === tier);
    if (!inTier.length) continue;
    // The printed headword that is exactly the word comes first, then the kosh's own order.
    const exact = (row: (typeof inTier)[number]) =>
      foldSanskrit(row.head) === folded || (row.sanskrit && foldSanskrit(row.sanskrit) === folded) ? 0 : 1;
    const seen = new Set<string>();
    const perFamily = new Map<string, number>();
    const picked = [...inTier]
      .sort((a, b) => exact(a) - exact(b) || a.kosh.rank - b.kosh.rank || a.page - b.page || a.line - b.line)
      .filter((row) => {
        const id = `${row.granthKey}:${row.page}:${row.line}`;
        if (seen.has(id)) return false;
        seen.add(id);
        const n = perFamily.get(row.kosh.family) ?? 0;
        if (n >= PER_FAMILY) return false;
        perFamily.set(row.kosh.family, n + 1);
        return true;
      })
      .sort((a, b) => a.kosh.rank - b.kosh.rank || a.page - b.page || a.line - b.line);
    return buildCandidates(picked);
  }
  return [];
}

type Picked = { granthKey: string; page: number; line: number; head: string; sanskrit: string | null; kosh: KoshInfo };

async function buildCandidates(picked: Picked[]): Promise<Candidate[]> {
  // The PDF link is only for opening the page; a catalog that fails to load costs the link, not the lookup.
  const catalog = await getGranthCatalog().catch(() => null);
  const byGranth = new Map<string, Picked[]>();
  for (const p of picked) byGranth.set(p.granthKey, [...(byGranth.get(p.granthKey) ?? []), p]);
  const out: Candidate[] = [];
  await Promise.all(
    [...byGranth].map(async ([granthKey, rows]) => {
      const wanted = rows.flatMap((r) => [r.page, r.page + 1]);
      const [pages, printed] = await Promise.all([loadPages(granthKey, wanted), printedPages(granthKey, rows.map((r) => r.page))]);
      const pdfUrl = (catalog && describeGranth(catalog, granthKey)?.pdfUrl) || null;
      for (const row of rows) {
        const page = pages.get(row.page);
        if (!page) continue;
        const entry = cutEntry(row.kosh.format, page, row.line);
        const next = entry.continues ? pages.get(row.page + 1) : undefined;
        const lines = next ? [...entry.lines, ...pageOpening(row.kosh.format, next)] : entry.lines;
        out.push({
          id: `${granthKey}:${row.page}:${row.line}`,
          granthKey,
          kosh: row.kosh,
          citation: koshCitationName(row.kosh),
          pdfPage: row.page,
          printedPage: printed.get(row.page) ?? null,
          head: row.head,
          sanskrit: row.sanskrit,
          text: lines.join("\n").trim(),
          continues: entry.continues,
          pdfUrl,
        });
      }
    })
  );
  return out.sort((a, b) => a.kosh.rank - b.kosh.rank || a.pdfPage - b.pdfPage);
}

/**
 * Apte prints a compound under its first word, after "सम.": देवेन्द्र is
 * "-इन्द्रः" inside देव's entry (Maharaj Saheb's देवेश / देवेन्द्र example).
 * Finds that sub-entry for `first` + `second`.
 */
export async function findApteSubentry(first: string, second: string): Promise<Candidate | null> {
  const apte = (await findCandidates(first, { tier: 1 })).filter((c) => c.kosh.family === "apte");
  const wanted = new Set(headwordKeys(second));
  for (const candidate of apte) {
    const pages = await loadPages(candidate.granthKey, [candidate.pdfPage, candidate.pdfPage + 1, candidate.pdfPage + 2]);
    for (const pageNumber of [candidate.pdfPage, candidate.pdfPage + 1, candidate.pdfPage + 2]) {
      const page = pages.get(pageNumber);
      if (!page) continue;
      const ordered = page.order.map((i) => page.lines[i]);
      for (let i = 0; i < ordered.length; i += 1) {
        for (const m of ordered[i].matchAll(/[-–—]\s*([ऀ-ॣ]{2,})/gu)) {
          if (!headwordKeys(m[1]).some((k) => wanted.has(k))) continue;
          const printed = await printedPages(candidate.granthKey, [pageNumber]);
          return {
            ...candidate,
            id: `${candidate.granthKey}:${pageNumber}:sub:${i}`,
            pdfPage: pageNumber,
            printedPage: printed.get(pageNumber) ?? null,
            head: `${first} (सम.) -${m[1]}`,
            text: ordered.slice(Math.max(0, i - 1), i + 3).join("\n"),
            continues: false,
          };
        }
      }
    }
  }
  return null;
}

/**
 * Which of these words are headwords in the koshes, and in which tier (1:
 * the three koshes, 2: the other four). One query for all of them.
 */
export async function headwordTiers(words: string[]): Promise<Map<string, { tier: number; head: string }>> {
  const out = new Map<string, { tier: number; head: string }>();
  const keysOf = new Map(words.map((w) => [w, headwordKeys(w)]));
  const keys = [...new Set([...keysOf.values()].flat())];
  if (!keys.length) return out;
  const byKey = new Map<string, { tier: number; head: string }>();
  for (let i = 0; i < keys.length; i += 400) {
    const chunk = keys.slice(i, i + 400);
    const result = await getTursoClient().execute({
      sql: `SELECT key, granth_key, head FROM kosh_headwords WHERE key IN (${chunk.map(() => "?").join(",")})`,
      args: chunk,
    });
    for (const row of result.rows) {
      const tier = vyutpattiKosh(String(row.granth_key))?.tier;
      if (!tier) continue;
      const key = String(row.key);
      const current = byKey.get(key);
      if (!current || tier < current.tier) byKey.set(key, { tier, head: String(row.head) });
    }
  }
  for (const [word, wordKeys] of keysOf) {
    // The word's own spelling first (देव before देवः), then its stem forms.
    const hit = wordKeys.map((k) => byKey.get(k)).filter(Boolean).sort((a, b) => a!.tier - b!.tier)[0];
    if (hit) out.set(word, hit);
  }
  return out;
}
