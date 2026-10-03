// Turns what the reader asked for into passages: the exact PDF pages of each
// run of gathas (or each page range), plus the pages they chose to add before
// and after. The preview, the page count, the size estimate and the built file
// all read these, so they can never disagree. No network, no database.

export type GathaRow = {
  gatha: number;
  gathaTo: number | null;
  volumeKey: string;
  pdfUrl: string;
  pageStart: number;
  pageEnd: number;
};

export type Passage = {
  id: string;
  label: string;
  volumeKey: string;
  pdfUrl: string;
  /** The pages the gathas (or the asked-for range) are on. */
  first: number;
  last: number;
  before: number;
  after: number;
  /** The PDF's page count, so added pages stop at its end. */
  pageCount: number | null;
};

export type Plan = { passages: Passage[]; missing: number[]; asked: number[] };

const DIGITS: Record<string, string> = {};
"०१२३४५६७८९".split("").forEach((c, i) => (DIGITS[c] = String(i)));
"૦૧૨૩૪૫૬૭૮૯".split("").forEach((c, i) => (DIGITS[c] = String(i)));
const MAX_ASKED = 2000;

/**
 * "12-18, 25", "12 to 18 25", "१२–१८": the numbers asked for, in order, each
 * once. Anything that is not a number or a range is reported, not guessed.
 */
export function parseAsked(spec: string): { numbers: number[]; error: string | null } {
  const text = String(spec || "")
    .replace(/[०-९૦-૯]/g, (c) => DIGITS[c])
    .replace(/\s*(?:to|thi|से|–|—|-)\s*/gi, "-");
  const numbers: number[] = [];
  const seen = new Set<number>();
  for (const part of text.split(/[\s,;]+/).filter(Boolean)) {
    const m = part.match(/^(\d{1,5})(?:-(\d{1,5}))?$/);
    if (!m) return { numbers, error: `"${part}" is not a number or a range like 12-18.` };
    const a = Number(m[1]);
    const b = m[2] ? Number(m[2]) : a;
    for (let n = Math.min(a, b); n <= Math.max(a, b); n += 1) {
      if (n < 1 || seen.has(n)) continue;
      if (numbers.length >= MAX_ASKED) return { numbers, error: `That is more than ${MAX_ASKED} numbers; ask for fewer.` };
      seen.add(n);
      numbers.push(n);
    }
  }
  return { numbers, error: null };
}

export function askedNumbers(spec: string) {
  return parseAsked(spec).numbers;
}

function runs(numbers: number[]) {
  const out: Array<[number, number]> = [];
  for (const n of [...new Set(numbers)].sort((a, b) => a - b)) {
    const last = out[out.length - 1];
    if (last && n === last[1] + 1) last[1] = n;
    else out.push([n, n]);
  }
  return out;
}

const covers = (row: GathaRow, n: number) => row.gatha === n || (row.gathaTo != null && row.gathaTo > row.gatha && n > row.gatha && n <= row.gathaTo);

/**
 * One passage per run of consecutive gathas in one PDF; a run that crosses into
 * another volume is split there. `unitWord` names them ("Gathas 12–18").
 */
export function planGathas(spec: string, rows: GathaRow[], pageCounts: Map<string, number | null>, unitWord = "Gatha"): Plan {
  const asked = askedNumbers(spec);
  const found = new Map<number, GathaRow[]>();
  for (const n of asked) {
    const hit = rows.filter((r) => covers(r, n));
    if (hit.length) found.set(n, hit);
  }
  const missing = asked.filter((n) => !found.has(n));
  const passages: Passage[] = [];
  for (const [from, to] of runs([...found.keys()])) {
    let current: (Passage & { a: number; b: number }) | null = null;
    for (let n = from; n <= to; n += 1) {
      for (const row of found.get(n) ?? []) {
        if (current && current.pdfUrl === row.pdfUrl && row.pageStart <= current.last + 1 && row.pageStart >= current.first - 1) {
          current.first = Math.min(current.first, row.pageStart);
          current.last = Math.max(current.last, row.pageEnd);
          current.b = n;
          continue;
        }
        current = {
          id: `${row.volumeKey}:${n}:${passages.length}`,
          label: "",
          volumeKey: row.volumeKey,
          pdfUrl: row.pdfUrl,
          first: row.pageStart,
          last: Math.max(row.pageStart, row.pageEnd),
          before: 0,
          after: 0,
          pageCount: pageCounts.get(row.volumeKey) ?? null,
          a: n,
          b: n,
        };
        passages.push(current);
      }
    }
  }
  for (const p of passages as Array<Passage & { a?: number; b?: number }>) {
    p.label = label(unitWord, p.a ?? 0, p.b ?? 0);
    delete p.a;
    delete p.b;
  }
  return { passages, missing, asked };
}

function label(unitWord: string, a: number, b: number) {
  return a === b ? `${unitWord} ${a}` : `${unitWord}s ${a}–${b}`;
}

/** Page ranges of one volume: "45-60, 72" → passages; pages past the end are reported missing. */
export function planPages(spec: string, volumeKey: string, pdfUrl: string, pageCount: number | null): Plan {
  const asked = askedNumbers(spec);
  const ok = asked.filter((p) => p >= 1 && (pageCount == null || p <= pageCount));
  const missing = asked.filter((p) => !ok.includes(p));
  const passages = runs(ok).map(([a, b], i) => ({
    id: `${volumeKey}:p${a}:${i}`,
    label: a === b ? `Page ${a}` : `Pages ${a}–${b}`,
    volumeKey,
    pdfUrl,
    first: a,
    last: b,
    before: 0,
    after: 0,
    pageCount,
  }));
  return { passages, missing, asked };
}

/** The pages a passage puts in the file, the added ones included, clipped to the PDF. */
export function passagePages(p: Passage) {
  const start = Math.max(1, p.first - p.before);
  const end = p.pageCount ? Math.min(p.pageCount, p.last + p.after) : p.last + p.after;
  const out: number[] = [];
  for (let n = start; n <= end; n += 1) out.push(n);
  return out;
}

/** Pages per PDF in reading order, each page once, as the build endpoint takes them. */
export function buildParts(passages: Passage[]) {
  const parts: Array<{ pdfUrl: string; pages: number[] }> = [];
  for (const p of passages) {
    let part = parts.find((x) => x.pdfUrl === p.pdfUrl);
    if (!part) parts.push((part = { pdfUrl: p.pdfUrl, pages: [] }));
    for (const n of passagePages(p)) if (!part.pages.includes(n)) part.pages.push(n);
  }
  for (const part of parts) part.pages.sort((a, b) => a - b);
  return parts;
}

export function pageTotal(passages: Passage[]) {
  return buildParts(passages).reduce((n, p) => n + p.pages.length, 0);
}

/** Bytes per page of each PDF, from its size and page count. */
export function estimateBytes(parts: Array<{ pdfUrl: string; pages: number[] }>, bytesPerPage: Map<string, number>) {
  return parts.reduce((sum, part) => sum + part.pages.length * (bytesPerPage.get(part.pdfUrl) ?? 120_000), 0);
}

/**
 * Splits the pages into groups that each stay under `limit` bytes, keeping a
 * passage whole where it fits, for sending as several emails.
 */
export function splitForEmail(passages: Passage[], bytesPerPage: Map<string, number>, limit: number) {
  const groups: Passage[][] = [];
  let current: Passage[] = [];
  const size = (list: Passage[]) => estimateBytes(buildParts(list), bytesPerPage);
  for (const p of passages) {
    if (size([p]) > limit) {
      // One passage alone is too big: cut it into page runs that fit.
      if (current.length) {
        groups.push(current);
        current = [];
      }
      const per = Math.max(1, Math.floor(limit / (bytesPerPage.get(p.pdfUrl) ?? 120_000)));
      const pages = passagePages(p);
      for (let i = 0; i < pages.length; i += per) {
        const slice = pages.slice(i, i + per);
        groups.push([{ ...p, id: `${p.id}:${i}`, first: slice[0], last: slice[slice.length - 1], before: 0, after: 0 }]);
      }
      continue;
    }
    if (current.length && size([...current, p]) > limit) {
      groups.push(current);
      current = [];
    }
    current.push(p);
  }
  if (current.length) groups.push(current);
  return groups;
}
