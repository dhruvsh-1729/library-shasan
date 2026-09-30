// Printed (granth) page numbers for every OCR page, read from the page text.
//
// Written to the side table ocr_printed_pages, never to ocr_pages: its UPDATE
// triggers would re-index the old full-text tables for every row touched. The
// older ocr_pages.printed_page (Google runs) is not used: checked against the
// scans it often holds a shloka or sutra number from the running head.
//
// How a page number is found:
//   1. Candidates are bare numbers on their own line, or at either end of a
//      line, among the first and last EDGE_LINES lines of the page; numbers
//      between dandas (॥ १२ ॥) only for a book that plain numbers leave mostly
//      unnumbered.
//   2. A number found on REPEATED_VALUE or more pages is a shloka, gatha or
//      margin line number (5, 10, 15…), not a page number, and is dropped.
//   3. Printed = PDF page + offset (or, for a scan with two book pages per
//      PDF page, left = 2 × PDF page + offset). An offset counts only when a
//      chain of at least MIN_RUN pages, each within CHAIN_GAP of the next,
//      agrees with it — a misread (२१० for २९०) or a stray number never does.
//   4. A short run (≤ MAX_SANDWICH pages) of one offset between two runs of
//      the same other offset is a repeated misread (९ read as १) and is
//      dropped; a real change of numbering never returns to the offset before.
//   5. Numbers only go up, except where a section restarts at ≤ RESTART_MAX
//      (front or back matter): runs that go backwards are dropped.
//   6. A page with no usable number is filled only when the nearest numbered
//      pages on both sides share the offset and are ≤ MAX_GAP pages apart.
//
//   node --env-file=.env scripts/backfill_printed_pages.mjs            # every granth not done yet
//   node --env-file=.env scripts/backfill_printed_pages.mjs 071 069    # these granths (redone)
//   node --env-file=.env scripts/backfill_printed_pages.mjs --redo     # every granth, redone
import { createClient } from "@libsql/client";

const EDGE_LINES = 4;
const REPEATED_VALUE = 4;
const MIN_RUN = 5;
const CHAIN_GAP = 3;
const MAX_SANDWICH = 8;
const MAX_GAP = 20;
const RESTART_MAX = 40;
const CONCURRENCY = 4;

const DIGITS = { "૦": 0, "૧": 1, "૨": 2, "૩": 3, "૪": 4, "૫": 5, "૬": 6, "૭": 7, "૮": 8, "૯": 9, "०": 0, "१": 1, "२": 2, "३": 3, "४": 4, "५": 5, "६": 6, "७": 7, "८": 8, "९": 9 };
const toInt = (s) => Number([...s].map((c) => (c in DIGITS ? DIGITS[c] : c)).join(""));
const NUMBER = "[0-9૦-૯०-९]{1,4}";
const PLAIN = {
  bare: new RegExp(`^[\\s\\-–—.()\\[\\]|]*(${NUMBER})[\\s\\-–—.()\\[\\]|]*$`, "u"),
  start: new RegExp(`^(${NUMBER})\\s+\\S`, "u"),
  end: new RegExp(`\\S\\s+(${NUMBER})$`, "u"),
};
// Pothi-style books print the number between dandas: ॥ १२ ॥ (OCR: || 12 ||).
// Verse numbers look the same, so these are tried only when plain numbers
// number too little of the book.
const DANDA_WRAP = "[\\s\\-–—.()\\[\\]|॥।]*";
const DANDA = {
  bare: new RegExp(`^${DANDA_WRAP}(${NUMBER})${DANDA_WRAP}$`, "u"),
  start: PLAIN.start,
  end: new RegExp(`\\S\\s+[॥।|]*\\s*(${NUMBER})\\s*[॥।|]*$`, "u"),
};
const DANDA_FALLBACK_BELOW = 0.3;
// Watermark and repository lines printed on every page of some scans.
const BOILERPLATE = /jain\s*education|private\s*&?\s*personal|jainelibrary|www\.|\.org\b|\.com\b/i;

/** Candidate page numbers read near the top and bottom of one page's text. */
export function pageNumberCandidates(content, patterns = PLAIN) {
  const lines = String(content ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line && !BOILERPLATE.test(line));
  const edge = [...new Set([...lines.slice(0, EDGE_LINES), ...lines.slice(-EDGE_LINES)])];
  const out = new Set();
  for (const line of edge) {
    const bare = line.match(patterns.bare);
    if (bare) { out.add(toInt(bare[1])); continue; }
    const start = line.match(patterns.start);
    if (start) out.add(toInt(start[1]));
    const end = line.match(patterns.end);
    if (end) out.add(toInt(end[1]));
  }
  return [...out].filter((n) => n > 0);
}

// scale 1: printed = page + o. scale 2 (two book pages per PDF page):
// left = 2·page + o, right = left + 1, so a number n read on the page gives o
// either as the left (n − 2·page) or as the right page (n − 1 − 2·page).
const SCALES = {
  1: { offsets: (n, page) => [n - page], label: (page, o) => String(page + o) },
  2: { offsets: (n, page) => [n - 2 * page, n - 1 - 2 * page], label: (page, o) => `${2 * page + o}-${2 * page + o + 1}` },
};

function longestChain(sortedPages) {
  let best = 0;
  let run = 0;
  for (let i = 0; i < sortedPages.length; i += 1) {
    run = i > 0 && sortedPages[i] - sortedPages[i - 1] <= CHAIN_GAP ? run + 1 : 1;
    best = Math.max(best, run);
  }
  return best;
}

/**
 * Printed numbers only go up through a book, except where a section restarts
 * at a small number (front or back matter with its own numbering). Within each
 * section, keeps the runs of the longest (by pages) increasing chain and drops
 * the rest: a misread run (८८० for ८९०) goes backwards against its neighbours.
 */
function dropOutOfOrderRuns(read, valueOf) {
  const runs = [];
  for (const page of [...read.keys()].sort((a, b) => a - b)) {
    const o = read.get(page);
    const last = runs[runs.length - 1];
    if (last && last.offset === o) last.pages.push(page); else runs.push({ offset: o, pages: [page] });
  }
  for (const run of runs) {
    run.first = valueOf(run.pages[0], run.offset);
    run.last = valueOf(run.pages[run.pages.length - 1], run.offset);
  }
  const sections = [];
  for (const run of runs) {
    const prev = sections.length ? sections[sections.length - 1] : null;
    const restart = prev && run.first <= RESTART_MAX && run.pages.length >= MIN_RUN && run.first < prev[prev.length - 1].last;
    if (!prev || restart) sections.push([run]); else prev.push(run);
  }
  for (const section of sections) {
    const best = section.map((run) => ({ weight: run.pages.length, from: -1 }));
    for (let i = 0; i < section.length; i += 1) {
      for (let j = 0; j < i; j += 1) {
        if (section[j].last < section[i].first && best[j].weight + section[i].pages.length > best[i].weight) {
          best[i] = { weight: best[j].weight + section[i].pages.length, from: j };
        }
      }
    }
    let end = 0;
    for (let i = 1; i < section.length; i += 1) if (best[i].weight > best[end].weight) end = i;
    const keep = new Set();
    for (let i = end; i >= 0; i = best[i].from) keep.add(i);
    section.forEach((run, i) => { if (!keep.has(i)) for (const page of run.pages) read.delete(page); });
  }
}

/**
 * On a two-page scan one number read fits two offsets (as the left page or as
 * the right one). Only the true offset explains a page where both numbers are
 * read, so each run takes, of its offset and the ones either side, the one
 * that most pages show both numbers for.
 */
function settleSpreadRuns(read, cands) {
  const runs = [];
  for (const page of [...read.keys()].sort((a, b) => a - b)) {
    const o = read.get(page);
    const last = runs[runs.length - 1];
    if (last && last.offset === o) last.pages.push(page); else runs.push({ offset: o, pages: [page] });
  }
  const pairs = (pages, o) => pages.filter((p) => { const s = new Set(cands.get(p) ?? []); return s.has(2 * p + o) && s.has(2 * p + o + 1); }).length;
  for (const run of runs) {
    const here = pairs(run.pages, run.offset);
    const best = [run.offset - 1, run.offset + 1].map((o) => [o, pairs(run.pages, o)]).sort((x, y) => y[1] - x[1])[0];
    if (best[1] > here) for (const p of run.pages) read.set(p, best[0]);
  }
}

function deriveAtScale(cands, pageList, scale) {
  const { offsets: offsetsOf, label } = SCALES[scale];
  const support = new Map();
  for (const page of pageList) {
    const seen = new Set();
    for (const n of cands.get(page)) for (const o of offsetsOf(n, page)) seen.add(o);
    for (const o of seen) (support.get(o) ?? support.set(o, []).get(o)).push(page);
  }
  const chain = new Map();
  for (const [o, list] of support) {
    const c = longestChain(list);
    if (c >= MIN_RUN) chain.set(o, c);
  }
  // Numbers read that an offset explains. On a two-page scan a number alone
  // fits two offsets (as the left page or as the right one) equally well;
  // where both of a spread's numbers are read, only the true offset explains
  // both, so it collects more votes.
  const votes = new Map();
  for (const page of pageList) for (const n of cands.get(page)) for (const o of offsetsOf(n, page)) votes.set(o, (votes.get(o) ?? 0) + 1);
  const better = (o, best) =>
    best == null || chain.get(o) > chain.get(best) || (chain.get(o) === chain.get(best) && votes.get(o) > votes.get(best));

  const read = new Map();
  for (const page of pageList) {
    let best = null;
    for (const n of cands.get(page)) {
      for (const o of offsetsOf(n, page)) {
        if (chain.has(o) && better(o, best)) best = o;
      }
    }
    if (best != null) read.set(page, best);
  }

  for (let changed = true; changed; ) {
    changed = false;
    const runs = [];
    for (const page of [...read.keys()].sort((a, b) => a - b)) {
      const o = read.get(page);
      const last = runs[runs.length - 1];
      if (last && last.offset === o) last.pages.push(page); else runs.push({ offset: o, pages: [page] });
    }
    for (let i = 1; i + 1 < runs.length; i += 1) {
      if (runs[i].pages.length <= MAX_SANDWICH && runs[i - 1].offset === runs[i + 1].offset) {
        for (const page of runs[i].pages) read.delete(page);
        changed = true;
      }
    }
  }

  dropOutOfOrderRuns(read, (page, o) => (scale === 2 ? 2 * page + o : page + o));
  if (scale === 2) settleSpreadRuns(read, cands);

  const result = new Map();
  const readPages = [...read.keys()].sort((a, b) => a - b);
  for (const page of readPages) result.set(page, { printed: label(page, read.get(page)), how: "read" });
  for (let i = 0; i + 1 < readPages.length; i += 1) {
    const a = readPages[i];
    const b = readPages[i + 1];
    if (b - a < 2 || b - a > MAX_GAP || read.get(a) !== read.get(b)) continue;
    for (let page = a + 1; page < b; page += 1) result.set(page, { printed: label(page, read.get(a)), how: "between" });
  }
  return { result, offsets: [...new Set(read.values())].sort((x, y) => x - y), scale };
}

/** Every number that could be the page number on this text, plain or between dandas. */
export function anyPageNumberCandidates(content) {
  return [...new Set([...pageNumberCandidates(content, PLAIN), ...pageNumberCandidates(content, DANDA)])];
}

/** Candidates of one page; `contents` (several readings of the page) are pooled. */
function candidatesOf(page, patterns) {
  const texts = page.contents ?? [page.content];
  return [...new Set(texts.flatMap((text) => pageNumberCandidates(text, patterns)))];
}

function deriveWith(pages, patterns) {
  const raw = new Map(pages.map((page) => [page.page, candidatesOf(page, patterns)]));
  const valuePages = new Map();
  for (const list of raw.values()) for (const n of list) valuePages.set(n, (valuePages.get(n) ?? 0) + 1);
  const cands = new Map([...raw].map(([page, list]) => [page, list.filter((n) => valuePages.get(n) < REPEATED_VALUE)]));
  const pageList = pages.map((p) => p.page).sort((a, b) => a - b);

  const single = deriveAtScale(cands, pageList, 1);
  const spread = deriveAtScale(cands, pageList, 2);
  return readCount(spread) > 1.5 * readCount(single) && readCount(spread) >= pages.length * 0.3 ? spread : single;
}

const readCount = (r) => [...r.result.values()].filter((v) => v.how === "read").length;

/**
 * Printed page per PDF page: Map pdfPage → { printed, how }, plus the offsets
 * and scale used. Each page is { page, content } or { page, contents: [...] }
 * for several readings of the same page (text layer, image OCR passes).
 */
export function derivePrintedPages(pages) {
  const plain = deriveWith(pages, PLAIN);
  if (plain.result.size >= pages.length * DANDA_FALLBACK_BELOW) return plain;
  const danda = deriveWith(pages, DANDA);
  return danda.result.size > plain.result.size ? danda : plain;
}

async function ensureTables(turso) {
  await turso.batch([
    `CREATE TABLE IF NOT EXISTS ocr_printed_pages (
       granth_key TEXT NOT NULL,
       page_number INTEGER NOT NULL,
       printed_page TEXT NOT NULL,
       how TEXT NOT NULL,
       PRIMARY KEY (granth_key, page_number)
     ) WITHOUT ROWID`,
    `CREATE TABLE IF NOT EXISTS ocr_printed_pages_runs (
       granth_key TEXT PRIMARY KEY,
       pdf_pages INTEGER NOT NULL,
       read_pages INTEGER NOT NULL,
       between_pages INTEGER NOT NULL,
       scale INTEGER NOT NULL,
       offsets TEXT NOT NULL,
       run_at TEXT NOT NULL
     )`,
  ], "write");
}

async function backfillGranth(turso, granthKey) {
  const rows = await turso.execute({
    sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
    args: [granthKey],
  });
  const pages = rows.rows
    .map((row) => ({ page: Number(row.page_number), content: String(row.content ?? "") }))
    .filter((p) => p.page > 0);
  const { result, offsets, scale } = derivePrintedPages(pages);

  const writes = [{ sql: "DELETE FROM ocr_printed_pages WHERE granth_key = ?", args: [granthKey] }];
  let readPages = 0;
  for (const [page, { printed, how }] of result) {
    if (how === "read") readPages += 1;
    writes.push({
      sql: "INSERT INTO ocr_printed_pages (granth_key, page_number, printed_page, how) VALUES (?, ?, ?, ?)",
      args: [granthKey, page, printed, how],
    });
  }
  const betweenPages = result.size - readPages;
  writes.push({
    sql: `INSERT INTO ocr_printed_pages_runs (granth_key, pdf_pages, read_pages, between_pages, scale, offsets, run_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(granth_key) DO UPDATE SET pdf_pages = excluded.pdf_pages, read_pages = excluded.read_pages,
            between_pages = excluded.between_pages, scale = excluded.scale, offsets = excluded.offsets, run_at = excluded.run_at`,
    args: [granthKey, pages.length, readPages, betweenPages, scale, JSON.stringify(offsets), new Date().toISOString()],
  });
  // One batch is one transaction: the granth's old rows are never left half replaced.
  await turso.batch(writes, "write");
  return { granthKey, pdfPages: pages.length, read: readPages, between: betweenPages, scale, offsets };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
  await ensureTables(turso);
  const args = process.argv.slice(2);
  const named = args.filter((a) => !a.startsWith("--"));
  let keys = named;
  if (!keys.length) {
    const all = (await turso.execute("SELECT granth_key FROM ocr_granths ORDER BY granth_key")).rows.map((r) => String(r.granth_key));
    const done = args.includes("--redo")
      ? new Set()
      : new Set((await turso.execute("SELECT granth_key FROM ocr_printed_pages_runs")).rows.map((r) => String(r.granth_key)));
    keys = all.filter((k) => !done.has(k));
  }
  console.log(`${keys.length} granths to do`);
  let next = 0;
  let finished = 0;
  const worker = async () => {
    while (next < keys.length) {
      const key = keys[next++];
      try {
        const r = await backfillGranth(turso, key);
        finished += 1;
        console.log(`[${finished}/${keys.length}] ${key}: ${r.read + r.between}/${r.pdfPages} pages numbered (read ${r.read}, between ${r.between})${r.scale === 2 ? " two pages per scan" : ""} offsets ${JSON.stringify(r.offsets)}`);
      } catch (error) {
        console.error(`FAILED ${key}: ${error instanceof Error ? error.message : error}`);
      }
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
}
