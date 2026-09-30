// Gatha → page mapping read from a granth's OCR text, for books the old HTML
// index never covered.
//
// A gatha (verse) ends with its number between dandas: "… जोगमाओज्जं ॥ २३॥".
// In a commentary the verse is printed first and the commentary repeats the
// marker at its end, so a gatha's page is where its marker first appears in
// reading order. The book is read as sequences 1, 2, 3 …; numbering that
// restarts at 1 and then runs on (2, 3, 4 …) opens a new adhikar (chapter).
// A marker is kept only as part of such a run, so a quoted verse ("॥ १ ॥"
// inside a commentary), a verse number cited from another text, or a pothi
// page number printed between dandas does not become a gatha.
//
//   node --env-file=.env scripts/detect_gatha_map.mjs <granth_key> [--json]
import { createClient } from "@libsql/client";

const DIGIT = { "०": 0, "१": 1, "२": 2, "३": 3, "४": 4, "५": 5, "६": 6, "७": 7, "८": 8, "९": 9, "૦": 0, "૧": 1, "૨": 2, "૩": 3, "૪": 4, "૫": 5, "૬": 6, "૭": 7, "૮": 8, "૯": 9 };
const toInt = (s) => Number([...s].map((c) => (c in DIGIT ? DIGIT[c] : c)).join(""));
// ॥ 23 ॥ / ।। २३ ।। / || 23 || (OCR of dandas); a single danda each side too.
const MARKER = /(?:॥|।।|\|\||[।|])\s*([0-9०-९૦-૯]{1,4})\s*(?:॥|।।|\|\||[।|])/gu;
const MAX_STEP = 3;      // a gatha may be missing from the OCR: 12 → 14 still continues
const STEP_COST = 0.4;   // …but each skipped number costs a little
const MAX_PAGE_GAP = 25; // consecutive gathas lie within this many pages
const RESTART_COST = 6;  // opening a new adhikar must be worth several gathas
const LOOKBACK = 400;    // occurrences searched back for the previous gatha

// A table of contents lists chapters as "॥ ४ ॥" and page ranges ("१२१-१२२"):
// its markers are not gathas.
const CONTENTS_HEADING = /अनुक्रम|विषयसूची|અનુક્રમ|વિષયસૂચિ|अनुक्रमणिका|विषयानुक्रम/u;
const PAGE_RANGE = /(?<![0-9०-९૦-૯])[0-9०-९૦-૯]{1,4}\s*[-–]\s*[0-9०-९૦-૯]{1,4}(?![0-9०-९૦-૯])/gu;
export function isContentsPage(content) {
  const text = String(content ?? "");
  return CONTENTS_HEADING.test(text) || (text.match(PAGE_RANGE)?.length ?? 0) >= 6;
}

// A verse is usually introduced by a label: "શ્લોક :-", "श्लोक", "गाथा - २१", "मूलम्".
const VERSE_LABEL = /श्लोक|શ્લોક|गाथा|ગાથા|मूलम्|मूल[:ः]|મૂળ|मू\.|મૂ\./u;
const LABEL_WINDOW = 400;

/**
 * Marker occurrences in reading order: [{ page, n, edge, labelled }] — edge: in
 * the first/last 3 lines; labelled: a verse label shortly before it on the page.
 */
export function markerOccurrences(pages) {
  const out = [];
  for (const { page, content } of pages) {
    if (isContentsPage(content)) continue;
    const lines = String(content ?? "").split("\n");
    const nonEmpty = lines.map((l, i) => [l, i]).filter(([l]) => l.trim());
    const edgeIdx = new Set([...nonEmpty.slice(0, 3), ...nonEmpty.slice(-3)].map(([, i]) => i));
    let offset = 0;
    const text = lines.join("\n");
    lines.forEach((line, i) => {
      for (const m of line.matchAll(MARKER)) {
        const at = offset + m.index;
        out.push({
          page,
          n: toInt(m[1]),
          edge: edgeIdx.has(i) && line.trim().length <= m[0].length + 30,
          labelled: VERSE_LABEL.test(text.slice(Math.max(0, at - LABEL_WINDOW), at)),
        });
      }
      offset += line.length + 1;
    });
  }
  return out;
}

/**
 * The gathas of a book: the best chain through the markers in reading order.
 * In a chain each gatha is 1..MAX_STEP above the one before (a verse the OCR
 * lost is skipped) within MAX_PAGE_GAP pages, or the numbering restarts at 1
 * or 2 (a new adhikar), which costs RESTART_COST so a quoted verse or two does
 * not open a chapter. Each chosen gatha gets the first page its marker
 * appears on after the previous gatha (the verse, not the commentary's repeat).
 * Returns [{ adhikar, gatha, page }].
 */
export function detectGathas(pages, printedPageOf = new Map()) {
  // A pothi prints its page number between dandas in the head or foot: drop
  // a marker in an edge line whose number is that page's printed number.
  const all = markerOccurrences(pages).filter((o) => !(o.edge && String(printedPageOf.get(o.page)) === String(o.n)));
  // Keep one occurrence per (n, page) run: repeats of the same number on the same page add nothing.
  const occ = all.filter((o, i) => !(i > 0 && all[i - 1].n === o.n && all[i - 1].page === o.page));
  const n = occ.length;
  const score = new Float64Array(n);
  const from = new Int32Array(n).fill(-1);
  let bestPrefix = -Infinity;
  let bestPrefixAt = -1;
  const prefix = new Float64Array(n); // best score of any chain ending before i
  const prefixAt = new Int32Array(n);
  for (let i = 0; i < n; i += 1) {
    prefix[i] = bestPrefix;
    prefixAt[i] = bestPrefixAt;
    const o = occ[i];
    let best = o.n <= 2 ? 1 : -Infinity; // a chain may start only at gatha 1 or 2
    let arg = -1;
    if (o.n <= 2 && prefix[i] - RESTART_COST + 1 > best) { best = prefix[i] - RESTART_COST + 1; arg = prefixAt[i]; }
    for (let j = i - 1; j >= 0 && j >= i - LOOKBACK; j -= 1) {
      const q = occ[j];
      if (o.page - q.page > MAX_PAGE_GAP) break;
      const step = o.n - q.n;
      if (step < 1 || step > MAX_STEP || score[j] === -Infinity) continue;
      const s = score[j] + 1 - STEP_COST * (step - 1);
      if (s > best) { best = s; arg = j; }
    }
    score[i] = best;
    from[i] = arg;
    if (best > bestPrefix) { bestPrefix = best; bestPrefixAt = i; }
  }
  // Walk back the best chain.
  const chain = [];
  for (let i = bestPrefixAt; i >= 0; i = from[i]) chain.push(i);
  chain.reverse();

  const out = [];
  let adhikar = 0;
  let prevIdx = -1;
  let prevN = Infinity;
  for (const i of chain) {
    const o = occ[i];
    if (o.n <= prevN) adhikar += 1;
    // Its page: the first of its occurrences after the previous gatha. For a
    // chapter's first gatha, the first one after a verse label: the chapter
    // title ("॥ दानद्वात्रिंशिका ॥१॥") or the commentator's invocation before
    // it also ends in ॥१॥.
    const mine = [];
    for (let k = prevIdx + 1; k <= i; k += 1) if (occ[k].n === o.n) mine.push(occ[k]);
    // Only a chapter's first gatha has a title or invocation before it.
    const opensChapter = o.n <= prevN;
    const page = ((opensChapter && mine.find((x) => x.labelled)) || mine[0]).page;
    out.push({ adhikar, gatha: o.n, page });
    prevIdx = i;
    prevN = o.n;
  }
  const adhikars = new Set(out.map((r) => r.adhikar)).size;
  // A single chapter needs no adhikar number (like most imported mappings).
  return out.map((r) => ({ ...r, adhikar: adhikars > 1 ? r.adhikar : null }));
}

export async function loadGranth(turso, key) {
  const pages = (await turso.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number", args: [key] })).rows
    .map((r) => ({ page: Number(r.page_number), content: String(r.content ?? "") }));
  const printed = new Map(
    (await turso.execute({ sql: "SELECT page_number, printed_page FROM ocr_printed_pages WHERE granth_key = ?", args: [key] })).rows
      .map((r) => [Number(r.page_number), String(r.printed_page)])
  );
  return { pages, printed };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
  const key = process.argv[2];
  const { pages, printed } = await loadGranth(turso, key);
  const rows = detectGathas(pages, printed);
  if (process.argv.includes("--json")) console.log(JSON.stringify(rows));
  else {
    const byAdhikar = new Map();
    for (const r of rows) byAdhikar.set(r.adhikar, [...(byAdhikar.get(r.adhikar) ?? []), r]);
    for (const [a, list] of byAdhikar) console.log(`adhikar ${a ?? "-"}: gathas ${list[0].gatha}..${list[list.length - 1].gatha} (${list.length} found) pages ${list[0].page}..${list[list.length - 1].page}`);
  }
}
