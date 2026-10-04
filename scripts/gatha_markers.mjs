// Gatha / sutra → page mapping read from a granth's OCR text, for the books
// the old HTML index never covered. Generalises scripts/detect_gatha_map.mjs
// (which only reads "॥ २३ ॥") to the other ways these editions number their
// units, each checked on the scans of the books that use it:
//
//   danda       … जोगमाओज्जं ॥ २३॥   ॥२०६-२१०॥            (verse ends with its number)
//   dandaSlash  ॥૩/૧૦॥                                     (chapter / verse: Vitrag Stotra)
//   bracket     [૭૭૨] …   • [૧૧૭] …   [૭૮૧ થી ૭૮૪] …      (Deepratnasagar Gujarati anuvad: line starts with it)
//   mool        मू. (४८) …   मू.(४६२)                       (Agam Suttani: mool sutra/gatha number)
//   bhashya     [भा.७६९]                                   (Agam Suttani: bhashya gathas)
//   suBracket   [सू० ३५५]                                  (Sthanang / Samvayang editions)
//   sutraLabel  • સૂત્ર-૭૪,૭૫ :-    सूत्र - १२             (label at the start of a line)
//   gathaLabel  ગાથા-૧૨ / गाथा : १२ / શ્લોક-૧૦              (label at the start of a line)
//   paren       … કરતા હતા.(૪૦)                             (verse number closing a sentence)
//
// The book is read as a sequence of numbers in reading order. A chain keeps
// a number only when it continues the one before (1..MAX_STEP higher, within
// maxPageGap pages); numbering that restarts at 1 opens a new chapter. So a
// cross-reference ("सूत्र-१२मां"), a quoted verse or a printed page number
// does not become a unit. A later volume of a work continues its numbering
// (part 3 of Dharma Sangrahani starts at gatha 1137), so the first chain may
// start at any number.
//
// Each unit gets the first page its marker appears on after the previous
// unit: for a verse that is where the verse (not its commentary's repeat) is.
import { createClient } from "@libsql/client";

const DIGIT = { "०": 0, "१": 1, "२": 2, "३": 3, "४": 4, "५": 5, "६": 6, "७": 7, "८": 8, "९": 9, "૦": 0, "૧": 1, "૨": 2, "૩": 3, "૪": 4, "૫": 5, "૬": 6, "૭": 7, "૮": 8, "૯": 9 };
export const toInt = (s) => Number([...String(s)].map((c) => (c in DIGIT ? DIGIT[c] : c)).join(""));
const D = "[0-9०-९૦-૯]{1,4}";
const RANGE = `(?:\\s*(?:-|–|થી|से|स|,|\\.\\s*)\\s*(${D}))?`;
const DANDA = "(?:॥|।।|\\|\\||[।|])";

export const FAMILIES = {
  danda: new RegExp(`${DANDA}\\s*(${D})(?:\\s*[-–]\\s*(${D}))?\\s*${DANDA}`, "gu"),
  dandaSlash: new RegExp(`(?:॥|।।|\\|\\|)\\s*(${D})\\s*[/।]\\s*(${D})\\s*(?:॥|।।|\\|\\|)`, "gu"),
  bracket: new RegExp(`^\\s*[•·*]?\\s*\\[\\s*(${D})${RANGE}\\s*\\]`, "gmu"),
  mool: new RegExp(`(?:मू|મૂ)\\s*[.ःः०ઠ:]?\\s*\\(\\s*(${D})${RANGE}\\s*\\)`, "gu"),
  bhashya: new RegExp(`\\[\\s*भा\\s*[.०]\\s*(${D})${RANGE}\\s*\\]`, "gu"),
  suBracket: new RegExp(`\\[\\s*(?:सू|સૂ)\\s*[०.]\\s*(${D})${RANGE}\\s*\\]`, "gu"),
  sutraLabel: new RegExp(`^\\s*[•·*]?\\s*(?:સૂત્ર|सूत्र|સૂત્રો|सूत्राणि)\\s*[-–:.]\\s*(${D})${RANGE}`, "gmu"),
  gathaLabel: new RegExp(`^\\s*[•·*]?\\s*(?:ગાથા|गाथा|ગાર્થા|શ્લોક|श्लोक)\\s*[-–:.]?\\s*(${D})${RANGE}`, "gmu"),
  paren: new RegExp(`[^\\s(]\\s*\\(\\s*(${D})\\s*\\)`, "gu"),
};

// A table of contents lists chapters as "॥ ४ ॥" and page ranges ("१२१-१२२").
// Only a heading line counts: "અનુક્રમે" / "अनुक्रमेण" in running text is the
// word "in order", and dropping those pages lost real units.
const CONTENTS_HEADING = /(?:अनुक्रमणिका|अनुक्रमः|अनुक्रम|विषयसूचिः?|विषयसूची|विषयानुक्रमः?|અનુક્રમણિકા|અનુક્રમ|વિષયસૂચિ|વિષયસૂચી|વિષયાનુક્રમ)(?![\u0900-\u0AFF])/u;
const PAGE_RANGE = /(?<![0-9०-९૦-૯])[0-9०-९૦-૯]{1,4}\s*[-–]\s*[0-9०-९૦-૯]{1,4}(?![0-9०-९૦-૯])/gu;
export function isContentsPage(content) {
  const text = String(content ?? "");
  const heading = text.split("\n").some((line) => line.trim().length <= 40 && CONTENTS_HEADING.test(line));
  return heading || (text.match(PAGE_RANGE)?.length ?? 0) >= 6;
}
const VERSE_LABEL = /श्लोक|શ્લોક|गाथा|ગાથા|मूलम्|मूल[:ः]|મૂળ|मू\.|મૂ\./u;

/** Marker occurrences in reading order: [{ page, n, to, chap, edge, labelled }]. */
export function occurrences(pages, family, regex = null, maxRange = 30) {
  // "custom": a book's own marker, group 1 = number (group 2 = range end)
  const re = family === "custom" ? new RegExp(regex, "gmu") : FAMILIES[family];
  const out = [];
  for (const { page, content } of pages) {
    const text = String(content ?? "");
    if (isContentsPage(text)) continue;
    const lines = text.split("\n");
    const nonEmpty = lines.map((l, i) => [l, i]).filter(([l]) => l.trim());
    const edgeIdx = new Set([...nonEmpty.slice(0, 3), ...nonEmpty.slice(-3)].map(([, i]) => i));
    let offset = 0;
    lines.forEach((line, i) => {
      re.lastIndex = 0;
      for (const m of line.matchAll(re)) {
        const at = offset + m.index;
        let chap = null;
        let n = toInt(m[1]);
        let to = m[2] != null ? toInt(m[2]) : n;
        if (family === "dandaSlash") { chap = n; n = toInt(m[2]); to = n; }
        if (!(to >= n) || to - n > maxRange) to = n; // "[૩૭૯, ૧૮૦]" is an OCR slip, not a range back
        out.push({
          id: out.length, page, n, to, chap, line: i,
          edge: edgeIdx.has(i) && line.trim().length <= m[0].length + 30,
          labelled: VERSE_LABEL.test(text.slice(Math.max(0, at - 400), at)),
        });
      }
      offset += line.length + 1;
    });
  }
  return out;
}

// bridgeMax: a run of numbers the OCR lost or misread ("[૭૭૨]" read as "[993]")
// may be stepped over, up to this many numbers, at bridgeCost; 0 disables.
export const DEFAULTS = { maxStep: 3, stepCost: 0.4, maxPageGap: 25, restartCost: 6, lookback: 400, startAny: true, restarts: true, minN: 1, bridgeMax: 0, bridgeCost: 4, maxRange: 30 };

/**
 * The units of a book: the best chain through the occurrences.
 * Returns [{ chapter, chap, n, to, page }]: chapter is the 1-based ordinal of
 * the run the unit is in (or the printed chapter for dandaSlash).
 */
export function detectUnits(pages, printedPageOf = new Map(), family = "danda", options = {}) {
  const o = { ...DEFAULTS, ...options };
  // A pothi prints its page number between dandas in the head or foot.
  let source = o.occurrences ?? occurrences(pages, family, o.regex, o.maxRange);
  // hundreds: [10, 11, …] for editions that print only the last two digits
  // ("॥ ४ ॥" for 1004): each such number is offered as every listed hundred and
  // the chain keeps the reading that continues the numbering.
  if (o.hundreds && !o.occurrences) {
    source = source.flatMap((x) => (x.n < 100 && (o.hundredsFrom == null || x.page >= o.hundredsFrom)
      ? [x, ...o.hundreds.map((h) => ({ ...x, n: h * 100 + x.n, to: h * 100 + x.to }))]
      : [x]));
  }
  const all = source.filter((x) => x.n >= o.minN && (o.maxN == null || x.n <= o.maxN)
    && (o.pageFrom == null || x.page >= o.pageFrom) && (o.pageTo == null || x.page <= o.pageTo)
    && !(x.edge && String(printedPageOf.get(x.page)) === String(x.n)));
  const occ = all.filter((x, i) => !(i > 0 && all[i - 1].n === x.n && all[i - 1].page === x.page && all[i - 1].chap === x.chap));
  const n = occ.length;
  const score = new Float64Array(n);
  const from = new Int32Array(n).fill(-1);
  let bestPrefix = -Infinity;
  let bestPrefixAt = -1;
  const prefix = new Float64Array(n);
  const prefixAt = new Int32Array(n);
  const opens = (x) => x.n <= 2;
  for (let i = 0; i < n; i += 1) {
    prefix[i] = bestPrefix;
    prefixAt[i] = bestPrefixAt;
    const x = occ[i];
    let best = o.startAny || opens(x) ? 1 : -Infinity;
    let arg = -1;
    if (o.restarts && opens(x) && prefix[i] - o.restartCost + 1 > best) { best = prefix[i] - o.restartCost + 1; arg = prefixAt[i]; }
    for (let j = i - 1; j >= 0 && j >= i - o.lookback; j -= 1) {
      const q = occ[j];
      if (x.page - q.page > o.maxPageGap) break;
      if (score[j] === -Infinity) continue;
      let s = -Infinity;
      if (x.chap != null && q.chap != null && x.chap !== q.chap) {
        // explicit chapter numbers: the next chapter opens at its first verse
        if (x.chap > q.chap && x.chap - q.chap <= 2 && opens(x)) s = score[j] + 1 - (x.chap - q.chap - 1) * 2;
      } else {
        const step = x.n - q.to;
        if (step >= 1 && step <= o.maxStep) s = score[j] + 1 + (x.to - x.n) - o.stepCost * (step - 1);
        else if (step > o.maxStep && step <= o.bridgeMax) s = score[j] + 1 + (x.to - x.n) - o.bridgeCost;
      }
      if (s > best) { best = s; arg = j; }
    }
    score[i] = best;
    from[i] = arg;
    if (best > bestPrefix) { bestPrefix = best; bestPrefixAt = i; }
  }
  const chain = [];
  for (let i = bestPrefixAt; i >= 0; i = from[i]) chain.push(i);
  chain.reverse();

  const out = [];
  let chapter = 0;
  let prevIdx = -1;
  let prev = null;
  for (const i of chain) {
    const x = occ[i];
    const opensChapter = !prev || (x.chap != null ? x.chap !== prev.chap : x.n <= prev.to);
    if (opensChapter) chapter += 1;
    // Its page: the first of its occurrences after the page chosen for the
    // previous unit (a verse printed in a block of mool before its
    // commentary is where the block is, not where the commentary repeats it).
    const mine = [];
    for (let k = prevIdx + 1; k <= i; k += 1) if (occ[k].n === x.n && occ[k].chap === x.chap) mine.push(k);
    const labelled = family === "danda" && opensChapter ? mine.find((k) => occ[k].labelled) : undefined;
    const chosen = labelled ?? mine[0];
    // "• સૂત્ર-૧૩૦/૨ થી ૧૩૨": the numbers after N start where the range is
    // printed, which can be pages after N's own first appearance.
    const restPage = x.to > x.n && occ[chosen].to < x.to ? x.page : undefined;
    out.push({ chapter, chap: x.chap, n: x.n, to: x.to, page: occ[chosen].page, restPage, occ: mine.map((k) => occ[k].id) });
    prevIdx = chosen;
    prev = x;
  }
  return out;
}

/**
 * Every numbered series in the book, best first: the best chain, then the best
 * chain through what it left, and so on. A commentary that repeats the verse
 * numbers, quoted verses or a second text printed alongside show up as their
 * own series, so a reviewer can see which one is the book's.
 */
export function seriesList(pages, printedPageOf, family, options = {}, count = 4) {
  let pool = occurrences(pages, family, options.regex, options.maxRange ?? DEFAULTS.maxRange);
  if (options.hundreds) pool = pool.flatMap((x) => (x.n < 100 && (options.hundredsFrom == null || x.page >= options.hundredsFrom)
    ? [x, ...options.hundreds.map((h) => ({ ...x, n: h * 100 + x.n, to: h * 100 + x.to }))] : [x]));
  pool = pool.map((x, i) => ({ ...x, id: i }));
  const out = [];
  for (let k = 0; k < count && pool.length; k += 1) {
    const units = detectUnits(pages, printedPageOf, family, { ...options, occurrences: pool });
    if (units.length < 5) break;
    const used = new Set(units.flatMap((u) => u.occ));
    out.push(units);
    pool = pool.filter((x) => !used.has(x.id));
  }
  return out;
}

/** The text around a unit's marker on its page, for checking by eye. */
export function contextOf(pages, family, unit, width = 160, regex = null) {
  const page = pages.find((p) => p.page === unit.page);
  if (!page) return "";
  const re = family === "custom" ? new RegExp(regex, "gmu") : FAMILIES[family];
  re.lastIndex = 0;
  const text = page.content;
  for (const m of text.matchAll(re)) {
    const n = family === "dandaSlash" ? toInt(m[2]) : toInt(m[1]);
    if (n === unit.n) return text.slice(Math.max(0, m.index - width), m.index + m[0].length + 40).replace(/\n/g, " ⏎ ");
  }
  return "";
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

/** Per chapter: first..last, units found, numbers missing inside the run. */
export function summarize(units) {
  const byChapter = new Map();
  for (const u of units) byChapter.set(u.chapter, [...(byChapter.get(u.chapter) ?? []), u]);
  return [...byChapter.entries()].map(([chapter, list]) => {
    const covered = new Set();
    for (const u of list) for (let k = u.n; k <= u.to; k += 1) covered.add(k);
    const first = list[0].n;
    const last = list.at(-1).to;
    const missing = [];
    for (let k = first; k <= last; k += 1) if (!covered.has(k)) missing.push(k);
    return { chapter, chap: list[0].chap, first, last, found: list.length, missing, pageStart: list[0].page, pageEnd: list.at(-1).page };
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
  const [key, family = "danda"] = process.argv.slice(2).filter((a) => !a.startsWith("--"));
  const opts = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith("--") && a.includes("=")).map((a) => {
    const [k] = a.slice(2).split("=");
    const v = a.slice(a.indexOf("=") + 1);
    return [k, v === "true" ? true : v === "false" ? false : k === "regex" ? v : k === "hundreds" ? v.split(",").map(Number) : Number(v)];
  }));
  const { pages, printed } = await loadGranth(turso, key);
  if (process.argv.includes("--series")) {
    for (const [k, units] of seriesList(pages, printed, family, opts, 5).entries()) {
      const sum = summarize(units);
      console.log(`series ${k + 1}: ${units.length} units, ${sum.length} chapters, pages ${units[0].page}..${units.at(-1).page}`);
      for (const c of sum.slice(0, 8)) console.log(`   ch ${c.chapter}${c.chap != null ? ` (${c.chap})` : ""}: ${c.first}..${c.last} found ${c.found} missing ${c.missing.length} pages ${c.pageStart}..${c.pageEnd}`);
      if (sum.length > 8) console.log(`   … ${sum.length - 8} more chapters`);
    }
    process.exit(0);
  }
  const units = opts.series > 1 ? seriesList(pages, printed, family, opts, opts.series)[opts.series - 1] ?? [] : detectUnits(pages, printed, family, opts);
  if (process.argv.includes("--json")) console.log(JSON.stringify(units));
  else if (process.argv.includes("--context")) {
    const step = Math.max(1, Math.floor(units.length / 12));
    for (let i = 0; i < units.length; i += step) console.log(`[${units[i].chapter}:${units[i].n}@p${units[i].page}] …${contextOf(pages, family, units[i], 160, opts.regex)}`);
  } else for (const c of summarize(units)) console.log(`ch ${c.chapter}${c.chap != null ? ` (${c.chap})` : ""}: ${c.first}..${c.last} found ${c.found} missing ${c.missing.length}${c.missing.length ? ` [${c.missing.slice(0, 12).join(",")}${c.missing.length > 12 ? "…" : ""}]` : ""} pages ${c.pageStart}..${c.pageEnd}`);
}
