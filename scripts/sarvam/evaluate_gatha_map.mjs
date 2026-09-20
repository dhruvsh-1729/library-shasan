// Checks whether granth_gatha_map actually points at the right pages.
//
// The map was built from anchors in HTML index files (href "...#page=34"), so
// nothing ever confirmed the page really holds that verse. A gatha ends with
// its number between dandas — ॥१७॥ — so the test is simple: does the OCR text
// of the page the map names contain that number in that form?
import { writeFile } from "node:fs/promises";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";

const DEV_DIGITS = "०१२३४५६७८९";
const GUJ_DIGITS = "૦૧૨૩૪૫૬૭૮૯";

const toScript = (n, digits) => String(n).split("").map((d) => digits[Number(d)]).join("");

/** Every plausible way a verse number is written at the end of a gatha. */
function markerPatterns(n) {
  const forms = [String(n), toScript(n, DEV_DIGITS), toScript(n, GUJ_DIGITS)];
  const out = [];
  for (const f of forms) {
    const e = f.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    out.push(new RegExp(`[॥।|]\\s*${e}\\s*[॥।|]`, "u")); // ॥17॥
    out.push(new RegExp(`\\(\\s*${e}\\s*\\)`, "u"));                          // (17)
  }
  return out;
}

const args = process.argv.slice(2);
const outPath = args.find((a) => a.startsWith("--out="))?.split("=")[1];
const onlyBook = args.find((a) => a.startsWith("--book="))?.split("=")[1];
const window = Number(args.find((a) => a.startsWith("--window="))?.split("=")[1] ?? 1);

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// pull the whole map for the books we care about
let query = sb.from("granth_gatha_map")
  .select("book_code,adhikar,gatha,page_start,page_end,page_count,sequence_index,anchor_text")
  .order("book_code").order("sequence_index");
if (onlyBook) query = query.eq("book_code", onlyBook);

const rows = [];
const PAGE = 1000;
for (let from = 0; ; from += PAGE) {
  const { data, error } = await query.range(from, from + PAGE - 1);
  if (error) throw new Error(error.message);
  rows.push(...data);
  if (data.length < PAGE) break;
}
console.log(`gatha rows: ${rows.length.toLocaleString()}`);

const byBook = new Map();
for (const r of rows) {
  if (!byBook.has(r.book_code)) byBook.set(r.book_code, []);
  byBook.get(r.book_code).push(r);
}

const report = [];
for (const [book, entries] of byBook) {
  const pages = await turso.execute({
    sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
    args: [book],
  });
  const text = new Map(pages.rows.map((p) => [Number(p.page_number), String(p.content)]));
  if (text.size === 0) {
    report.push({ book, rows: entries.length, ocrPages: 0, note: "no OCR text for this book" });
    continue;
  }

  let onNamedPage = 0, withinWindow = 0, notFound = 0, outOfRange = 0, nonMonotonic = 0;
  const misses = [];
  let prevPage = -Infinity, prevSeq = -Infinity;

  for (const e of entries) {
    const start = Number(e.page_start);
    if (!Number.isFinite(start) || start < 1 || start > text.size) outOfRange += 1;
    if (Number(e.sequence_index) > prevSeq && start < prevPage) nonMonotonic += 1;
    prevPage = start; prevSeq = Number(e.sequence_index);

    const pats = markerPatterns(e.gatha);
    const hitOn = pats.some((p) => p.test(text.get(start) ?? ""));
    if (hitOn) { onNamedPage += 1; continue; }

    let near = false;
    for (let d = 1; d <= window && !near; d += 1) {
      near = pats.some((p) => p.test(text.get(start - d) ?? "") || p.test(text.get(start + d) ?? ""));
    }
    if (near) withinWindow += 1;
    else {
      notFound += 1;
      if (misses.length < 5) misses.push({ adhikar: e.adhikar, gatha: e.gatha, page_start: start });
    }
  }

  const n = entries.length;
  report.push({
    book, rows: n, ocrPages: text.size,
    onNamedPage, withinWindow, notFound, outOfRange, nonMonotonic,
    exactRate: +(onNamedPage / n).toFixed(4),
    windowRate: +((onNamedPage + withinWindow) / n).toFixed(4),
    misses,
  });
  console.log(`  ${book}: ${n} gathas | exact ${(100 * onNamedPage / n).toFixed(1)}% | ±${window} ${(100 * (onNamedPage + withinWindow) / n).toFixed(1)}% | notFound ${notFound} | outOfRange ${outOfRange} | nonMonotonic ${nonMonotonic}`);
}

const tot = report.reduce((a, r) => ({
  rows: a.rows + (r.rows || 0),
  exact: a.exact + (r.onNamedPage || 0),
  win: a.win + (r.onNamedPage || 0) + (r.withinWindow || 0),
}), { rows: 0, exact: 0, win: 0 });
console.log(`\noverall: ${(100 * tot.exact / tot.rows).toFixed(1)}% land on the named page, ${(100 * tot.win / tot.rows).toFixed(1)}% within ±${window}`);

if (outPath) {
  await writeFile(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), window, report }, null, 2));
  console.log(`report -> ${outPath}`);
}
