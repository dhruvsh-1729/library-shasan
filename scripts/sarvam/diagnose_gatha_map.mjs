// Works out WHY a gatha mapping misses, which decides whether it is fixable.
//
// Three different failures look alike in a hit-rate table:
//   * a constant page offset (the whole book is shifted by n) — fixable in SQL
//   * poor OCR on the page (the marker is there, we just cannot read it)
//   * a book that does not use ॥n॥ markers at all — not a mapping fault
// Trying every offset separates the first from the rest, and cross-referencing
// the granth's quality score separates the second from the third.
import { writeFile } from "node:fs/promises";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { scoreGranth } from "./assess_quality.mjs";

const DEV = "०१२३४५६७८९";
const GUJ = "૦૧૨૩૪૫૬૭૮૯";
const toScript = (n, d) => String(n).split("").map((c) => d[Number(c)]).join("");

function markerPatterns(n) {
  const out = [];
  for (const f of [String(n), toScript(n, DEV), toScript(n, GUJ)]) {
    const e = f.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    out.push(new RegExp(`[॥।|]\\s*${e}\\s*[॥।|]`, "u"));
    out.push(new RegExp(`\\(\\s*${e}\\s*\\)`, "u"));
  }
  return out;
}
/** Does the page carry ANY verse marker? Distinguishes "wrong page" from "book has no markers". */
const ANY_MARKER = /[॥।|]\s*[0-9०-९૦-૯]{1,4}\s*[॥।|]/u;

const OFFSETS = [];
for (let i = -6; i <= 6; i += 1) OFFSETS.push(i);

const outPath = process.argv.find((a) => a.startsWith("--out="))?.split("=")[1];
const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const rows = [];
for (let from = 0; ; from += 1000) {
  const { data, error } = await sb.from("granth_gatha_map")
    .select("book_code,adhikar,gatha,page_start,sequence_index")
    .order("book_code").order("sequence_index").range(from, from + 999);
  if (error) throw new Error(error.message);
  rows.push(...data);
  if (data.length < 1000) break;
}

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
  if (!pages.rows.length) { report.push({ book, gathas: entries.length, diagnosis: "NO_OCR_TEXT" }); continue; }

  const text = new Map(pages.rows.map((p) => [Number(p.page_number), String(p.content)]));
  const quality = scoreGranth(pages.rows.map((r) => ({ content: r.content }))).score;
  const pagesWithAnyMarker = pages.rows.filter((p) => ANY_MARKER.test(String(p.content))).length;
  const markerCoverage = pagesWithAnyMarker / pages.rows.length;

  const scores = OFFSETS.map((off) => {
    let hit = 0;
    for (const e of entries) {
      const pats = markerPatterns(e.gatha);
      if (pats.some((p) => p.test(text.get(Number(e.page_start) + off) ?? ""))) hit += 1;
    }
    return { off, rate: hit / entries.length };
  });
  const best = scores.reduce((a, b) => (b.rate > a.rate ? b : a));
  const zero = scores.find((s) => s.off === 0);

  let diagnosis;
  if (best.rate >= 0.8 && best.off !== 0 && best.rate - zero.rate > 0.15) diagnosis = "OFFSET";
  else if (best.rate >= 0.8) diagnosis = "OK";
  else if (markerCoverage < 0.15) diagnosis = "NO_MARKERS_IN_TEXT";
  else if (quality < 75) diagnosis = "POOR_OCR";
  else diagnosis = "MAPPING_SUSPECT";

  report.push({
    book, gathas: entries.length, ocrPages: pages.rows.length,
    quality, markerCoverage: +markerCoverage.toFixed(3),
    rateAt0: +zero.rate.toFixed(4), bestOffset: best.off, bestRate: +best.rate.toFixed(4),
    diagnosis,
  });
}

report.sort((a, b) => (a.rateAt0 ?? 0) - (b.rateAt0 ?? 0));
const tally = report.reduce((acc, r) => ({ ...acc, [r.diagnosis]: (acc[r.diagnosis] ?? 0) + 1 }), {});
const gathasBy = (d) => report.filter((r) => r.diagnosis === d).reduce((n, r) => n + (r.gathas || 0), 0);

console.log(`books: ${report.length}`);
for (const [d, n] of Object.entries(tally).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${d.padEnd(20)} ${String(n).padStart(4)} books   ${gathasBy(d).toLocaleString().padStart(7)} gathas`);
}
const offsets = report.filter((r) => r.diagnosis === "OFFSET");
if (offsets.length) {
  console.log(`\nfixable by shifting page_start:`);
  for (const r of offsets) console.log(`  ${r.book.padEnd(6)} offset ${String(r.bestOffset).padStart(3)}  ${(100*r.rateAt0).toFixed(1)}% -> ${(100*r.bestRate).toFixed(1)}%  (${r.gathas} gathas)`);
}
if (outPath) { await writeFile(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), tally, report }, null, 2)); console.log(`\nreport -> ${outPath}`); }
