// Turns the per-chunk Sarvam HTML into the page-wise CSV the library pipeline
// already uses, matching the existing 17-column schema exactly.
import { readFile, writeFile, readdir } from "node:fs/promises";
import path from "node:path";

const [, , htmlDir, outCsv, metaJson] = process.argv;
if (!htmlDir || !outCsv || !metaJson) {
  console.error("usage: build_granth_csv.mjs <htmlDir> <out.csv> <meta.json>");
  process.exit(1);
}
const meta = JSON.parse(await readFile(metaJson, "utf8"));

const COLUMNS = [
  "granth_key", "book_number", "library_code", "granth_name", "source_rel_path",
  "pdf_url", "page_number", "content", "method", "status", "quality_score",
  "chars", "google_reason", "needs_review", "embedded_score", "local_score", "error",
];

/** Block-level tags become newlines so the page keeps its line structure. */
function htmlToText(html) {
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    // Table cells keep a separator instead of running into each other.
    .replace(/<\/(td|th)>/gi, " | ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|h[1-6]|li|tr|blockquote)>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/[ \t]+/g, " ")
    .replace(/[ \t]*\n[ \t]*/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function csvCell(value) {
  const s = value == null ? "" : String(value);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const files = (await readdir(htmlDir))
  .filter((f) => f.endsWith(".html"))
  .sort((a, b) => Number(a.match(/_p(\d+)-/)[1]) - Number(b.match(/_p(\d+)-/)[1]));

const pages = [];
for (const f of files) {
  const html = await readFile(path.join(htmlDir, f), "utf8");
  const containers = html.match(
    /<div class="page-body-container"[^>]*>[\s\S]*?(?=<div class="page-body-container"|<\/body>)/g
  );
  if (!containers) throw new Error(`${f}: no page containers found`);
  const firstPage = Number(f.match(/_p(\d+)-/)[1]);
  containers.forEach((c, i) => {
    pages.push({ pageNumber: firstPage + i, text: htmlToText(c) });
  });
}

pages.sort((a, b) => a.pageNumber - b.pageNumber);

const expected = meta.page_count;
if (pages.length !== expected) {
  throw new Error(`page count mismatch: built ${pages.length}, expected ${expected}`);
}
for (let i = 0; i < pages.length; i += 1) {
  if (pages[i].pageNumber !== i + 1) {
    throw new Error(`page numbering gap at index ${i}: got ${pages[i].pageNumber}`);
  }
}

const lines = [COLUMNS.join(",")];
for (const p of pages) {
  lines.push([
    meta.granth_key, meta.book_number, meta.library_code ?? "", meta.granth_name,
    meta.source_rel_path, meta.pdf_url, p.pageNumber, p.text,
    "sarvam_doc_ai", "accepted", "", p.text.length, "", "false", "", "", "",
  ].map(csvCell).join(","));
}

const csv = lines.join("\n") + "\n";
await writeFile(outCsv, csv, "utf8");
const empty = pages.filter((p) => !p.text.trim()).length;
console.log(`pages: ${pages.length} (1..${pages[pages.length - 1].pageNumber}), empty: ${empty}`);
console.log(`chars: ${pages.reduce((n, p) => n + p.text.length, 0).toLocaleString()}`);
console.log(`csv  : ${outCsv} (${csv.length.toLocaleString()} bytes)`);
