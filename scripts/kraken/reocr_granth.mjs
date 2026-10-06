// Re-OCRs one granth with our own Kraken model (Railway service) and sends only
// the pages it flags as tables/charts to Sarvam. Then it publishes exactly like
// scripts/sarvam/reocr_granth.mjs: searchable PDF + CSV to UploadThing, Turso
// text, line boxes, Supabase pointers, status. Each step is checkpointed.
//
//   node --env-file=.env scripts/kraken/reocr_granth.mjs <granth_key> [--pdf=<local.pdf>] [--dry-run] [--keep-old] [--no-sarvam]
//
// --dry-run   stops after building the PDF + CSV in the work dir (nothing uploaded or written)
// --pdf=      use a local scan (e.g. the Kingston copy) instead of downloading documents.pdf_url
// --no-sarvam keep Kraken's text for flagged pages too (no Sarvam spend)
import { mkdir, writeFile, readFile, readdir, rm, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { PDFDocument } from "pdf-lib";
import { submitPdf, waitForJob } from "./kraken_client.mjs";
import { stripTextLayer } from "../sarvam/strip_text_layer.mjs";
import { addTextLayer } from "../sarvam/add_text_layer.mjs";

const run = promisify(execFile);
const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, "../..");
const SARVAM = path.join(ROOT, "scripts/sarvam");
const WORK_ROOT = process.env.REOCR_WORK || "/tmp/reocr-work";
const MODEL_TAG = process.env.KRAKEN_MODEL_TAG || "kraken_r4";

const args = process.argv.slice(2);
const granthKey = args.find((a) => !a.startsWith("--"));
const flag = (n) => args.includes(`--${n}`);
const opt = (n) => args.find((a) => a.startsWith(`--${n}=`))?.split("=").slice(1).join("=");
const dryRun = flag("dry-run"), keepOld = flag("keep-old"), noSarvam = flag("no-sarvam"), localPdf = opt("pdf");
if (!granthKey) {
  console.error("usage: reocr_granth.mjs <granth_key> [--pdf=<local.pdf>] [--dry-run] [--keep-old] [--no-sarvam]");
  process.exit(1);
}

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const work = path.join(WORK_ROOT, `kraken_${granthKey}`);
await mkdir(work, { recursive: true });
const statePath = path.join(work, "state.json");
const state = existsSync(statePath) ? JSON.parse(await readFile(statePath, "utf8")) : { steps: {} };
const save = () => writeFile(statePath, JSON.stringify(state, null, 2));
const log = (m) => console.log(`[${granthKey}] ${m}`);

async function step(name, fn) {
  if (state.steps[name]?.done) { log(`= ${name} (already done)`); return state.steps[name].value; }
  const t = Date.now();
  const value = await fn();
  state.steps[name] = { done: true, value, ms: Date.now() - t };
  await save();
  log(`+ ${name} (${((Date.now() - t) / 1000).toFixed(1)}s)`);
  return value;
}

async function callScript(script, scriptArgs, opts = {}) {
  const { stdout } = await run(process.execPath, [`--env-file=${path.join(ROOT, ".env")}`, script, ...scriptArgs],
    { cwd: ROOT, maxBuffer: 64 * 1024 * 1024, ...opts });
  return stdout;
}

/** Same conversion build_granth_csv.mjs applies to Sarvam HTML (block tags -> newlines, cells -> " | "). */
function htmlToText(html) {
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, " ").replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<\/(td|th)>/gi, " | ").replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|h[1-6]|li|tr|blockquote)>/gi, "\n").replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/[ \t]+/g, " ").replace(/[ \t]*\n[ \t]*/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

const csvCell = (v) => { const s = v == null ? "" : String(v); return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
const CSV_COLUMNS = ["granth_key", "book_number", "library_code", "granth_name", "source_rel_path", "pdf_url", "page_number",
  "content", "method", "status", "quality_score", "chars", "google_reason", "needs_review", "embedded_score", "local_score", "error"];

// ---------------------------------------------------------------- 1. resolve
const meta = await step("resolve", async () => {
  const g = await turso.execute({
    sql: "SELECT granth_key, book_number, library_code, granth_name, source_rel_path, page_count, xlsx_url, xlsx_key FROM ocr_granths WHERE granth_key = ?",
    args: [granthKey],
  });
  if (!g.rows.length) throw new Error(`granth ${granthKey} not in ocr_granths`);
  const row = g.rows[0];
  const relPath = String(row.source_rel_path);
  const { data: docs, error } = await sb.from("documents").select("custom_id,pdf_name,pdf_url,csv_url").eq("original_relative_path", relPath);
  if (error) throw new Error(`supabase documents: ${error.message}`);
  if (!docs?.length) throw new Error(`no documents row for ${relPath}`);
  const { data: files } = await sb.from("granth_ocr_files").select("ut_key").eq("custom_id", docs[0].custom_id);
  return {
    granthKey, bookNumber: String(row.book_number ?? granthKey), libraryCode: row.library_code == null ? "" : String(row.library_code),
    granthName: String(row.granth_name ?? ""), sourceRelPath: relPath, customId: docs[0].custom_id,
    oldPdfUrl: docs[0].pdf_url, oldCsvUrl: docs[0].csv_url, oldPdfKey: files?.[0]?.ut_key ?? null,
    oldCsvKey: docs[0].csv_url ? String(docs[0].csv_url).split("/f/")[1] : null, oldXlsxKey: row.xlsx_key ?? null,
  };
});
log(meta.granthName);

// ---------------------------------------------------------------- 2. source PDF
const srcPdf = path.join(work, "source.pdf");
await step("source", async () => {
  if (localPdf) { await writeFile(srcPdf, await readFile(localPdf)); return { from: localPdf, bytes: (await stat(srcPdf)).size }; }
  let last;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      const res = await fetch(meta.oldPdfUrl);
      if (!res.ok) throw new Error(`pdf download ${res.status}`);
      const bytes = Buffer.from(await res.arrayBuffer());
      if (bytes.length < 1024) throw new Error("pdf download too small");
      await writeFile(srcPdf, bytes);
      return { from: meta.oldPdfUrl, bytes: bytes.length };
    } catch (e) { last = e; log(`  download attempt ${attempt}: ${e.message}`); await new Promise((r) => setTimeout(r, 4000 * attempt)); }
  }
  throw last;
});
const pageCount = (await PDFDocument.load(await readFile(srcPdf), { ignoreEncryption: true, updateMetadata: false })).getPageCount();

// ---------------------------------------------------------------- 3. Kraken
const krakenPath = path.join(work, "kraken.json");
const kr = await step("kraken", async () => {
  const job = await submitPdf(srcPdf);
  log(`  kraken job ${job.id}: ${job.pages} pages`);
  const res = await waitForJob(job.id, (s) => log(`  kraken ${s.done}/${s.pages} pages, ${s.s_per_page ?? "-"} s/page, flagged ${s.needs_sarvam}, errors ${s.errors}`));
  await writeFile(krakenPath, JSON.stringify(res));
  const pages = Object.values(res.result);
  const errors = Object.entries(res.result).filter(([, v]) => v.error).map(([p]) => Number(p));
  const flagged = Object.entries(res.result).filter(([, v]) => v.needs_sarvam).map(([p]) => Number(p)).sort((a, b) => a - b);
  if (res.pages !== pageCount) throw new Error(`service saw ${res.pages} pages, PDF has ${pageCount}`);
  return { pages: pages.length, errors, flagged, seconds: res.seconds };
});
const kraken = JSON.parse(await readFile(krakenPath, "utf8")).result;
// Pages the service could not process go to Sarvam too.
const toSarvam = noSarvam ? [] : [...new Set([...kr.flagged, ...kr.errors])].sort((a, b) => a - b);
log(`${toSarvam.length} of ${pageCount} pages go to Sarvam`);

// ---------------------------------------------------------------- 4. Sarvam for the flagged pages only
const sarvamDir = path.join(work, "sarvam");
const sarvam = await step("sarvam", async () => {
  if (!toSarvam.length) return { pages: {}, meta: {} };
  const src = await PDFDocument.load(await readFile(srcPdf), { ignoreEncryption: true });
  const sub = await PDFDocument.create();
  (await sub.copyPages(src, toSarvam.map((p) => p - 1))).forEach((p) => sub.addPage(p));
  await mkdir(sarvamDir, { recursive: true });
  const subPdf = path.join(sarvamDir, "flagged.pdf");
  await writeFile(subPdf, await sub.save());
  const chunks = path.join(sarvamDir, "chunks"), out = path.join(sarvamDir, "out");
  await callScript(path.join(SARVAM, "split_pdf.mjs"), [subPdf, chunks, "10"]);
  const stdout = await callScript(path.join(SARVAM, "run_digitise.mjs"), [chunks, out], {
    env: { ...process.env, SARVAM_CONCURRENCY: process.env.SARVAM_CONCURRENCY || "3", SARVAM_LANGUAGE: process.env.SARVAM_LANGUAGE || "gu-IN" },
  });
  const tail = stdout.trim().split("\n").slice(-1)[0];
  if (!/0 failures/.test(tail)) throw new Error(`sarvam incomplete: ${tail}`);
  // sub-PDF page s (1-based) is book page toSarvam[s-1]
  const pages = {}, blocks = {};
  for (const f of (await readdir(path.join(out, "html"))).filter((x) => x.endsWith(".html"))) {
    const first = Number(f.match(/_p(\d+)-/)[1]);
    const html = await readFile(path.join(out, "html", f), "utf8");
    const containers = html.match(/<div class="page-body-container"[^>]*>[\s\S]*?(?=<div class="page-body-container"|<\/body>)/g) ?? [];
    containers.forEach((c, i) => { pages[toSarvam[first + i - 1]] = htmlToText(c); });
  }
  for (const d of await readdir(path.join(out, "zips"), { withFileTypes: true })) {
    const m = d.isDirectory() && d.name.match(/_p(\d+)-(\d+)/);
    if (!m) continue;
    const metaDir = path.join(out, "zips", d.name, d.name, "metadata");
    if (!existsSync(metaDir)) continue;
    for (const f of (await readdir(metaDir)).filter((x) => /^page_\d+\.json$/.test(x))) {
      const idx = Number(f.match(/page_(\d+)/)[1]);
      blocks[toSarvam[Number(m[1]) + idx - 2]] = JSON.parse(await readFile(path.join(metaDir, f), "utf8"));
    }
  }
  const missing = toSarvam.filter((p) => pages[p] == null);
  if (missing.length) throw new Error(`sarvam returned no text for pages ${missing.join(",")}`);
  return { pages, meta: blocks };
});

// ---------------------------------------------------------------- 5. merge
const merged = [];
for (let p = 1; p <= pageCount; p += 1) {
  const k = kraken[String(p)] ?? {};
  if (sarvam.pages[p] != null) merged.push({ page: p, text: sarvam.pages[p], method: "sarvam_doc_ai", meta: sarvam.meta[p] ?? null, boxes: null });
  else merged.push({ page: p, text: k.text ?? "", method: MODEL_TAG, boxes: (k.lines ?? []).map((l) => l.box), lines: k.lines ?? [] });
}

// ---------------------------------------------------------------- 6. artefacts
const base = path.basename(meta.sourceRelPath, ".pdf");
const csvName = `${base}_kraken.csv`, csvPath = path.join(work, csvName);
const newPdfPath = path.join(work, path.basename(meta.sourceRelPath));
const writeCsv = async (pdfUrl) => {
  const rows = [CSV_COLUMNS.join(",")];
  for (const m of merged) {
    rows.push([meta.granthKey, meta.bookNumber, meta.libraryCode, meta.granthName, meta.sourceRelPath, pdfUrl, m.page, m.text,
      m.method, "accepted", "", m.text.length, "", "false", "", "", ""].map(csvCell).join(","));
  }
  await writeFile(csvPath, rows.join("\n") + "\n", "utf8");
};
await step("build-csv", async () => { await writeCsv("PENDING"); return { bytes: (await stat(csvPath)).size }; });

await step("build-pdf", async () => {
  const metaByPage = new Map();
  for (const m of merged) {
    if (m.method === "sarvam_doc_ai") { if (m.meta) metaByPage.set(m.page, m.meta); continue; }
    metaByPage.set(m.page, { image_width: 1, image_height: 1, blocks: m.lines.map((l, i) => ({
      text: l.text, reading_order: i, coordinates: { x1: l.box[0], y1: l.box[1], x2: l.box[2], y2: l.box[3] } })) });
  }
  const tmp = `${newPdfPath}.stripped.tmp.pdf`;
  await stripTextLayer(srcPdf, tmp);
  const a = await addTextLayer({ srcPdf: tmp, metaByPage, outPdf: newPdfPath });
  await rm(tmp, { force: true });
  return { pages: a.pages, blocksDrawn: a.blocksDrawn, bytes: (await stat(newPdfPath)).size };
});

const summary = {
  pages: pageCount, kraken: merged.filter((m) => m.method === MODEL_TAG).length, sarvam: merged.filter((m) => m.method === "sarvam_doc_ai").length,
  emptyPages: merged.filter((m) => !m.text.trim()).length, chars: merged.reduce((n, m) => n + m.text.length, 0),
};
log(`built: ${JSON.stringify(summary)}`);
if (dryRun) { log(`dry run: PDF + CSV are in ${work}; nothing uploaded or written`); process.exit(0); }

// ---------------------------------------------------------------- 7. upload
const uploaded = await step("upload", async () => {
  const out = path.join(work, "uploaded.json");
  await callScript(path.join(SARVAM, "upload_assets.mjs"), [newPdfPath, `--out=${out}`]);
  const pdf = JSON.parse(await readFile(out, "utf8"))[0];
  await writeCsv(pdf.url);
  const out2 = path.join(work, "uploaded_csv.json");
  await callScript(path.join(SARVAM, "upload_assets.mjs"), [csvPath, `--out=${out2}`]);
  return { pdf, csv: JSON.parse(await readFile(out2, "utf8"))[0] };
});

// ---------------------------------------------------------------- 8. database (text, pointers, search index)
await step("update-db", async () => {
  const cfgPath = path.join(work, "update.json");
  const gatha = await sb.from("granth_gatha_map").select("*", { count: "exact", head: true }).eq("custom_id", meta.customId);
  await writeFile(cfgPath, JSON.stringify({
    granthKey: meta.granthKey, customId: meta.customId, csvPath, expectedPages: pageCount, expectedGathaRows: gatha.count ?? 0,
    newPdfUrl: uploaded.pdf.url, newPdfKey: uploaded.pdf.key, newCsvUrl: uploaded.csv.url, newCsvKey: uploaded.csv.key,
    newCsvFilename: csvName, newCsvCustomId: `${meta.granthKey}__${MODEL_TAG}__ocr_csv`,
  }, null, 2));
  await callScript(path.join(SARVAM, "update_granth_sources.mjs"), [cfgPath]);
  return { gathaRows: gatha.count ?? 0 };
});

// ---------------------------------------------------------------- 9. line boxes
// One box per non-empty text line, in order (what lib/line-boxes.ts expects). Sarvam pages have
// no line boxes, so any old row for them is removed rather than left mismatching the new text.
await step("line-boxes", async () => {
  const stmts = [];
  for (const m of merged) {
    if (m.method === MODEL_TAG && m.boxes.length && m.boxes.length === m.text.split("\n").filter((l) => l.trim()).length) {
      stmts.push({ sql: `INSERT INTO ocr_line_boxes (granth_key, page_number, boxes, source) VALUES (?, ?, ?, ?)
              ON CONFLICT(granth_key, page_number) DO UPDATE SET boxes = excluded.boxes, source = excluded.source, words = NULL, updated_at = CURRENT_TIMESTAMP`,
        args: [granthKey, m.page, JSON.stringify(m.boxes), MODEL_TAG] });
    } else {
      stmts.push({ sql: "DELETE FROM ocr_line_boxes WHERE granth_key = ? AND page_number = ?", args: [granthKey, m.page] });
    }
  }
  for (let i = 0; i < stmts.length; i += 200) await turso.batch(stmts.slice(i, i + 200), "write");
  return { written: stmts.filter((s) => s.sql.startsWith("INSERT")).length, cleared: stmts.filter((s) => s.sql.startsWith("DELETE")).length };
});

// ---------------------------------------------------------------- 10. verify + status
await step("verify", async () => {
  const pages = await turso.execute({ sql: "SELECT COUNT(*) n, SUM(length(content)) chars FROM ocr_pages WHERE granth_key = ?", args: [granthKey] });
  const { data: doc } = await sb.from("documents").select("pdf_url,csv_url").eq("custom_id", meta.customId).single();
  if (!doc?.pdf_url?.includes(uploaded.pdf.key) || !doc?.csv_url?.includes(uploaded.csv.key)) throw new Error("database still points at the old assets");
  if (Number(pages.rows[0].n) !== pageCount) throw new Error(`ocr_pages has ${pages.rows[0].n} rows, expected ${pageCount}`);
  const { error } = await sb.from("documents").update({ status: "processed", updated_at: new Date().toISOString() }).eq("custom_id", meta.customId);
  if (error) throw new Error(`status: ${error.message}`);
  return { pages: Number(pages.rows[0].n), chars: Number(pages.rows[0].chars) };
});

// ---------------------------------------------------------------- 11. old assets
await step("delete-old", async () => {
  if (keepOld) return { skipped: true };
  const keys = [meta.oldPdfKey, meta.oldCsvKey, meta.oldXlsxKey].filter((k) => k && k !== uploaded.pdf.key && k !== uploaded.csv.key);
  if (!keys.length) return { deleted: 0 };
  await callScript(path.join(SARVAM, "delete_old_assets.mjs"), keys);
  return { deleted: keys.length, keys };
});

log(`done: ${JSON.stringify(summary)}`);
