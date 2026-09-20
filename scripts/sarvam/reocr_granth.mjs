// Re-runs one granth end to end through Sarvam and repoints everything that
// referenced the old assets. Each step is checkpointed, so a run that dies
// half way resumes instead of paying for the same pages twice.
//
//   node --env-file=.env scripts/sarvam/reocr_granth.mjs <granth_key> [--keep-old]
import { mkdir, writeFile, readFile, rm, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";

const run = promisify(execFile);
const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, "../..");
const WORK_ROOT = process.env.REOCR_WORK || "/tmp/reocr-work";

const granthKey = process.argv[2];
const keepOld = process.argv.includes("--keep-old");
if (!granthKey) {
  console.error("usage: reocr_granth.mjs <granth_key> [--keep-old]");
  process.exit(1);
}

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const work = path.join(WORK_ROOT, granthKey);
await mkdir(work, { recursive: true });
const statePath = path.join(work, "state.json");
const state = existsSync(statePath) ? JSON.parse(await readFile(statePath, "utf8")) : { steps: {} };
const save = () => writeFile(statePath, JSON.stringify(state, null, 2));

function log(msg) {
  console.log(`[${granthKey}] ${msg}`);
}

async function step(name, fn) {
  if (state.steps[name]?.done) {
    log(`= ${name} (already done)`);
    return state.steps[name].value;
  }
  const started = Date.now();
  const value = await fn();
  state.steps[name] = { done: true, value, ms: Date.now() - started };
  await save();
  log(`+ ${name} (${((Date.now() - started) / 1000).toFixed(1)}s)`);
  return value;
}

const node = process.execPath;
const nodeArgs = (args) => [`--env-file=${path.join(ROOT, ".env")}`, ...args];
async function callScript(script, args, opts = {}) {
  const { stdout } = await run(node, nodeArgs([path.join(HERE, script), ...args]), {
    cwd: ROOT,
    maxBuffer: 64 * 1024 * 1024,
    ...opts,
  });
  return stdout;
}

// ---------------------------------------------------------------- 1. resolve
const meta = await step("resolve", async () => {
  const g = await turso.execute({
    sql: `SELECT granth_key, book_number, library_code, granth_name, source_rel_path, page_count
          FROM ocr_granths WHERE granth_key = ?`,
    args: [granthKey],
  });
  if (!g.rows.length) throw new Error(`granth ${granthKey} not in ocr_granths`);
  const row = g.rows[0];
  const relPath = String(row.source_rel_path);

  const { data: docs, error } = await sb
    .from("documents")
    .select("custom_id,original_relative_path,pdf_name,pdf_url,csv_url")
    .eq("original_relative_path", relPath);
  if (error) throw new Error(`supabase documents: ${error.message}`);
  if (!docs?.length) throw new Error(`no documents row for ${relPath}`);

  const { data: files } = await sb
    .from("granth_ocr_files")
    .select("id,ut_key,ufs_url,file_name,custom_id")
    .eq("custom_id", docs[0].custom_id);

  const xl = await turso.execute({
    sql: "SELECT xlsx_url, xlsx_key FROM ocr_granths WHERE granth_key = ?",
    args: [granthKey],
  });

  return {
    granthKey,
    bookNumber: String(row.book_number ?? granthKey),
    libraryCode: row.library_code == null ? "" : String(row.library_code),
    granthName: String(row.granth_name ?? ""),
    sourceRelPath: relPath,
    declaredPages: Number(row.page_count ?? 0),
    customId: docs[0].custom_id,
    pdfName: docs[0].pdf_name,
    oldPdfUrl: docs[0].pdf_url,
    oldCsvUrl: docs[0].csv_url,
    oldPdfKey: files?.[0]?.ut_key ?? null,
    oldCsvKey: docs[0].csv_url ? String(docs[0].csv_url).split("/f/")[1] : null,
    oldXlsxUrl: xl.rows[0]?.xlsx_url ?? null,
    oldXlsxKey: xl.rows[0]?.xlsx_key ?? null,
  };
});
log(`${meta.granthName} — ${meta.declaredPages} pages declared`);

// ---------------------------------------------------------------- 2. download
const srcPdf = path.join(work, "source.pdf");
await step("download", async () => {
  if (!meta.oldPdfUrl) throw new Error("no pdf_url to download");
  const res = await fetch(meta.oldPdfUrl);
  if (!res.ok) throw new Error(`pdf download ${res.status}`);
  await writeFile(srcPdf, Buffer.from(await res.arrayBuffer()));
  return { bytes: (await stat(srcPdf)).size };
});

// ---------------------------------------------------------------- 3. split
const chunksDir = path.join(work, "chunks");
await step("split", async () => {
  await callScript("split_pdf.mjs", [srcPdf, chunksDir, "10"]);
  const manifest = JSON.parse(await readFile(path.join(chunksDir, "chunks.json"), "utf8"));
  return { pages: manifest.total, chunks: manifest.chunks.length };
});

// ---------------------------------------------------------------- 4. digitise
const outDir = path.join(work, "out");
await step("digitise", async () => {
  const stdout = await callScript("run_digitise.mjs", [chunksDir, outDir], {
    env: { ...process.env, SARVAM_CONCURRENCY: process.env.SARVAM_CONCURRENCY || "3", SARVAM_LANGUAGE: "gu-IN" },
  });
  const tail = stdout.trim().split("\n").slice(-1)[0];
  if (!/0 failures/.test(tail)) throw new Error(`digitise incomplete: ${tail}`);
  return { summary: tail };
});

// ---------------------------------------------------------------- 5. artefacts
const csvName = `${path.basename(meta.sourceRelPath, ".pdf")}_sarvam.csv`;
const csvPath = path.join(work, csvName);
const newPdfPath = path.join(work, path.basename(meta.sourceRelPath));

await step("build-csv", async () => {
  const metaFile = path.join(work, "csvmeta.json");
  await writeFile(metaFile, JSON.stringify({
    granth_key: meta.granthKey,
    book_number: meta.bookNumber,
    library_code: meta.libraryCode,
    granth_name: meta.granthName,
    source_rel_path: meta.sourceRelPath,
    pdf_url: "PENDING",
    page_count: state.steps.split.value.pages,
  }));
  await callScript("build_granth_csv.mjs", [path.join(outDir, "html"), csvPath, metaFile]);
  return { bytes: (await stat(csvPath)).size };
});

await step("build-pdf", async () => {
  await callScript("build_searchable_pdf.mjs", [srcPdf, path.join(outDir, "zips"), newPdfPath], {
    env: { ...process.env, NODE_OPTIONS: "--max-old-space-size=6144" },
  });
  return { bytes: (await stat(newPdfPath)).size };
});

// ---------------------------------------------------------------- 6. upload
const uploaded = await step("upload", async () => {
  const outFile = path.join(work, "uploaded.json");
  await callScript("upload_assets.mjs", [newPdfPath, csvPath, `--out=${outFile}`]);
  const list = JSON.parse(await readFile(outFile, "utf8"));
  const pdf = list.find((f) => f.file.endsWith(".pdf"));
  const csv = list.find((f) => f.file.endsWith(".csv"));
  if (!pdf || !csv) throw new Error("upload did not return both assets");
  return { pdf, csv };
});

// The CSV embeds pdf_url, so rewrite it now that the new PDF has a URL.
await step("csv-pdfurl", async () => {
  const body = await readFile(csvPath, "utf8");
  await writeFile(csvPath, body.split("PENDING").join(uploaded.pdf.url));
  const outFile = path.join(work, "uploaded_csv2.json");
  await callScript("upload_assets.mjs", [csvPath, `--out=${outFile}`]);
  const list = JSON.parse(await readFile(outFile, "utf8"));
  state.steps.upload.value.csvSuperseded = uploaded.csv.key;
  uploaded.csv = list[0];
  state.steps.upload.value.csv = list[0];
  await save();
  return { csvKey: list[0].key };
});

// ---------------------------------------------------------------- 7. database
await step("update-db", async () => {
  const cfgPath = path.join(work, "update.json");
  const gathaCount = await sb
    .from("granth_gatha_map")
    .select("*", { count: "exact", head: true })
    .eq("custom_id", meta.customId);
  await writeFile(cfgPath, JSON.stringify({
    granthKey: meta.granthKey,
    customId: meta.customId,
    csvPath,
    expectedPages: state.steps.split.value.pages,
    expectedGathaRows: gathaCount.count ?? 0,
    newPdfUrl: uploaded.pdf.url,
    newPdfKey: uploaded.pdf.key,
    newCsvUrl: uploaded.csv.url,
    newCsvKey: uploaded.csv.key,
    newCsvFilename: csvName,
    newCsvCustomId: `${meta.granthKey}__sarvam__ocr_csv`,
  }, null, 2));
  await callScript("update_granth_sources.mjs", [cfgPath]);
  return { gathaRows: gathaCount.count ?? 0 };
});

// ---------------------------------------------------------------- 8. verify
const verdict = await step("verify", async () => {
  const pages = await turso.execute({
    sql: "SELECT COUNT(*) n, SUM(length(content)) chars FROM ocr_pages WHERE granth_key = ?",
    args: [granthKey],
  });
  const { data: doc } = await sb
    .from("documents").select("pdf_url,csv_url").eq("custom_id", meta.customId).single();
  const okPdf = doc?.pdf_url?.includes(uploaded.pdf.key);
  const okCsv = doc?.csv_url?.includes(uploaded.csv.key);
  const stored = Number(pages.rows[0].n);
  const expected = state.steps.split.value.pages;
  if (!okPdf || !okCsv) throw new Error("database still points at the old assets");
  if (stored !== expected) throw new Error(`ocr_pages has ${stored} rows, expected ${expected}`);
  return { pages: stored, chars: Number(pages.rows[0].chars), okPdf, okCsv };
});
log(`verified: ${verdict.pages} pages, ${verdict.chars.toLocaleString()} chars`);

// ---------------------------------------------------------------- 9. cleanup
await step("delete-old", async () => {
  if (keepOld) return { skipped: true };
  const keys = [meta.oldPdfKey, meta.oldCsvKey, meta.oldXlsxKey].filter(Boolean);
  const superseded = state.steps.upload.value.csvSuperseded;
  if (superseded) keys.push(superseded);
  if (!keys.length) return { deleted: 0 };
  await callScript("delete_old_assets.mjs", keys);
  return { deleted: keys.length, keys };
});

await rm(work, { recursive: true, force: true });
log("done");
