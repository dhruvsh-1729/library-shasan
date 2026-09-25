// Publishes Google Document AI OCR into the live library, one granth at a time,
// as soon as that granth's OCR is complete. Runs unattended: it polls the OCR
// run's state file, and for each finished granth it
//
//   1. merges and cleans the page text
//   2. gates on quality against the text currently live (never publishes worse)
//   3. builds a searchable PDF (old text layer stripped, Google lines laid in)
//      and checks the new layer actually extracts as the new text
//   4. uploads the PDF + page CSV to UploadThing
//   5. repoints Supabase + Turso (page text, search indexes, printed page
//      numbers, status) via the same update script the Sarvam run used
//   6. verifies the live file and every database reference
//   7. only then deletes the superseded UploadThing files
//
// Every step is checkpointed per granth, so a crash or restart resumes where it
// stopped. A granth that fails any step is quarantined with the reason and the
// worker moves on; nothing is ever half-published.
//
//   node --env-file=.env scripts/google/publish_worker.mjs <books.json> <ocrWorkDir> <publishDir> [--once] [--only=id] [--no-delete]
import { readFile, writeFile, mkdir, rename, stat, unlink, readdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { UTApi } from "uploadthing/server";
import { stripTextLayer } from "../sarvam/strip_text_layer.mjs";
import { addTextLayer } from "../sarvam/add_text_layer.mjs";

const run = promisify(execFile);
const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, "../..");
const [booksPath, ocrDir, pubDir] = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const once = process.argv.includes("--once");
const noDelete = process.argv.includes("--no-delete");
const only = process.argv.find((a) => a.startsWith("--only="))?.split("=")[1];
// --shard=k/n: this worker handles every n-th book starting at k, so several
// workers can run side by side without ever touching the same granth.
const [shardK, shardN] = (process.argv.find((a) => a.startsWith("--shard="))?.split("=")[1] ?? "0/1").split("/").map(Number);
if (!booksPath || !ocrDir || !pubDir) {
  console.error("usage: publish_worker.mjs <books.json> <ocrWorkDir> <publishDir> [--once] [--only=id] [--no-delete]");
  process.exit(1);
}
await mkdir(pubDir, { recursive: true });

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const ut = new UTApi({ token: process.env.UPLOADTHING_TOKEN });
const UFS = "https://pk3cp5aaix.ufs.sh/f/";

const stamp = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const summaryPath = path.join(pubDir, shardN > 1 ? `summary-${shardK}of${shardN}.json` : "summary.json");
const summary = existsSync(summaryPath) ? JSON.parse(await readFile(summaryPath, "utf8")) : {};
async function writeSummary() {
  await writeFile(summaryPath + ".tmp", JSON.stringify(summary, null, 1));
  await rename(summaryPath + ".tmp", summaryPath);
}

// ------------------------------------------------------------ text cleanup
const lex = JSON.parse(await readFile(path.join(path.dirname(ocrDir), "lexicon.json"), "utf8"));
const GU = /[઀-૿]/;
const nfc = (s) => String(s ?? "").normalize("NFC").replace(/[‌‍]/g, "");

/**
 * Only mechanical, provable fixes:
 *  - the Gujarati digit ૫ where the word around it is letters (it is the letter પ)
 *  - a vowel-length / ૫-પ swap that turns a word never seen in 40k clean pages
 *    into one seen at least 20 times
 */
const SWAPS = [["િ", "ી"], ["ુ", "ૂ"], ["ઇ", "ઈ"], ["ઉ", "ઊ"], ["૫", "પ"]];
function variants(w) {
  const pos = [];
  [...w].forEach((ch, i) => { for (const [a, b] of SWAPS) { if (ch === a) pos.push([i, b]); else if (ch === b) pos.push([i, a]); } });
  const out = new Set();
  const n = Math.min(3, pos.length);
  const rec = (start, chosen) => {
    if (chosen.length) { const arr = [...w]; for (const [i, c] of chosen) arr[i] = c; out.add(arr.join("")); }
    if (chosen.length === n) return;
    for (let k = start; k < pos.length; k += 1) rec(k + 1, [...chosen, pos[k]]);
  };
  rec(0, []);
  return out;
}
const fixCache = new Map();
function fixWord(w) {
  if (fixCache.has(w)) return fixCache.get(w);
  let best = w;
  if (!(lex.gu[w] > 0) && w.length > 1) {
    let bestCount = 0;
    for (const v of variants(w)) { const c = lex.gu[v] ?? 0; if (c >= 20 && c > bestCount) { best = v; bestCount = c; } }
  }
  fixCache.set(w, best);
  return best;
}
function cleanText(text) {
  let t = nfc(text);
  // ૫ touching Gujarati letters/signs (not other digits) is the letter પ.
  t = t.replace(/(?<=[ઁ-ૣ])૫|૫(?=[ઁ-ૣ])/g, (m, off, s) => {
    const prev = s[off - 1] ?? "", next = s[off + 1] ?? "";
    return /[૦-૯]/.test(prev) || /[૦-૯]/.test(next) ? m : "પ";
  });
  let fixes = 0;
  t = t.replace(/[઀-૿]+/g, (w) => { const f = fixWord(w); if (f !== w) fixes += 1; return f; });
  return { text: t, fixes };
}

// ------------------------------------------------------------ quality
// Scanning-library watermarks stamped on every page are not book text.
const WATERMARK = /kobatirth|Kailashsagarsuri|Mahavir\s+Jain\s+Aradhana\s+Kendra|For\s+Private\s+(And|&)\s+Personal(\s+Use(\s+Only)?)?|jainelibrary\.org|Jain\s+Education\s+International/i;
const stripWatermark = (t) => String(t ?? "").split("\n").filter((ln) => !WATERMARK.test(ln)).join("\n");
function quality(pages) {
  let gu = 0, guOk = 0, dev = 0, devOk = 0, mixed = 0, empty = 0;
  for (const p of pages) {
    // Measure old and new alike without watermark lines, or a blank page that
    // only carried a watermark counts as text in the old version.
    const t = nfc(stripWatermark(p));
    // A page "has text" only if it holds a few real words: old OCR often left
    // character soup on covers and plates, which is emptier than an empty page.
    const realWords = (t.match(/[ऀ-ॿ઀-૿]{2,}/g) ?? []).filter((w) => lex.gu[w] || lex.dev[w]).length
      + (t.match(/[A-Za-z]{3,}/g) ?? []).length;
    if (t.trim().length < 40 || realWords < 5) empty += 1;
    for (const w of t.match(/[઀-૿]{2,}/g) ?? []) { gu += 1; if (lex.gu[w]) guOk += 1; }
    for (const w of t.match(/[ऀ-ॿ]{2,}/g) ?? []) { dev += 1; if (lex.dev[w]) devOk += 1; }
    // Letters and vowel signs only: the dandas । ॥ sit in the Devanagari block
    // but are ordinary Gujarati punctuation, and digits are shared too.
    for (const w of t.match(/[ऀ-ॣॱ-ॿઁ-ૣ]+/g) ?? []) {
      if (/[ऀ-ॣॱ-ॿ]/.test(w) && /[ઁ-ૣ]/.test(w)) mixed += 1;
    }
  }
  return { pages: pages.length, empty, gu, guValid: gu ? guOk / gu : null, dev, devValid: dev ? devOk / dev : null, mixed };
}
/** New text must not be worse than what is live. */
function gate(oldQ, newQ) {
  const why = [];
  if (newQ.pages !== oldQ.pages && oldQ.pages) why.push(`page count ${newQ.pages} vs live ${oldQ.pages}`);
  if (newQ.empty > oldQ.empty + Math.max(3, 0.02 * newQ.pages)) why.push(`more empty pages (${newQ.empty} vs ${oldQ.empty})`);
  // No slack: where the live text is already good, "about as good" is not a
  // reason to replace it, so any drop in either script keeps the live text.
  const worse = (k, n) => oldQ[n] > 300 && newQ[k] != null && oldQ[k] != null && newQ[k] < oldQ[k];
  if (worse("guValid", "gu")) why.push(`Gujarati valid ${(100 * newQ.guValid).toFixed(1)}% < live ${(100 * oldQ.guValid).toFixed(1)}%`);
  if (worse("devValid", "dev")) why.push(`Devanagari valid ${(100 * newQ.devValid).toFixed(1)}% < live ${(100 * oldQ.devValid).toFixed(1)}%`);
  if (newQ.mixed > oldQ.mixed + 5) why.push(`more mixed-script words (${newQ.mixed} vs ${oldQ.mixed})`);
  if (!oldQ.pages) {
    // No live text to compare against: require the OCR itself to look sane.
    if (newQ.empty > 0.1 * newQ.pages) why.push(`${newQ.empty} of ${newQ.pages} pages empty`);
    if (newQ.gu > 300 && newQ.guValid < 0.85) why.push(`Gujarati valid only ${(100 * newQ.guValid).toFixed(1)}%`);
  }
  return why;
}

// ------------------------------------------------------------ printed pages
const DIGITS = { "૦": 0, "૧": 1, "૨": 2, "૩": 3, "૪": 4, "૫": 5, "૬": 6, "૭": 7, "૮": 8, "૯": 9, "०": 0, "१": 1, "२": 2, "३": 3, "४": 4, "५": 5, "६": 6, "७": 7, "८": 8, "९": 9 };
const toInt = (s) => Number([...s].map((c) => (c in DIGITS ? DIGITS[c] : c)).join(""));
/**
 * The printed page number is a bare number in the running head or foot.
 * Candidates are read per page, then kept only where they agree with a
 * constant offset from the PDF page seen across the book, so a verse number
 * or year in a heading is never mistaken for the page number.
 */
function printedPages(pages) {
  const cands = pages.map((p) => {
    const out = [];
    for (const l of p.lines ?? []) {
      if (!l.box || !(l.box[1] < 0.12 || l.box[3] > 0.9)) continue;
      for (const m of l.text.matchAll(/(?<![\p{L}\p{M}\p{N}])([0-9૦-૯०-९]{1,4})(?![\p{L}\p{M}\p{N}])/gu)) out.push(toInt(m[1]));
    }
    return out;
  });
  const offsets = new Map();
  cands.forEach((cs, i) => cs.forEach((n) => { const o = n - (i + 1); offsets.set(o, (offsets.get(o) ?? 0) + 1); }));
  // Books can restart numbering (front matter vs body), so accept every offset
  // backed by a run of at least 5 pages, and assign pages to the offset they match.
  const good = [...offsets.entries()].filter(([, c]) => c >= 5).map(([o]) => o);
  return cands.map((cs, i) => {
    const hit = cs.find((n) => good.includes(n - (i + 1)));
    return hit != null ? String(hit) : null;
  });
}

// ------------------------------------------------------------ helpers
const COLUMNS = ["granth_key", "book_number", "library_code", "granth_name", "source_rel_path", "pdf_url", "page_number", "content", "method", "status", "quality_score", "chars", "google_reason", "needs_review", "embedded_score", "local_score", "error"];
const csvCell = (v) => { const s = v == null ? "" : String(v); return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };

async function uploadFile(filePath, name, type) {
  const bytes = await readFile(filePath);
  for (let a = 1; a <= 4; a += 1) {
    const res = await ut.uploadFiles(new File([bytes], name, { type }));
    if (!res.error) return { key: res.data.key, url: res.data.ufsUrl ?? res.data.url, size: bytes.length };
    if (a === 4) throw new Error(`upload ${name}: ${JSON.stringify(res.error)}`);
    await new Promise((r) => setTimeout(r, 5000 * a));
  }
}
async function remoteHead(url) {
  const r = await fetch(url, { headers: { Range: "bytes=0-7" } });
  const buf = Buffer.from(await r.arrayBuffer());
  return { ok: buf.toString("latin1").startsWith("%PDF"), size: Number((r.headers.get("content-range") ?? "").split("/")[1] || 0) };
}
async function pdfPages(file) {
  const { stdout } = await run("pdfinfo", [file]);
  return Number(stdout.match(/Pages:\s+(\d+)/)[1]);
}
async function pdfPageText(file, n) {
  const { stdout } = await run("pdftotext", ["-enc", "UTF-8", "-f", String(n), "-l", String(n), file, "-"], { maxBuffer: 1 << 24 });
  return stdout;
}
function tokenOverlap(a, b) {
  const A = new Set(nfc(a).match(/[ऀ-ॿ઀-૿]{2,}/g) ?? []);
  const B = new Set(nfc(b).match(/[ऀ-ॿ઀-૿]{2,}/g) ?? []);
  if (!A.size) return 1;
  let s = 0; for (const t of A) if (B.has(t)) s += 1;
  return s / A.size;
}

async function ensurePrintedColumn() {
  const cols = await turso.execute("PRAGMA table_info(ocr_pages)");
  if (!cols.rows.some((r) => r.name === "printed_page")) {
    await turso.execute("ALTER TABLE ocr_pages ADD COLUMN printed_page TEXT");
    console.log(`${stamp()} added ocr_pages.printed_page`);
  }
}

// ------------------------------------------------------------ one granth
async function publish(book) {
  const dir = path.join(pubDir, book.id);
  await mkdir(dir, { recursive: true });
  const statePath = path.join(dir, "state.json");
  const st = existsSync(statePath) ? JSON.parse(await readFile(statePath, "utf8")) : { steps: {} };
  const save = async () => { await writeFile(statePath + ".tmp", JSON.stringify(st, null, 1)); await rename(statePath + ".tmp", statePath); };
  const log = (m) => console.log(`${stamp()} [${book.id}] ${m}`);
  async function step(name, fn) {
    if (st.steps[name]?.done) return st.steps[name].value;
    const t0 = Date.now();
    const value = await fn();
    st.steps[name] = { done: true, value, secs: Math.round((Date.now() - t0) / 1000) };
    await save();
    log(`+ ${name}`);
    return value;
  }

  // 1. who is this granth in every system
  const meta = await step("resolve", async () => {
    const { data: docs, error } = await sb.from("documents").select("custom_id,pdf_name,pdf_url,csv_url,status").eq("original_relative_path", book.rel);
    if (error) throw new Error(`documents: ${error.message}`);
    if (docs.length !== 1) throw new Error(`expected 1 documents row for ${book.rel}, found ${docs.length}`);
    const { data: files } = await sb.from("granth_ocr_files").select("id,ut_key,ufs_url,file_name").eq("custom_id", docs[0].custom_id);
    if (files?.length !== 1) throw new Error(`expected 1 granth_ocr_files row, found ${files?.length}`);
    const g = await turso.execute({ sql: "SELECT granth_key, book_number, library_code, granth_name, source_rel_path, xlsx_key FROM ocr_granths WHERE source_rel_path = ?", args: [book.rel] });
    const { count: gathaRows } = await sb.from("granth_gatha_map").select("id", { count: "exact", head: true }).eq("custom_id", docs[0].custom_id);
    const tursoRow = g.rows[0] ?? null;
    return {
      customId: docs[0].custom_id,
      pdfName: docs[0].pdf_name,
      oldPdfUrl: docs[0].pdf_url,
      oldCsvKey: docs[0].csv_url ? String(docs[0].csv_url).split("/f/")[1] ?? null : null,
      fileId: files[0].id,
      oldPdfKey: files[0].ut_key,
      granthKey: tursoRow ? String(tursoRow.granth_key) : book.id,
      tursoExists: Boolean(tursoRow),
      bookNumber: tursoRow?.book_number == null ? (book.key || "") : String(tursoRow.book_number),
      libraryCode: tursoRow?.library_code == null ? "" : String(tursoRow.library_code),
      granthName: tursoRow ? String(tursoRow.granth_name ?? "") : path.basename(book.rel, ".pdf"),
      oldXlsxKey: tursoRow?.xlsx_key ? String(tursoRow.xlsx_key) : null,
      gathaRows: gathaRows ?? 0,
    };
  });

  // 2. the OCR, merged across parts, cleaned
  const pagesFile = path.join(dir, "pages.json");
  const built = await step("merge", async () => {
    const ocrState = JSON.parse(await readFile(path.join(ocrDir, "state.json"), "utf8"));
    const parts = Object.values(ocrState.parts).filter((p) => p.book === book.id).sort((a, b) => a.firstPage - b.firstPage);
    if (!parts.length || parts.some((p) => !p.done)) throw new Error("OCR not complete");
    const pages = [];
    for (const p of parts) {
      const j = JSON.parse(await readFile(path.join(ocrDir, "out", `${p.id}.json`), "utf8"));
      pages.push(...j.pages);
    }
    pages.sort((a, b) => a.page - b.page);
    const n = await pdfPages(book.local);
    if (pages.length !== n) throw new Error(`OCR has ${pages.length} pages, PDF has ${n}`);
    pages.forEach((p, i) => { if (p.page !== i + 1) throw new Error(`page gap at ${i + 1}`); });
    // Scanning-library watermarks stamped on every page are not book text; left
    // in, a search for "Jain" or "Mahavir" would match every page of the book.
    let watermarkLines = 0;
    for (const p of pages) {
      const keep = p.lines.filter((l) => !WATERMARK.test(l.text));
      watermarkLines += p.lines.length - keep.length;
      p.lines = keep;
      p.text = p.text.split("\n").filter((ln) => !WATERMARK.test(ln)).join("\n");
    }
    let fixes = 0;
    for (const p of pages) {
      const c = cleanText(p.text); p.clean = c.text; fixes += c.fixes;
      for (const l of p.lines) l.clean = cleanText(l.text).text;
    }
    const printed = printedPages(pages);
    pages.forEach((p, i) => { p.printed = printed[i]; });
    await writeFile(pagesFile, JSON.stringify(pages));
    return { pages: pages.length, fixes, watermarkLines, printedFound: printed.filter(Boolean).length };
  });
  const pages = JSON.parse(await readFile(pagesFile, "utf8"));

  // 3. quality gate against what is live now
  await step("gate", async () => {
    const live = await turso.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number", args: [meta.granthKey] });
    const oldQ = quality(live.rows.map((r) => String(r.content ?? "")));
    const newQ = quality(pages.map((p) => p.clean));
    const why = gate(oldQ, newQ);
    if (why.length) {
      const err = new Error(`quality gate: ${why.join("; ")}`); err.quarantine = true; err.detail = { oldQ, newQ }; throw err;
    }
    return { oldQ, newQ };
  });

  // 4. artefacts
  const outPdf = path.join(dir, path.basename(book.rel));
  const csvName = `${path.basename(book.rel, ".pdf")}_google.csv`;
  const csvPath = path.join(dir, csvName);
  await step("build-pdf", async () => {
    const tmp = outPdf + ".stripped.pdf";
    const s = await stripTextLayer(book.local, tmp);
    const metaByPage = new Map(pages.map((p) => [p.page, {
      image_width: 1, image_height: 1,
      blocks: p.lines.filter((l) => l.box && l.clean.trim()).map((l, i) => ({
        text: l.clean, reading_order: i, coordinates: { x1: l.box[0], y1: l.box[1], x2: l.box[2], y2: l.box[3] },
      })),
    }]));
    const a = await addTextLayer({ srcPdf: tmp, metaByPage, outPdf });
    await unlink(tmp);
    if ((await pdfPages(outPdf)) !== pages.length) throw new Error("rebuilt PDF page count changed");
    // The layer must extract as the new text, or search inside the PDF fails.
    const probes = [0.2, 0.5, 0.8].map((f) => Math.max(1, Math.round(pages.length * f)));
    const overlaps = [];
    for (const n of probes) overlaps.push(tokenOverlap(pages[n - 1].clean, await pdfPageText(outPdf, n)));
    if (Math.min(...overlaps) < 0.85) throw new Error(`text layer does not extract cleanly (overlap ${overlaps.map((x) => x.toFixed(2)).join(",")})`);
    return { stripped: s.removedBlocks, blocks: a.blocksDrawn, bytes: (await stat(outPdf)).size, overlaps };
  });
  await step("build-csv", async () => {
    const lines = [COLUMNS.join(",")];
    for (const p of pages) {
      lines.push([meta.granthKey, meta.bookNumber, meta.libraryCode, meta.granthName, book.rel, "PENDING", p.page, p.clean,
        "google_docai", "accepted", "", p.clean.length, "", "false", "", "", ""].map(csvCell).join(","));
    }
    await writeFile(csvPath, lines.join("\n") + "\n", "utf8");
    return { rows: pages.length };
  });

  // 5. upload (new files first; nothing old is touched yet)
  const up = await step("upload", async () => {
    const pdf = await uploadFile(outPdf, path.basename(book.rel), "application/pdf");
    const body = (await readFile(csvPath, "utf8")).split("PENDING").join(pdf.url);
    await writeFile(csvPath, body, "utf8");
    const csv = await uploadFile(csvPath, csvName, "text/csv");
    const h = await remoteHead(pdf.url);
    if (!h.ok || h.size !== pdf.size) throw new Error(`uploaded PDF not served correctly (${JSON.stringify(h)})`);
    return { pdf, csv };
  });

  // 6. databases
  await step("update-db", async () => {
    if (!meta.tursoExists) {
      const now = stamp();
      await turso.execute({
        // xlsx_filename / xlsx_custom_id are NOT NULL; update_granth_sources
        // rewrites both to the uploaded CSV straight after this insert.
        sql: `INSERT INTO ocr_granths (granth_key, book_number, library_code, granth_name, source_rel_path, xlsx_filename, xlsx_custom_id, page_count, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (granth_key) DO NOTHING`,
        args: [meta.granthKey, meta.bookNumber, meta.libraryCode, meta.granthName, book.rel, csvName, `${meta.granthKey}__google__ocr_csv`, pages.length, now, now],
      });
    }
    const printedPath = path.join(dir, "printed.json");
    await writeFile(printedPath, JSON.stringify(Object.fromEntries(pages.map((p) => [p.page, p.printed]))));
    const cfgPath = path.join(dir, "update.json");
    await writeFile(cfgPath, JSON.stringify({
      granthKey: meta.granthKey, customId: meta.customId, csvPath, expectedPages: pages.length, expectedGathaRows: meta.gathaRows,
      newPdfUrl: up.pdf.url, newPdfKey: up.pdf.key, newCsvUrl: up.csv.url, newCsvKey: up.csv.key,
      newCsvFilename: csvName, newCsvCustomId: `${meta.granthKey}__google__ocr_csv`, printedPath,
    }, null, 1));
    const { stdout } = await run(process.execPath, [`--env-file=${path.join(ROOT, ".env")}`, path.join(ROOT, "scripts/sarvam/update_granth_sources.mjs"), cfgPath], { cwd: ROOT, maxBuffer: 1 << 26 });
    if (/!! expected/.test(stdout)) log(`note: ${stdout.split("\n").filter((l) => l.includes("!!")).join(" | ")}`);
    // Pages beyond the new count would be stale leftovers from an older run.
    await turso.execute({ sql: "DELETE FROM ocr_pages_suffix WHERE granth_key = ? AND page_number > ?", args: [meta.granthKey, pages.length] });
    await turso.execute({ sql: "DELETE FROM ocr_pages WHERE granth_key = ? AND page_number > ?", args: [meta.granthKey, pages.length] });
    // Printed page numbers were written in the same UPDATE as the text.
    await turso.execute({ sql: "UPDATE ocr_granths SET page_count = ? WHERE granth_key = ?", args: [pages.length, meta.granthKey] });
    const { error: e1 } = await sb.from("granth_ocr_files").update({ file_size: up.pdf.size }).eq("id", meta.fileId);
    if (e1) throw new Error(`granth_ocr_files size: ${e1.message}`);
    const { error: e2 } = await sb.from("documents").update({ status: "processed", updated_at: new Date().toISOString() }).eq("custom_id", meta.customId);
    if (e2) throw new Error(`documents status: ${e2.message}`);
    return { ok: true };
  });

  // 7. verify everything points at the new files and holds the new text
  await step("verify", async () => {
    const { data: doc } = await sb.from("documents").select("pdf_url,csv_url,status").eq("custom_id", meta.customId).single();
    const { data: f } = await sb.from("granth_ocr_files").select("ufs_url,ut_key").eq("id", meta.fileId).single();
    const problems = [];
    if (!doc?.pdf_url?.includes(up.pdf.key)) problems.push("documents.pdf_url");
    if (!doc?.csv_url?.includes(up.csv.key)) problems.push("documents.csv_url");
    if (f?.ut_key !== up.pdf.key) problems.push("granth_ocr_files.ut_key");
    const t = await turso.execute({ sql: "SELECT COUNT(*) n FROM ocr_pages WHERE granth_key = ?", args: [meta.granthKey] });
    if (Number(t.rows[0].n) !== pages.length) problems.push(`ocr_pages count ${t.rows[0].n}`);
    const probe = Math.max(1, Math.round(pages.length / 2));
    const s = await turso.execute({ sql: "SELECT content FROM ocr_pages WHERE granth_key = ? AND page_number = ?", args: [meta.granthKey, probe] });
    if (String(s.rows[0]?.content ?? "") !== pages[probe - 1].clean) problems.push(`page ${probe} text not the new text`);
    const h = await remoteHead(up.pdf.url);
    if (!h.ok) problems.push("new PDF not served");
    if (problems.length) throw new Error(`verify failed: ${problems.join(", ")}`);
    return { ok: true };
  });

  // 8. only now remove what was superseded
  if (noDelete) {
    log("delete-old held back (--no-delete); rerun without it to remove the superseded files");
    return { granthKey: meta.granthKey, ...st.steps.gate.value, fixes: built.fixes, printedFound: built.printedFound, newPdf: up.pdf.url, deleteHeld: true };
  }
  await step("delete-old", async () => {
    const keys = [meta.oldPdfKey, meta.oldCsvKey, meta.oldXlsxKey].filter((k) => k && k !== up.pdf.key && k !== up.csv.key);
    if (!keys.length) return { keys };
    const res = await ut.deleteFiles(keys);
    return { keys, success: res?.success ?? null, deletedCount: res?.deletedCount ?? null };
  });

  // Free the large rebuilt PDF; the live copy is in UploadThing now.
  if (existsSync(outPdf)) await unlink(outPdf);
  return { granthKey: meta.granthKey, ...st.steps.gate.value, fixes: built.fixes, printedFound: built.printedFound, newPdf: up.pdf.url };
}

// ------------------------------------------------------------ loop
await ensurePrintedColumn();
const books = JSON.parse(await readFile(booksPath, "utf8")).filter((b, i) => (!only || b.id === only) && i % shardN === shardK);
// Books finished by any earlier worker layout (unsharded, or a different shard
// count) stay finished: read every summary file, not just this worker's own.
for (const f of (await readdir(pubDir)).filter((f) => /^summary.*\.json$/.test(f) && path.join(pubDir, f) !== summaryPath)) {
  const prev = JSON.parse(await readFile(path.join(pubDir, f), "utf8"));
  for (const b of books) if (prev[b.id] && !summary[b.id]) summary[b.id] = prev[b.id];
}
for (;;) {
  const ocrState = existsSync(path.join(ocrDir, "state.json")) ? JSON.parse(await readFile(path.join(ocrDir, "state.json"), "utf8")) : { parts: {} };
  const ready = books.filter((b) => {
    if (["published", "quarantined"].includes(summary[b.id]?.status)) return false;
    if (noDelete && summary[b.id]?.status === "published-old-kept") return false;
    const parts = Object.values(ocrState.parts).filter((p) => p.book === b.id);
    return parts.length && parts.every((p) => p.done);
  });
  for (const book of ready) {
    try {
      const r = await publish(book);
      summary[book.id] = { status: r.deleteHeld ? "published-old-kept" : "published", at: stamp(), ...r };
      console.log(`${stamp()} PUBLISHED ${book.id}  gu ${(100 * (r.oldQ.guValid ?? 0)).toFixed(1)}%->${(100 * (r.newQ.guValid ?? 0)).toFixed(1)}%  dev ${(100 * (r.oldQ.devValid ?? 0)).toFixed(1)}%->${(100 * (r.newQ.devValid ?? 0)).toFixed(1)}%  printed ${r.printedFound}/${r.newQ.pages}`);
    } catch (e) {
      const tries = (summary[book.id]?.tries ?? 0) + 1;
      const quarantine = e.quarantine || tries >= 3;
      summary[book.id] = { status: quarantine ? "quarantined" : "retry", tries, at: stamp(), error: e.message, detail: e.detail ?? null };
      console.log(`${stamp()} ${quarantine ? "QUARANTINED" : "RETRY LATER"} ${book.id}: ${e.message}`);
    }
    await writeSummary();
  }
  const counts = Object.values(summary).reduce((a, s) => ({ ...a, [s.status]: (a[s.status] ?? 0) + 1 }), {});
  const finalStates = noDelete ? ["published", "published-old-kept", "quarantined"] : ["published", "quarantined"];
  const allDone = books.every((b) => finalStates.includes(summary[b.id]?.status));
  console.log(`${stamp()} status: ${JSON.stringify(counts)} of ${books.length}${allDone ? " — ALL DONE" : ""}`);
  if (allDone || once) break;
  await new Promise((r) => setTimeout(r, 60_000));
}
