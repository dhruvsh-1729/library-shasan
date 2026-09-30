// Rebuilds the invisible text layer of books our Google OCR published, so the
// layer lies over the print (add_text_layer now fits each line to its box),
// and swaps the live PDF for the rebuilt one. Only the PDF changes: page text,
// indexes and line boxes stay as they are.
//
// For each book: the local original is stripped of any text layer and gets
// ours back, from the OCR's line boxes carrying the page text as it is in
// Turso now (so later corrections are kept; pages whose lines no longer match
// the boxes use the OCR's own line text). The result must have the same page
// count and extract as that text; then it is uploaded, the four tables that
// hold the PDF link are repointed, the new file is checked live, and only then
// is the old file deleted. Every step is checkpointed per book.
//
//   node --env-file=.env scripts/google/relayer_pdfs.mjs <workDir> [--only=key] [--shard=k/n] [--no-delete]
import { readFile, writeFile, mkdir, unlink, stat } from "node:fs/promises";
import { existsSync, readdirSync, statSync, readFileSync } from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { createClient as createTurso } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { UTApi } from "uploadthing/server";
import { stripTextLayer } from "../sarvam/strip_text_layer.mjs";
import { addTextLayer } from "../sarvam/add_text_layer.mjs";

const run = promisify(execFile);
const RUNS_ROOT = "/media/dell/KINGSTON/ocr_work";
const RUNS = [["modern_publish", "modern_books.json"], ["ld_publish", "ld_books.json"], ["pilot_publish", "pilot_books.json"]];
const [workDir] = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const only = process.argv.find((a) => a.startsWith("--only="))?.split("=")[1];
const [shardK, shardN] = (process.argv.find((a) => a.startsWith("--shard="))?.split("=")[1] ?? "0/1").split("/").map(Number);
const noDelete = process.argv.includes("--no-delete");
if (!workDir) throw new Error("usage: relayer_pdfs.mjs <workDir> [--only=key] [--shard=k/n] [--no-delete]");

const turso = createTurso({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const ut = new UTApi({ token: process.env.UPLOADTHING_TOKEN });
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const nfc = (s) => String(s ?? "").normalize("NFC");

// The latest verified publish of each book, with its local original.
function publishedBooks() {
  const latest = new Map();
  for (const [dir, booksFile] of RUNS) {
    const root = path.join(RUNS_ROOT, dir);
    if (!existsSync(root)) continue;
    const books = JSON.parse(readFileSync(path.join(RUNS_ROOT, booksFile), "utf8"));
    const byId = new Map(books.map((b) => [String(b.id), b]));
    for (const id of readdirSync(root)) {
      const statePath = path.join(root, id, "state.json");
      const pagesPath = path.join(root, id, "pages.json");
      if (!existsSync(statePath) || !existsSync(pagesPath)) continue;
      const state = JSON.parse(readFileSync(statePath, "utf8"));
      const r = state.steps?.resolve?.value;
      if (!r?.granthKey || !state.steps?.verify?.done || !byId.get(id)?.local) continue;
      const when = statSync(statePath).mtimeMs;
      const prev = latest.get(r.granthKey);
      if (!prev || prev.when < when) latest.set(r.granthKey, { granthKey: r.granthKey, customId: r.customId, local: byId.get(id).local, rel: byId.get(id).rel, pagesPath, when });
    }
  }
  return [...latest.values()].sort((a, b) => a.granthKey.localeCompare(b.granthKey));
}

async function uploadFile(filePath, name) {
  const bytes = await readFile(filePath);
  for (let a = 1; a <= 4; a += 1) {
    const res = await ut.uploadFiles(new File([bytes], name, { type: "application/pdf" }));
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
  let s = 0;
  for (const t of A) if (B.has(t)) s += 1;
  return s / A.size;
}
async function sbUpdate(table, values, match) {
  const { error } = await sb.from(table).update(values).match(match);
  if (error) throw new Error(`${table}: ${error.message}`);
}

async function relayer(book) {
  const dir = path.join(workDir, book.granthKey);
  await mkdir(dir, { recursive: true });
  const statePath = path.join(dir, "state.json");
  const st = existsSync(statePath) ? JSON.parse(await readFile(statePath, "utf8")) : { steps: {} };
  const step = async (name, fn) => {
    if (st.steps[name]?.done) return st.steps[name].value;
    const value = await fn();
    st.steps[name] = { done: true, value, at: new Date().toISOString() };
    await writeFile(statePath, JSON.stringify(st, null, 1));
    return value;
  };
  if (st.steps["delete-old"]?.done || (noDelete && st.steps.verify?.done)) return "done before";

  const live = await step("resolve", async () => {
    const { data, error } = await sb.from("documents").select("pdf_url").eq("custom_id", book.customId).single();
    if (error) throw new Error(`documents: ${error.message}`);
    const { data: f } = await sb.from("granth_ocr_files").select("ut_key").eq("custom_id", book.customId).maybeSingle();
    return { oldPdfUrl: data.pdf_url, oldPdfKey: f?.ut_key ?? String(data.pdf_url).split("/f/")[1] ?? null };
  });

  const outPdf = path.join(dir, path.basename(book.rel));
  const built = await step("build-pdf", async () => {
    const ocrPages = JSON.parse(await readFile(book.pagesPath, "utf8"));
    const rows = (await turso.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ?", args: [book.granthKey] })).rows;
    const stored = new Map(rows.map((r) => [Number(r.page_number), String(r.content ?? "")]));
    let fromStored = 0;
    const meta = new Map();
    for (const p of ocrPages) {
      const lines = p.lines.filter((l) => l.box && String(l.clean ?? "").trim());
      const storedLines = (stored.get(p.page) ?? "").split("\n").filter((l) => l.trim());
      const useStored = storedLines.length === lines.length;
      if (useStored) fromStored += 1;
      meta.set(p.page, {
        image_width: 1,
        image_height: 1,
        blocks: lines.map((l, i) => ({ text: useStored ? storedLines[i] : l.clean, reading_order: i, coordinates: { x1: l.box[0], y1: l.box[1], x2: l.box[2], y2: l.box[3] } })),
      });
    }
    const tmp = `${outPdf}.stripped.pdf`;
    await stripTextLayer(book.local, tmp);
    await addTextLayer({ srcPdf: tmp, metaByPage: meta, outPdf });
    await unlink(tmp);
    const n = await pdfPages(outPdf);
    if (n !== (await pdfPages(book.local))) throw new Error(`page count changed to ${n}`);
    // The layer must still extract as the page text.
    const probes = [0.2, 0.5, 0.8].map((f) => Math.max(1, Math.round(n * f)));
    const overlaps = [];
    for (const k of probes) overlaps.push(tokenOverlap(stored.get(k) ?? "", await pdfPageText(outPdf, k)));
    if (Math.min(...overlaps) < 0.85) throw new Error(`text layer does not extract cleanly (${overlaps.map((x) => x.toFixed(2))})`);
    return { pages: n, fromStored, overlaps, bytes: (await stat(outPdf)).size };
  });

  const up = await step("upload", async () => {
    const u = await uploadFile(outPdf, path.basename(book.rel));
    const h = await remoteHead(u.url);
    if (!h.ok || h.size !== u.size) throw new Error(`uploaded PDF not served correctly (${JSON.stringify(h)})`);
    return u;
  });

  await step("update-db", async () => {
    await sbUpdate("documents", { pdf_url: up.url, updated_at: new Date().toISOString() }, { custom_id: book.customId });
    await sbUpdate("granth_ocr_files", { ufs_url: up.url, ut_url: up.url, ut_key: up.key, file_size: up.size }, { custom_id: book.customId });
    await sbUpdate("granth_library_files", { pdf_url: up.url }, { custom_id: book.customId });
    await sbUpdate("granth_gatha_map", { pdf_url: up.url }, { custom_id: book.customId });
    return { pdf: up.url };
  });

  await step("verify", async () => {
    const { data } = await sb.from("documents").select("pdf_url").eq("custom_id", book.customId).single();
    if (data?.pdf_url !== up.url) throw new Error("documents still points at the old PDF");
    const h = await remoteHead(up.url);
    if (!h.ok) throw new Error("new PDF not served");
    return { ok: true };
  });

  if (!noDelete) {
    await step("delete-old", async () => {
      if (!live.oldPdfKey || live.oldPdfKey === up.key) return { skipped: true };
      const res = await ut.deleteFiles([live.oldPdfKey]);
      return { key: live.oldPdfKey, success: res?.success ?? null };
    });
  }
  if (existsSync(outPdf)) await unlink(outPdf);
  return `relayered (${built.fromStored}/${built.pages} pages from stored text, overlap ${built.overlaps.map((x) => x.toFixed(2))})`;
}

await mkdir(workDir, { recursive: true });
const books = publishedBooks().filter((b, i) => (!only || b.granthKey === only) && i % shardN === shardK);
log(`books ${books.length}`);
for (const book of books) {
  try {
    log(book.granthKey, await relayer(book));
  } catch (error) {
    log(book.granthKey, "FAILED", error instanceof Error ? error.message : String(error));
  }
}
log("finished");
