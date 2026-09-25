// Bulk OCR of whole granth PDFs through Google Document AI batch processing.
//
// Uploads each PDF (split to <=500 pages, the batch limit) to a private GCS
// bucket, keeps up to MAX_OPS batch operations running (the per-region quota
// is 5), and as each finishes downloads the sharded output and writes one
// JSON file per PDF part with per-page text plus line boxes for the text layer.
//
// Everything is checkpointed in <work>/state.json, so a rerun resumes: parts
// already uploaded are not uploaded again and parts already OCRed are never
// sent (or paid for) twice.
//
//   node scripts/google/gocr_run.mjs <books.json> <workDir> [--limit=N] [--dry-run]
import { readFile, writeFile, mkdir, stat, rename } from "node:fs/promises";
import { existsSync, createReadStream } from "node:fs";
import { Readable } from "node:stream";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { PDFDocument } from "pdf-lib";

const PROJECT = "461791694388";
const BUCKET = "granth-ocr-461791694388";
const PROCESSOR = `projects/${PROJECT}/locations/us/processors/d3b66bb08eeb47f5/processorVersions/pretrained-ocr-v2.1-2024-08-07`;
const API = "https://us-documentai.googleapis.com/v1";
const MAX_OPS = 5;
const FILES_PER_OP = 8;
const PART_PAGES = 500;
const UPLOAD_CONCURRENCY = 3;
// No field mask: batch masks accept top-level fields only, and "pages" alone
// drops the per-line layout the text layer needs, so take the full document.

const [booksPath, workDir] = process.argv.slice(2);
const limit = Number(process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] ?? 0);
const dryRun = process.argv.includes("--dry-run");
if (!booksPath || !workDir) {
  console.error("usage: gocr_run.mjs <books.json> <workDir> [--limit=N] [--dry-run]");
  process.exit(1);
}

const statePath = path.join(workDir, "state.json");
await mkdir(path.join(workDir, "split"), { recursive: true });
await mkdir(path.join(workDir, "out"), { recursive: true });
const state = existsSync(statePath)
  ? JSON.parse(await readFile(statePath, "utf8"))
  : { parts: {}, ops: {} };

let saving = Promise.resolve();
function save() {
  // Serialise writes and replace atomically so a crash never leaves half a file.
  saving = saving.then(async () => {
    await writeFile(statePath + ".tmp", JSON.stringify(state, null, 1));
    await rename(statePath + ".tmp", statePath);
  });
  return saving;
}
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

let token = null;
let mintedAt = 0;
function auth() {
  if (!token || Date.now() - mintedAt > 30 * 60_000) {
    token = execFileSync("/home/dell/google-cloud-sdk/bin/gcloud", ["auth", "application-default", "print-access-token"]).toString().trim();
    mintedAt = Date.now();
  }
  return { Authorization: `Bearer ${token}`, "x-goog-user-project": PROJECT };
}

async function api(url, init = {}, attempts = 6) {
  for (let a = 1; ; a += 1) {
    const res = await fetch(url, { ...init, headers: { ...auth(), ...(init.headers ?? {}) } });
    if (res.ok) return res;
    const body = await res.text();
    if (res.status === 401) { token = null; }
    if (a >= attempts || !(res.status === 401 || res.status === 429 || res.status >= 500)) {
      throw new Error(`${init.method ?? "GET"} ${url.slice(0, 120)} -> ${res.status} ${body.slice(0, 300)}`);
    }
    await new Promise((r) => setTimeout(r, 3000 * a));
  }
}

// ------------------------------------------------------------------ parts
const books = JSON.parse(await readFile(booksPath, "utf8")).slice(0, limit || undefined);

/** One entry per uploadable file: whole books, or 500-page pieces of long ones. */
async function planParts(book) {
  const partsOut = [];
  if (book.pages <= PART_PAGES) {
    partsOut.push({ id: `${book.id}__p1`, book: book.id, local: book.local, firstPage: 1, pages: book.pages });
    return partsOut;
  }
  const dir = path.join(workDir, "split", book.id);
  await mkdir(dir, { recursive: true });
  let src = null;
  for (let start = 1; start <= book.pages; start += PART_PAGES) {
    const n = Math.min(PART_PAGES, book.pages - start + 1);
    const local = path.join(dir, `p${start}.pdf`);
    if (!existsSync(local)) {
      src ??= await PDFDocument.load(await readFile(book.local), { ignoreEncryption: true });
      const out = await PDFDocument.create();
      const copied = await out.copyPages(src, Array.from({ length: n }, (_, i) => start - 1 + i));
      copied.forEach((p) => out.addPage(p));
      await writeFile(local + ".tmp", await out.save());
      await rename(local + ".tmp", local);
    }
    partsOut.push({ id: `${book.id}__p${start}`, book: book.id, local, firstPage: start, pages: n });
  }
  return partsOut;
}

for (const book of books) {
  const already = Object.values(state.parts).some((p) => p.book === book.id);
  if (already) continue;
  for (const p of await planParts(book)) {
    state.parts[p.id] = { ...p, gcs: `gs://${BUCKET}/in/${p.id}.pdf`, uploaded: false, op: null, done: false, tries: 0 };
  }
  await save();
}
const allParts = Object.values(state.parts).filter((p) => books.some((b) => b.id === p.book));
log(`books ${books.length}, parts ${allParts.length}, pages ${allParts.reduce((n, p) => n + p.pages, 0)}`);
if (dryRun) process.exit(0);

// ------------------------------------------------------------------ upload
async function upload(part) {
  const size = (await stat(part.local)).size;
  const name = part.gcs.replace(`gs://${BUCKET}/`, "");
  const init = await api(
    `https://storage.googleapis.com/upload/storage/v1/b/${BUCKET}/o?uploadType=resumable&name=${encodeURIComponent(name)}`,
    { method: "POST", headers: { "Content-Type": "application/json", "X-Upload-Content-Type": "application/pdf", "X-Upload-Content-Length": String(size) }, body: "{}" }
  );
  const session = init.headers.get("location");
  for (let a = 1; a <= 4; a += 1) {
    try {
      const res = await fetch(session, {
        method: "PUT",
        headers: { "Content-Length": String(size), "Content-Type": "application/pdf" },
        body: Readable.toWeb(createReadStream(part.local)),
        duplex: "half",
      });
      if (res.ok) return size;
      throw new Error(`upload ${res.status} ${(await res.text()).slice(0, 200)}`);
    } catch (e) {
      if (a === 4) throw e;
      await new Promise((r) => setTimeout(r, 5000 * a));
    }
  }
}

let uploadedBytes = 0;
const uploadQueue = allParts.filter((p) => !p.uploaded);
async function uploadWorker() {
  for (;;) {
    const part = uploadQueue.shift();
    if (!part) return;
    try {
      uploadedBytes += await upload(part);
      state.parts[part.id].uploaded = true;
      await save();
    } catch (e) {
      log(`UPLOAD FAILED ${part.id}: ${e.message}`);
      state.parts[part.id].uploadError = e.message;
      await save();
    }
  }
}
const uploading = Promise.all(Array.from({ length: UPLOAD_CONCURRENCY }, uploadWorker));
let uploadsFinished = false;
uploading.then(() => { uploadsFinished = true; log(`uploads finished (${(uploadedBytes / 1e9).toFixed(2)} GB this run)`); });

// ------------------------------------------------------------------ batches
// Page-level fields only (deeper paths are rejected). This keeps text, size and
// line boxes and drops the per-page image and token/symbol detail, which made
// each page's output ~1.4 MB instead of ~40 KB.
const FIELD_MASK = "text,pages.pageNumber,pages.dimension,pages.layout,pages.lines";

async function submit(parts) {
  const body = {
    inputDocuments: { gcsDocuments: { documents: parts.map((p) => ({ gcsUri: p.gcs, mimeType: "application/pdf" })) } },
    documentOutputConfig: { gcsOutputConfig: { gcsUri: `gs://${BUCKET}/out/`, fieldMask: FIELD_MASK } },
    processOptions: { ocrConfig: { hints: { languageHints: ["gu", "sa", "en"] } } },
  };
  const res = await api(`${API}/${PROCESSOR}:batchProcess`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const op = (await res.json()).name;
  state.ops[op] = { parts: parts.map((p) => p.id), submittedAt: new Date().toISOString(), done: false };
  for (const p of parts) { state.parts[p.id].op = op; state.parts[p.id].tries += 1; state.parts[p.id].ocrOk = false; }
  await save();
  log(`submitted ${op.split("/").pop()} with ${parts.length} parts / ${parts.reduce((n, p) => n + p.pages, 0)} pages`);
}

async function listObjects(prefix) {
  const names = [];
  let pageToken = "";
  do {
    const res = await api(`https://storage.googleapis.com/storage/v1/b/${BUCKET}/o?prefix=${encodeURIComponent(prefix)}${pageToken ? `&pageToken=${pageToken}` : ""}`);
    const j = await res.json();
    names.push(...(j.items ?? []).map((i) => i.name));
    pageToken = j.nextPageToken ?? "";
  } while (pageToken);
  return names;
}

async function pool(items, n, fn) {
  const q = [...items];
  await Promise.all(Array.from({ length: Math.min(n, q.length) }, async () => { for (;;) { const it = q.shift(); if (it === undefined) return; await fn(it); } }));
}

/** Turns one part's sharded Document AI output into per-page text + line boxes. */
async function collect(part, outPrefix) {
  const shards = (await listObjects(outPrefix.replace(`gs://${BUCKET}/`, ""))).filter((n) => n.endsWith(".json"));
  const pages = new Map();
  await pool(shards, 6, async (name) => {
    const res = await api(`https://storage.googleapis.com/storage/v1/b/${BUCKET}/o/${encodeURIComponent(name)}?alt=media`);
    const doc = await res.json();
    const text = doc.text ?? "";
    const slice = (anchor) =>
      (anchor?.textSegments ?? []).map((s) => text.slice(Number(s.startIndex ?? 0), Number(s.endIndex ?? 0))).join("");
    for (const pg of doc.pages ?? []) {
      const lines = (pg.lines ?? []).map((l) => {
        const v = l.layout?.boundingPoly?.normalizedVertices ?? [];
        const xs = v.map((p) => p.x ?? 0);
        const ys = v.map((p) => p.y ?? 0);
        return {
          text: slice(l.layout?.textAnchor).replace(/\n$/, ""),
          box: xs.length ? [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] : null,
        };
      });
      const n = part.firstPage - 1 + Number(pg.pageNumber);
      pages.set(n, { page: n, text: slice(pg.layout?.textAnchor), width: pg.dimension?.width ?? null, height: pg.dimension?.height ?? null, lines });
    }
  });
  const sorted = [...pages.values()].sort((a, b) => a.page - b.page);
  if (sorted.length !== part.pages) throw new Error(`${part.id}: expected ${part.pages} pages, got ${sorted.length}`);
  const file = path.join(workDir, "out", `${part.id}.json`);
  await writeFile(file + ".tmp", JSON.stringify({ part: part.id, book: part.book, firstPage: part.firstPage, pages: sorted }));
  await rename(file + ".tmp", file);
  return sorted.length;
}

/** Marks an op's parts as OCR-complete (or failed). Collection happens separately. */
async function poll(opName) {
  const res = await api(`${API}/${opName}`);
  const op = await res.json();
  if (!op.done) return false;
  const statuses = op.metadata?.individualProcessStatuses ?? [];
  for (const partId of state.ops[opName].parts) {
    const part = state.parts[partId];
    if (part.done) continue;
    const st = statuses.find((s) => s.inputGcsSource === part.gcs);
    if (st && (!st.status || !st.status.code)) {
      part.ocrOk = true;
      part.outDest = st.outputGcsDestination;
    } else {
      part.op = null;
      part.ocrOk = false;
      part.error = JSON.stringify(st?.status ?? op.error ?? "no status");
      log(`  ${partId}: FAILED ${part.error}`);
    }
  }
  state.ops[opName].done = true;
  state.ops[opName].finishedAt = new Date().toISOString();
  state.ops[opName].error = op.error ?? null;
  await save();
  log(`op ${opName.split("/").pop()} finished`);
  return true;
}

// Background collector: never blocks submitting new OCR work.
const collecting = new Set();
function startCollectors() {
  // Small (field-masked) results first: they download in seconds, whereas the
  // early unmasked ones are ~70x larger and would hold the queue for minutes.
  const isSmall = (s) => state.ops[s.op]?.submittedAt >= "2026-09-24T16:15";
  const pending = allParts.filter((p) => { const s = state.parts[p.id]; return s.ocrOk && !s.done && !collecting.has(p.id); })
    .sort((a, b) => Number(isSmall(state.parts[b.id])) - Number(isSmall(state.parts[a.id])));
  for (const p of pending) {
    if (collecting.size >= 4) break;
    collecting.add(p.id);
    const part = state.parts[p.id];
    collect(part, part.outDest)
      .then(async (n) => { part.done = true; part.pagesOut = n; await save(); log(`  collected ${p.id}: ${n} pages`); })
      .catch(async (e) => {
        part.collectTries = (part.collectTries ?? 0) + 1;
        part.error = e.message;
        // Repeated download/parse failure: send the part through OCR again.
        if (part.collectTries >= 5) { part.ocrOk = false; part.op = null; }
        await save();
        log(`  collect ${p.id} failed (${part.collectTries}): ${e.message}`);
      })
      .finally(() => collecting.delete(p.id));
  }
}

const started = Date.now();
for (;;) {
  const running = Object.entries(state.ops).filter(([, o]) => !o.done).map(([n]) => n);
  for (const name of running) {
    try { await poll(name); } catch (e) { log(`poll ${name.split("/").pop()}: ${e.message}`); }
  }
  startCollectors();
  const stillRunning = Object.values(state.ops).filter((o) => !o.done).length;
  const ready = allParts.filter((p) => { const s = state.parts[p.id]; return s.uploaded && !s.done && !s.op && !s.ocrOk && s.tries < 3; });
  // Wait for a full op's worth of files unless uploads are over.
  let slots = MAX_OPS - stillRunning;
  while (slots > 0 && (ready.length >= FILES_PER_OP || (uploadsFinished && ready.length > 0))) {
    const batch = ready.splice(0, FILES_PER_OP).map((p) => state.parts[p.id]);
    try { await submit(batch); } catch (e) { log(`submit failed: ${e.message}`); break; }
    slots -= 1;
  }
  const done = allParts.filter((p) => state.parts[p.id].done);
  const donePages = done.reduce((n, p) => n + p.pages, 0);
  const awaiting = allParts.filter((p) => state.parts[p.id].ocrOk && !state.parts[p.id].done).length;
  const mins = (Date.now() - started) / 60000;
  log(`progress: ${done.length}/${allParts.length} parts, ${donePages} pages, ${Object.values(state.ops).filter((o) => !o.done).length} ops running, ${awaiting} awaiting download, uploaded ${allParts.filter((p) => state.parts[p.id].uploaded).length}, ${(donePages / Math.max(mins, 0.1)).toFixed(0)} pages/min`);
  const exhausted = allParts.filter((p) => { const s = state.parts[p.id]; return !s.done && !s.op && !s.ocrOk && (s.tries >= 3 || s.uploadError); });
  const runningNow = Object.values(state.ops).filter((o) => !o.done).length;
  if (uploadsFinished && done.length + exhausted.length === allParts.length && runningNow === 0 && collecting.size === 0) {
    log(`finished: ${done.length} parts done, ${exhausted.length} gave up`);
    for (const p of exhausted) log(`  GAVE UP ${p.id}: ${state.parts[p.id].error ?? state.parts[p.id].uploadError}`);
    break;
  }
  await new Promise((r) => setTimeout(r, 20000));
}
