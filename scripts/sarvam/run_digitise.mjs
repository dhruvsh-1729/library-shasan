// Digitises every chunk produced by split_pdf.mjs and extracts the HTML.
// State is checkpointed per chunk so an interrupted run resumes instead of
// re-spending credits on pages that already came back.
import { mkdir, readFile, writeFile, readdir, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { submitDigitise, waitForJob, getDownloadUrl, SarvamError } from "./sarvam_client.mjs";

const run = promisify(execFile);

const chunksDir = process.argv[2];
const outDir = process.argv[3];
const language = process.env.SARVAM_LANGUAGE || undefined;
const concurrency = Number(process.env.SARVAM_CONCURRENCY || 3);

if (!chunksDir || !outDir) {
  console.error("usage: run_digitise.mjs <chunksDir> <outDir>");
  process.exit(1);
}

const manifest = JSON.parse(await readFile(path.join(chunksDir, "chunks.json"), "utf8"));
await mkdir(path.join(outDir, "zips"), { recursive: true });
await mkdir(path.join(outDir, "html"), { recursive: true });
const statePath = path.join(outDir, "state.json");
const state = existsSync(statePath) ? JSON.parse(await readFile(statePath, "utf8")) : { chunks: {} };

async function saveState() {
  await writeFile(statePath, JSON.stringify(state, null, 2));
}

/** Retries on 429/5xx with backoff; Sarvam rate-limits per minute. */
async function withRetry(label, fn, attempts = 5) {
  let delay = 8000;
  for (let i = 1; i <= attempts; i += 1) {
    try {
      return await fn();
    } catch (e) {
      const retryable = e instanceof SarvamError && (e.status === 429 || e.status >= 500);
      if (!retryable || i === attempts) throw e;
      console.log(`  ${label}: ${e.status}, retry ${i}/${attempts - 1} in ${delay / 1000}s`);
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 60000);
    }
  }
}

async function processChunk(chunk) {
  const prior = state.chunks[chunk.name];
  const htmlPath = path.join(outDir, "html", `${chunk.name}.html`);
  if (prior?.done && existsSync(htmlPath)) {
    console.log(`= ${chunk.name} already done`);
    return;
  }

  const pdfPath = path.join(chunksDir, chunk.name);
  const job = await withRetry(chunk.name, () =>
    submitDigitise(pdfPath, { language, outputFormat: "html" })
  );

  const { state: jobState, status } = await waitForJob(job.job_id, { intervalMs: 5000 });
  if (jobState !== "completed" && jobState !== "partially_completed") {
    state.chunks[chunk.name] = { done: false, jobId: job.job_id, jobState, status };
    await saveState();
    console.log(`! ${chunk.name} ${jobState}`);
    return;
  }

  const dl = await withRetry(chunk.name, () => getDownloadUrl(job.job_id));
  const buf = Buffer.from(await (await fetch(dl.url)).arrayBuffer());
  const zipPath = path.join(outDir, "zips", `${chunk.name}.zip`);
  await writeFile(zipPath, buf);

  const workDir = path.join(outDir, "zips", chunk.name);
  await mkdir(workDir, { recursive: true });
  await run("unzip", ["-o", "-q", zipPath, "-d", workDir]);

  // Locate the single primary HTML file inside the extracted tree.
  const found = [];
  async function walk(dir) {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      const p = path.join(dir, entry.name);
      if (entry.isDirectory()) await walk(p);
      else if (entry.name.endsWith(".html")) found.push(p);
    }
  }
  await walk(workDir);
  if (!found.length) throw new Error(`${chunk.name}: no HTML in output`);

  const html = await readFile(found[0], "utf8");
  await writeFile(htmlPath, html);

  state.chunks[chunk.name] = {
    done: true,
    jobId: job.job_id,
    jobState,
    usage: status.usage ?? null,
    firstPage: chunk.firstPage,
    lastPage: chunk.lastPage,
    htmlBytes: html.length,
  };
  await saveState();
  const u = status.usage ?? {};
  console.log(`+ ${chunk.name} p${chunk.firstPage}-${chunk.lastPage} pages=${u.pages_succeeded}/${u.pages_total} html=${html.length}B`);
}

const queue = [...manifest.chunks];
let failures = 0;
async function worker(id) {
  for (;;) {
    const chunk = queue.shift();
    if (!chunk) return;
    try {
      await processChunk(chunk);
    } catch (e) {
      failures += 1;
      const detail = e instanceof SarvamError ? `${e.status} ${JSON.stringify(e.body).slice(0, 300)}` : e.message;
      console.log(`X ${chunk.name} FAILED: ${detail}`);
      state.chunks[chunk.name] = { done: false, error: detail };
      await saveState();
    }
  }
}

await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
const done = Object.values(state.chunks).filter((c) => c.done).length;
const pages = Object.values(state.chunks).reduce((n, c) => n + (c.usage?.pages_succeeded ?? 0), 0);
console.log(`\ndone ${done}/${manifest.chunks.length} chunks, ${pages} pages digitised, ${failures} failures`);
