// Runs the sampled page images through Google Document AI (Enterprise Document
// OCR) and records the text it returns. Results are checkpointed per page so an
// interrupted run resumes without paying to re-read pages.
import { readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(execFile);
const arg = (n, d) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1] ?? d;

const benchDir = arg("bench", "/tmp/bench");
const concurrency = Number(arg("concurrency", 4));
const processorName = (await readFile("/tmp/docai_processor.txt", "utf8")).trim();
const outPath = path.join(benchDir, "google.json");

async function token() {
  const { stdout } = await run("/home/dell/google-cloud-sdk/bin/gcloud",
    ["auth", "application-default", "print-access-token"], { maxBuffer: 1 << 20 });
  return stdout.trim();
}

const manifest = JSON.parse(await readFile(path.join(benchDir, "manifest.json"), "utf8"));
const results = existsSync(outPath) ? JSON.parse(await readFile(outPath, "utf8")) : {};

let accessToken = await token();
let mintedAt = Date.now();
let done = 0, failed = 0;

async function ocrOne(item) {
  const id = `${item.granthKey}_p${item.page}`;
  if (results[id]?.text != null) return;

  // tokens last an hour; refresh well inside that
  if (Date.now() - mintedAt > 40 * 60_000) { accessToken = await token(); mintedAt = Date.now(); }

  const bytes = await readFile(item.image);
  const body = JSON.stringify({
    rawDocument: { content: bytes.toString("base64"), mimeType: "image/png" },
  });

  for (let attempt = 1; attempt <= 4; attempt += 1) {
    const res = await fetch(`https://us-documentai.googleapis.com/v1/${processorName}:process`, {
      method: "POST",
      headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body,
    });
    const text = await res.text();
    if (res.ok) {
      let payload;
      try { payload = JSON.parse(text); } catch { payload = null; }
      results[id] = { text: payload?.document?.text ?? "", pages: payload?.document?.pages?.length ?? 0 };
      return;
    }
    if (res.status === 401) { accessToken = await token(); mintedAt = Date.now(); continue; }
    if (res.status === 429 || res.status >= 500) { await new Promise((r) => setTimeout(r, 3000 * attempt)); continue; }
    results[id] = { text: null, error: `${res.status} ${text.slice(0, 200)}` };
    return;
  }
  results[id] = { text: null, error: "exhausted retries" };
}

const queue = [...manifest.items];
async function worker() {
  for (;;) {
    const item = queue.shift();
    if (!item) return;
    try { await ocrOne(item); } catch (e) { results[`${item.granthKey}_p${item.page}`] = { text: null, error: e.message }; }
    const id = `${item.granthKey}_p${item.page}`;
    if (results[id]?.text != null) done += 1; else failed += 1;
    if ((done + failed) % 20 === 0) {
      await writeFile(outPath, JSON.stringify(results, null, 2));
      process.stdout.write(`  ${done + failed}/${manifest.items.length}  ok=${done} failed=${failed}\r`);
    }
  }
}

await Promise.all(Array.from({ length: concurrency }, worker));
await writeFile(outPath, JSON.stringify(results, null, 2));
console.log(`\nGoogle Document AI: ${done} pages read, ${failed} failed`);
console.log(`  approx cost: $${(done * 1.5 / 1000).toFixed(2)}`);
console.log(`  -> ${outPath}`);
