// Works through the quality report worst-first, re-OCRing granths until the
// page budget or the time budget runs out. One granth failing never stops the
// batch; it is recorded and the run moves on.
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { createClient } from "@libsql/client";
import { scoreGranth, THRESHOLDS } from "./assess_quality.mjs";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, "../..");

const arg = (name, fallback) => {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.split("=")[1] : fallback;
};

const reportPath = arg("report", path.join(ROOT, ".sarvam/quality_report.json"));
const maxPages = Number(arg("max-pages", 40000));       // ₹0.50/page
const maxGranths = Number(arg("max-granths", 1000));
const minutes = Number(arg("minutes", 0));
const verdicts = arg("verdicts", "REDO").split(",");
const parallel = Math.max(1, Number(arg("parallel", 1)));
const keepOld = process.argv.includes("--keep-old");
const dryRun = process.argv.includes("--dry-run");

const logDir = arg("log-dir", path.join(ROOT, ".sarvam/logs"));
await mkdir(logDir, { recursive: true });
const resultsPath = path.join(logDir, "batch_results.json");
const results = existsSync(resultsPath) ? JSON.parse(await readFile(resultsPath, "utf8")) : {};

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });

/**
 * The report is a snapshot. A granth may already have been redone since - by an
 * earlier run, or by hand - so re-score the text that is actually stored before
 * paying to OCR it again.
 */
async function currentScore(key) {
  const pages = await turso.execute({
    sql: "SELECT content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
    args: [key],
  });
  if (!pages.rows.length) return null;
  return scoreGranth(pages.rows.map((r) => ({ content: r.content }))).score;
}

const report = JSON.parse(await readFile(reportPath, "utf8"));
const queue = report.report
  .filter((r) => verdicts.includes(r.verdict))
  .filter((r) => results[r.granth_key]?.status !== "ok")
  .sort((a, b) => a.score - b.score);

console.log(`queue: ${queue.length} granths (verdicts: ${verdicts.join(",")})`);
console.log(`budget: ${maxPages.toLocaleString()} pages  |  ${maxGranths} granths` + (minutes ? `  |  ${minutes} min` : ""));

const deadline = minutes ? Date.now() + minutes * 60_000 : Infinity;
let pagesSpent = 0;
let done = 0;

function runOne(key) {
  return new Promise((resolve) => {
    const args = [`--env-file=${path.join(ROOT, ".env")}`, path.join(HERE, "reocr_granth.mjs"), key];
    if (keepOld) args.push("--keep-old");
    const child = spawn(process.execPath, args, { cwd: ROOT, env: process.env });
    let out = "";
    child.stdout.on("data", (d) => { out += d; process.stdout.write(d); });
    child.stderr.on("data", (d) => { out += d; });
    child.on("close", (code) => resolve({ code, out }));
  });
}

let cursor = 0;
let stopped = null;

async function worker(slot) {
  for (;;) {
    if (stopped) return;
    if (done >= maxGranths) { stopped = "granth limit"; return; }
    if (pagesSpent >= maxPages) { stopped = "page budget"; return; }
    if (Date.now() > deadline) { stopped = "time budget"; return; }

    const entry = queue[cursor];
    cursor += 1;
    if (!entry) return;

    const pages = entry.metrics?.pages || 0;
    if (pagesSpent + pages > maxPages) {
      console.log(`skip ${entry.granth_key} (${pages} pages would exceed budget)`);
      continue;
    }
    const current = await currentScore(entry.granth_key);
    if (current != null && current >= THRESHOLDS.review) {
      console.log(`skip ${entry.granth_key}: already scores ${current}`);
      results[entry.granth_key] = { status: "ok", score_before: entry.score, score_now: current, pages, skipped: true, finishedAt: new Date().toISOString() };
      await writeFile(resultsPath, JSON.stringify(results, null, 2));
      continue;
    }

    pagesSpent += pages; // reserve up front so parallel workers cannot overspend

    console.log(`\n=== [w${slot}] ${entry.granth_key}  score ${entry.score}  ${pages} pages  (${entry.reasons.slice(0,2).join("; ")}) ===`);
    if (dryRun) { done += 1; continue; }

    const started = Date.now();
    const { code, out } = await runOne(entry.granth_key);
    const secs = Math.round((Date.now() - started) / 1000);

    results[entry.granth_key] = {
      status: code === 0 ? "ok" : "failed",
      score_before: entry.score,
      pages,
      seconds: secs,
      finishedAt: new Date().toISOString(),
      ...(code === 0 ? {} : { tail: out.trim().split("\n").slice(-6).join("\n") }),
    };
    await writeFile(resultsPath, JSON.stringify(results, null, 2));

    if (code === 0) { done += 1; console.log(`--- ${entry.granth_key} ok in ${secs}s (${pagesSpent}/${maxPages} pages reserved)`); }
    else { pagesSpent -= pages; console.log(`--- ${entry.granth_key} FAILED after ${secs}s`); }
  }
}

await Promise.all(Array.from({ length: parallel }, (_, i) => worker(i + 1)));
if (stopped) console.log(`\nstopping: ${stopped} reached`);

const ok = Object.values(results).filter((r) => r.status === "ok").length;
const failed = Object.values(results).filter((r) => r.status === "failed").length;
console.log(`\nbatch finished: ${ok} ok, ${failed} failed, ${pagesSpent.toLocaleString()} pages this run (~₹${(pagesSpent * 0.5).toLocaleString()})`);
console.log(`results -> ${resultsPath}`);
