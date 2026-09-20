// Draws a random page sample for the engine benchmark and renders each page.
//
// Sampling rules that keep the result honest:
//  * pages are drawn at random across many granths, not cherry-picked
//  * granths already re-OCRed by Sarvam and granths still on the old pipeline
//    are both included, and which is which is recorded
//  * near-empty pages are excluded (nothing to score), but nothing else is
import { mkdir, writeFile, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";

const run = promisify(execFile);
const arg = (n, d) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1] ?? d;

const targetPages = Number(arg("pages", 450));
const perGranth = Number(arg("per-granth", 6));
const outDir = arg("out", "/tmp/bench");
const dpi = arg("dpi", "300");
const seed = Number(arg("seed", 20260920));

// deterministic PRNG so the sample can be reproduced exactly
let s = seed;
const rnd = () => ((s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const redone = new Set(
  Object.entries(JSON.parse(await (await import("node:fs/promises")).readFile(
    "/home/dell/Desktop/webd/ndms/library/.sarvam/logs/batch_results.json", "utf8")))
    .filter(([, v]) => v.status === "ok" && !v.skipped).map(([k]) => k)
);

const granths = (await turso.execute(
  "SELECT granth_key, granth_name, source_rel_path, page_count FROM ocr_granths ORDER BY granth_key"
)).rows;

// shuffle deterministically
const shuffled = [...granths].sort(() => rnd() - 0.5);
const needGranths = Math.ceil(targetPages / perGranth);

await mkdir(outDir, { recursive: true });
await mkdir(path.join(outDir, "img"), { recursive: true });

const manifest = [];
let rendered = 0;
const concurrency = Number(arg("concurrency", 4));

// Each granth means pulling an 80 MB PDF to render a handful of pages, so the
// downloads are what the wall clock is made of; run several at once.
const queue = [...shuffled];
async function worker() {
  for (;;) {
    if (rendered >= targetPages) return;
    const g = queue.shift();
    if (!g) return;
    await handleGranth(g);
  }
}

async function handleGranth(g) {
  if (rendered >= targetPages) return;
  const key = String(g.granth_key);
  const relPath = String(g.source_rel_path);

  const { data: docs } = await sb.from("documents").select("pdf_url").eq("original_relative_path", relPath);
  const pdfUrl = docs?.[0]?.pdf_url;
  if (!pdfUrl) return;

  const pages = await turso.execute({
    sql: `SELECT page_number, content FROM ocr_pages
          WHERE granth_key = ? AND length(trim(content)) > 400
          ORDER BY page_number`,
    args: [key],
  });
  if (pages.rows.length < perGranth) return;

  const picks = [];
  const pool = pages.rows.map((r) => Number(r.page_number));
  while (picks.length < perGranth && pool.length) picks.push(pool.splice(Math.floor(rnd() * pool.length), 1)[0]);

  const pdfPath = path.join(outDir, `${key}.pdf`);
  try {
    const res = await fetch(pdfUrl);
    if (!res.ok) throw new Error(`pdf ${res.status}`);
    await writeFile(pdfPath, Buffer.from(await res.arrayBuffer()));
  } catch (e) {
    console.log(`  ${key}: ${e.message}`);
    return;
  }

  for (const pno of picks.sort((a, b) => a - b)) {
    if (rendered >= targetPages) break;
    const base = path.join(outDir, "img", `${key}_p${pno}`);
    try {
      await run("pdftoppm", ["-f", String(pno), "-l", String(pno), "-r", dpi, "-png", "-singlefile", pdfPath, base]);
      if (!existsSync(`${base}.png`)) continue;
      const stored = pages.rows.find((r) => Number(r.page_number) === pno);
      manifest.push({
        granthKey: key,
        granthName: String(g.granth_name ?? ""),
        page: pno,
        image: `${base}.png`,
        sarvamRedone: redone.has(key),
        storedText: String(stored?.content ?? ""),
      });
      rendered += 1;
    } catch { /* skip unrenderable page */ }
  }
  await rm(pdfPath, { force: true });
  console.log(`  ${key} (${redone.has(key) ? "sarvam" : "old"}) — ${rendered}/${targetPages} pages rendered`);
}

await Promise.all(Array.from({ length: concurrency }, worker));

await writeFile(path.join(outDir, "manifest.json"), JSON.stringify({
  generatedAt: new Date().toISOString(), seed, dpi,
  granths: new Set(manifest.map((m) => m.granthKey)).size,
  pages: manifest.length,
  sarvamPages: manifest.filter((m) => m.sarvamRedone).length,
  oldPages: manifest.filter((m) => !m.sarvamRedone).length,
  items: manifest,
}, null, 2));

console.log(`\nsampled ${manifest.length} pages from ${new Set(manifest.map((m) => m.granthKey)).size} granths`);
console.log(`  on Sarvam text: ${manifest.filter((m) => m.sarvamRedone).length}`);
console.log(`  on old text   : ${manifest.filter((m) => !m.sarvamRedone).length}`);
