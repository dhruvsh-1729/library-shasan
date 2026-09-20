// Did Sarvam actually improve the text, or just change it?
//
// The quality score cannot answer that on its own — it measures error
// signatures, and could in principle be biased toward whatever Sarvam
// produces. So this asks a third, independent reader.
//
// For each sampled page: render the image from the original PDF, read it with
// Tesseract, then measure how much of each version that independent reading
// corroborates. The version an outside reader agrees with more is the more
// faithful one. Tesseract is weak, but it is weak in its OWN way — it has no
// reason to agree with Sarvam's mistakes or with the old pipeline's.
import { mkdtemp, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";

const run = promisify(execFile);
const arg = (n, d) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1] ?? d;

const granthKey = arg("granth", "025");
const backupPath = arg("backup");
const pdfPath = arg("pdf");
const sample = Number(arg("pages", 12));
const dpi = arg("dpi", "200");

const TOKEN = /[ऀ-ॿ઀-૿]{2,}/gu;
const toks = (s) => new Set((String(s).normalize("NFC").match(TOKEN) ?? []));

/** Share of the version's tokens that the independent reading also saw. */
function corroboration(version, independent) {
  const a = toks(version);
  const b = toks(independent);
  if (!a.size || !b.size) return null;
  let shared = 0;
  for (const t of a) if (b.has(t)) shared += 1;
  return { precision: shared / a.size, tokens: a.size, shared };
}

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const backup = JSON.parse(await readFile(backupPath, "utf8"));
const oldByPage = new Map(backup.ocr_pages.map((p) => [Number(p.page_number), String(p.content ?? "")]));

const nowRows = await turso.execute({
  sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
  args: [granthKey],
});
const newByPage = new Map(nowRows.rows.map((p) => [Number(p.page_number), String(p.content ?? "")]));

// Sample pages that have substantial text in BOTH versions, spread through the book.
const candidates = [...newByPage.keys()]
  .filter((p) => (oldByPage.get(p) ?? "").trim().length > 300 && (newByPage.get(p) ?? "").trim().length > 300);
const step = Math.max(1, Math.floor(candidates.length / sample));
const picked = candidates.filter((_, i) => i % step === 0).slice(0, sample);

const work = await mkdtemp(path.join(tmpdir(), "cmp-"));
const rows = [];
try {
  console.log(`granth ${granthKey}: comparing ${picked.length} pages against an independent Tesseract reading\n`);
  console.log("  page   OLD agrees   NEW agrees   verdict");
  for (const pno of picked) {
    const base = path.join(work, `p${pno}`);
    await run("pdftoppm", ["-f", String(pno), "-l", String(pno), "-r", dpi, "-png", pdfPath, base]);
    const { stdout: ls } = await run("bash", ["-c", `ls ${base}*.png 2>/dev/null | head -1`]);
    const png = ls.trim();
    if (!png) continue;
    await run("tesseract", [png, base, "-l", "guj+hin", "--psm", "3"], { maxBuffer: 32 * 1024 * 1024 }).catch(() => null);
    let indep = "";
    try { indep = await readFile(`${base}.txt`, "utf8"); } catch { continue; }

    const o = corroboration(oldByPage.get(pno) ?? "", indep);
    const n = corroboration(newByPage.get(pno) ?? "", indep);
    if (!o || !n) continue;
    const delta = n.precision - o.precision;
    const verdict = Math.abs(delta) < 0.02 ? "same" : delta > 0 ? "NEW better" : "old better";
    console.log(`  ${String(pno).padStart(4)}   ${(100*o.precision).toFixed(1).padStart(9)}%   ${(100*n.precision).toFixed(1).padStart(9)}%   ${verdict}`);
    rows.push({ page: pno, old: o.precision, neu: n.precision, delta });
    await rm(`${base}.txt`, { force: true });
    await rm(png, { force: true });
  }
} finally {
  await rm(work, { recursive: true, force: true });
}

if (rows.length) {
  const mean = (f) => rows.reduce((n, r) => n + f(r), 0) / rows.length;
  const better = rows.filter((r) => r.delta > 0.02).length;
  const worse = rows.filter((r) => r.delta < -0.02).length;
  console.log(`\n  pages compared      : ${rows.length}`);
  console.log(`  OLD corroborated at : ${(100 * mean((r) => r.old)).toFixed(1)}%`);
  console.log(`  NEW corroborated at : ${(100 * mean((r) => r.neu)).toFixed(1)}%`);
  console.log(`  change              : ${(100 * mean((r) => r.delta) >= 0 ? "+" : "")}${(100 * mean((r) => r.delta)).toFixed(1)} points`);
  console.log(`  NEW better on ${better}, old better on ${worse}, unchanged on ${rows.length - better - worse}`);
}
