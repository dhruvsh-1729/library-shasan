// Page-level check on stored OCR text, by re-reading the actual page image with
// an independent engine (Tesseract) and measuring how much of the stored text
// it corroborates.
//
// Tesseract is not ground truth — it is the engine whose output we replaced —
// so a low score is not proof the stored text is wrong. It is a *disagreement
// flag*: the pages where two engines diverge are the ones worth a human eye.
// High agreement, by contrast, is meaningful: two independent readers rarely
// invent the same words.
import { mkdtemp, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";

const run = promisify(execFile);
const arg = (n, d) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1] ?? d;

const granthKey = arg("granth");
const perGranth = Number(arg("pages", 6));
const granthCount = Number(arg("granths", 1));
const outPath = arg("out");
const dpi = arg("dpi", "200");

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const TOKEN = /[ऀ-ॿ઀-૿]{2,}/gu;
const tokens = (s) => new Set((String(s).normalize("NFC").match(TOKEN) ?? []));

/** Fraction of stored tokens the second engine also saw. */
function agreement(stored, other) {
  const a = tokens(stored);
  const b = tokens(other);
  if (a.size === 0) return { score: null, storedTokens: 0, otherTokens: b.size, shared: 0 };
  let shared = 0;
  for (const t of a) if (b.has(t)) shared += 1;
  return { score: shared / a.size, storedTokens: a.size, otherTokens: b.size, shared };
}

async function pickGranths() {
  if (granthKey) return [granthKey];
  const res = await turso.execute(
    `SELECT granth_key FROM ocr_granths ORDER BY RANDOM() LIMIT ${Math.max(1, granthCount)}`
  );
  return res.rows.map((r) => String(r.granth_key));
}

const results = [];
for (const key of await pickGranths()) {
  const g = await turso.execute({
    sql: "SELECT granth_name, source_rel_path FROM ocr_granths WHERE granth_key = ?",
    args: [key],
  });
  if (!g.rows.length) { console.log(`${key}: not found`); continue; }
  const relPath = String(g.rows[0].source_rel_path);

  const { data: docs } = await sb.from("documents").select("pdf_url").eq("original_relative_path", relPath);
  const pdfUrl = docs?.[0]?.pdf_url;
  if (!pdfUrl) { console.log(`${key}: no pdf_url`); continue; }

  const pages = await turso.execute({
    sql: `SELECT page_number, content FROM ocr_pages
          WHERE granth_key = ? AND length(trim(content)) > 200
          ORDER BY RANDOM() LIMIT ?`,
    args: [key, perGranth],
  });
  if (!pages.rows.length) { console.log(`${key}: no substantial pages`); continue; }

  const work = await mkdtemp(path.join(tmpdir(), `verify-${key}-`));
  const pdfPath = path.join(work, "src.pdf");
  try {
    const res = await fetch(pdfUrl);
    if (!res.ok) throw new Error(`pdf ${res.status}`);
    await writeFile(pdfPath, Buffer.from(await res.arrayBuffer()));

    console.log(`\n${key} — ${g.rows[0].granth_name}`);
    for (const row of pages.rows) {
      const pno = Number(row.page_number);
      const base = path.join(work, `p${pno}`);
      await run("pdftoppm", ["-f", String(pno), "-l", String(pno), "-r", dpi, "-png", pdfPath, base]);

      // pdftoppm suffixes the page number with variable padding
      const { stdout: ls } = await run("bash", ["-c", `ls ${base}*.png 2>/dev/null | head -1`]);
      const png = ls.trim();
      if (!png) { console.log(`  p${pno}: render failed`); continue; }

      await run("tesseract", [png, base, "-l", "guj+hin", "--psm", "3"], { maxBuffer: 32 * 1024 * 1024 })
        .catch(() => null);
      let other = "";
      try { other = await readFile(`${base}.txt`, "utf8"); } catch { /* tesseract produced nothing */ }

      const a = agreement(String(row.content), other);
      const pct = a.score == null ? "n/a" : `${(100 * a.score).toFixed(1)}%`;
      const flag = a.score != null && a.score < 0.35 ? "  <-- LOW, worth a look" : "";
      console.log(`  p${String(pno).padStart(4)}  agreement ${pct.padStart(6)}  stored ${String(a.storedTokens).padStart(4)} tok / tesseract ${String(a.otherTokens).padStart(4)}${flag}`);
      results.push({ granthKey: key, page: pno, ...a });
    }
  } catch (e) {
    console.log(`  ${key}: ${e.message}`);
  } finally {
    await rm(work, { recursive: true, force: true });
  }
}

const scored = results.filter((r) => r.score != null);
if (scored.length) {
  const mean = scored.reduce((n, r) => n + r.score, 0) / scored.length;
  const low = scored.filter((r) => r.score < 0.35);
  console.log(`\n${scored.length} pages checked | mean agreement ${(100 * mean).toFixed(1)}% | ${low.length} flagged below 35%`);
}
if (outPath) await writeFile(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2));
