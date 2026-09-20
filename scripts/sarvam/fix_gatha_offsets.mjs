// Shifts page_start/page_end/next_page_start for books whose whole gatha map is
// off by a constant. diagnose_gatha_map.mjs finds the offset by testing every
// shift and keeping the one that makes the verse markers line up; only books
// where that jumps the hit rate decisively are touched here.
import { readFile, writeFile } from "node:fs/promises";
import { createClient as createSupabase } from "@supabase/supabase-js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const diagPath = args.find((a) => a.startsWith("--diagnosis="))?.split("=")[1] ?? ".sarvam/gatha_diagnosis.json";
const backupPath = args.find((a) => a.startsWith("--backup="))?.split("=")[1] ?? ".sarvam/gatha_offset_backup.json";
const minRate = Number(args.find((a) => a.startsWith("--min-rate="))?.split("=")[1] ?? 0.8);

const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const diag = JSON.parse(await readFile(diagPath, "utf8"));
const targets = diag.report.filter((r) => r.diagnosis === "OFFSET" && r.bestRate >= minRate && r.bestOffset !== 0);

console.log(`${dryRun ? "[dry-run]" : "[apply]"} books to shift: ${targets.length}`);
const backup = {};

for (const t of targets) {
  const rows = [];
  for (let from = 0; ; from += 1000) {
    const { data, error } = await sb.from("granth_gatha_map")
      .select("id,page_start,next_page_start,page_end")
      .eq("book_code", t.book).order("id").range(from, from + 999);
    if (error) throw new Error(error.message);
    rows.push(...data);
    if (data.length < 1000) break;
  }
  backup[t.book] = rows;
  console.log(`  ${t.book}: ${rows.length} rows, offset ${t.bestOffset}  (${(100 * t.rateAt0).toFixed(1)}% -> ${(100 * t.bestRate).toFixed(1)}%)`);
  if (dryRun) continue;

  const shift = (v) => (v == null ? null : Number(v) + t.bestOffset);
  for (let i = 0; i < rows.length; i += 200) {
    await Promise.all(rows.slice(i, i + 200).map((r) =>
      sb.from("granth_gatha_map").update({
        page_start: shift(r.page_start),
        next_page_start: shift(r.next_page_start),
        page_end: shift(r.page_end),
      }).eq("id", r.id)
    ));
    process.stdout.write(`    ${Math.min(i + 200, rows.length)}/${rows.length}\r`);
  }
  console.log(`    shifted ${rows.length} rows          `);
}

if (!dryRun && Object.keys(backup).length) {
  await writeFile(backupPath, JSON.stringify({ appliedAt: new Date().toISOString(), targets, backup }, null, 2));
  console.log(`\nbackup of previous values -> ${backupPath}`);
}
