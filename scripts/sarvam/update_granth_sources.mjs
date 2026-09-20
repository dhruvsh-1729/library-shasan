// Repoints a granth's PDF/CSV to freshly uploaded UploadThing objects and
// replaces its OCR text in Turso. Deletes nothing — old objects are removed
// only after this has run and been verified.
import { readFile } from "node:fs/promises";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { parse } from "csv-parse/sync";

const cfg = JSON.parse(await readFile(process.argv[2], "utf8"));
const dryRun = process.argv.includes("--dry-run");
const label = dryRun ? "[dry-run]" : "[apply]";

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// Mirrors buildOCRSuffixIndexContent in lib/ocr-search-index.ts: the suffix
// table is maintained by the app, not by a trigger on ocr_pages.
const WORD_TOKEN_PATTERN = /[\p{L}\p{N}\p{M}_]+/gu;
const segmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
const reverseGraphemes = (s) =>
  Array.from(segmenter.segment(s), (x) => x.segment).reverse().join("");
const buildSuffix = (content) =>
  (String(content ?? "").match(WORD_TOKEN_PATTERN) ?? []).map(reverseGraphemes).join(" ");

const rows = parse(await readFile(cfg.csvPath, "utf8"), { columns: true, skip_empty_lines: false });
console.log(`${label} csv rows: ${rows.length}`);
if (rows.length !== cfg.expectedPages) throw new Error(`expected ${cfg.expectedPages} rows`);

// ---------- Supabase ----------
async function sbUpdate(table, patch, match, expect) {
  const q = sb.from(table).update(patch);
  for (const [k, v] of Object.entries(match)) q.eq(k, v);
  if (dryRun) {
    const probe = sb.from(table).select("*", { count: "exact", head: true });
    for (const [k, v] of Object.entries(match)) probe.eq(k, v);
    const { count } = await probe;
    console.log(`${label} ${table}: would update ${count} row(s) -> ${Object.keys(patch).join(",")}`);
    return;
  }
  const { data, error } = await q.select("*");
  if (error) throw new Error(`${table}: ${error.message}`);
  console.log(`${label} ${table}: updated ${data.length} row(s)${expect != null && data.length !== expect ? `  !! expected ${expect}` : ""}`);
}

await sbUpdate("documents", { pdf_url: cfg.newPdfUrl, csv_url: cfg.newCsvUrl }, { custom_id: cfg.customId }, 1);
await sbUpdate("granth_ocr_files", { ufs_url: cfg.newPdfUrl, ut_url: cfg.newPdfUrl, ut_key: cfg.newPdfKey }, { custom_id: cfg.customId }, 1);
await sbUpdate("granth_library_files", { pdf_url: cfg.newPdfUrl }, { custom_id: cfg.customId }, 1);
await sbUpdate("granth_gatha_map", { pdf_url: cfg.newPdfUrl }, { custom_id: cfg.customId }, cfg.expectedGathaRows);

// ---------- Turso ----------
if (dryRun) {
  const cur = await turso.execute({ sql: "SELECT COUNT(*) n FROM ocr_pages WHERE granth_key = ?", args: [cfg.granthKey] });
  console.log(`${label} ocr_pages: would rewrite ${cur.rows[0].n} row(s)`);
  console.log(`${label} ocr_pages_suffix: would rewrite the matching rows`);
  console.log(`${label} ocr_granths: would repoint xlsx_url -> ${cfg.newCsvUrl}`);
} else {
  const now = new Date().toISOString().slice(0, 19).replace("T", " ");

  // The old pipeline did not always store every page, so a re-OCR can carry
  // pages the table has never seen. Create those rows before rewriting, rather
  // than dropping text on the floor.
  const existing = await turso.execute({
    sql: "SELECT id, page_number FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
    args: [cfg.granthKey],
  });
  const have = new Set(existing.rows.map((r) => Number(r.page_number)));
  const missing = rows.map((r) => Number(r.page_number)).filter((n) => !have.has(n));
  if (missing.length) {
    console.log(`${label} ocr_pages: inserting ${missing.length} page(s) the table was missing`);
    for (let i = 0; i < missing.length; i += 50) {
      await turso.batch(
        missing.slice(i, i + 50).map((n) => ({
          sql: `INSERT INTO ocr_pages (granth_key, page_number, content, created_at, updated_at)
                VALUES (?, ?, '', ?, ?)
                ON CONFLICT (granth_key, page_number) DO NOTHING`,
          args: [cfg.granthKey, n, now, now],
        })),
        "write"
      );
    }
  }

  const ids = await turso.execute({
    sql: "SELECT id, page_number FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
    args: [cfg.granthKey],
  });
  const idByPage = new Map(ids.rows.map((r) => [Number(r.page_number), Number(r.id)]));
  let updated = 0;
  const BATCH = 25;
  for (let i = 0; i < rows.length; i += BATCH) {
    const stmts = [];
    for (const row of rows.slice(i, i + BATCH)) {
      const page = Number(row.page_number);
      const content = row.content;
      const pageId = idByPage.get(page);
      if (!pageId) throw new Error(`no ocr_pages row for page ${page}`);
      stmts.push({
        sql: "UPDATE ocr_pages SET content = ?, updated_at = ? WHERE id = ?",
        args: [content, now, pageId],
      });
      stmts.push({
        sql: `INSERT INTO ocr_pages_suffix (page_id, granth_key, page_number, reversed_content, updated_at)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT(page_id) DO UPDATE SET reversed_content = excluded.reversed_content, updated_at = excluded.updated_at`,
        args: [pageId, cfg.granthKey, page, buildSuffix(content), now],
      });
      updated += 1;
    }
    await turso.batch(stmts, "write");
    if ((i / BATCH) % 4 === 0) process.stdout.write(`  ${Math.min(i + BATCH, rows.length)}/${rows.length}\r`);
  }
  console.log(`${label} ocr_pages + ocr_pages_suffix: rewrote ${updated} page(s)          `);

  await turso.execute({
    sql: `UPDATE ocr_granths
          SET xlsx_url = ?, xlsx_key = ?, xlsx_filename = ?, xlsx_custom_id = ?, sheet_name = ?,
              text_row_count = ?, updated_at = ?
          WHERE granth_key = ?`,
    args: [cfg.newCsvUrl, cfg.newCsvKey, cfg.newCsvFilename, cfg.newCsvCustomId, null, rows.length, now, cfg.granthKey],
  });
  console.log(`${label} ocr_granths: repointed to the CSV`);
}
