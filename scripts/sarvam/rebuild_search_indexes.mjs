// Canonicalises ocr_pages.content to NFC and rebuilds ocr_pages_suffix with
// code-point reversal.
//
// Why both, in one pass:
//  * The corpus holds the same glyph written two ways (nukta before vs after
//    virama), so a typed query only ever finds one of them.
//  * ocr_pages_suffix reversed by grapheme cluster, which cannot represent a
//    match that starts inside a conjunct, so "ends with" silently lost hits.
//
// Updating content fires the existing FTS triggers, so the search and trigram
// indexes rebuild themselves. The suffix table is app-maintained and is
// rewritten here explicitly.
import { createClient } from "@libsql/client";
import { buildOCRSuffixIndexContent } from "../../lib/ocr-search-index.ts";

const dryRun = process.argv.includes("--dry-run");
const BATCH = Number(process.env.BATCH || 200);
const client = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });

const total = Number((await client.execute("SELECT COUNT(*) n FROM ocr_pages")).rows[0].n);
console.log(`${dryRun ? "[dry-run]" : "[apply]"} ocr_pages rows: ${total.toLocaleString()}`);

let lastId = 0;
let scanned = 0;
let contentRewritten = 0;
let suffixRewritten = 0;

for (;;) {
  const page = await client.execute({
    sql: `SELECT id, granth_key, page_number, content FROM ocr_pages
          WHERE id > ? ORDER BY id LIMIT ?`,
    args: [lastId, BATCH],
  });
  if (page.rows.length === 0) break;

  const statements = [];
  for (const row of page.rows) {
    const id = Number(row.id);
    lastId = id;
    scanned += 1;

    const content = String(row.content ?? "");
    const normalized = content.normalize("NFC");
    const suffix = buildOCRSuffixIndexContent(normalized);

    if (normalized !== content) {
      contentRewritten += 1;
      statements.push({ sql: "UPDATE ocr_pages SET content = ? WHERE id = ?", args: [normalized, id] });
    }
    suffixRewritten += 1;
    statements.push({
      sql: `INSERT INTO ocr_pages_suffix (page_id, granth_key, page_number, reversed_content)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(page_id) DO UPDATE SET reversed_content = excluded.reversed_content`,
      args: [id, String(row.granth_key), Number(row.page_number), suffix],
    });
  }

  if (!dryRun && statements.length) await client.batch(statements, "write");
  if (scanned % 5000 < BATCH) {
    process.stdout.write(`  ${scanned.toLocaleString()}/${total.toLocaleString()}  contentNFC=${contentRewritten}\r`);
  }
  if (dryRun && scanned >= 5000) {
    console.log(`\n[dry-run] stopped after ${scanned} rows`);
    break;
  }
}

console.log(`\nscanned ${scanned.toLocaleString()} pages`);
console.log(`  content normalised to NFC : ${contentRewritten.toLocaleString()}`);
console.log(`  suffix rows rebuilt       : ${suffixRewritten.toLocaleString()}`);
