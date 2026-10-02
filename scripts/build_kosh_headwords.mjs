// Indexes the first line of every entry in the vyutpatti koshes (see
// lib/kosh-headwords.mjs) into Turso kosh_headwords, so the vyutpatti page can
// find चोर on Shabda Ratna Mahodadhi 2, PDF page 61, line 14, without reading
// 53 MB of kosh text per word. Rerun after a kosh's OCR text changes.
//
//   node --env-file=.env scripts/build_kosh_headwords.mjs          # every kosh
//   node --env-file=.env scripts/build_kosh_headwords.mjs 381 375  # these koshes
import { createClient } from "@libsql/client";
import { VYUTPATTI_KOSHES, entryKeys, headwordLines } from "../lib/kosh-headwords.mjs";

const BATCH = 400;
const client = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });

await client.batch(
  [
    `CREATE TABLE IF NOT EXISTS kosh_headwords (
       key TEXT NOT NULL,
       granth_key TEXT NOT NULL,
       page_number INTEGER NOT NULL,
       line_no INTEGER NOT NULL,
       head TEXT NOT NULL,
       sanskrit TEXT
     )`,
    "CREATE INDEX IF NOT EXISTS kosh_headwords_key ON kosh_headwords (key)",
    "CREATE INDEX IF NOT EXISTS kosh_headwords_granth ON kosh_headwords (granth_key)",
  ],
  "write"
);

const named = process.argv.slice(2);
const koshes = named.length ? VYUTPATTI_KOSHES.filter((k) => named.includes(k.key)) : VYUTPATTI_KOSHES;

for (const kosh of koshes) {
  const result = await client.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number", args: [kosh.key] });
  const rows = [];
  for (const row of result.rows) {
    for (const entry of headwordLines(kosh.format, row.content)) {
      for (const key of entryKeys(entry)) rows.push([key, kosh.key, Number(row.page_number), entry.line, entry.head, entry.sanskrit]);
    }
  }
  const statements = [{ sql: "DELETE FROM kosh_headwords WHERE granth_key = ?", args: [kosh.key] }];
  for (let i = 0; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    statements.push({
      sql: `INSERT INTO kosh_headwords (key, granth_key, page_number, line_no, head, sanskrit) VALUES ${chunk.map(() => "(?, ?, ?, ?, ?, ?)").join(", ")}`,
      args: chunk.flat(),
    });
  }
  // One transaction per kosh: a failed run leaves the kosh's old rows in place.
  await client.batch(statements, "write");
  console.log(`${kosh.key} ${kosh.title}${kosh.part ? ` ${kosh.part}` : ""}: ${result.rows.length} pages, ${rows.length} index rows`);
}
