// Restores a granth the publish worker replaced back to its pre-Google state:
// the original PDF (byte-identical copy on the Kingston drive) and the page text
// captured in the morning backup, written through the same update path the
// publisher uses. Then removes the Google PDF/CSV it had uploaded.
//
//   node --env-file=.env scripts/google/rollback_granth.mjs <id> <publishDir> <backup.jsonl> <books.json>
import { readFile, writeFile, stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import readline from "node:readline";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { UTApi } from "uploadthing/server";

const run = promisify(execFile);
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../..");
const [id, pubDir, backupPath, booksPath] = process.argv.slice(2);
const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const ut = new UTApi({ token: process.env.UPLOADTHING_TOKEN });

const book = JSON.parse(await readFile(booksPath, "utf8")).find((b) => b.id === id);
const st = JSON.parse(await readFile(path.join(pubDir, id, "state.json"), "utf8"));
const meta = st.steps.resolve.value;
const google = st.steps.upload?.value;

// Old text from the backup.
let old = null;
for await (const line of readline.createInterface({ input: createReadStream(backupPath) })) {
  const g = JSON.parse(line);
  if (g.key === meta.granthKey) { old = g; break; }
}
if (!old) throw new Error(`no backup for ${meta.granthKey}`);
const pages = old.pages.sort((a, b) => a[0] - b[0]);
const { stdout } = await run("pdfinfo", [book.local]);
const n = Number(stdout.match(/Pages:\s+(\d+)/)[1]);
if (pages.length !== n) throw new Error(`backup has ${pages.length} pages, PDF ${n}`);

const COLUMNS = ["granth_key", "book_number", "library_code", "granth_name", "source_rel_path", "pdf_url", "page_number", "content", "method", "status", "quality_score", "chars", "google_reason", "needs_review", "embedded_score", "local_score", "error"];
const cell = (v) => { const s = v == null ? "" : String(v); return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };

const up = async (file, name, type) => {
  const res = await ut.uploadFiles(new File([await readFile(file)], name, { type }));
  if (res.error) throw new Error(JSON.stringify(res.error));
  return { key: res.data.key, url: res.data.ufsUrl ?? res.data.url, size: (await stat(file)).size };
};
const pdf = await up(book.local, path.basename(book.rel), "application/pdf");
const csvName = `${path.basename(book.rel, ".pdf")}_restored.csv`;
const csvPath = path.join(pubDir, id, csvName);
const rows = [COLUMNS.join(",")];
for (const [p, content] of pages) rows.push([meta.granthKey, meta.bookNumber, meta.libraryCode, meta.granthName, book.rel, pdf.url, p, content, "restored_pre_google", "accepted", "", String(content).length, "", "false", "", "", ""].map(cell).join(","));
await writeFile(csvPath, rows.join("\n") + "\n", "utf8");
const csv = await up(csvPath, csvName, "text/csv");

const cfgPath = path.join(pubDir, id, "rollback.json");
await writeFile(cfgPath, JSON.stringify({
  granthKey: meta.granthKey, customId: meta.customId, csvPath, expectedPages: n, expectedGathaRows: meta.gathaRows,
  newPdfUrl: pdf.url, newPdfKey: pdf.key, newCsvUrl: csv.url, newCsvKey: csv.key,
  newCsvFilename: csvName, newCsvCustomId: `${meta.granthKey}__restored__ocr_csv`,
}, null, 1));
console.log((await run(process.execPath, [`--env-file=${path.join(ROOT, ".env")}`, path.join(ROOT, "scripts/sarvam/update_granth_sources.mjs"), cfgPath], { cwd: ROOT, maxBuffer: 1 << 26 })).stdout);
await turso.execute({ sql: "UPDATE ocr_pages SET printed_page = NULL WHERE granth_key = ?", args: [meta.granthKey] });
await sb.from("granth_ocr_files").update({ file_size: pdf.size }).eq("id", meta.fileId);
await sb.from("documents").update({ status: "processed_with_review_pages", updated_at: new Date().toISOString() }).eq("custom_id", meta.customId);

// Verify, then drop the Google files.
const { data: doc } = await sb.from("documents").select("pdf_url,csv_url").eq("custom_id", meta.customId).single();
const t = await turso.execute({ sql: "SELECT content FROM ocr_pages WHERE granth_key = ? AND page_number = ?", args: [meta.granthKey, Math.round(n / 2)] });
const same = String(t.rows[0]?.content ?? "") === String(pages[Math.round(n / 2) - 1][1]);
if (doc?.pdf_url !== pdf.url || doc?.csv_url !== csv.url || !same) throw new Error("rollback verification failed; Google files kept");
if (google) {
  const r = await ut.deleteFiles([google.pdf.key, google.csv.key]);
  console.log("removed Google files:", JSON.stringify(r));
}
console.log(`rolled back ${id}: ${n} pages restored, pdf ${pdf.url}`);
