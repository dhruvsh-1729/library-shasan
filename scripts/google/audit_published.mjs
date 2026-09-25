// Independent check of every granth the publisher says it published: reads the
// live systems directly rather than trusting the worker's own logs.
//   node --env-file=.env scripts/google/audit_published.mjs <publishDir>
import { readFile, readdir, writeFile, unlink } from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
const run = promisify(execFile);
const dir = process.argv[2];
const deep = Number(process.argv.find((a) => a.startsWith("--deep="))?.split("=")[1] ?? 999999);
const sample = Number(process.argv.find((a) => a.startsWith("--sample="))?.split("=")[1] ?? 0);
const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const summary = {};
for (const f of (await readdir(dir)).filter((f) => /^summary.*\.json$/.test(f))) Object.assign(summary, JSON.parse(await readFile(path.join(dir, f), "utf8")));
// Books deliberately restored to their pre-Google text are not audited as Google books.
try { for (const id of Object.keys(JSON.parse(await readFile(path.join(dir, "rollbacks.json"), "utf8")))) delete summary[id]; } catch {}
const tok = (s) => new Set(String(s).normalize("NFC").match(/[ऀ-ॿ઀-૿]{2,}/g) ?? []);
let ok = 0, bad = 0, deepDone = 0;
let chosen = Object.entries(summary).filter(([, s]) => s.status === "published");
if (sample) chosen = chosen.sort(() => Math.random() - 0.5).slice(0, sample);
for (const [id, s] of chosen) {
  const st = JSON.parse(await readFile(path.join(dir, id, "state.json"), "utf8"));
  const meta = st.steps.resolve.value, up = st.steps.upload.value, n = st.steps.merge.value.pages;
  const problems = [];
  const { data: doc } = await sb.from("documents").select("pdf_url,csv_url,status").eq("custom_id", meta.customId).single();
  const { data: f } = await sb.from("granth_ocr_files").select("ut_key,ufs_url,file_size").eq("id", meta.fileId).single();
  if (doc?.pdf_url !== up.pdf.url) problems.push("documents.pdf_url not new");
  if (doc?.csv_url !== up.csv.url) problems.push("documents.csv_url not new");
  if (doc?.status !== "processed") problems.push(`status ${doc?.status}`);
  if (f?.ut_key !== up.pdf.key || f?.ufs_url !== up.pdf.url) problems.push("granth_ocr_files not new");
  const t = await turso.execute({ sql: "SELECT COUNT(*) n, SUM(printed_page IS NOT NULL) pp FROM ocr_pages WHERE granth_key = ?", args: [meta.granthKey] });
  if (Number(t.rows[0].n) !== n) problems.push(`turso pages ${t.rows[0].n} != ${n}`);
  const g = await turso.execute({ sql: "SELECT xlsx_url FROM ocr_granths WHERE granth_key = ?", args: [meta.granthKey] });
  if (g.rows[0]?.xlsx_url !== up.csv.url) problems.push("ocr_granths csv not new");
  // Full download is slow; beyond --deep books, check the live file's header/size only.
  let overlap = null;
  if (deepDone >= deep) {
    const h = await fetch(up.pdf.url, { headers: { Range: "bytes=0-7" } });
    const b = Buffer.from(await h.arrayBuffer());
    if (!b.toString("latin1").startsWith("%PDF")) problems.push("live PDF not served");
    if (Number((h.headers.get("content-range") ?? "").split("/")[1] || 0) !== up.pdf.size) problems.push("live PDF size differs");
  } else {
  deepDone += 1;
  // live PDF: download, page count, and the text layer on a middle page matches Turso
  const tmp = `/tmp/claude-1000/audit_${id}.pdf`;
  const r = await fetch(up.pdf.url); const buf = Buffer.from(await r.arrayBuffer()); await writeFile(tmp, buf);
  const pages = Number((await run("pdfinfo", [tmp])).stdout.match(/Pages:\s+(\d+)/)[1]);
  if (pages !== n) problems.push(`live PDF has ${pages} pages`);
  const mid = Math.round(n / 2);
  const layer = (await run("pdftotext", ["-enc", "UTF-8", "-f", String(mid), "-l", String(mid), tmp, "-"], { maxBuffer: 1 << 24 })).stdout;
  const tp = await turso.execute({ sql: "SELECT content FROM ocr_pages WHERE granth_key = ? AND page_number = ?", args: [meta.granthKey, mid] });
  const A = tok(tp.rows[0]?.content ?? ""), B = tok(layer); let sh = 0; for (const x of A) if (B.has(x)) sh++;
  overlap = A.size ? sh / A.size : 1;
  if (overlap < 0.85) problems.push(`PDF layer vs Turso overlap ${overlap.toFixed(2)}`);
  await unlink(tmp);
  }
  // old files really gone
  for (const k of st.steps["delete-old"]?.value?.keys ?? []) {
    const h = await fetch(`https://pk3cp5aaix.ufs.sh/f/${k}`, { method: "HEAD" });
    if (h.status === 200) problems.push(`old file ${k.slice(0, 8)} still served`);
  }
  if (!st.steps["delete-old"]?.done) problems.push("old files not deleted");
  const line = `${id.padEnd(12)} pages ${String(n).padStart(5)}  printed ${t.rows[0].pp}/${n}  layer~turso ${overlap == null ? "(header only)" : overlap.toFixed(2)}  ${problems.length ? "PROBLEMS: " + problems.join("; ") : "OK"}`;
  console.log(line); problems.length ? bad++ : ok++;
}
console.log(`\naudited ${ok + bad}: ${ok} OK, ${bad} with problems`);
