#!/usr/bin/env node
// Puts a granth's original PDF back (22 Nov 2026): the 2026-10-01 text-layer
// rebuild blanked a few typeset PDFs (no scanned image: their text was the
// print). The originals are on the Kingston SSD. The file is uploaded and the
// granth's rows in Supabase (documents, granth_ocr_files, granth_library_files,
// granth_gatha_map) are pointed at it; the old file is kept. Turso's text is
// not touched. Check the page count against the granth's pages first.
//
//   node --env-file=.env scripts/restore_original_pdf.mjs [--dry-run] <source_rel_path>=<pdf path> ...
//
// Then drop "pdf_blank" for the granth in data/granth-catalog-overrides.json.

import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const pairs = args.filter((a) => !a.startsWith("--")).map((a) => {
  const at = a.indexOf("=");
  if (at < 1) throw new Error(`expected <source_rel_path>=<pdf path>, got ${a}`);
  return { rel: a.slice(0, at), pdfPath: a.slice(at + 1) };
});
if (!pairs.length) {
  console.error("usage: restore_original_pdf.mjs [--dry-run] <source_rel_path>=<pdf path> ...");
  process.exit(1);
}

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const utapi = new UTApi({ token: process.env.UPLOADTHING_TOKEN });

for (const { rel, pdfPath } of pairs) {
  const { data: docs, error } = await sb.from("documents").select("custom_id,pdf_url").eq("original_relative_path", rel);
  if (error) throw new Error(error.message);
  if (docs?.length !== 1) throw new Error(`${rel}: expected one documents row, found ${docs?.length ?? 0}`);
  const customId = docs[0].custom_id;
  const bytes = await readFile(pdfPath);
  const info = await stat(pdfPath);
  console.log(`${rel}: ${path.basename(pdfPath)} (${(info.size / 1024 / 1024).toFixed(1)} MB) → ${customId}`);
  if (dryRun) continue;
  const upload = await utapi.uploadFiles(new UTFile([bytes], path.basename(rel), { type: "application/pdf", lastModified: info.mtimeMs }));
  if (upload.error) throw new Error(`${rel}: upload failed: ${JSON.stringify(upload.error)}`);
  const url = upload.data.ufsUrl ?? upload.data.url;
  const key = upload.data.key;
  const updates = [
    ["documents", { pdf_url: url, size_bytes: info.size, updated_at: new Date().toISOString() }],
    ["granth_ocr_files", { ufs_url: url, ut_url: url, ut_key: key, file_size: info.size }],
    ["granth_library_files", { pdf_url: url }],
    ["granth_gatha_map", { pdf_url: url }],
  ];
  for (const [table, patch] of updates) {
    const { data, error: e } = await sb.from(table).update(patch).eq("custom_id", customId).select("custom_id");
    if (e) throw new Error(`${rel}: ${table}: ${e.message}`);
    console.log(`  ${table}: ${data.length} row(s)`);
  }
  console.log(`  now ${url} (was ${docs[0].pdf_url})`);
}
