#!/usr/bin/env node
// Uploads the PDF of a text-only granth (one whose OCR text came into Turso
// from a spreadsheet, the "GG 76 Prat" set) and records it in Supabase the way
// the other granth PDFs are: a granth_ocr_files row and a documents row with
// status processed_existing_turso, since its text is already indexed. Turso is
// not touched. Link the printed document_custom_id to the granth in
// data/granth-catalog-overrides.json afterwards.
//
// Match the PDF to the granth's text before running this (same page count,
// OCR of sampled pages against the Turso pages); nothing here checks it.
//
//   node scripts/link_text_only_pdfs.mjs [--dry-run] <granth_key>=<pdf path> ...
//
// The file is uploaded as "<granth_key>_<pdf file name>", like the library's
// other files. Running it again skips a granth whose document already exists.

import "dotenv/config";
import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";

const COLLECTION = "Shrirang Library_5-9-2024 OCR _ed";
const CUSTOM_ID_PREFIX = "Shrirang_Library_5-9-2024_OCR__ed__";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const pairs = args.filter((a) => !a.startsWith("--")).map((a) => {
  const at = a.indexOf("=");
  if (at < 1) throw new Error(`expected <granth_key>=<pdf path>, got ${a}`);
  return { granthKey: a.slice(0, at), pdfPath: a.slice(at + 1) };
});
if (!pairs.length) {
  console.error("usage: link_text_only_pdfs.mjs [--dry-run] <granth_key>=<pdf path> ...");
  process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const utapi = new UTApi({ token: process.env.UPLOADTHING_TOKEN });

for (const { granthKey, pdfPath } of pairs) {
  const bytes = await readFile(pdfPath);
  const info = await stat(pdfPath);
  const md5 = createHash("md5").update(bytes).digest("hex");
  const fileName = `${granthKey}_${path.basename(pdfPath)}`;
  const customId = `${CUSTOM_ID_PREFIX}${fileName.replace(/\s+/g, "_")}__OCR_${md5.slice(0, 12)}`;

  const { data: existing, error: lookupError } = await supabase.from("documents").select("id").eq("custom_id", customId);
  if (lookupError) throw new Error(lookupError.message);
  if (existing?.length) {
    console.log(`${granthKey}: already uploaded (documents ${existing[0].id}) ${customId}`);
    continue;
  }
  console.log(`${granthKey}: ${fileName} (${(info.size / 1024 / 1024).toFixed(1)} MB) → ${customId}`);
  if (dryRun) continue;

  // No UploadThing customId: ours is longer than its external_id column takes.
  const upload = await utapi.uploadFiles(new UTFile([bytes], fileName, { type: "application/pdf", lastModified: info.mtimeMs }));
  if (upload.error) throw new Error(`${granthKey}: upload failed: ${JSON.stringify(upload.error)}`);
  const ut = upload.data;
  const url = ut.ufsUrl ?? ut.url;

  const { data: fileRow, error: fileError } = await supabase
    .from("granth_ocr_files")
    .insert({
      ufs_url: url,
      ut_key: ut.key,
      ut_url: url,
      file_name: fileName,
      file_size: info.size,
      file_hash: md5,
      file_type: "application/pdf",
      custom_id: customId,
      original_rel_path: fileName,
      collection: COLLECTION,
      subcollection: null,
      last_modified: Math.round(info.mtimeMs),
    })
    .select("id")
    .single();
  if (fileError) throw new Error(`${granthKey}: granth_ocr_files: ${fileError.message}`);

  const { data: docRow, error: docError } = await supabase
    .from("documents")
    .insert({
      original_relative_path: fileName,
      custom_id: customId,
      pdf_name: fileName,
      pdf_url: url,
      size_bytes: info.size,
      modified_time_iso: info.mtime.toISOString(),
      status: "processed_existing_turso",
      error: null,
      updated_at: new Date().toISOString(),
    })
    .select("id")
    .single();
  if (docError) throw new Error(`${granthKey}: documents: ${docError.message}`);

  console.log(`  granth_ocr_files ${fileRow.id} · documents ${docRow.id} · ${url}`);
}
