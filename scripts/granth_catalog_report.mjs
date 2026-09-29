#!/usr/bin/env node
// Builds the granth catalog exactly as the app does (lib/granth-catalog-build.mjs
// over the live Supabase and Turso metadata plus data/granth-catalog-overrides.json)
// and reports what needs a person's eye:
//   - problems the builder found (a PDF shared by two granths, a bad override…)
//   - indexed granths linked to no PDF that are not text-only by design
//   - duplicates kept out of search, and the granth each one points at
// With --out <file> the whole catalog is also written as JSON for review.
//
//   node scripts/granth_catalog_report.mjs [--out .tmp/granth-catalog.json]

import "dotenv/config";
import { readFileSync, writeFileSync } from "node:fs";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { buildGranthCatalog, granthDisplayName } from "../lib/granth-catalog-build.mjs";

const outIndex = process.argv.indexOf("--out");
const outFile = outIndex > 0 ? process.argv[outIndex + 1] : null;

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const supabase = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

async function fetchAll(table, columns) {
  const rows = [];
  for (let from = 0; ; from += 1000) {
    const { data, error } = await supabase.from(table).select(columns).range(from, from + 999);
    if (error) throw new Error(`${table}: ${error.message}`);
    rows.push(...(data ?? []));
    if (!data || data.length < 1000) break;
  }
  return rows;
}

const [tursoRows, documents, libraryFiles, books] = await Promise.all([
  turso
    .execute("SELECT granth_key, book_number, library_code, granth_name, source_rel_path, page_count FROM ocr_granths")
    .then((r) => r.rows.map((row) => ({ ...row }))),
  fetchAll("documents", "custom_id,original_relative_path,pdf_name,pdf_url,status"),
  fetchAll("granth_library_files", "custom_id,book_id"),
  fetchAll("granth_library_books", "id,title_english,title_display,author_text,book_codes"),
]);
const overrides = JSON.parse(readFileSync(new URL("../data/granth-catalog-overrides.json", import.meta.url), "utf8"));
const { entries, issues } = buildGranthCatalog({ turso: tursoRows, documents, libraryFiles, books, overrides });

const byKey = new Map(entries.map((e) => [e.granth_key, e]));
const duplicates = entries.filter((e) => e.duplicate_of);
const unlinked = entries.filter((e) => !e.document_custom_id && !e.text_only && !e.duplicate_of);
const linked = new Set(entries.map((e) => e.document_custom_id).filter(Boolean));
const unindexedDocs = documents.filter((d) => d.status !== "pdf_uploaded_not_searchable" && !linked.has(d.custom_id));

console.log(`${entries.length} indexed granths · ${entries.length - duplicates.length} searchable · ${duplicates.length} duplicates kept out`);
console.log(`\nProblems (${issues.length})`);
for (const issue of issues) console.log(`  ${issue.granth_key}: ${issue.problem} — ${issue.candidates.join(" | ")}`);
console.log(`\nIndexed granths with no PDF that are not text-only (${unlinked.length})`);
for (const e of unlinked) console.log(`  ${e.granth_key}: ${granthDisplayName(e)}  [${e.source_rel_path}]`);
console.log(`\nSearchable documents with no indexed granth (${unindexedDocs.length})`);
for (const d of unindexedDocs) console.log(`  ${d.pdf_name}`);
console.log(`\nDuplicates kept out of search (${duplicates.length})`);
for (const e of duplicates) {
  const target = byKey.get(e.duplicate_of);
  console.log(`  ${e.granth_key} → ${e.duplicate_of}${target ? ` (${granthDisplayName(target)})` : "  !! target not indexed"}`);
}

if (outFile) {
  writeFileSync(outFile, JSON.stringify(entries, null, 1));
  console.log(`\nWrote ${entries.length} entries to ${outFile}`);
}
process.exit(0);
