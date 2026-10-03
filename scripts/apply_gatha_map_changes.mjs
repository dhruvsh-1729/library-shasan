// Applies a reviewed change set to granth_gatha_map: row updates by id and
// row deletes. The change set is produced by the gatha-map audit (see
// docs/gatha-map-audit-2026-10.md) and is never computed here, so what is
// written is exactly what was checked.
//
//   { "updates": [{ "id": 1, "page_start": 23, ... }], "deletes": [5, 6],
//     "inserts": [{ ...granth_gatha_map row, "_newbook": "Title" }],
//     "book_inserts": [{ "title", "code", "html", ... }], "book_code_appends": [{ "id": 151, "code": "x51…" }] }
//
// A row in "inserts" whose "_newbook" names a book in "book_inserts" gets that
// new catalog row's id.
//
//   node --env-file=.env scripts/apply_gatha_map_changes.mjs changes.json            (dry run)
//   node --env-file=.env scripts/apply_gatha_map_changes.mjs changes.json --execute
//
// Before writing, every touched row is saved to <changes>.before.json so the
// run can be undone by applying that file's rows back.
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { createClient } from "@supabase/supabase-js";

const [file, flag] = process.argv.slice(2);
if (!file) throw new Error("usage: apply_gatha_map_changes.mjs <changes.json> [--execute]");
const execute = flag === "--execute";
const { updates = [], deletes = [], inserts = [], book_inserts = [], book_code_appends = [] } = JSON.parse(readFileSync(file, "utf8"));
const COLUMNS = new Set([
  "book_code", "page_start", "next_page_start", "page_end", "page_count", "adhikar", "gatha",
  "gatha_to", "unit", "unit_label", "parent_path", "verification",
]);

for (const u of updates) {
  for (const key of Object.keys(u)) if (key !== "id" && !COLUMNS.has(key)) throw new Error(`row ${u.id}: column ${key} is not allowed`);
}

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const ids = [...new Set([...updates.map((u) => u.id), ...deletes])];
const before = [];
for (let i = 0; i < ids.length; i += 500) {
  const { data, error } = await sb.from("granth_gatha_map").select("*").in("id", ids.slice(i, i + 500));
  if (error) throw new Error(error.message);
  before.push(...data);
}
if (before.length !== ids.length) throw new Error(`expected ${ids.length} rows, found ${before.length}`);
for (let i = 0; i < inserts.length; i += 150) {
  const keys = inserts.slice(i, i + 150).map((r) => r.source_anchor_key);
  const { data: clash, error } = await sb.from("granth_gatha_map").select("source_anchor_key").in("source_anchor_key", keys);
  if (error) throw new Error(error.message);
  if (clash.length) throw new Error(`${clash.length} inserts already exist (e.g. ${clash[0].source_anchor_key})`);
}
console.log(`${updates.length} updates, ${deletes.length} deletes, ${inserts.length} inserts, ${book_inserts.length} new books (${execute ? "EXECUTE" : "dry run"})`);
if (!execute) process.exit(0);

writeFileSync(`${file}.before.json`, JSON.stringify(before));
let next = 0;
let done = 0;
await Promise.all(
  Array.from({ length: 12 }, async () => {
    while (next < updates.length) {
      const { id, ...fields } = updates[next++];
      for (let attempt = 0; ; attempt += 1) {
        const { error } = await sb.from("granth_gatha_map").update({ ...fields, updated_at: new Date().toISOString() }).eq("id", id);
        if (!error) break;
        if (attempt === 3) throw new Error(`row ${id}: ${error.message}`);
        await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
      }
      if (++done % 1000 === 0) console.log(`  ${done} updated`);
    }
  })
);
const bookIdByTitle = new Map();
for (const b of book_inserts) {
  const row = {
    source_row_hash: createHash("sha1").update(`${b.title}|${b.code}|${b.raw_text ?? ""}`).digest("hex").slice(0, 20),
    title_english: b.title, title_display: b.title_display ?? null, author_text: b.author_text ?? null,
    details_text: b.details_text ?? null, book_codes: [b.code], index_href: b.html, index_href_type: "html",
    cover_rel_path: b.cover_rel_path ?? null, raw_text: b.raw_text ?? null,
  };
  const { data, error } = await sb.from("granth_library_books").upsert(row, { onConflict: "source_row_hash" }).select("id").single();
  if (error) throw new Error(`book ${b.title}: ${error.message}`);
  bookIdByTitle.set(b.title, data.id);
}
for (const { id, code } of book_code_appends) {
  const { data, error } = await sb.from("granth_library_books").select("book_codes").eq("id", id).single();
  if (error) throw new Error(error.message);
  if (!data.book_codes.includes(code)) {
    const { error: e2 } = await sb.from("granth_library_books").update({ book_codes: [...data.book_codes, code] }).eq("id", id);
    if (e2) throw new Error(e2.message);
  }
}
const insertRows = inserts.map(({ _newbook, ...row }) => ({ ...row, book_id: _newbook ? bookIdByTitle.get(_newbook) : row.book_id }));
for (let i = 0; i < insertRows.length; i += 500) {
  const { error } = await sb.from("granth_gatha_map").insert(insertRows.slice(i, i + 500));
  if (error) throw new Error(`insert: ${error.message}`);
}
for (let i = 0; i < deletes.length; i += 200) {
  const { error } = await sb.from("granth_gatha_map").delete().in("id", deletes.slice(i, i + 200));
  if (error) throw new Error(error.message);
}
console.log(`done: ${done} updated, ${deletes.length} deleted, ${insertRows.length} inserted; previous rows in ${file}.before.json`);
