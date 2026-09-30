// Writes gatha → page mappings found by scripts/detect_gatha_map.mjs to
// Supabase (granth_library_files + granth_gatha_map), for granths the HTML
// index never covered. Rows are marked source_kind / source_html_rel_path
// "ocr-detected" so they are never confused with the imported ones.
//
// A granth is written only when its detection passes the check given:
//   --expect=32   exactly gathas 1..32 in one chapter, in page order (a
//                 dvātriṃśikā); without it every chapter must be complete
// and its text is aligned with the served PDF (the printed-page verification
// found equal page counts and no shift: gatha pages are PDF pages).
//
//   node --env-file=.env scripts/import_detected_gatha_map.mjs --book-id=151 --expect=32 339 340 …   (dry run)
//   … --execute
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createClient as createTurso } from "@libsql/client";
import { createClient } from "@supabase/supabase-js";
import { detectGathas, loadGranth, markerOccurrences } from "./detect_gatha_map.mjs";

const args = process.argv.slice(2);
const opt = (name) => args.find((a) => a.startsWith(`--${name}=`))?.split("=")[1];
const execute = args.includes("--execute");
const expect = opt("expect") ? Number(opt("expect")) : null;
const bookId = opt("book-id") ? Number(opt("book-id")) : null;
const alignment = opt("alignment"); // JSON from the printed-page verification: [{ key, shift, text_pages, pdf_pages }]
const keys = args.filter((a) => !a.startsWith("--"));
const SOURCE = "ocr-detected";

const turso = createTurso({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const sha = (s) => createHash("sha1").update(s).digest("hex").slice(0, 28);
const aligned = alignment ? new Map(JSON.parse(readFileSync(alignment, "utf8")).map((r) => [r.key, r])) : null;

function check(rows) {
  const chapters = new Map();
  for (const r of rows) chapters.set(r.adhikar, [...(chapters.get(r.adhikar) ?? []), r]);
  const ordered = rows.every((r, i) => i === 0 || r.page >= rows[i - 1].page || r.adhikar !== rows[i - 1].adhikar);
  if (!ordered) return "pages go backwards";
  if (expect != null) {
    if (chapters.size !== 1) return `${chapters.size} chapters, expected 1`;
    const nums = rows.map((r) => r.gatha).join(",");
    const want = Array.from({ length: expect }, (_, i) => i + 1).join(",");
    return nums === want ? null : `gathas ${rows.map((r) => r.gatha).join(",")} are not 1..${expect}`;
  }
  for (const [a, list] of chapters) {
    const nums = list.map((r) => r.gatha);
    for (let i = 0; i < nums.length; i += 1) if (nums[i] !== i + 1) return `adhikar ${a}: gathas not 1..${nums.length} (at ${nums[i]})`;
  }
  return null;
}

const counts = new Map((await turso.execute("SELECT granth_key, page_count, source_rel_path, granth_name FROM ocr_granths")).rows.map((r) => [String(r.granth_key), r]));
const { data: book } = bookId ? await sb.from("granth_library_books").select("id,title_display,title_english,book_codes").eq("id", bookId).single() : { data: null };

let written = 0;
for (const key of keys) {
  const g = counts.get(key);
  const { data: docRows } = g
    ? await sb.from("documents").select("custom_id,pdf_name,pdf_url,original_relative_path").eq("original_relative_path", g.source_rel_path).limit(2)
    : { data: [] };
  const doc = docRows?.length === 1 ? docRows[0] : null;
  if (!g || !doc?.pdf_url) { console.log(`SKIP ${key}: no served PDF`); continue; }
  if (book && !book.book_codes.includes(key)) { console.log(`SKIP ${key}: not one of book ${bookId}'s codes`); continue; }
  if (aligned) {
    const a = aligned.get(key);
    if (!a || a.shift !== 0 || a.pdf_pages !== a.text_pages) { console.log(`SKIP ${key}: text/PDF alignment not verified`, JSON.stringify(a ?? null)); continue; }
  }
  const { data: existing } = await sb.from("granth_gatha_map").select("id,source_html_rel_path").eq("book_code", key).limit(1000);
  if ((existing ?? []).some((r) => r.source_html_rel_path !== SOURCE)) { console.log(`SKIP ${key}: already has an imported mapping`); continue; }

  const { pages, printed } = await loadGranth(turso, key);
  const rows = detectGathas(pages, printed);
  const problem = check(rows);
  if (problem) { console.log(`SKIP ${key}: ${problem}`); continue; }

  // Page ranges like the HTML importer: a gatha runs to the page before the
  // next gatha's page. The last gatha of a chapter runs to the last page its
  // marker appears on (the end of its commentary).
  const occ = markerOccurrences(pages);
  const out = rows.map((r, i) => {
    const nextStart = rows.slice(i + 1).find((x) => x.adhikar === r.adhikar && x.page > r.page)?.page ?? null;
    const lastOwn = Math.max(r.page, ...occ.filter((o) => o.n === r.gatha && o.page >= r.page && (nextStart == null || o.page < nextStart)).map((o) => o.page));
    return { ...r, next_page_start: nextStart, page_end: nextStart != null ? Math.max(r.page, nextStart - 1) : lastOwn };
  });

  const fileRow = {
    source_file_key: `${SOURCE}:${key}`,
    book_id: book?.id ?? null,
    book_code: key,
    custom_id: doc.custom_id,
    pdf_file_name: doc.pdf_name,
    pdf_rel_path: g.source_rel_path,
    pdf_url: doc.pdf_url,
    page_count: Number(g.page_count),
    source_kind: SOURCE,
  };
  console.log(`${execute ? "WRITE" : "would write"} ${key}: ${out.length} gathas, pages ${out[0].page}..${out[out.length - 1].page_end} — ${doc.pdf_name}`);
  if (!execute) continue;

  const { data: file, error: fileError } = await sb.from("granth_library_files").upsert(fileRow, { onConflict: "source_file_key" }).select("id").single();
  if (fileError) throw fileError;
  const gathaRows = out.map((r, i) => ({
    source_anchor_key: sha(`${SOURCE}|${key}|${r.adhikar ?? ""}|${r.gatha}`),
    book_id: book?.id ?? null,
    library_file_id: file.id,
    custom_id: doc.custom_id,
    book_code: key,
    source_html_rel_path: SOURCE,
    source_html_title: book?.title_display ?? String(g.granth_name),
    pdf_file_name: doc.pdf_name,
    pdf_rel_path: g.source_rel_path,
    pdf_url: doc.pdf_url,
    adhikar: r.adhikar,
    gatha: r.gatha,
    anchor_text: `गाथा ${r.gatha}`,
    anchor_label: printed.get(r.page) ?? null,
    href: `${doc.pdf_name}#page=${r.page}`,
    page_start: r.page,
    next_page_start: r.next_page_start,
    page_end: r.page_end,
    page_count: Number(g.page_count),
    sequence_index: i,
  }));
  const { error } = await sb.from("granth_gatha_map").upsert(gathaRows, { onConflict: "source_anchor_key" });
  if (error) throw error;
  written += 1;
}
console.log(execute ? `wrote ${written} granths` : "dry run — add --execute to write");
