// Builds granth_gatha_map rows for granths the old HTML index never covered,
// from the reviewed decisions in data/gatha-detect/decisions.json and the
// granth's OCR text (scripts/gatha_markers.mjs).
//
// Each decision was made by reading the book: which numbered series is the
// book's own (not its commentary's or a quoted text's), what a unit is called
// there, how its chapters are numbered, and a sample of units checked on the
// page. A decision either maps the book or records why it has nothing to map
// (a kosh, a prose retelling, no verse numbers in the OCR).
//
// Rows match the audited map: the unit and the label the book prints, the
// chapter (adhikar) and the levels above it, the page its marker is on, and
// page_end = the page before the next unit anywhere in the PDF. A number the
// OCR lost inside a run is kept when its neighbours pin it to a few pages
// (verification "unverified"); units read from the page are "verified".
//
//   node --env-file=.env scripts/build_detected_gatha_map.mjs [keys…] [--out=file.json]   (dry run)
//   … --execute    replaces the ocr-detected rows of those granths
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { createClient as createTurso } from "@libsql/client";
import { createClient } from "@supabase/supabase-js";
import { detectUnits, loadGranth, seriesList, summarize } from "./gatha_markers.mjs";

const SOURCE = "ocr-detected";
const DECISIONS = "data/gatha-detect/decisions.json";
const MAX_FILL_SPAN = 6;  // a lost number is kept only when its neighbours are this close
const MAX_FILL_COUNT = 10;
const LAST_UNIT_SPAN = 10; // the book's last unit runs to its last closing repeat within this many pages

const sha = (s) => createHash("sha1").update(s).digest("hex").slice(0, 28);

/** Units of one part of a decision: [{ chapter, n, to, page, filled }]. */
export function partUnits(pages, printed, part) {
  const opts = part.options ?? {};
  const units = opts.series > 1
    ? seriesList(pages, printed, part.family, opts, opts.series)[opts.series - 1] ?? []
    : detectUnits(pages, printed, part.family, opts);
  // numbers a reviewer found on the wrong page are left out, not guessed
  const excluded = new Set(part.exclude ?? []);
  const out = [];
  for (let i = 0; i < units.length; i += 1) {
    const u = units[i];
    if (excluded.has(u.n)) continue;
    const prev = out.at(-1);
    if (prev && prev.chapter === u.chapter && !prev.filled) {
      const gap = u.n - prev.to - 1;
      if (gap > 0 && gap <= MAX_FILL_COUNT && u.page - prev.page <= MAX_FILL_SPAN) {
        for (let k = prev.to + 1; k < u.n; k += 1) if (!excluded.has(k)) out.push({ chapter: u.chapter, chap: u.chap, n: k, to: k, page: prev.page, pageEnd: u.page, filled: true });
      }
    }
    out.push({ ...u, filled: false });
  }
  return out.map((u) => ({ ...u, filled: Boolean(u.filled) }));
}

/** The adhikar number and parent path of detected chapter `c` (1-based) under a part's chapter rule. */
function chapterOf(part, unit, chapterCount) {
  const ch = part.chapters ?? {};
  let adhikar;
  if (ch.numbering === "chap") adhikar = unit.chap;
  else if (Array.isArray(ch.adhikars)) adhikar = ch.adhikars[unit.chapter - 1] ?? null;
  else if (chapterCount > 1 || ch.label) adhikar = (ch.start ?? 1) + unit.chapter - 1;
  else adhikar = null;
  const name = Array.isArray(ch.names) ? ch.names[unit.chapter - 1] : null;
  const levels = [ch.prefix, name ?? (ch.label && adhikar != null ? `${ch.label} ${adhikar}` : null)].filter(Boolean);
  return { adhikar, parentPath: levels.length ? levels.join(" › ") : null };
}

// Families whose number is printed before the unit ("[૭૭૨] …", "मू. (४८) …"):
// a unit there can run onto the page where the next one starts. "॥ २३ ॥"
// closes its verse, so that verse is done by the next one's page.
const LEADING = new Set(["bracket", "mool", "bhashya", "suBracket", "sutraLabel", "gathaLabel"]);
const isLeading = (part) => part.leading ?? (LEADING.has(part.family) || (part.family === "custom" && String(part.options?.regex ?? "").startsWith("^")));

const UNIT_NAMES = {
  mool: ["मूल", "મૂલ"], sutra: ["सूत्र", "સૂત્ર"], gatha: ["गाथा", "ગાથા"], shlok: ["श्लोक", "શ્લોક"],
  niryukti: ["निर्युक्ति", "નિર્યુક્તિ"], bhashya: ["भाष्य", "ભાષ્ય"], karika: ["कारिका", "કારિકા"],
};

export function buildRows(decision, pages, printed) {
  const rows = [];
  for (const part of decision.parts) {
    const leading = isLeading(part);
    const units = partUnits(pages, printed, part);
    const chapterCount = new Set(units.map((u) => u.chapter)).size;
    for (const [ui, u] of units.entries()) {
      const { adhikar, parentPath } = chapterOf(part, u, chapterCount);
      const group = `${decision.parts.indexOf(part)}:${ui}`;
      // A group printed as one ("[૭૮૧ થી ૭૮૪]", "સૂત્ર-૭૪,૭૫") gives each of its
      // numbers a row on the group's page, so any lookup finds every number.
      const excluded = new Set(part.exclude ?? []);
      for (let k = u.n + 1; k <= u.to; k += 1) if (!excluded.has(k)) rows.push({
        unit: part.unit, unit_label: part.unit_label ?? null, adhikar, parent_path: parentPath,
        gatha: k, gatha_to: null,
        // printed with a leading label the group starts where its range is; closed
        // by "॥२०-२१॥" it runs from N's page to the range's page
        page_start: leading ? u.restPage ?? u.page : u.page, min_end: leading ? null : u.restPage ?? null,
        fill_end: u.filled ? u.pageEnd : null, leading, group,
        verification: u.filled ? "unverified" : "verified",
      });
      rows.push({
        unit: part.unit,
        unit_label: part.unit_label ?? null,
        adhikar,
        parent_path: parentPath,
        gatha: u.n,
        gatha_to: null,
        page_start: u.page,
        fill_end: u.filled ? u.pageEnd : null,
        leading,
        group,
        verification: u.filled ? "unverified" : "verified",
      });
    }
  }
  // The extractor groups a chapter by (parent_path, adhikar), not by unit: a
  // book with mool sutras and bhashya gathas both numbered from 1 would offer
  // "1" twice in one chapter. Each such unit series becomes its own chapter.
  const unitsOf = new Map();
  for (const r of rows) {
    const k = `${r.parent_path ?? ""}|${r.adhikar ?? ""}`;
    unitsOf.set(k, (unitsOf.get(k) ?? new Set()).add(r.unit));
  }
  for (const r of rows) {
    if (unitsOf.get(`${r.parent_path ?? ""}|${r.adhikar ?? ""}`).size < 2) continue;
    const name = UNIT_NAMES[r.unit]?.[/[\u0A80-\u0AFF]/u.test(r.unit_label ?? "") ? 1 : 0] ?? r.unit;
    r.parent_path = [name, r.parent_path].filter(Boolean).join(" › ");
  }
  // Reading order; page_end = the page before the next unit that starts later
  // anywhere in the book (the audited rule), next_page_start within the chapter.
  rows.sort((a, b) => a.page_start - b.page_start || a.gatha - b.gatha);
  const starts = [...new Set(rows.filter((r) => r.verification === "verified").map((r) => r.page_start))].sort((a, b) => a - b);
  const lastPage = pages.at(-1)?.page ?? 0;
  for (const [i, r] of rows.entries()) {
    const nextInChapter = rows.slice(i + 1).find((x) => x.unit === r.unit && x.adhikar === r.adhikar && x.parent_path === r.parent_path && x.page_start > r.page_start);
    r.next_page_start = nextInChapter?.page_start ?? null;
    const following = starts.find((p) => p > r.page_start);
    // A lost number may sit on its following neighbour's page too; the app
    // ends a span at next_page_start - 1, so point that past the neighbour.
    if (r.fill_end != null) { r.page_end = r.fill_end; r.next_page_start = r.fill_end + 1; }
    else if (following != null && r.leading) {
      // done on its own page when the next unit in reading order starts there too
      const next = rows.slice(i + 1).find((x) => x.verification === "verified" && x.group !== r.group);
      r.page_end = next && next.page_start === r.page_start ? r.page_start : following;
      r.next_page_start = r.page_end + 1;
    }
    else if (following != null) r.page_end = Math.max(r.page_start, following - 1);
    else r.page_end = Math.min(lastPage, r.page_start + LAST_UNIT_SPAN, lastUnitEnd(pages, r) ?? r.page_start);
    if (r.min_end != null && r.page_end < r.min_end) { r.page_end = r.min_end; r.next_page_start = r.min_end + 1; }
    delete r.min_end;
    delete r.fill_end;
    delete r.leading;
  }
  for (const r of rows) {
    delete r.group;
  }
  return rows;
}

/** The book's last unit: to the last page that still closes its number between dandas ("॥ ३२ ॥", the end of its commentary), at most LAST_UNIT_SPAN pages on. */
function lastUnitEnd(pages, row) {
  const forms = [String(row.gatha), ...["०१२३४५६७८९", "૦૧૨૩૪૫૬૭૮૯"].map((d) => String(row.gatha).replace(/[0-9]/g, (c) => d[c]))];
  const marker = new RegExp(`(?:॥|।।|\\|\\|)\\s*(?:${forms.join("|")})\\s*(?:॥|।।|\\|\\|)`, "u");
  let end = row.page_start;
  for (const p of pages) if (p.page > row.page_start && p.page <= row.page_start + LAST_UNIT_SPAN && marker.test(p.content)) end = p.page;
  return end;
}

/** Self-checks every built book must pass before it is written. */
export function check(rows, pageCount) {
  const problems = [];
  if (!rows.length) problems.push("no rows");
  const seen = new Set();
  for (const r of rows) {
    const k = `${r.unit}|${r.parent_path}|${r.adhikar}|${r.gatha}`;
    if (seen.has(k)) problems.push(`duplicate ${k}`);
    seen.add(k);
    if (!(r.page_start >= 1 && r.page_start <= pageCount)) problems.push(`page ${r.page_start} outside 1..${pageCount}`);
    if (r.page_end < r.page_start) problems.push(`page_end before start at ${k}`);
  }
  return [...new Set(problems)].slice(0, 5);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const execute = args.includes("--execute");
  const outFile = args.find((a) => a.startsWith("--out="))?.slice(6);
  const only = new Set(args.filter((a) => !a.startsWith("--")));
  const decisions = JSON.parse(readFileSync(DECISIONS, "utf8")).granths.filter((d) => !only.size || only.has(d.key));

  const turso = createTurso({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
  const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
  const granths = new Map((await turso.execute("SELECT granth_key, page_count, source_rel_path, granth_name FROM ocr_granths")).rows.map((r) => [String(r.granth_key), r]));
  const { data: books } = await sb.from("granth_library_books").select("id,title_display,book_codes");
  const bookOf = new Map();
  for (const b of books ?? []) for (const c of b.book_codes ?? []) bookOf.set(c, b);

  const built = [];
  for (const d of decisions) {
    if (d.action !== "map") continue;
    const g = granths.get(d.key);
    // the PDF: by the granth's own path, or the catalog's exact document id
    // (spreadsheet-OCR granths whose path is the .xlsx)
    const { data: docRows } = !g ? { data: [] } : d.document_custom_id
      ? await sb.from("documents").select("custom_id,pdf_name,pdf_url").eq("custom_id", d.document_custom_id).limit(2)
      : await sb.from("documents").select("custom_id,pdf_name,pdf_url").eq("original_relative_path", g.source_rel_path).limit(2);
    const doc = docRows?.length === 1 ? docRows[0] : null;
    if (!g || !doc?.pdf_url) { console.log(`SKIP ${d.key}: no served PDF`); continue; }
    const existing = [];
    for (let from = 0; ; from += 1000) {
      const { data, error } = await sb.from("granth_gatha_map").select("id,source_html_rel_path,unit,adhikar,gatha,gatha_to").eq("book_code", d.key).order("id").range(from, from + 999);
      if (error) throw error;
      existing.push(...data);
      if (data.length < 1000) break;
    }
    const indexRows = existing.filter((r) => r.source_html_rel_path !== SOURCE);
    if (indexRows.length && !d.supplement) { console.log(`SKIP ${d.key}: has index-imported rows`); continue; }

    const { pages, printed } = await loadGranth(turso, d.key);
    let rows = buildRows(d, pages, printed);
    if (d.supplement) {
      // only what the index lacks: a number the index has (in that chapter, or
      // anywhere when it gives no chapter) is never added again
      const has = new Set();
      for (const r of indexRows) for (let k = r.gatha; k <= (r.gatha_to ?? r.gatha); k += 1) { has.add(`${r.unit}|${r.adhikar ?? ""}|${k}`); has.add(`${r.unit}|*|${k}`); }
      rows = rows.filter((r) => !has.has(`${r.unit}|${r.adhikar ?? ""}|${r.gatha}`) && !(r.adhikar == null && has.has(`${r.unit}|*|${r.gatha}`)));
      if (!rows.length) { console.log(`ok    ${d.key}: index already has every number found`); continue; }
    }
    const problems = check(rows, Number(g.page_count));
    const verified = rows.filter((r) => r.verification === "verified").length;
    const chapters = new Set(rows.map((r) => `${r.unit}|${r.parent_path}|${r.adhikar}`)).size;
    console.log(`${problems.length ? "FAIL" : execute ? "WRITE" : "ok   "} ${d.key.padEnd(18)} ${String(rows.length).padStart(5)} rows (${verified} read, ${rows.length - verified} between neighbours) in ${chapters} chapters, pages ${rows[0]?.page_start}..${rows.at(-1)?.page_end}${problems.length ? ` — ${problems.join("; ")}` : ""}`);
    if (problems.length) continue;
    const book = bookOf.get(d.key) ?? null;
    built.push({ key: d.key, rows });
    if (!execute) continue;

    const { data: file, error: fileError } = await sb.from("granth_library_files").upsert({
      source_file_key: `${SOURCE}:${d.key}`,
      book_id: book?.id ?? null,
      book_code: d.key,
      custom_id: doc.custom_id,
      pdf_file_name: doc.pdf_name,
      pdf_rel_path: g.source_rel_path,
      pdf_url: doc.pdf_url,
      page_count: Number(g.page_count),
      source_kind: SOURCE,
    }, { onConflict: "source_file_key" }).select("id").single();
    if (fileError) throw fileError;
    const { error: delError } = await sb.from("granth_gatha_map").delete().eq("book_code", d.key).eq("source_html_rel_path", SOURCE);
    if (delError) throw delError;
    const gathaRows = rows.map((r, i) => ({
      source_anchor_key: sha(`${SOURCE}|${d.key}|${r.unit}|${r.parent_path ?? ""}|${r.adhikar ?? ""}|${r.gatha}`),
      book_id: book?.id ?? null,
      library_file_id: file.id,
      custom_id: doc.custom_id,
      book_code: d.key,
      source_html_rel_path: SOURCE,
      source_html_title: book?.title_display ?? String(g.granth_name),
      pdf_file_name: doc.pdf_name,
      pdf_rel_path: g.source_rel_path,
      pdf_url: doc.pdf_url,
      adhikar: r.adhikar,
      gatha: r.gatha,
      gatha_to: r.gatha_to,
      unit: r.unit,
      unit_label: r.unit_label,
      parent_path: r.parent_path,
      verification: r.verification,
      anchor_text: [r.parent_path, `${r.unit_label ?? r.unit} ${r.gatha}${r.gatha_to ? `-${r.gatha_to}` : ""}`].filter(Boolean).join(" › "),
      anchor_label: printed.get(r.page_start) ?? null,
      href: `${doc.pdf_name}#page=${r.page_start}`,
      page_start: r.page_start,
      next_page_start: r.next_page_start,
      page_end: r.page_end,
      page_count: Number(g.page_count),
      sequence_index: i,
    }));
    for (let i = 0; i < gathaRows.length; i += 500) {
      const { error } = await sb.from("granth_gatha_map").insert(gathaRows.slice(i, i + 500));
      if (error) throw error;
    }
  }
  if (outFile) writeFileSync(outFile, JSON.stringify(built));
  console.log(execute ? `wrote ${built.length} granths` : `dry run: ${built.length} granths would be written — add --execute`);
}
