// Applies word-level OCR corrections to live granths without re-OCR: the page
// text in Turso (plus its search suffix index), the page CSV, and the PDF's
// invisible text layer. Only the text spans that contain a corrected word are
// redrawn; every other span and every page image stays exactly as it was.
//
// Input: one JSON file per granth, { key, id, kind, customId, pages: { n: { before, after, map } } }
// where `before` must equal the live Turso text (anything else is refused), `after`
// is the corrected text, and `map` is { wrongWord: rightWord } for that page.
//
//   node --env-file=.env scripts/google/apply_text_fixes.mjs <fixDir> <workDir> [--only=file] [--no-delete] [--limit=n]
import { readFile, writeFile, mkdir, readdir, rename, unlink, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import "regenerator-runtime/runtime.js";
import { PDFDocument, PDFName, PDFArray, PDFRawStream, PDFHexString, PDFOperator, PDFDict, decodePDFRawStream } from "pdf-lib";
import fontkit from "@pdf-lib/fontkit";
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
import { UTApi } from "uploadthing/server";
import { scriptRuns } from "../sarvam/add_text_layer.mjs";

const run = promisify(execFile);
const [fixDir, workDir] = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const noDelete = process.argv.includes("--no-delete");
const only = process.argv.find((a) => a.startsWith("--only="))?.split("=")[1];
const limit = Number(process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] ?? Infinity);
const [shardK, shardN] = (process.argv.find((a) => a.startsWith("--shard="))?.split("=")[1] ?? "0/1").split("/").map(Number);
await mkdir(workDir, { recursive: true });

const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const ut = new UTApi({ token: process.env.UPLOADTHING_TOKEN });
const stamp = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const keyOf = (url) => (url ? String(url).split("/f/")[1] ?? null : null);

// Same token pattern the corrections were computed with (fixlib2.py TOKRE).
const TOK = /[ऀ-ॣॱ-ॿઁ-ૣૹ-૿૦-૯]+/gu;
const applyMap = (text, map) => String(text).replace(TOK, (w) => (Object.hasOwn(map, w) ? map[w] : w));

const FONTS = {
  gujarati: "/usr/share/fonts/truetype/fonts-gujr-extra/Rekha.ttf",
  devanagari: "/usr/share/fonts/truetype/Sarai/Sarai.ttf",
  latin: "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
};
function utf16beHex(s) {
  let hex = "FEFF";
  for (const ch of s) {
    const cp = ch.codePointAt(0);
    if (cp > 0xffff) { const v = cp - 0x10000; hex += (0xd800 | (v >> 10)).toString(16).padStart(4, "0") + (0xdc00 | (v & 0x3ff)).toString(16).padStart(4, "0"); }
    else hex += cp.toString(16).padStart(4, "0");
  }
  return hex.toUpperCase();
}
function fromUtf16beHex(hex) {
  const h = hex.replace(/\s+/g, "");
  let s = "";
  for (let i = h.startsWith("FEFF") ? 4 : 0; i + 4 <= h.length; i += 4) s += String.fromCharCode(parseInt(h.slice(i, i + 4), 16));
  return s;
}
function sanitise(font, text) {
  let out = "";
  for (const ch of text) {
    if (/\s/.test(ch)) { out += " "; continue; }
    try { font.widthOfTextAtSize(ch, 10); out += ch; } catch { out += " "; }
  }
  return out;
}
const latin1 = (bytes) => Buffer.from(bytes).toString("latin1");
const SPAN = /\/Span <<\s*\/ActualText <([0-9A-Fa-f\s]+)>\s*>> BDC([\s\S]*?)EMC/g;

/**
 * Redraws, on each listed page, only the ActualText spans whose text changes under
 * that page's map, at the position and size the span had. Returns per-page counts.
 */
/**
 * Some source scans (made with PDF24) carry their own old OCR as invisible text
 * (render mode 3) inside a form XObject rather than in the page content, where the
 * earlier layer stripping never looked. Removes just those invisible text blocks;
 * any visible drawing in the form is kept.
 */
function stripHiddenFormText(doc) {
  const seen = new Set();
  let blocks = 0, forms = 0;
  const visit = (resources) => {
    const xo = resources?.lookupMaybe(PDFName.of("XObject"), PDFDict);
    if (!xo) return;
    for (const [, ref] of xo.entries()) {
      const key = ref.toString();
      if (seen.has(key)) continue;
      seen.add(key);
      const o = doc.context.lookup(ref);
      if (!(o instanceof PDFRawStream) || o.dict.get(PDFName.of("Subtype"))?.toString() !== "/Form") continue;
      const body = latin1(decodePDFRawStream(o).decode());
      let n = 0;
      const kept = body.replace(/BT[\s\S]*?ET/g, (b) => (/(^|\s)3 Tr(\s|$)/.test(b) ? ((n += 1), "") : b));
      if (n) {
        const fresh = doc.context.flateStream(Buffer.from(kept, "latin1"));
        for (const [k, v] of o.dict.entries()) if (!["/Filter", "/DecodeParms", "/Length"].includes(k.toString())) fresh.dict.set(k, v);
        doc.context.assign(ref, fresh);
        blocks += n; forms += 1;
      }
      visit(o.dict.lookupMaybe(PDFName.of("Resources"), PDFDict));
    }
  };
  for (const page of doc.getPages()) visit(page.node.Resources());
  return { forms, blocks };
}

async function patchPdf(src, out, pageMaps, interimOut) {
  const doc = await PDFDocument.load(await readFile(src), { ignoreEncryption: true });
  doc.registerFontkit(fontkit);
  const hidden = stripHiddenFormText(doc);
  if (hidden.blocks) await writeFile(interimOut, await doc.save({ useObjectStreams: true }));
  const fonts = {};
  const font = async (name) => (fonts[name] ??= await doc.embedFont(await readFile(FONTS[name]), { subset: true }));
  const pages = doc.getPages();
  const report = {};
  for (const [pnum, map] of Object.entries(pageMaps)) {
    const page = pages[Number(pnum) - 1];
    if (!page) throw new Error(`PDF has no page ${pnum}`);
    const ref = page.node.get(PDFName.of("Contents"));
    const c = page.node.context.lookup(ref);
    const streams = c instanceof PDFArray ? c.asArray().map((r) => page.node.context.lookup(r)) : [c];
    // Streams are joined with a newline: back to back, "…Q" + "Q…" would fuse
    // into one unknown operator. Earlier builds joined them without one, so also
    // split any "QQ" already fused that way.
    let joined = streams.filter((s) => s instanceof PDFRawStream).map((s) => latin1(decodePDFRawStream(s).decode())).join("\n");
    joined = joined.replace(/(^|\s)QQ(?=\s)/g, "$1Q\nQ");
    const redraw = [];
    const kept = joined.replace(SPAN, (seg, hex, body) => {
      const text = fromUtf16beHex(hex);
      const next = applyMap(text, map);
      if (next === text) return seg;
      const tf = body.match(/\/[\w-]+ ([\d.]+) Tf/);
      const tm = body.match(/1 0 0 1 ([-\d.]+) ([-\d.]+) Tm/);
      if (!tf || !tm) throw new Error(`page ${pnum}: span without position`);
      redraw.push({ text: next, size: Number(tf[1]), x: Number(tm[1]), y: Number(tm[2]) });
      // Placeholder (a PDF comment) where the redrawn span goes back, so the
      // page keeps its reading order for copy/paste and phrase search.
      return `%FIXSPAN${redraw.length - 1}%\n`;
    });
    report[pnum] = redraw.length;
    if (!redraw.length) continue;
    // Draw the replacements into a scratch content stream (pdf-lib embeds the fonts
    // and graphics states in the page resources), then splice the operators back in.
    const cs = page.getContentStream(false);
    const pieces = [];
    for (const b of redraw) {
      const start = cs.operators.length;
      const dict = PDFDict.fromMapWithContext(new Map([[PDFName.of("ActualText"), PDFHexString.of(utf16beHex(b.text))]]), doc.context);
      page.pushOperators(PDFOperator.of("BDC", [PDFName.of("Span"), dict]));
      let x = b.x;
      for (const r of scriptRuns(b.text)) {
        const f = await font(r.script);
        const safe = sanitise(f, r.text);
        if (!safe.trim()) continue;
        page.drawText(safe, { x, y: b.y, size: b.size, font: f, opacity: 0 });
        x += f.widthOfTextAtSize(safe, b.size);
      }
      page.pushOperators(PDFOperator.of("EMC", []));
      pieces.push(cs.operators.slice(start).map((op) => op.toString()).join("\n") + "\n");
    }
    const final = kept.replace(/%FIXSPAN(\d+)%\n/g, (_, i) => pieces[Number(i)]);
    page.node.set(PDFName.of("Contents"), page.node.context.register(page.node.context.flateStream(Buffer.from(final, "latin1"))));
  }
  await writeFile(out, await doc.save({ useObjectStreams: true }));
  return { report, hidden };
}
async function pageText(file, n) {
  return (await run("pdftotext", ["-enc", "UTF-8", "-f", String(n), "-l", String(n), file, "-"], { maxBuffer: 1 << 26 })).stdout;
}
const words = (s) => String(s).normalize("NFC").match(TOK) ?? [];
function bag(s) { const m = new Map(); for (const w of words(s)) m.set(w, (m.get(w) ?? 0) + 1); return m; }

async function uploadFile(filePath, name, type) {
  const bytes = await readFile(filePath);
  for (let a = 1; a <= 4; a += 1) {
    const res = await ut.uploadFiles(new File([bytes], name, { type }));
    if (!res.error) return { key: res.data.key, url: res.data.ufsUrl ?? res.data.url, size: bytes.length };
    if (a === 4) throw new Error(`upload ${name}: ${JSON.stringify(res.error)}`);
    await new Promise((r) => setTimeout(r, 5000 * a));
  }
}
async function download(url, file) {
  for (let a = 1; a <= 4; a += 1) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      await writeFile(file, Buffer.from(await r.arrayBuffer()));
      return;
    } catch (e) { if (a === 4) throw e; await new Promise((r) => setTimeout(r, 5000 * a)); }
  }
}
const segmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
const buildSuffix = (content) => (String(content ?? "").match(/[\p{L}\p{N}\p{M}_]+/gu) ?? [])
  .map((s) => Array.from(segmenter.segment(s), (x) => x.segment).reverse().join("")).join(" ");
const COLUMNS = ["granth_key", "book_number", "library_code", "granth_name", "source_rel_path", "pdf_url", "page_number", "content", "method", "status", "quality_score", "chars", "google_reason", "needs_review", "embedded_score", "local_score", "error"];
const csvCell = (v) => { const s = v == null ? "" : String(v); return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };

async function fixBook(fixFile) {
  const fx = JSON.parse(await readFile(path.join(fixDir, fixFile), "utf8"));
  const tag = path.basename(fixFile, ".json");
  const dir = path.join(workDir, tag);
  await mkdir(dir, { recursive: true });
  const statePath = path.join(dir, "state.json");
  const st = existsSync(statePath) ? JSON.parse(await readFile(statePath, "utf8")) : { steps: {} };
  const save = async () => { await writeFile(statePath + ".tmp", JSON.stringify(st, null, 1)); await rename(statePath + ".tmp", statePath); };
  const log = (m) => console.log(`${stamp()} [${tag}] ${m}`);
  async function step(name, fn) {
    if (st.steps[name]?.done) return st.steps[name].value;
    const value = await fn();
    st.steps[name] = { done: true, value, at: stamp() };
    await save();
    return value;
  }
  const pageNums = Object.keys(fx.pages).map(Number).sort((a, b) => a - b);

  const meta = await step("resolve", async () => {
    const { data: doc, error } = await sb.from("documents").select("custom_id,pdf_url,csv_url,original_relative_path,status").eq("custom_id", fx.customId).single();
    if (error) throw new Error(`documents: ${error.message}`);
    const { data: files } = await sb.from("granth_ocr_files").select("id,ut_key,ufs_url").eq("custom_id", fx.customId);
    if (files?.length !== 1) throw new Error(`expected 1 granth_ocr_files row, found ${files?.length}`);
    const g = await turso.execute({ sql: "SELECT granth_key, book_number, library_code, granth_name, source_rel_path, xlsx_key, xlsx_filename, xlsx_custom_id FROM ocr_granths WHERE granth_key = ?", args: [fx.key] });
    if (!g.rows.length) throw new Error("no ocr_granths row");
    const { count: gathaRows } = await sb.from("granth_gatha_map").select("id", { count: "exact", head: true }).eq("custom_id", fx.customId);
    const r = g.rows[0];
    return {
      oldPdfUrl: doc.pdf_url, oldPdfKey: files[0].ut_key, oldCsvKey: keyOf(doc.csv_url), oldXlsxKey: r.xlsx_key ? String(r.xlsx_key) : null,
      fileId: files[0].id, status: doc.status, gathaRows: gathaRows ?? 0,
      bookNumber: r.book_number == null ? "" : String(r.book_number), libraryCode: r.library_code == null ? "" : String(r.library_code),
      granthName: String(r.granth_name ?? ""), rel: String(r.source_rel_path ?? doc.original_relative_path ?? ""),
      csvFilename: String(r.xlsx_filename ?? `${fx.key}.csv`), csvCustomId: String(r.xlsx_custom_id ?? `${fx.key}__ocr_csv`),
    };
  });

  // The corrections were computed from a snapshot; refuse if the live text moved since.
  await step("check-live", async () => {
    if (!pageNums.length) return { pages: 0 };
    const live = await turso.execute({ sql: `SELECT page_number, content FROM ocr_pages WHERE granth_key = ? AND page_number IN (${pageNums.join(",")})`, args: [fx.key] });
    const byPage = new Map(live.rows.map((r) => [Number(r.page_number), String(r.content ?? "")]));
    const drift = pageNums.filter((n) => byPage.get(n) !== fx.pages[n].before);
    if (drift.length) throw new Error(`live text differs from snapshot on pages ${drift.slice(0, 10).join(",")}`);
    return { pages: pageNums.length };
  });

  const srcPdf = path.join(dir, "live.pdf");
  const outPdf = path.join(dir, "fixed.pdf");
  await step("patch-pdf", async () => {
    await download(meta.oldPdfUrl, srcPdf);
    const pageMaps = Object.fromEntries(pageNums.map((n) => [n, fx.pages[n].map]));
    const interimPdf = path.join(dir, "interim.pdf");
    const { report, hidden } = await patchPdf(srcPdf, outPdf, pageMaps, interimPdf);
    // The reference the patched file is checked against: the live file, or the
    // live file with the hidden old OCR removed when there was any.
    const base = hidden.blocks ? interimPdf : srcPdf;
    const count = async (f) => Number((await run("pdfinfo", [f])).stdout.match(/Pages:\s+(\d+)/)[1]);
    const a = await count(srcPdf), b = await count(outPdf);
    if (a !== b) throw new Error(`page count changed ${a} -> ${b}`);
    const problems = [];
    if (hidden.blocks) {
      // Removing the hidden layer may only take words away, and must leave the
      // current layer (the text in Turso) fully extractable.
      const probe = [...new Set([...pageNums.slice(0, 5), 1, Math.round(a / 3), Math.round((2 * a) / 3), a])].filter((n) => n >= 1 && n <= a);
      const liveRows = await turso.execute({ sql: `SELECT page_number, content FROM ocr_pages WHERE granth_key = ? AND page_number IN (${probe.join(",")})`, args: [fx.key] });
      const tursoBy = new Map(liveRows.rows.map((r) => [Number(r.page_number), String(r.content ?? "")]));
      for (const n of probe) {
        const L = bag(await pageText(srcPdf, n)), I = bag(await pageText(interimPdf, n));
        const want = new Set(words(tursoBy.get(n) ?? ""));
        // With two overlapping layers gone to one, a word boundary can shift; a
        // "new" word is fine only if it is a word of the current page text.
        const strange = [...I].filter(([w, c]) => c > (L.get(w) ?? 0) && !want.has(w)).length;
        if (strange > 3) problems.push(`p${n} hidden-strip produced ${strange} unexpected words`);
        if (want.size >= 20) { let hit = 0; for (const w of want) if (I.has(w)) hit += 1; if (hit / want.size < 0.85) problems.push(`p${n} layer overlap ${(hit / want.size).toFixed(2)} after hidden-strip`); }
      }
    }
    // Every patched page must extract as before with exactly the corrections
    // applied: same words in the same order, only the mapped words changed.
    let checked = 0;
    for (const n of pageNums) {
      if (!report[n]) continue;
      const before = await pageText(base, n), after = await pageText(outPdf, n);
      const got = words(after).join(" ");
      const want = words(applyMap(before.normalize("NFC"), fx.pages[n].map)).join(" ");
      // The extractor sometimes glues two words of a line together; then the
      // corrected word sits inside a longer token, so also accept the map applied
      // inside words (its keys are misspellings that occur nowhere else).
      const keys = Object.keys(fx.pages[n].map).sort((x, y) => y.length - x.length);
      const inner = words(keys.reduce((t, k) => t.split(k).join(fx.pages[n].map[k]), before.normalize("NFC"))).join(" ");
      // Last resort for glued words: same token count, and every differing token is
      // the expected one with a mapped key replaced at its start or end.
      const glued = () => {
        const W = want.split(" "), G = got.split(" "), m = fx.pages[n].map;
        if (W.length !== G.length) return false;
        return W.every((w, i) => w === G[i] || keys.some((k) =>
          (w.startsWith(k) && G[i] === m[k] + w.slice(k.length)) || (w.endsWith(k) && G[i] === w.slice(0, -k.length) + m[k])));
      };
      if (want !== got && inner !== got && !glued()) problems.push(`p${n} text differs from expected`);
      checked += 1;
    }
    // Pages not patched must extract exactly as the reference (spot-check a few).
    const untouched = [1, Math.round(b / 2), b].filter((n) => !report[n]);
    for (const n of untouched) if ((await pageText(base, n)) !== (await pageText(outPdf, n))) problems.push(`untouched p${n} changed`);
    if (problems.length) throw new Error(`patched PDF check failed: ${problems.slice(0, 8).join("; ")}`);
    if (existsSync(interimPdf)) await unlink(interimPdf);
    const spans = Object.values(report).reduce((s, x) => s + x, 0);
    const noSpan = pageNums.filter((n) => !report[n]).length;   // text fixed in DB, but the word wasn't in the PDF layer
    return { spans, checked, pagesWithoutSpan: noSpan, hiddenBlocksRemoved: hidden.blocks, hiddenForms: hidden.forms, bytes: (await stat(outPdf)).size };
  });

  const patched = st.steps["patch-pdf"].value;
  if (!pageNums.length && !patched.hiddenBlocksRemoved) {
    for (const f of [srcPdf, outPdf]) if (existsSync(f)) await unlink(f);
    return { tag, nothing: true, ...patched };
  }
  const csvPath = path.join(dir, meta.csvFilename.endsWith(".csv") ? meta.csvFilename : `${meta.csvFilename}.csv`);
  const up = await step("upload", async () => {
    const pdfName = path.basename(meta.rel || `${fx.key}.pdf`);
    const pdf = await uploadFile(outPdf, pdfName.endsWith(".pdf") ? pdfName : `${pdfName}.pdf`, "application/pdf");
    const all = await turso.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number", args: [fx.key] });
    const lines = [COLUMNS.join(",")];
    for (const r of all.rows) {
      const n = Number(r.page_number);
      const content = fx.pages[n] ? fx.pages[n].after : String(r.content ?? "");
      lines.push([fx.key, meta.bookNumber, meta.libraryCode, meta.granthName, meta.rel, pdf.url, n, content,
        fx.kind === "google" ? "google_docai" : "sarvam", "accepted", "", content.length, "", "false", "", "", ""].map(csvCell).join(","));
    }
    await writeFile(csvPath, lines.join("\n") + "\n", "utf8");
    const csv = await uploadFile(csvPath, path.basename(csvPath), "text/csv");
    const h = await fetch(pdf.url, { headers: { Range: "bytes=0-7" } });
    const head = Buffer.from(await h.arrayBuffer()).toString("latin1");
    if (!head.startsWith("%PDF")) throw new Error("uploaded PDF not served");
    return { pdf, csv, rows: all.rows.length };
  });

  await step("update-db", async () => {
    const upd = async (table, patch, col, val, expect) => {
      const { data, error } = await sb.from(table).update(patch).eq(col, val).select("id");
      if (error) throw new Error(`${table}: ${error.message}`);
      if (expect != null && data.length !== expect) log(`note: ${table} updated ${data.length}, expected ${expect}`);
    };
    await upd("documents", { pdf_url: up.pdf.url, csv_url: up.csv.url, updated_at: new Date().toISOString() }, "custom_id", fx.customId, 1);
    await upd("granth_ocr_files", { ufs_url: up.pdf.url, ut_url: up.pdf.url, ut_key: up.pdf.key, file_size: up.pdf.size }, "custom_id", fx.customId, 1);
    await upd("granth_library_files", { pdf_url: up.pdf.url }, "custom_id", fx.customId, null);
    if (meta.gathaRows) await upd("granth_gatha_map", { pdf_url: up.pdf.url }, "custom_id", fx.customId, meta.gathaRows);
    const now = stamp();
    const ids = pageNums.length ? await turso.execute({ sql: `SELECT id, page_number FROM ocr_pages WHERE granth_key = ? AND page_number IN (${pageNums.join(",")})`, args: [fx.key] }) : { rows: [] };
    const idBy = new Map(ids.rows.map((r) => [Number(r.page_number), Number(r.id)]));
    for (let i = 0; i < pageNums.length; i += 50) {
      const stmts = [];
      for (const n of pageNums.slice(i, i + 50)) {
        const id = idBy.get(n), content = fx.pages[n].after;
        stmts.push({ sql: "UPDATE ocr_pages SET content = ?, updated_at = ? WHERE id = ?", args: [content, now, id] });
        stmts.push({
          sql: `INSERT INTO ocr_pages_suffix (page_id, granth_key, page_number, reversed_content, updated_at) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(page_id) DO UPDATE SET reversed_content = excluded.reversed_content, updated_at = excluded.updated_at`,
          args: [id, fx.key, n, buildSuffix(content), now],
        });
      }
      await turso.batch(stmts, "write");
    }
    await turso.execute({
      sql: "UPDATE ocr_granths SET xlsx_url = ?, xlsx_key = ?, updated_at = ? WHERE granth_key = ?",
      args: [up.csv.url, up.csv.key, now, fx.key],
    });
    return { pages: pageNums.length };
  });

  await step("verify", async () => {
    const { data: doc } = await sb.from("documents").select("pdf_url,csv_url,status").eq("custom_id", fx.customId).single();
    const { data: f } = await sb.from("granth_ocr_files").select("ut_key").eq("id", meta.fileId).single();
    const problems = [];
    if (doc?.pdf_url !== up.pdf.url) problems.push("documents.pdf_url");
    if (doc?.csv_url !== up.csv.url) problems.push("documents.csv_url");
    if (doc?.status !== meta.status) problems.push(`status changed to ${doc?.status}`);
    if (f?.ut_key !== up.pdf.key) problems.push("granth_ocr_files.ut_key");
    const live = pageNums.length ? await turso.execute({ sql: `SELECT page_number, content FROM ocr_pages WHERE granth_key = ? AND page_number IN (${pageNums.join(",")})`, args: [fx.key] }) : { rows: [] };
    const bad = live.rows.filter((r) => String(r.content) !== fx.pages[Number(r.page_number)].after).length;
    if (bad || live.rows.length !== pageNums.length) problems.push(`${bad} pages not updated`);
    const t = await turso.execute({ sql: "SELECT COUNT(*) n FROM ocr_pages WHERE granth_key = ?", args: [fx.key] });
    if (Number(t.rows[0].n) !== up.rows) problems.push(`page count ${t.rows[0].n} != ${up.rows}`);
    if (problems.length) throw new Error(`verify failed: ${problems.join(", ")}`);
    return { ok: true };
  });

  if (noDelete) { log("old files kept (--no-delete)"); return { tag, kept: true, ...st.steps["patch-pdf"].value }; }
  await step("delete-old", async () => {
    const keys = [meta.oldPdfKey, meta.oldCsvKey, meta.oldXlsxKey].filter((k) => k && k !== up.pdf.key && k !== up.csv.key);
    const uniq = [...new Set(keys)];
    const res = uniq.length ? await ut.deleteFiles(uniq) : null;
    return { keys: uniq, deletedCount: res?.deletedCount ?? null };
  });
  for (const f of [srcPdf, outPdf]) if (existsSync(f)) await unlink(f);
  return { tag, ...st.steps["patch-pdf"].value };
}

const summaryPath = path.join(workDir, `summary-${shardK}of${shardN}.json`);
const summary = existsSync(summaryPath) ? JSON.parse(await readFile(summaryPath, "utf8")) : {};
const files = (await readdir(fixDir)).filter((f) => f.endsWith(".json") && (!only || f === only)).sort()
  .filter((_, i) => i % shardN === shardK);
let done = 0;
for (const f of files) {
  if (["done", "clean"].includes(summary[f]?.status) || (noDelete && summary[f]?.status === "kept")) continue;
  if (done >= limit) break;
  try {
    const r = await fixBook(f);
    summary[f] = { status: r.kept ? "kept" : r.nothing ? "clean" : "done", at: stamp(), ...r };
    console.log(`${stamp()} DONE ${f} spans ${r.spans} pages ${r.checked}${r.pagesWithoutSpan ? ` (${r.pagesWithoutSpan} pages text-only)` : ""}${r.hiddenBlocksRemoved ? ` hidden-old-OCR blocks removed ${r.hiddenBlocksRemoved}` : ""}`);
  } catch (e) {
    const tries = (summary[f]?.tries ?? 0) + 1;
    summary[f] = { status: tries >= 3 ? "failed" : "retry", tries, at: stamp(), error: e.message };
    console.log(`${stamp()} ${tries >= 3 ? "FAILED" : "RETRY"} ${f}: ${e.message}`);
  }
  done += 1;
  await writeFile(summaryPath + ".tmp", JSON.stringify(summary, null, 1)); await rename(summaryPath + ".tmp", summaryPath);
}
const counts = Object.values(summary).reduce((a, s) => ({ ...a, [s.status]: (a[s.status] ?? 0) + 1 }), {});
console.log(`${stamp()} status ${JSON.stringify(counts)} of ${files.length}`);
