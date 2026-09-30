#!/usr/bin/env node
// Line boxes for books whose Google OCR was not published (the text already
// live read better): each stored line of a page is matched to the Google line
// it is, by text similarity, and gets that line's box. The page text stays as
// it is; only ocr_line_boxes is written (source "aligned").
//
// Alignment is a monotonic dynamic programme over the two line lists that may
// also join two Google lines into one stored line or split one Google line
// over two stored lines (the two OCRs break lines differently). A page is kept
// only if nearly every stored line found its line; a line that did not gets an
// empty box and is simply not ringed.
//
//   node --env-file=.env scripts/align_line_boxes.mjs <ocr out dir>... [--only=key]
import { existsSync, readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { createClient } from "@libsql/client";
import { foldSanskrit } from "../lib/sanskrit-fold.mjs";

const dirs = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const only = process.argv.find((a) => a.startsWith("--only="))?.split("=")[1];
const db = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
// Turso drops a connection now and then under load; every write here is an
// upsert or a plain update, so a retry is safe.
async function retry(fn, tries = 5) {
  for (let a = 1; ; a += 1) {
    try {
      return await fn();
    } catch (error) {
      const msg = String(error?.message ?? error) + String(error?.cause ?? "");
      if (a >= tries || !/ECONNRESET|EPIPE|ECONNREFUSED|socket hang up|ETIMEDOUT|fetch failed|EAI_AGAIN|502|503|504/i.test(msg)) throw error;
      await new Promise((r) => setTimeout(r, 1000 * a));
    }
  }
}

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

const MIN_SIM = 0.35;
const MIN_MATCHED = 0.85;

function bigrams(text) {
  const t = foldSanskrit(String(text ?? "")).replace(/\s+/g, "");
  const out = new Map();
  for (let i = 0; i + 1 < t.length; i += 1) {
    const g = t.slice(i, i + 2);
    out.set(g, (out.get(g) ?? 0) + 1);
  }
  return { grams: out, size: Math.max(0, t.length - 1) };
}
function dice(a, b) {
  if (!a.size || !b.size) return 0;
  let common = 0;
  for (const [g, n] of a.grams) common += Math.min(n, b.grams.get(g) ?? 0);
  return (2 * common) / (a.size + b.size);
}
function union(a, b) {
  return [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])];
}

/** One box per stored line ([0,0,0,0] when it found none), or null if the page does not line up. */
function alignPage(stored, google) {
  const L = stored.map((t) => bigrams(t));
  const G = google.map((g) => bigrams(g.text));
  const GG = google.map((g, j) => (j + 1 < google.length ? bigrams(`${g.text} ${google[j + 1].text}`) : null));
  const LL = stored.map((t, i) => (i + 1 < stored.length ? bigrams(`${t} ${stored[i + 1]}`) : null));
  const n = stored.length;
  const m = google.length;
  const score = Array.from({ length: n + 1 }, () => new Float64Array(m + 1));
  const move = Array.from({ length: n + 1 }, () => new Int8Array(m + 1));
  for (let i = 0; i <= n; i += 1) {
    for (let j = 0; j <= m; j += 1) {
      if (!i && !j) continue;
      let best = -1;
      let how = 0;
      const tryMove = (s, k) => { if (s > best) { best = s; how = k; } };
      if (i && j) { const s = dice(L[i - 1], G[j - 1]); if (s >= MIN_SIM) tryMove(score[i - 1][j - 1] + s, 1); }
      if (i && j >= 2 && GG[j - 2]) { const s = dice(L[i - 1], GG[j - 2]); if (s >= MIN_SIM) tryMove(score[i - 1][j - 2] + s * 1.05, 2); }
      if (i >= 2 && j && LL[i - 2]) { const s = dice(LL[i - 2], G[j - 1]); if (s >= MIN_SIM) tryMove(score[i - 2][j - 1] + s * 1.05, 3); }
      if (i) tryMove(score[i - 1][j], 4); // stored line without a Google line
      if (j) tryMove(score[i][j - 1], 5); // Google line without a stored line
      score[i][j] = best;
      move[i][j] = how;
    }
  }
  const boxes = new Array(n).fill(null);
  let i = n;
  let j = m;
  while (i > 0 || j > 0) {
    const k = move[i][j];
    if (k === 1) { boxes[i - 1] = google[j - 1].box; i -= 1; j -= 1; }
    else if (k === 2) { boxes[i - 1] = union(google[j - 2].box, google[j - 1].box); i -= 1; j -= 2; }
    else if (k === 3) {
      // one printed line holds two stored lines: share its box by text length
      const [x0, y0, x1, y1] = google[j - 1].box;
      const a = stored[i - 2].length;
      const b = stored[i - 1].length;
      const cut = x0 + ((x1 - x0) * a) / Math.max(1, a + b);
      boxes[i - 2] = [x0, y0, cut, y1];
      boxes[i - 1] = [cut, y0, x1, y1];
      i -= 2; j -= 1;
    }
    else if (k === 4) i -= 1;
    else j -= 1;
  }
  const matched = boxes.filter(Boolean).length;
  if (!n || matched / n < MIN_MATCHED) return null;
  return boxes.map((b) => (b ? b.map((v) => Math.round(v * 10000) / 10000) : [0, 0, 0, 0]));
}

// every Google OCR part file, by book
const partsByBook = new Map();
for (const dir of dirs) {
  if (!existsSync(dir)) continue;
  for (const f of readdirSync(dir)) {
    const m = f.match(/^(.+)__p\d+\.json$/);
    if (!m) continue;
    (partsByBook.get(m[1]) ?? partsByBook.set(m[1], []).get(m[1])).push(path.join(dir, f));
  }
}
const have = new Set((await retry(() => db.execute("SELECT DISTINCT granth_key FROM ocr_line_boxes WHERE source != 'aligned' OR source IS NULL"))).rows.map((r) => String(r.granth_key)));
const aligned = new Set((await retry(() => db.execute("SELECT DISTINCT granth_key FROM ocr_line_boxes WHERE source = 'aligned'"))).rows.map((r) => String(r.granth_key)));
const books = [...partsByBook.keys()].filter((k) => (!only || k === only) && !have.has(k) && (process.argv.includes("--redo") || !aligned.has(k)));
log(`books to align ${books.length}`);
for (const key of books) {
  const google = new Map();
  for (const f of partsByBook.get(key)) {
    for (const p of JSON.parse(readFileSync(f, "utf8")).pages ?? []) google.set(Number(p.page), (p.lines ?? []).filter((l) => l.box && String(l.text ?? "").trim()));
  }
  const rows = (await retry(() => db.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ?", args: [key] }))).rows;
  if (!rows.length) { log(key, "no pages in Turso"); continue; }
  const statements = [];
  for (const row of rows) {
    const page = Number(row.page_number);
    const lines = String(row.content ?? "").split("\n").filter((t) => t.trim());
    const g = google.get(page);
    if (!g?.length || !lines.length) continue;
    const boxes = alignPage(lines, g);
    if (!boxes) continue;
    statements.push({
      sql: `INSERT INTO ocr_line_boxes (granth_key, page_number, boxes, source, words) VALUES (?, ?, ?, 'aligned', NULL)
            ON CONFLICT(granth_key, page_number) DO UPDATE SET boxes = excluded.boxes, source = excluded.source, words = NULL, updated_at = CURRENT_TIMESTAMP`,
      args: [key, page, JSON.stringify(boxes)],
    });
  }
  for (let k = 0; k < statements.length; k += 200) await retry(() => db.batch(statements.slice(k, k + 200), "write"));
  log(`${key}: ${statements.length} of ${rows.length} pages aligned`);
}
log("finished");
