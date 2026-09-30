#!/usr/bin/env node
// Loads the printed-line boxes our Google OCR runs found into Turso
// (ocr_line_boxes), so the viewer and the PDF export can ring a searched word
// where it is printed instead of where a PDF's invisible text layer says it is
// (see lib/line-box-rings.ts).
//
// Reads <publish run>/<book>/pages.json + state.json from each run directory;
// a book published by several runs takes the latest run's boxes. Each page
// stores the boxes of its non-empty lines, in the order of the page text
// (the text the publish step wrote to ocr_pages, one line per box).
//
//   node --env-file=.env scripts/import_line_boxes.mjs [runDir ...]
import fs from "node:fs";
import path from "node:path";
import { createClient } from "@libsql/client";

const runs = process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["modern_publish", "ld_publish", "pilot_publish"].map((d) => path.join("/media/dell/KINGSTON/ocr_work", d));
const db = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
await db.execute(`CREATE TABLE IF NOT EXISTS ocr_line_boxes (
  granth_key TEXT NOT NULL,
  page_number INTEGER NOT NULL,
  boxes TEXT NOT NULL,
  source TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (granth_key, page_number)
)`);

// The latest publish of each book.
const latest = new Map();
for (const run of runs) {
  if (!fs.existsSync(run)) continue;
  for (const dir of fs.readdirSync(run)) {
    const pagesPath = path.join(run, dir, "pages.json");
    const statePath = path.join(run, dir, "state.json");
    if (!fs.existsSync(pagesPath) || !fs.existsSync(statePath)) continue;
    const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
    const granthKey = state.steps?.resolve?.value?.granthKey;
    if (!granthKey || !state.steps?.verify?.done) continue;
    const when = fs.statSync(statePath).mtimeMs;
    const prev = latest.get(granthKey);
    if (!prev || prev.when < when) latest.set(granthKey, { pagesPath, when, source: path.basename(run) });
  }
}

let books = 0;
let pages = 0;
for (const [granthKey, { pagesPath, source }] of latest) {
  const list = JSON.parse(fs.readFileSync(pagesPath, "utf8"));
  const statements = [];
  for (const page of list) {
    const boxes = (page.lines ?? [])
      .filter((line) => line.box && String(line.clean ?? "").trim())
      .map((line) => line.box.map((v) => Math.round(Number(v) * 10000) / 10000));
    if (!boxes.length) continue;
    statements.push({
      sql: `INSERT INTO ocr_line_boxes (granth_key, page_number, boxes, source) VALUES (?, ?, ?, ?)
            ON CONFLICT(granth_key, page_number) DO UPDATE SET boxes = excluded.boxes, source = excluded.source, updated_at = CURRENT_TIMESTAMP`,
      args: [granthKey, Number(page.page), JSON.stringify(boxes), source],
    });
  }
  for (let i = 0; i < statements.length; i += 200) await db.batch(statements.slice(i, i + 200), "write");
  books += 1;
  pages += statements.length;
  if (books % 25 === 0) console.log(`${books} books, ${pages} pages`);
}
console.log(`done: ${books} books, ${pages} pages`);
