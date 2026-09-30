#!/usr/bin/env node
// Measures where the words of every OCR line are printed (scripts/word_segments.py,
// from the local original PDF) and stores them in ocr_line_boxes.words, so a
// ring goes around the printed word itself (lib/line-box-rings.ts).
//
// The local originals are found through the OCR runs' books.json files.
//   node --env-file=.env scripts/import_word_segments.mjs [--only=key] [--parallel=8] [--redo]
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import { createClient } from "@libsql/client";

const ROOT = "/media/dell/KINGSTON/ocr_work";
const HERE = path.dirname(new URL(import.meta.url).pathname);
const only = process.argv.find((a) => a.startsWith("--only="))?.split("=")[1];
const parallel = Number(process.argv.find((a) => a.startsWith("--parallel="))?.split("=")[1] ?? 8);
const redo = process.argv.includes("--redo");
const sourceFilter = process.argv.find((a) => a.startsWith("--source="))?.split("=")[1];
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

// granth key -> local original PDF
function localOriginals() {
  const map = new Map();
  const runs = [["modern_publish", "modern_books.json"], ["ld_publish", "ld_books.json"], ["pilot_publish", "pilot_books.json"]];
  for (const [dir, file] of runs) {
    if (!existsSync(path.join(ROOT, file))) continue;
    const byId = new Map(JSON.parse(readFileSync(path.join(ROOT, file), "utf8")).map((b) => [String(b.id), b]));
    const pubRoot = path.join(ROOT, dir);
    if (!existsSync(pubRoot)) continue;
    for (const id of readdirSync(pubRoot)) {
      const statePath = path.join(pubRoot, id, "state.json");
      if (!existsSync(statePath)) continue;
      const key = JSON.parse(readFileSync(statePath, "utf8")).steps?.resolve?.value?.granthKey;
      const local = byId.get(id)?.local;
      if (key && local && existsSync(local)) map.set(key, local);
    }
  }
  for (const file of ["g144/books_deva.json", "g144/books_guj.json"]) {
    if (!existsSync(path.join(ROOT, file))) continue;
    for (const b of JSON.parse(readFileSync(path.join(ROOT, file), "utf8"))) if (b.local && existsSync(b.local) && !map.has(b.key)) map.set(b.key, b.local);
  }
  return map;
}

function segment(pdf, pages) {
  return new Promise((resolve, reject) => {
    const child = spawn("python3", [path.join(HERE, "word_segments.py")], { stdio: ["pipe", "pipe", "pipe"] });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    child.on("close", (code) => (code === 0 ? resolve(JSON.parse(out)) : reject(new Error(err.slice(-400) || `exit ${code}`))));
    child.stdin.end(JSON.stringify({ pdf, pages }));
  });
}

const locals = localOriginals();
const keys = (await retry(() => db.execute(`SELECT granth_key, COUNT(*) n, SUM(words IS NULL) todo FROM ocr_line_boxes${sourceFilter ? ` WHERE source = '${sourceFilter.replace(/'/g, "")}'` : ""} GROUP BY granth_key`))).rows
  .map((r) => ({ key: String(r.granth_key), todo: Number(r.todo) }))
  .filter((r) => (!only || r.key === only) && (redo || r.todo > 0));
log(`books ${keys.length}, with a local original ${keys.filter((k) => locals.has(k.key)).length}`);

let next = 0;
let done = 0;
async function worker() {
  while (next < keys.length) {
    const { key } = keys[next++];
    const pdf = locals.get(key);
    if (!pdf) { log(key, "no local original"); continue; }
    try {
      const rows = (await retry(() => db.execute({ sql: `SELECT page_number, boxes FROM ocr_line_boxes WHERE granth_key = ?${redo ? "" : " AND words IS NULL"}`, args: [key] }))).rows;
      const pages = Object.fromEntries(rows.map((r) => [String(r.page_number), JSON.parse(String(r.boxes))]));
      const result = await segment(pdf, pages);
      const statements = Object.entries(result).map(([page, words]) => ({
        sql: "UPDATE ocr_line_boxes SET words = ? WHERE granth_key = ? AND page_number = ?",
        args: [JSON.stringify(words), key, Number(page)],
      }));
      for (let i = 0; i < statements.length; i += 200) await retry(() => db.batch(statements.slice(i, i + 200), "write"));
      done += 1;
      log(`${key} ${statements.length} pages (${done}/${keys.length})`);
    } catch (error) {
      log(key, "FAILED", error instanceof Error ? error.message : String(error));
    }
  }
}
await Promise.all(Array.from({ length: parallel }, worker));
log("finished");
