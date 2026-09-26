#!/usr/bin/env node
// Builds / refreshes the folded Sanskrit search index (lib/ocr-folded-index.mjs)
// for every ocr_pages row that has no entry, a stale entry, or one folded by an
// older SANSKRIT_FOLD_VERSION. Safe to re-run; it resumes where it stopped.
//
//   node scripts/ensure_folded_search_index.mjs            # whole library
//   node scripts/ensure_folded_search_index.mjs 375 380    # only these granth keys
//   --batchSize N (default 100), --workers N (default 4, whole-library runs only)
import "dotenv/config";
import { createClient } from "@libsql/client";
import { syncFoldedIndex } from "../lib/ocr-folded-index.mjs";

const args = process.argv.slice(2);
let batchSize = 100;
let workers = 4;
const granthKeys = [];
for (let i = 0; i < args.length; i += 1) {
  if (args[i] === "--batchSize") batchSize = Math.max(1, Number.parseInt(args[++i], 10) || 100);
  else if (args[i] === "--workers") workers = Math.max(1, Number.parseInt(args[++i], 10) || 4);
  else granthKeys.push(args[i]);
}

const client = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const total = Number((await client.execute("SELECT COUNT(*) n FROM ocr_pages")).rows[0].n);
const started = Date.now();
const progress = (label) => (done, lastId) => {
  const secs = Math.round((Date.now() - started) / 1000);
  process.stdout.write(`\r[${label}] folded ${done.toLocaleString()} page(s), last id ${lastId}, ${secs}s   `);
};

let folded = 0;
if (granthKeys.length) {
  for (const key of granthKeys) folded += await syncFoldedIndex(client, { granthKey: key, batchSize, onProgress: progress(key) });
} else {
  // Workers take disjoint id ranges; one final pass (with the orphan sweep)
  // catches anything written meanwhile.
  const bounds = (await client.execute("SELECT MIN(id) lo, MAX(id) hi FROM ocr_pages")).rows[0];
  const lo = Number(bounds.lo);
  const hi = Number(bounds.hi);
  const span = Math.ceil((hi - lo + 1) / workers);
  const doneBy = new Array(workers).fill(0);
  const results = await Promise.all(
    Array.from({ length: workers }, (_, w) =>
      syncFoldedIndex(client, {
        batchSize,
        minId: lo + w * span,
        maxId: Math.min(hi, lo + (w + 1) * span - 1),
        skipOrphans: true,
        onProgress: (done) => {
          doneBy[w] = done;
          progress(`all ${total.toLocaleString()}`)(doneBy.reduce((a, b) => a + b, 0), `w${w}`);
        },
      })
    )
  );
  folded = results.reduce((a, b) => a + b, 0);
  folded += await syncFoldedIndex(client, { batchSize, onProgress: progress("final") });
}
console.log(`\nfolded ${folded.toLocaleString()} page(s) in ${Math.round((Date.now() - started) / 1000)}s`);
