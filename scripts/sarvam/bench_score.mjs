// Scores the OCR engines against each other on the sampled pages.
//
// WHAT THIS CAN AND CANNOT TELL YOU
//
// There is no hand-transcribed ground truth for these pages, so none of these
// numbers is "accuracy". What they are:
//
//  * pairwise agreement — raw, assumption-free: how often two engines produced
//    the same word. High agreement between two engines says they read the page
//    the same way, not that either is right.
//
//  * consensus agreement — where 2 of 3 engines produce the same token, that
//    token is treated as probably correct, and each engine is scored against
//    it. This is a real technique but it has a real failure mode: if two
//    engines share a weakness they can outvote the one that is right, so an
//    engine that is better than the others is PENALISED by this measure. Read
//    it as a floor, not a verdict.
//
//  * script and garbage profiles — assumption-free and diagnostic. A Gujarati/
//    Sanskrit book should contain almost no Latin letters and no replacement
//    characters; an engine emitting them is making things up or failing.
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const arg = (n, d) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1] ?? d;
const benchDir = arg("bench", "/tmp/bench");
const outPath = arg("out", path.join(benchDir, "scores.json"));

const manifest = JSON.parse(await readFile(path.join(benchDir, "manifest.json"), "utf8"));
const google = JSON.parse(await readFile(path.join(benchDir, "google.json"), "utf8"));
const tess = JSON.parse(await readFile(path.join(benchDir, "tesseract.json"), "utf8"));

const TOKEN = /[ऀ-ॿ઀-૿]{2,}/gu;
const norm = (s) => String(s ?? "").normalize("NFC");
const toks = (s) => norm(s).match(TOKEN) ?? [];
const tokSet = (s) => new Set(toks(s));

function profile(text) {
  const t = norm(text);
  const letters = Math.max(1, (t.match(/\p{L}/gu) ?? []).length);
  return {
    chars: t.length,
    tokens: toks(t).length,
    latinRatio: (t.match(/[A-Za-z]/g) ?? []).length / letters,
    devanagari: (t.match(/[ऀ-ॿ]/gu) ?? []).length,
    gujarati: (t.match(/[઀-૿]/gu) ?? []).length,
    replacement: (t.match(/[�-]/gu) ?? []).length,
    digitInWord: (t.match(/[ऀ-ॿ઀-૿][0-9][ऀ-ॿ઀-૿]/gu) ?? []).length,
    mixedScriptTokens: toks(t).filter((x) => /[ऀ-ॿ]/u.test(x) && /[઀-૿]/u.test(x)).length,
  };
}

/** |A ∩ B| / |A| — share of A's words that B also produced. */
function overlap(a, b) {
  const A = tokSet(a), B = tokSet(b);
  if (!A.size) return null;
  let shared = 0;
  for (const t of A) if (B.has(t)) shared += 1;
  return shared / A.size;
}

const ENGINES = ["stored", "google", "tesseract"];
const rows = [];

for (const item of manifest.items) {
  const id = `${item.granthKey}_p${item.page}`;
  const texts = {
    stored: item.storedText,
    google: google[id]?.text ?? null,
    tesseract: tess[id]?.text ?? null,
  };
  if (ENGINES.some((e) => texts[e] == null || toks(texts[e]).length < 20)) continue;

  // consensus: a token seen by >= 2 engines
  const counts = new Map();
  for (const e of ENGINES) for (const t of tokSet(texts[e])) counts.set(t, (counts.get(t) ?? 0) + 1);
  const consensus = new Set([...counts].filter(([, n]) => n >= 2).map(([t]) => t));

  const row = { id, granthKey: item.granthKey, page: item.page, sarvamRedone: item.sarvamRedone, consensusSize: consensus.size };
  for (const e of ENGINES) {
    const set = tokSet(texts[e]);
    let hit = 0;
    for (const t of consensus) if (set.has(t)) hit += 1;
    let unique = 0;
    for (const t of set) if ((counts.get(t) ?? 0) === 1) unique += 1;
    row[e] = {
      ...profile(texts[e]),
      consensusRecall: consensus.size ? hit / consensus.size : null,   // of agreed words, how many did it find
      uniqueRatio: set.size ? unique / set.size : null,                // words only it saw
    };
  }
  for (const a of ENGINES) for (const b of ENGINES) if (a !== b) row[`${a}->${b}`] = overlap(texts[a], texts[b]);
  rows.push(row);
}

function mean(list, pick) {
  const v = list.map(pick).filter((x) => x != null && Number.isFinite(x));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}
const pct = (x) => (x == null ? "  n/a" : `${(100 * x).toFixed(1)}%`);

function report(label, list) {
  if (!list.length) return;
  console.log(`\n=== ${label} (${list.length} pages) ===`);
  console.log("  engine      consensus  unique   latin   mixed   digits  repl   tokens/pg");
  for (const e of ENGINES) {
    console.log(
      `  ${e.padEnd(11)}` +
      `${pct(mean(list, (r) => r[e].consensusRecall)).padStart(9)}` +
      `${pct(mean(list, (r) => r[e].uniqueRatio)).padStart(8)}` +
      `${pct(mean(list, (r) => r[e].latinRatio)).padStart(8)}` +
      `${mean(list, (r) => r[e].mixedScriptTokens).toFixed(1).padStart(8)}` +
      `${mean(list, (r) => r[e].digitInWord).toFixed(1).padStart(8)}` +
      `${mean(list, (r) => r[e].replacement).toFixed(1).padStart(7)}` +
      `${mean(list, (r) => r[e].tokens).toFixed(0).padStart(11)}`
    );
  }
  console.log("\n  pairwise overlap (share of row engine's words the column engine also saw)");
  console.log("             " + ENGINES.map((e) => e.padStart(11)).join(""));
  for (const a of ENGINES) {
    console.log("  " + a.padEnd(11) + ENGINES.map((b) => (a === b ? "     —" : pct(mean(list, (r) => r[`${a}->${b}`]))).padStart(11)).join(""));
  }
}

report("ALL SAMPLED PAGES", rows);
report("pages whose stored text is SARVAM", rows.filter((r) => r.sarvamRedone));
report("pages whose stored text is the OLD pipeline", rows.filter((r) => !r.sarvamRedone));

await writeFile(outPath, JSON.stringify({ generatedAt: new Date().toISOString(), pages: rows.length, rows }, null, 2));
console.log(`\nscored ${rows.length} pages -> ${outPath}`);
