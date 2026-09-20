// Post-migration check: does the suffix index now find every real "ends with"
// match, and do the two encodings of the same glyph collapse together?
import { createClient } from "@libsql/client";

const t = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const WORD = /[\p{L}\p{N}\p{M}_]/u;
const isB = (c) => !c || !WORD.test(c);
const esc = (s) => `"${s.replace(/"/g, '""')}"`;
const revCp = (s) => Array.from(s).reverse().join("");

function endsHits(s, n) {
  const re = new RegExp(n, "gu");
  let m, k = 0;
  while ((m = re.exec(s)) !== null) if (isB(s[m.index + m[0].length])) k += 1;
  return k;
}

const TERMS = {
  "हिंसा": "हिंसा",
  "હિંસા": "હિંસા",
  "धर्म": "धर्म",
  "ज्ञान": "ज्ञान",
  "कर्म": "कर्म",
};

console.log("ENDS_WITH after the rebuild\n");
console.log("term".padEnd(10) + "ftsFound".padStart(10) + "truePages".padStart(11) + "MISSED".padStart(8));
for (const [label, needle] of Object.entries(TERMS)) {
  const found = await t.execute({
    sql: `SELECT p.id FROM ocr_pages_suffix_fts
          JOIN ocr_pages p ON p.id = ocr_pages_suffix_fts.rowid
          WHERE ocr_pages_suffix_fts MATCH ?`,
    args: [`${esc(revCp(needle.normalize("NFC")))}*`],
  });
  const inFts = new Set(found.rows.map((r) => Number(r.id)));
  const cand = await t.execute({
    sql: "SELECT id, content FROM ocr_pages WHERE content LIKE ?",
    args: [`%${needle}%`],
  });
  let truePages = 0, missed = 0;
  for (const row of cand.rows) {
    if (endsHits(String(row.content).normalize("NFC"), needle) > 0) {
      truePages += 1;
      if (!inFts.has(Number(row.id))) missed += 1;
    }
  }
  console.log(label.padEnd(10) + String(inFts.size).padStart(10) + String(truePages).padStart(11) + String(missed).padStart(8));
}

console.log("\nNFC: the two byte orders of the same glyph");
const a = "ज़्"; // ja + virama + nukta
const b = a.normalize("NFC");   // ja + nukta + virama
for (const [lbl, q] of [["virama-then-nukta", a], ["NFC (nukta-then-virama)", b]]) {
  const r = await t.execute({
    sql: "SELECT COUNT(*) n FROM ocr_pages_trigram_fts WHERE ocr_pages_trigram_fts MATCH ?",
    args: [esc(q)],
  });
  console.log(`  ${lbl.padEnd(26)} -> ${r.rows[0].n} pages`);
}
console.log("  (after normalising stored content these should agree)");
