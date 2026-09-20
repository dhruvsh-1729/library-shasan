// Scores the stored OCR text of every granth so the ones worth re-running
// through Sarvam can be picked out without eyeballing 482 books.
//
// Every signal here is an error the text itself reveals, not a fingerprint of
// which pipeline produced it — otherwise this would just re-flag everything
// that is not already Sarvam and tell us nothing.
import { writeFile } from "node:fs/promises";
import { createClient } from "@libsql/client";

const DEVANAGARI = /[ऀ-ॿ]/u;
const GUJARATI = /[઀-૿]/u;
const INDIC_TOKEN = /[ऀ-ॿ઀-૿]+/gu;

export const THRESHOLDS = {
  redo: 55,
  review: 75,
};

/**
 * Penalties are capped individually so one pathological signal cannot alone
 * sink a book that is otherwise sound, and so the score stays readable.
 */
export function scoreGranth(pages) {
  const texts = pages.map((p) => String(p.content ?? ""));
  const joined = texts.join("\n");
  const tokens = joined.match(INDIC_TOKEN) ?? [];
  const tokenCount = Math.max(1, tokens.length);
  const letters = Math.max(1, (joined.match(/\p{L}/gu) ?? []).length);

  // A single word holding both Devanagari and Gujarati letters is script
  // confusion — the reader sees छे where છે belongs.
  const mixedScript = tokens.filter((t) => DEVANAGARI.test(t) && GUJARATI.test(t)).length;
  // A digit wedged between two Indic letters is never real text; it is an
  // avagraha or a consonant misread as a numeral.
  const digitInWord = (joined.match(/[ऀ-ॿ઀-૿][0-9][ऀ-ॿ઀-૿]/gu) ?? []).length;
  const latin = (joined.match(/[A-Za-z]/g) ?? []).length;
  const charRuns = (joined.match(/([^\s\d])\1\1+/gu) ?? []).length;
  const blankPages = texts.filter((t) => t.trim().length < 40).length;
  const replacement = (joined.match(/[�-]/gu) ?? []).length;

  const m = {
    pages: pages.length,
    tokens: tokens.length,
    charsPerPage: Math.round(joined.length / Math.max(1, pages.length)),
    mixedScriptPer1k: (1000 * mixedScript) / tokenCount,
    digitInWordPer1k: (1000 * digitInWord) / tokenCount,
    latinRatio: latin / letters,
    charRunsPer1k: (1000 * charRuns) / tokenCount,
    blankPageRatio: blankPages / Math.max(1, pages.length),
    replacementPer10k: (10000 * replacement) / Math.max(1, joined.length),
  };

  const penalties = {
    mixedScript: Math.min(30, m.mixedScriptPer1k * 2.0),
    digitInWord: Math.min(25, m.digitInWordPer1k * 2.5),
    latin: Math.min(15, m.latinRatio * 800),
    charRuns: Math.min(10, m.charRunsPer1k * 1.5),
    blankPages: Math.min(20, m.blankPageRatio * 180),
    replacement: Math.min(10, m.replacementPer10k * 5),
  };
  const deduction = Object.values(penalties).reduce((a, b) => a + b, 0);
  const score = Math.max(0, Math.round((100 - deduction) * 10) / 10);

  const verdict = score < THRESHOLDS.redo ? "REDO" : score < THRESHOLDS.review ? "REVIEW" : "OK";

  const reasons = [];
  if (penalties.mixedScript > 5) reasons.push(`script confusion (${m.mixedScriptPer1k.toFixed(1)}/1k tokens)`);
  if (penalties.digitInWord > 5) reasons.push(`digits inside words (${m.digitInWordPer1k.toFixed(1)}/1k)`);
  if (penalties.latin > 5) reasons.push(`stray Latin (${(100 * m.latinRatio).toFixed(2)}% of letters)`);
  if (penalties.blankPages > 5) reasons.push(`${(100 * m.blankPageRatio).toFixed(1)}% near-empty pages`);
  if (penalties.charRuns > 3) reasons.push(`repeated-character noise (${m.charRunsPer1k.toFixed(1)}/1k)`);
  if (penalties.replacement > 3) reasons.push("replacement/private-use characters");

  return { metrics: m, penalties, score, verdict, reasons };
}

async function main() {
  const args = process.argv.slice(2);
  const only = args.find((a) => a.startsWith("--granth="))?.split("=")[1];
  const limit = Number(args.find((a) => a.startsWith("--limit="))?.split("=")[1] ?? 0);
  const out = args.find((a) => a.startsWith("--out="))?.split("=")[1];

  const client = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });

  let sql = `SELECT granth_key, granth_name, source_rel_path, page_count, xlsx_url
             FROM ocr_granths`;
  const params = [];
  if (only) { sql += " WHERE granth_key = ?"; params.push(only); }
  sql += " ORDER BY granth_key";
  if (limit) sql += ` LIMIT ${limit}`;

  const granths = (await client.execute({ sql, args: params })).rows;
  const report = [];

  for (let i = 0; i < granths.length; i += 1) {
    const g = granths[i];
    const key = String(g.granth_key);
    const pages = await client.execute({
      sql: "SELECT content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
      args: [key],
    });

    if (pages.rows.length === 0) {
      report.push({
        granth_key: key, granth_name: String(g.granth_name ?? ""),
        source_rel_path: String(g.source_rel_path ?? ""),
        score: 0, verdict: "REDO", reasons: ["no OCR text stored"],
        metrics: { pages: 0 },
      });
    } else {
      const res = scoreGranth(pages.rows.map((r) => ({ content: r.content })));
      const declared = Number(g.page_count ?? 0);
      if (declared && res.metrics.pages < declared) {
        res.reasons.push(`only ${res.metrics.pages} of ${declared} pages stored`);
        res.score = Math.max(0, res.score - 15);
        if (res.score < THRESHOLDS.redo) res.verdict = "REDO";
      }
      report.push({
        granth_key: key, granth_name: String(g.granth_name ?? ""),
        source_rel_path: String(g.source_rel_path ?? ""),
        ...res,
      });
    }
    if ((i + 1) % 25 === 0) process.stdout.write(`  scored ${i + 1}/${granths.length}\r`);
  }

  report.sort((a, b) => a.score - b.score);
  const counts = report.reduce((acc, r) => ({ ...acc, [r.verdict]: (acc[r.verdict] ?? 0) + 1 }), {});
  const pagesToRedo = report.filter((r) => r.verdict === "REDO").reduce((n, r) => n + (r.metrics.pages || 0), 0);

  console.log(`\nscored ${report.length} granths`);
  console.log(`  OK ${counts.OK ?? 0}   REVIEW ${counts.REVIEW ?? 0}   REDO ${counts.REDO ?? 0}`);
  console.log(`  pages in REDO granths: ${pagesToRedo.toLocaleString()}  (~₹${(pagesToRedo * 0.5).toLocaleString()} to re-OCR)`);
  console.log(`\nworst 15:`);
  console.log("  " + "granth".padEnd(8) + "score".padStart(7) + "  verdict  reasons");
  for (const r of report.slice(0, 15)) {
    console.log(`  ${r.granth_key.padEnd(8)}${String(r.score).padStart(7)}  ${r.verdict.padEnd(7)}  ${r.reasons.slice(0, 2).join("; ")}`);
  }

  if (out) {
    await writeFile(out, JSON.stringify({ generatedAt: new Date().toISOString(), thresholds: THRESHOLDS, counts, report }, null, 2));
    console.log(`\nreport -> ${out}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await main();
