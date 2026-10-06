// Prepares the vyutpatti box-table PDF for each vishay in a list, with the same
// engine and rules as the /vyutpatti page (Claude, kosh priority, scans).
// The list is read from a file given at run time and is not stored anywhere
// (Sahebji's rule); only the PDFs are written, one per vishay.
//
//   npx tsx --env-file=.env scripts/vyutpatti/batch_vyutpatti.mts <list.txt> <outDir> [--json=<dir>] [--max-usd=4]
//       [--single=<file.pdf>]   also (or, with --no-each, only) one PDF with every vishay, one after another
//       [--full-width]          tables across the whole page (no notes margin on the right)
//       [--no-each]             no per-vishay PDFs
//
// list.txt: one vishay per line in Devanagari, optionally "number<TAB>vishay<TAB>box".
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { buildVyutpattiPdf } from "@/lib/vyutpatti/pdf";
import { buildVyutpatti } from "@/lib/vyutpatti/pipeline";

const args = process.argv.slice(2);
const [listPath, outDir] = args.filter((a) => !a.startsWith("--"));
const opt = (n: string) => args.find((a) => a.startsWith(`--${n}=`))?.split("=").slice(1).join("=");
const jsonDir = opt("json");
const maxUsd = Number(opt("max-usd") ?? 4);
const single = opt("single");
const fullWidth = args.includes("--full-width");
const each = !args.includes("--no-each");
const sections: Parameters<typeof buildVyutpattiPdf>[0] = [];
if (!listPath || !outDir) {
  console.error("usage: batch_vyutpatti.mts <list.txt> <outDir> [--json=<dir>] [--max-usd=4]");
  process.exit(1);
}
mkdirSync(outDir, { recursive: true });
if (jsonDir) mkdirSync(jsonDir, { recursive: true });

const items = readFileSync(listPath, "utf8").split(/\r?\n/).map((l) => l.trim()).filter(Boolean).map((l) => {
  const f = l.split("\t");
  return f.length >= 2 ? { number: f[0].trim(), vishay: f[1].trim(), box: (f[2] ?? "").trim() } : { number: "", vishay: f[0], box: "" };
});

const safe = (s: string) => s.replace(/[\\/:*?"<>|]/g, "_").slice(0, 120);
let total = 0;
for (const [i, item] of items.entries()) {
  if (total >= maxUsd) { console.log(`stopping: spent $${total.toFixed(2)} (limit $${maxUsd})`); break; }
  const t = Date.now();
  try {
    const r = await buildVyutpatti(item, "claude", (m) => process.stdout.write(`  ${m}\r`));
    total += r.costUsd;
    const section = { number: r.number, vishay: r.vishay, box: r.box, rows: r.rows, lines: r.lines };
    sections.push(section);
    if (each) {
      const name = `${String(i + 1).padStart(2, "0")} ${safe(r.vishay)}.pdf`;
      writeFileSync(path.join(outDir, name), await buildVyutpattiPdf([section], { fullWidth }));
    }
    if (jsonDir) writeFileSync(path.join(jsonDir, `${String(i + 1).padStart(2, "0")}.json`), JSON.stringify(r, null, 1));
    const ai = r.words.filter((w) => w.ai).map((w) => w.word);
    console.log(`${i + 1}/${items.length} ${r.vishay}: ${r.rows.length} rows, parts ${r.parts.map((p) => (p.prefix ? `${p.word}-` : p.skip ? `(${p.word})` : p.word)).join(" + ")}` +
      `${ai.length ? `, AI: ${ai.join(", ")}` : ""}, $${r.costUsd.toFixed(3)}, ${((Date.now() - t) / 1000).toFixed(0)}s`);
    for (const n of r.notes) console.log(`    note: ${n}`);
  } catch (e) {
    console.log(`${i + 1}/${items.length} ${item.vishay}: FAILED ${e instanceof Error ? e.message : e}`);
  }
}
if (single && sections.length) {
  writeFileSync(single, await buildVyutpattiPdf(sections, { fullWidth }));
  console.log(`single PDF: ${single} (${sections.length} vishays)`);
}
console.log(`total $${total.toFixed(3)}`);
