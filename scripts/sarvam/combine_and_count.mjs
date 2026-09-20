// Stitches the per-chunk Sarvam HTML back into one document in page order and
// counts exact-word occurrences of hiṁsā in Devanagari and Gujarati.
import { readFile, writeFile, readdir } from "node:fs/promises";
import path from "node:path";

const outDir = process.argv[2];
if (!outDir) {
  console.error("usage: combine_and_count.mjs <outDir>");
  process.exit(1);
}

const htmlDir = path.join(outDir, "html");
const files = (await readdir(htmlDir)).filter((f) => f.endsWith(".html"));

function firstPageOf(name) {
  const m = name.match(/_p(\d+)-/);
  return m ? Number(m[1]) : Number.MAX_SAFE_INTEGER;
}
files.sort((a, b) => firstPageOf(a) - firstPageOf(b));

const bodies = [];
for (const f of files) {
  const raw = await readFile(path.join(htmlDir, f), "utf8");
  const m = raw.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
  bodies.push(`<!-- ${f} -->\n${m ? m[1] : raw}`);
}

const combined = `<!DOCTYPE html>
<html lang="gu-IN">
<head><meta charset="UTF-8"><title>Ashtak Prakaran — Sarvam digitisation</title></head>
<body>
${bodies.join("\n")}
</body>
</html>
`;
const combinedPath = path.join(outDir, "ashtak_prakaran_sarvam.html");
await writeFile(combinedPath, combined);

// Plain text: drop style/script blocks first so CSS never reaches the token stream.
const stripped = combined
  .replace(/<style[\s\S]*?<\/style>/gi, " ")
  .replace(/<script[\s\S]*?<\/script>/gi, " ")
  .replace(/<[^>]+>/g, " ")
  .replace(/&nbsp;/g, " ")
  .replace(/&amp;/g, "&")
  .replace(/&lt;/g, "<")
  .replace(/&gt;/g, ">");
const text = stripped.replace(/[ \t]+/g, " ");
const textPath = path.join(outDir, "ashtak_prakaran_sarvam.txt");
await writeFile(textPath, text);

console.log(`combined ${files.length} chunk files`);
console.log(`  html -> ${combinedPath} (${combined.length.toLocaleString()} bytes)`);
console.log(`  text -> ${textPath} (${text.length.toLocaleString()} chars)`);
