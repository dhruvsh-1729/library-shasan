// Reads the same sampled images with Tesseract, as a third independent engine.
import { readFile, writeFile, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(execFile);
const arg = (n, d) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1] ?? d;
const benchDir = arg("bench", "/tmp/bench");
const concurrency = Number(arg("concurrency", 4));
const outPath = path.join(benchDir, "tesseract.json");

const manifest = JSON.parse(await readFile(path.join(benchDir, "manifest.json"), "utf8"));
const results = existsSync(outPath) ? JSON.parse(await readFile(outPath, "utf8")) : {};
let n = 0;

const queue = [...manifest.items];
async function worker() {
  for (;;) {
    const item = queue.shift();
    if (!item) return;
    const id = `${item.granthKey}_p${item.page}`;
    if (results[id]?.text != null) { n += 1; continue; }
    const base = item.image.replace(/\.png$/, "_tess");
    try {
      await run("tesseract", [item.image, base, "-l", "guj+hin", "--psm", "3"], { maxBuffer: 32 << 20 });
      results[id] = { text: await readFile(`${base}.txt`, "utf8") };
      await rm(`${base}.txt`, { force: true });
    } catch (e) {
      results[id] = { text: null, error: e.message.slice(0, 120) };
    }
    n += 1;
    if (n % 20 === 0) { await writeFile(outPath, JSON.stringify(results, null, 2)); process.stdout.write(`  ${n}/${manifest.items.length}\r`); }
  }
}
await Promise.all(Array.from({ length: concurrency }, worker));
await writeFile(outPath, JSON.stringify(results, null, 2));
console.log(`\nTesseract: ${Object.values(results).filter((r) => r.text != null).length} pages read -> ${outPath}`);
