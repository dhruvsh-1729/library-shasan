// End-to-end: strip the old OCR text layer from the scanned PDF, then lay in
// Sarvam's text positioned by its per-block bounding boxes.
import { readdir, readFile, stat, unlink } from "node:fs/promises";
import path from "node:path";
import { stripTextLayer } from "./strip_text_layer.mjs";
import { addTextLayer } from "./add_text_layer.mjs";

const [, , srcPdf, zipsDir, outPdf] = process.argv;
if (!srcPdf || !zipsDir || !outPdf) {
  console.error("usage: build_searchable_pdf.mjs <src.pdf> <zipsDir> <out.pdf>");
  process.exit(1);
}

/** zips/<chunk>/<chunk>/metadata/page_NNN.json -> absolute book page number. */
async function loadMetadata(dir) {
  const byPage = new Map();
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const m = entry.name.match(/_p(\d+)-(\d+)/);
    if (!m) continue;
    const firstPage = Number(m[1]);
    const metaDir = path.join(dir, entry.name, entry.name, "metadata");
    let files;
    try {
      files = (await readdir(metaDir)).filter((f) => /^page_\d+\.json$/.test(f));
    } catch {
      continue;
    }
    for (const f of files.sort()) {
      const idx = Number(f.match(/page_(\d+)\.json/)[1]); // 1-based within chunk
      const meta = JSON.parse(await readFile(path.join(metaDir, f), "utf8"));
      byPage.set(firstPage + idx - 1, meta);
    }
  }
  return byPage;
}

const metaByPage = await loadMetadata(zipsDir);
console.log(`metadata pages loaded: ${metaByPage.size}`);

const tmp = `${outPdf}.stripped.tmp.pdf`;
console.log("stripping old text layer...");
const s = await stripTextLayer(srcPdf, tmp);
console.log(`  pages=${s.pages} touched=${s.touched} textBlocksRemoved=${s.removedBlocks} -> ${(s.bytes / 1024 / 1024).toFixed(1)} MB`);

console.log("adding Sarvam text layer...");
let done = 0;
const a = await addTextLayer({
  srcPdf: tmp,
  metaByPage,
  outPdf,
  onPage: () => {
    done += 1;
    if (done % 50 === 0) console.log(`  ${done} pages`);
  },
});
console.log(`  pages=${a.pages} blocksDrawn=${a.blocksDrawn} skipped=${a.blocksSkipped} -> ${(a.bytes / 1024 / 1024).toFixed(1)} MB`);
await unlink(tmp);
console.log(`\nwrote ${outPdf} (${((await stat(outPdf)).size / 1024 / 1024).toFixed(1)} MB)`);
