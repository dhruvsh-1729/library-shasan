// Splits a PDF into fixed-size chunks. Sarvam's digitise endpoint rejects
// anything over 10 pages per job, so the whole book has to go up in batches.
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { PDFDocument } from "pdf-lib";

const [, , srcPath, outDir, sizeArg] = process.argv;
if (!srcPath || !outDir) {
  console.error("usage: split_pdf.mjs <src.pdf> <outDir> [chunkSize=10]");
  process.exit(1);
}
const chunkSize = Number(sizeArg ?? 10);

const src = await PDFDocument.load(await readFile(srcPath), { ignoreEncryption: true });
const total = src.getPageCount();
await mkdir(outDir, { recursive: true });

const chunks = [];
for (let start = 0; start < total; start += chunkSize) {
  const end = Math.min(start + chunkSize, total);
  const out = await PDFDocument.create();
  const pages = await out.copyPages(src, Array.from({ length: end - start }, (_, i) => start + i));
  pages.forEach((p) => out.addPage(p));

  const name = `chunk_${String(chunks.length).padStart(3, "0")}_p${start + 1}-${end}.pdf`;
  await writeFile(path.join(outDir, name), await out.save());
  chunks.push({ index: chunks.length, name, firstPage: start + 1, lastPage: end, pageCount: end - start });
}

await writeFile(path.join(outDir, "chunks.json"), JSON.stringify({ srcPath, total, chunkSize, chunks }, null, 2));
console.log(`${total} pages -> ${chunks.length} chunks of <=${chunkSize} in ${outDir}`);
