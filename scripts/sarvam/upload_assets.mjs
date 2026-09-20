// Uploads the rebuilt searchable PDF and the page-wise CSV to UploadThing.
// Nothing is deleted here: old files stay live until every database reference
// has been repointed and verified.
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { UTApi } from "uploadthing/server";

const files = process.argv.slice(2);
if (!files.length) {
  console.error("usage: upload_assets.mjs <file> [file...]");
  process.exit(1);
}

const api = new UTApi({ token: process.env.UPLOADTHING_TOKEN });
const results = [];

for (const filePath of files) {
  const bytes = await readFile(filePath);
  const name = path.basename(filePath);
  const type = name.endsWith(".pdf") ? "application/pdf" : "text/csv";
  const size = (await stat(filePath)).size;
  console.log(`uploading ${name} (${(size / 1024 / 1024).toFixed(2)} MB)...`);

  const res = await api.uploadFiles(new File([bytes], name, { type }));
  if (res.error) {
    console.error(`  FAILED: ${JSON.stringify(res.error)}`);
    process.exitCode = 1;
    continue;
  }
  const d = res.data;
  console.log(`  key: ${d.key}`);
  console.log(`  url: ${d.ufsUrl ?? d.url}`);
  results.push({ file: name, key: d.key, url: d.ufsUrl ?? d.url, size: d.size });
}

console.log("\nJSON:");
console.log(JSON.stringify(results, null, 2));
