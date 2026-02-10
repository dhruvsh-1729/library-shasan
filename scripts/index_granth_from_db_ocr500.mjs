#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { once } from "node:events";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { spawn } from "node:child_process";
import { createClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";

const SOURCE_TABLE = "granth_ocr_files";
const DOCS_TABLE = "documents";
const PAGES_TABLE = "document_pages";
const PAGE_CHUNK_SIZE = 200;
const SOURCE_PAGE_SIZE = 1000;
const TMP_DIR = path.join(process.cwd(), ".tmp_ocr500");

const DEFAULT_DPI = 500;
const DEFAULT_LANGS = "guj+hin+eng";
const PSM_PRIMARY = 6;
const PSM_FALLBACK = 3;

function usage() {
  console.log(`Usage: node scripts/index_granth_from_db_ocr500.mjs [options]

Options:
  --limit N         Process at most N files (default: unlimited)
  --startAt N       Skip first N after filtering (default: 0)
  --concurrency N   Files processed in parallel (default: 1)
  --pageConcurrency N  Pages OCRed in parallel per file (default: 1)
  --collection X    Only process one collection value
  --reprocess       Re-process already processed docs
  --dpi N           OCR render DPI (default: 500)
  --langs X         Tesseract langs, e.g. guj+hin+eng
  --tessThreads N   Threads per tesseract process via OMP_THREAD_LIMIT (default: 1)
  --maxPages N      Optional debug cap per PDF (default: all pages)
  --dry-run         Show queue only, no writes
  --verbose         Extra logs
  --help            Show help
`);
}

function reqEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function parseIntFlag(name, raw, min) {
  if (raw == null || raw === "") throw new Error(`${name} requires a numeric value`);
  const n = Number.parseInt(String(raw), 10);
  if (!Number.isFinite(n) || Number.isNaN(n) || n < min) {
    throw new Error(`${name} must be integer >= ${min}`);
  }
  return n;
}

function parseArgs(argv) {
  const args = {
    limit: null,
    startAt: 0,
    concurrency: 1,
    pageConcurrency: Number.parseInt(process.env.OCR_PAGE_CONCURRENCY || "1", 10) || 1,
    collection: null,
    reprocess: false,
    dpi: DEFAULT_DPI,
    langs: process.env.OCR_LANGS || DEFAULT_LANGS,
    tessThreads: Number.parseInt(process.env.OCR_TESSERACT_THREADS || "1", 10) || 1,
    maxPages: null,
    dryRun: false,
    verbose: false,
    help: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--help" || arg === "-h") {
      args.help = true;
      continue;
    }
    if (arg === "--reprocess") {
      args.reprocess = true;
      continue;
    }
    if (arg === "--dry-run") {
      args.dryRun = true;
      continue;
    }
    if (arg === "--verbose") {
      args.verbose = true;
      continue;
    }
    if (arg === "--limit" || arg.startsWith("--limit=")) {
      const raw = arg.includes("=") ? arg.slice("--limit=".length) : argv[++i];
      args.limit = parseIntFlag("--limit", raw, 0);
      continue;
    }
    if (arg === "--startAt" || arg.startsWith("--startAt=")) {
      const raw = arg.includes("=") ? arg.slice("--startAt=".length) : argv[++i];
      args.startAt = parseIntFlag("--startAt", raw, 0);
      continue;
    }
    if (arg === "--concurrency" || arg.startsWith("--concurrency=")) {
      const raw = arg.includes("=") ? arg.slice("--concurrency=".length) : argv[++i];
      args.concurrency = parseIntFlag("--concurrency", raw, 1);
      continue;
    }
    if (arg === "--pageConcurrency" || arg.startsWith("--pageConcurrency=")) {
      const raw = arg.includes("=") ? arg.slice("--pageConcurrency=".length) : argv[++i];
      args.pageConcurrency = parseIntFlag("--pageConcurrency", raw, 1);
      continue;
    }
    if (arg === "--collection" || arg.startsWith("--collection=")) {
      args.collection = arg.includes("=") ? arg.slice("--collection=".length) : argv[++i];
      continue;
    }
    if (arg === "--dpi" || arg.startsWith("--dpi=")) {
      const raw = arg.includes("=") ? arg.slice("--dpi=".length) : argv[++i];
      args.dpi = parseIntFlag("--dpi", raw, 72);
      continue;
    }
    if (arg === "--langs" || arg.startsWith("--langs=")) {
      args.langs = arg.includes("=") ? arg.slice("--langs=".length) : argv[++i];
      continue;
    }
    if (arg === "--tessThreads" || arg.startsWith("--tessThreads=")) {
      const raw = arg.includes("=") ? arg.slice("--tessThreads=".length) : argv[++i];
      args.tessThreads = parseIntFlag("--tessThreads", raw, 1);
      continue;
    }
    if (arg === "--maxPages" || arg.startsWith("--maxPages=")) {
      const raw = arg.includes("=") ? arg.slice("--maxPages=".length) : argv[++i];
      args.maxPages = parseIntFlag("--maxPages", raw, 1);
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function shortErr(error, max = 1800) {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  return message.length > max ? `${message.slice(0, max)}...` : message;
}

function toErrorText(value) {
  if (value instanceof Error) return value.stack || value.message;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function normalizeText(raw) {
  return String(raw ?? "")
    .replace(/\u0000/g, "")
    .replace(/\f/g, "\n")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
}

function hasMeaningfulText(text) {
  return normalizeText(text).replace(/\s+/g, "").length > 0;
}

function csvEscape(value) {
  const s = value == null ? "" : String(value);
  return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}

async function runCommand(bin, args, options = {}) {
  const env = options.env ? { ...process.env, ...options.env } : process.env;
  return new Promise((resolve, reject) => {
    const proc = spawn(bin, args, {
      cwd: process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    proc.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    proc.on("error", (error) => {
      reject(new Error(`Failed to start "${bin}": ${shortErr(error)}`));
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`"${bin}" exited ${code}: ${stderr || stdout || "(no output)"}`));
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

async function ensureToolsInstalled() {
  await runCommand("pdfinfo", ["-v"]);
  await runCommand("pdftoppm", ["-v"]);
  await runCommand("tesseract", ["--version"]);
}

let cachedTesseractLangs = null;
async function getTesseractLangs() {
  if (cachedTesseractLangs) return cachedTesseractLangs;

  const { stdout } = await runCommand("tesseract", ["--list-langs"]);
  const langs = stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.toLowerCase().startsWith("list of available languages"));

  cachedTesseractLangs = new Set(langs);
  return cachedTesseractLangs;
}

async function resolveLangSpec(spec, verbose) {
  const requested = String(spec)
    .split("+")
    .map((x) => x.trim())
    .filter(Boolean);

  if (requested.length === 0) {
    throw new Error(`Empty --langs value`);
  }

  const available = await getTesseractLangs();
  const selected = requested.filter((lang) => available.has(lang));
  const missing = requested.filter((lang) => !available.has(lang));

  if (selected.length === 0) {
    throw new Error(
      `None of requested OCR langs are installed. requested=${requested.join(
        "+"
      )}, available=${[...available].join(",")}`
    );
  }

  if (missing.length > 0 && verbose) {
    console.warn(`⚠️ Missing tesseract langs skipped: ${missing.join(", ")}`);
  }

  return selected.join("+");
}

async function fetchSourceFiles(supabase, collection) {
  const out = [];
  let offset = 0;

  while (true) {
    let query = supabase
      .from(SOURCE_TABLE)
      .select("id,ufs_url,ut_key,file_name,file_size,custom_id,original_rel_path,collection,subcollection")
      .order("id", { ascending: true })
      .range(offset, offset + SOURCE_PAGE_SIZE - 1);

    if (collection) {
      query = query.eq("collection", collection);
    }

    const { data, error } = await query;
    if (error) throw new Error(`Fetch ${SOURCE_TABLE} failed: ${error.message}`);
    if (!data || data.length === 0) break;
    out.push(...data);
    offset += SOURCE_PAGE_SIZE;
    if (data.length < SOURCE_PAGE_SIZE) break;
  }

  return out;
}

async function fetchProcessedIds(supabase) {
  const out = new Set();
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from(DOCS_TABLE)
      .select("custom_id")
      .eq("status", "processed")
      .range(offset, offset + SOURCE_PAGE_SIZE - 1);

    if (error) throw new Error(`Fetch processed ids failed: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const row of data) {
      if (row.custom_id) out.add(String(row.custom_id));
    }

    offset += SOURCE_PAGE_SIZE;
    if (data.length < SOURCE_PAGE_SIZE) break;
  }

  return out;
}

async function upsertDocPending(supabase, payload) {
  const { error } = await supabase.from(DOCS_TABLE).upsert(
    {
      ...payload,
      status: "pending",
      error: null,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "custom_id" }
  );
  if (error) throw new Error(`documents upsert pending failed: ${error.message}`);
}

async function upsertDocFailed(supabase, payload, errorMessage) {
  const { error } = await supabase.from(DOCS_TABLE).upsert(
    {
      ...payload,
      status: "failed",
      error: errorMessage,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "custom_id" }
  );

  if (error) {
    console.error(`documents upsert failed-state failed for ${payload.custom_id}: ${error.message}`);
  }
}

async function markDocProcessed(supabase, customId, csvUrl, csvKey) {
  const { error } = await supabase
    .from(DOCS_TABLE)
    .update({
      status: "processed",
      error: null,
      csv_url: csvUrl,
      csv_key: csvKey,
      updated_at: new Date().toISOString(),
    })
    .eq("custom_id", customId);

  if (error) throw new Error(`mark processed failed: ${error.message}`);
}

async function clearPages(supabase, customId) {
  const { error } = await supabase.from(PAGES_TABLE).delete().eq("custom_id", customId);
  if (error) throw new Error(`document_pages delete failed: ${error.message}`);
}

async function upsertPageChunk(supabase, chunk) {
  if (chunk.length === 0) return;
  const { error } = await supabase.from(PAGES_TABLE).upsert(chunk, {
    onConflict: "custom_id,page_number",
  });
  if (error) throw new Error(`document_pages upsert failed: ${error.message}`);
}

function makeTempFilePaths(identity) {
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const safe = String(identity).replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 100);
  const base = path.join(TMP_DIR, `${safe}_${suffix}`);
  return {
    pdfPath: `${base}.pdf`,
    csvPath: `${base}.csv`,
    imageBase: `${base}_page`,
  };
}

async function unlinkSafe(filePath) {
  try {
    await fs.unlink(filePath);
  } catch {
    // ignore
  }
}

async function downloadToFile(url, outPath) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Download failed (${response.status} ${response.statusText}): ${url}`);
  if (!response.body) throw new Error(`No response body while downloading: ${url}`);
  const ws = fsSync.createWriteStream(outPath);
  await pipeline(Readable.fromWeb(response.body), ws);
}

async function getPdfPageCount(pdfPath) {
  const { stdout } = await runCommand("pdfinfo", [pdfPath]);
  const match = stdout.match(/^\s*Pages:\s+(\d+)/m);
  if (!match) throw new Error(`Could not parse page count from pdfinfo.`);
  return Number(match[1]);
}

async function renderPageToPng(pdfPath, pageNumber, imageBase, dpi) {
  await runCommand("pdftoppm", [
    "-f",
    String(pageNumber),
    "-l",
    String(pageNumber),
    "-singlefile",
    "-r",
    String(dpi),
    "-png",
    pdfPath,
    imageBase,
  ]);
  return `${imageBase}.png`;
}

async function runTesseract(imagePath, langs, psm, tessThreads) {
  const { stdout } = await runCommand("tesseract", [
    imagePath,
    "stdout",
    "-l",
    langs,
    "--oem",
    "1",
    "--psm",
    String(psm),
  ], {
    env: {
      OMP_THREAD_LIMIT: String(tessThreads),
      OMP_NUM_THREADS: String(tessThreads),
    },
  });
  return normalizeText(stdout);
}

async function uploadCsv(utapi, csvPath, csvName, csvCustomId, appId) {
  const buffer = await fs.readFile(csvPath);
  const utFile = new UTFile([buffer], csvName, {
    type: "text/csv",
    customId: csvCustomId,
    lastModified: Date.now(),
  });

  const result = await utapi.uploadFiles([utFile]);
  const first = Array.isArray(result) ? result[0] : result;

  if (!first) throw new Error(`uploadFiles returned empty`);
  if (first.error) {
    const text = toErrorText(first.error);
    try {
      const urls = await utapi.getFileUrls(csvCustomId, { keyType: "customId" });
      const hit = Array.isArray(urls?.data) ? urls.data[0] : null;
      if (hit?.url || hit?.key) {
        return {
          csvUrl: hit.url ?? (appId ? `https://${appId}.ufs.sh/f/${csvCustomId}` : null),
          csvKey: hit.key ?? null,
        };
      }
    } catch {
      // ignore and continue to fallback/error
    }

    if (/already exists|409/i.test(text) && appId) {
      return {
        csvUrl: `https://${appId}.ufs.sh/f/${csvCustomId}`,
        csvKey: null,
      };
    }
    throw new Error(`uploadFiles error: ${shortErr(text)}`);
  }

  const data = first.data || {};
  return {
    csvUrl: data.ufsUrl ?? data.url ?? null,
    csvKey: data.key ?? null,
  };
}

async function runConcurrent(items, concurrency, worker) {
  if (items.length === 0) return;
  let next = 0;

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const idx = next;
      next += 1;
      if (idx >= items.length) return;
      await worker(items[idx], idx);
    }
  });

  await Promise.all(workers);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    usage();
    return;
  }

  const SUPABASE_URL = reqEnv("SUPABASE_URL");
  const SUPABASE_SERVICE_ROLE_KEY = reqEnv("SUPABASE_SERVICE_ROLE_KEY");
  const UPLOADTHING_TOKEN = reqEnv("UPLOADTHING_TOKEN");
  const UPLOADTHING_APP_ID = process.env.UPLOADTHING_APP_ID || null;

  await ensureToolsInstalled();
  const langSpec = await resolveLangSpec(args.langs, args.verbose);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const utapi = new UTApi({ token: UPLOADTHING_TOKEN });

  await fs.mkdir(TMP_DIR, { recursive: true });

  console.log(`📡 Fetching OCR files from ${SOURCE_TABLE}...`);
  const sourceFiles = await fetchSourceFiles(supabase, args.collection);
  const withUrl = sourceFiles.filter((f) => f.ufs_url);
  console.log(`   Found ${sourceFiles.length} files (${withUrl.length} with URL).`);

  let processedIds = new Set();
  if (!args.reprocess) {
    console.log(`📡 Fetching already processed docs...`);
    processedIds = await fetchProcessedIds(supabase);
    console.log(`   ${processedIds.size} already processed.`);
  }

  const filtered = withUrl.filter((file) => {
    if (args.reprocess) return true;
    const identity = file.custom_id || file.ut_key;
    if (!identity) return true;
    return !processedIds.has(String(identity));
  });

  const start = Math.min(args.startAt, filtered.length);
  const end = args.limit == null ? filtered.length : Math.min(filtered.length, start + args.limit);
  const batch = filtered.slice(start, end);

  console.log(`   ${withUrl.length - filtered.length} skipped, ${batch.length} to process.`);
  console.log(
    `OCR mode: image-per-page, dpi=${args.dpi}, langs=${langSpec}, psm=${PSM_PRIMARY} fallback=${PSM_FALLBACK}, tessThreads=${args.tessThreads}`
  );
  console.log(`Processing (fileConcurrency=${args.concurrency}, pageConcurrency=${args.pageConcurrency})\n`);

  if (args.dryRun) {
    console.log(`DRY RUN sample:`);
    batch.slice(0, 5).forEach((f, i) => {
      console.log(`[${i + 1}] id=${f.id} file="${f.file_name}" collection="${f.collection}/${f.subcollection}"`);
    });
    return;
  }

  let ok = 0;
  let failed = 0;

  await runConcurrent(batch, args.concurrency, async (file, idx) => {
    const customId = file.custom_id || file.ut_key || `granth_${file.id}`;
    const pdfName = file.file_name || `file_${file.id}.pdf`;
    const pdfUrl = file.ufs_url;
    const label = `[${idx + 1}/${batch.length}] ${pdfName}`;
    const temp = makeTempFilePaths(customId);

    const docPayload = {
      original_relative_path: file.original_rel_path || null,
      custom_id: customId,
      pdf_name: pdfName,
      pdf_url: pdfUrl,
      size_bytes: file.file_size || null,
      modified_time_iso: null,
    };

    /** @type {fsSync.WriteStream | null} */
    let csvWriter = null;
    let csvClosed = false;

    const writeCsvLine = async (line) => {
      if (!csvWriter) throw new Error(`CSV writer not initialized`);
      if (!csvWriter.write(line)) {
        await once(csvWriter, "drain");
      }
    };

    const closeCsv = async () => {
      if (!csvWriter || csvClosed) return;
      csvWriter.end();
      await once(csvWriter, "finish");
      csvClosed = true;
    };

    try {
      await upsertDocPending(supabase, docPayload);

      console.log(`${label} downloading PDF...`);
      await downloadToFile(pdfUrl, temp.pdfPath);
      const stat = await fs.stat(temp.pdfPath);
      console.log(`${label} downloaded bytes=${stat.size}`);

      const totalPages = await getPdfPageCount(temp.pdfPath);
      const pagesToProcess = args.maxPages == null ? totalPages : Math.min(totalPages, args.maxPages);
      console.log(`${label} pageCount=${totalPages}, processingPages=${pagesToProcess}`);

      /** @type {{ page_number: number; text: string; chars: number }[]} */
      const pageResults = [];
      const pageNumbers = Array.from({ length: pagesToProcess }, (_, i) => i + 1);

      await runConcurrent(pageNumbers, args.pageConcurrency, async (page) => {
        const t0 = Date.now();
        const imageBase = `${temp.imageBase}_${page}`;
        const imagePath = `${imageBase}.png`;
        let psmUsed = PSM_PRIMARY;

        try {
          console.log(`${label} page ${page}/${pagesToProcess} render start`);
          await renderPageToPng(temp.pdfPath, page, imageBase, args.dpi);

          console.log(`${label} page ${page}/${pagesToProcess} ocr start`);
          let text = await runTesseract(imagePath, langSpec, PSM_PRIMARY, args.tessThreads);

          if (!hasMeaningfulText(text)) {
            text = await runTesseract(imagePath, langSpec, PSM_FALLBACK, args.tessThreads);
            psmUsed = PSM_FALLBACK;
          }

          const normalized = normalizeText(text);
          const charCount = normalized.replace(/\s+/g, "").length;
          const elapsedMs = Date.now() - t0;
          pageResults.push({ page_number: page, text: normalized, chars: charCount });

          const sample =
            args.verbose && charCount > 0 ? ` sample="${normalized.slice(0, 90).replace(/\n/g, " ")}"` : "";
          console.log(
            `${label} page ${page}/${pagesToProcess} done chars=${charCount} psm=${psmUsed} elapsedMs=${elapsedMs}${sample}`
          );
        } finally {
          await unlinkSafe(imagePath);
        }
      });

      pageResults.sort((a, b) => a.page_number - b.page_number);
      const nonEmptyPages = pageResults.filter((p) => p.chars > 0);
      const emptyPages = pageResults.length - nonEmptyPages.length;

      if (nonEmptyPages.length === 0) {
        throw new Error(
          `No extractable text found after OCR. totalPages=${totalPages}, processedPages=${pagesToProcess}, emptyPages=${emptyPages}`
        );
      }

      await clearPages(supabase, customId);

      csvWriter = fsSync.createWriteStream(temp.csvPath, { encoding: "utf8" });
      await writeCsvLine("pdf_name,custom_id,pdf_url,page_number,text\n");

      let chunk = [];
      for (const page of nonEmptyPages) {
        const pageRow = {
          custom_id: customId,
          page_number: page.page_number,
          text: page.text,
        };
        chunk.push(pageRow);

        await writeCsvLine(
          [
            csvEscape(pdfName),
            csvEscape(customId),
            csvEscape(pdfUrl),
            String(page.page_number),
            csvEscape(page.text),
          ].join(",") + "\n"
        );

        if (chunk.length >= PAGE_CHUNK_SIZE) {
          await upsertPageChunk(supabase, chunk);
          chunk = [];
        }
      }

      await upsertPageChunk(supabase, chunk);
      await closeCsv();

      const csvName = `${path.basename(pdfName, path.extname(pdfName))}__pages.csv`;
      const csvCustomId = `${customId}__pages_csv_ocr500`;

      console.log(`${label} uploading CSV...`);
      const { csvUrl, csvKey } = await uploadCsv(utapi, temp.csvPath, csvName, csvCustomId, UPLOADTHING_APP_ID);

      await markDocProcessed(supabase, customId, csvUrl, csvKey);

      ok += 1;
      console.log(`✅ ${label} done storedPages=${nonEmptyPages.length}, emptyPages=${emptyPages}\n`);
    } catch (error) {
      failed += 1;
      const msg = shortErr(error);
      console.error(`❌ ${label} failed: ${msg}\n`);
      await upsertDocFailed(supabase, docPayload, msg);
    } finally {
      try {
        await closeCsv();
      } catch {
        // ignore
      }
      await unlinkSafe(temp.pdfPath);
      await unlinkSafe(temp.csvPath);
    }
  });

  console.log(`🎉 Finished: processed=${ok}, failed=${failed}, skipped=${withUrl.length - filtered.length}`);
}

main().catch((error) => {
  console.error(shortErr(error));
  process.exit(1);
});
