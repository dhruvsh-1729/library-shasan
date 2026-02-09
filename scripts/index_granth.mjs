#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { once } from "node:events";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { spawn } from "node:child_process";
import { parse } from "csv-parse/sync";
import { createClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";

const TMP_DIR = path.join(process.cwd(), ".tmp");
const MANIFEST_PATH = path.join(process.cwd(), "data", "pdf_map.csv");
const PAGE_CHUNK_SIZE = 200;
const LIST_PAGE_SIZE = 200;

/**
 * @typedef {{
 *   relative_path: string;
 *   size_bytes: number | null;
 *   modified_time_iso: string | null;
 * }} ManifestRow
 */

/**
 * @typedef {{
 *   row: ManifestRow;
 *   expectedCustomId: string;
 *   expectedOcrName: string;
 *   file: any;
 * }} MatchedItem
 */

function printUsage() {
  console.log(`Usage: node scripts/index_granth.mjs [options]

Options:
  --limit N         Process at most N matched files (default: unlimited)
  --startAt N       Skip first N matched files (default: 0)
  --concurrency N   Number of PDFs to process in parallel (default: 1)
  --verbose         Print extra debug logs
  --help            Show this help
`);
}

function parseIntegerFlag(flagName, rawValue, min) {
  if (rawValue == null || rawValue === "") {
    throw new Error(`${flagName} requires a numeric value`);
  }
  const n = Number.parseInt(String(rawValue), 10);
  if (!Number.isFinite(n) || Number.isNaN(n) || n < min) {
    throw new Error(`${flagName} must be an integer >= ${min}`);
  }
  return n;
}

function parseCliArgs(argv) {
  const args = {
    limit: null,
    startAt: 0,
    concurrency: 1,
    verbose: false,
    help: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--help" || arg === "-h") {
      args.help = true;
      continue;
    }

    if (arg === "--verbose") {
      args.verbose = true;
      continue;
    }

    if (arg === "--limit" || arg.startsWith("--limit=")) {
      const raw = arg.includes("=") ? arg.slice("--limit=".length) : argv[++i];
      args.limit = parseIntegerFlag("--limit", raw, 0);
      continue;
    }

    if (arg === "--startAt" || arg.startsWith("--startAt=")) {
      const raw = arg.includes("=") ? arg.slice("--startAt=".length) : argv[++i];
      args.startAt = parseIntegerFlag("--startAt", raw, 0);
      continue;
    }

    if (arg === "--concurrency" || arg.startsWith("--concurrency=")) {
      const raw = arg.includes("=") ? arg.slice("--concurrency=".length) : argv[++i];
      args.concurrency = parseIntegerFlag("--concurrency", raw, 1);
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function getRequiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function toIntOrNull(value) {
  if (value == null || value === "") return null;
  const n = Number.parseInt(String(value), 10);
  return Number.isFinite(n) && !Number.isNaN(n) ? n : null;
}

function normalizePathForCustomId(originalRel) {
  return originalRel.replaceAll("\\", "/").replaceAll("/", "__").replaceAll(" ", "_") + "__OCR";
}

function expectedOcrNameFromOriginal(originalRel) {
  const fileName = path.basename(originalRel);
  const base = fileName.toLowerCase().endsWith(".pdf") ? fileName.slice(0, -4) : fileName;
  return `${base}_OCR.pdf`;
}

function csvEscape(value) {
  const s = value == null ? "" : String(value);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replaceAll('"', '""')}"`;
  }
  return s;
}

function trimOrNull(value) {
  if (value == null) return null;
  const str = String(value).trim();
  return str === "" ? null : str;
}

function buildPdfUrl(appId, customId) {
  return `https://${appId}.ufs.sh/f/${customId}`;
}

function shortError(error, max = 1800) {
  const message =
    error instanceof Error
      ? error.stack || error.message
      : typeof error === "string"
        ? error
        : JSON.stringify(error);
  if (!message) return "Unknown error";
  return message.length > max ? `${message.slice(0, max)}...` : message;
}

function describeListResponseShape(response) {
  if (Array.isArray(response)) {
    return { type: "array", length: response.length };
  }
  if (!response || typeof response !== "object") {
    return { type: typeof response };
  }

  const obj = /** @type {Record<string, any>} */ (response);
  const out = {
    type: "object",
    keys: Object.keys(obj),
    filesLength: Array.isArray(obj.files) ? obj.files.length : undefined,
    hasMoreType: typeof obj.hasMore,
    cursorType: typeof obj.cursor,
    nextCursorType: typeof obj.nextCursor,
  };

  if (Array.isArray(obj.files) && obj.files.length > 0 && obj.files[0] && typeof obj.files[0] === "object") {
    out.firstFileKeys = Object.keys(obj.files[0]);
  }

  return out;
}

async function parseManifestCsv(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  const records = parse(raw, {
    bom: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
  });

  if (!Array.isArray(records) || records.length === 0) {
    throw new Error(`Manifest is empty: ${filePath}`);
  }

  const header = records[0].map((v) => String(v));
  const relIdx = header.indexOf("relative_path");
  const sizeIdx = header.indexOf("size_bytes");
  const modIdx = header.indexOf("modified_time_iso");

  if (relIdx === -1 || sizeIdx === -1 || modIdx === -1) {
    throw new Error(
      `Manifest header must include relative_path,size_bytes,modified_time_iso. Found: ${header.join(",")}`
    );
  }

  /** @type {ManifestRow[]} */
  const out = [];

  for (let i = 1; i < records.length; i += 1) {
    const rec = records[i];
    if (!Array.isArray(rec) || rec.length === 0) continue;

    const values = rec.map((v) => String(v ?? ""));

    let relativePath = values[relIdx] ?? "";
    let sizeRaw = values[sizeIdx] ?? "";
    let modRaw = values[modIdx] ?? "";

    // Fallback for unquoted commas inside relative_path when header order is standard.
    if (relIdx === 0 && sizeIdx === 1 && modIdx === 2 && values.length > 3) {
      relativePath = values.slice(0, values.length - 2).join(",");
      sizeRaw = values[values.length - 2];
      modRaw = values[values.length - 1];
    }

    const cleanRel = String(relativePath).trim();
    if (!cleanRel) continue;

    out.push({
      relative_path: cleanRel,
      size_bytes: toIntOrNull(sizeRaw),
      modified_time_iso: trimOrNull(modRaw),
    });
  }

  return out;
}

async function listAllUploadThingFiles(utapi, verbose) {
  /** @type {any[]} */
  const all = [];

  let mode = "unknown"; // unknown | offset | cursor
  let offset = 0;
  let cursor = undefined;
  let pageCount = 0;
  let loggedShape = false;

  while (true) {
    if (pageCount > 10000) {
      throw new Error("listFiles pagination exceeded 10,000 pages; aborting to avoid infinite loop");
    }

    let response;

    if (mode === "offset") {
      response = await utapi.listFiles({ limit: LIST_PAGE_SIZE, offset });
    } else if (mode === "cursor") {
      response = cursor
        ? await utapi.listFiles({ limit: LIST_PAGE_SIZE, cursor })
        : await utapi.listFiles({ limit: LIST_PAGE_SIZE });
    } else {
      try {
        response = await utapi.listFiles({ limit: LIST_PAGE_SIZE });
      } catch {
        response = await utapi.listFiles();
      }
    }

    if (verbose && !loggedShape) {
      loggedShape = true;
      console.log(`[verbose] listFiles first response shape: ${JSON.stringify(describeListResponseShape(response))}`);
    }

    if (Array.isArray(response)) {
      all.push(...response);
      break;
    }

    const files = Array.isArray(response?.files) ? response.files : [];
    all.push(...files);

    if (mode === "unknown") {
      if (typeof response?.hasMore === "boolean") {
        mode = "offset";
      } else if (typeof response?.nextCursor === "string" || typeof response?.cursor === "string") {
        mode = "cursor";
      } else {
        break;
      }
    }

    if (mode === "offset") {
      const hasMore = Boolean(response?.hasMore);
      if (!hasMore || files.length === 0) break;
      offset += files.length;
      pageCount += 1;
      continue;
    }

    if (mode === "cursor") {
      const nextCursor =
        typeof response?.nextCursor === "string"
          ? response.nextCursor
          : typeof response?.cursor === "string"
            ? response.cursor
            : null;
      if (!nextCursor || files.length === 0) break;
      cursor = nextCursor;
      pageCount += 1;
      continue;
    }

    break;
  }

  const unique = [];
  const seen = new Set();
  for (const file of all) {
    const key = file?.key ? String(file.key) : "";
    const name = file?.name ? String(file.name) : "";
    const sig = key ? `k:${key}` : `n:${name}`;
    if (seen.has(sig)) continue;
    seen.add(sig);
    unique.push(file);
  }

  return unique;
}

function buildFileLookups(files) {
  const byKey = new Map();
  const byName = new Map();

  for (const file of files) {
    const key = file?.key ? String(file.key) : "";
    const name = file?.name ? String(file.name) : "";

    if (key && !byKey.has(key)) byKey.set(key, file);
    if (name && !byName.has(name)) byName.set(name, file);
  }

  return { byKey, byName };
}

function makeTempPaths(customId) {
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  const safeBase = customId.replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 120);
  return {
    pdfPath: path.join(TMP_DIR, `${safeBase}_${suffix}.pdf`),
    csvPath: path.join(TMP_DIR, `${safeBase}_${suffix}.csv`),
  };
}

async function unlinkIfExists(filePath) {
  try {
    await fs.unlink(filePath);
  } catch {
    // ignore
  }
}

async function downloadFile(url, outPath) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download PDF (${response.status} ${response.statusText}): ${url}`);
  }
  if (!response.body) {
    throw new Error(`Download response had no body: ${url}`);
  }

  const writeStream = fsSync.createWriteStream(outPath);
  await pipeline(Readable.fromWeb(response.body), writeStream);
}

async function runExtractor(pdfPath) {
  return new Promise((resolve, reject) => {
    const proc = spawn("python3", ["scripts/extract_pages.py", pdfPath], {
      cwd: process.cwd(),
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
      reject(new Error(`Failed to start python extractor: ${shortError(error)}`));
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`Python extractor exited ${code}: ${stderr || stdout || "no stderr/stdout"}`));
        return;
      }

      try {
        const parsed = JSON.parse(stdout);
        if (!Array.isArray(parsed)) {
          throw new Error("Extractor output is not a JSON array");
        }

        const pages = parsed.map((item, idx) => {
          const pageNumber = Number(item?.page_number ?? idx + 1);
          return {
            page_number: Number.isFinite(pageNumber) ? pageNumber : idx + 1,
            text: typeof item?.text === "string" ? item.text : String(item?.text ?? ""),
          };
        });

        resolve(pages);
      } catch (error) {
        reject(new Error(`Failed to parse extractor JSON: ${shortError(error)}`));
      }
    });
  });
}

async function upsertDocumentPending(supabase, payload) {
  const { error } = await supabase.from("documents").upsert(
    {
      ...payload,
      status: "pending",
      error: null,
      csv_url: null,
      csv_key: null,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "custom_id" }
  );

  if (error) {
    throw new Error(`documents upsert (pending) failed: ${error.message}`);
  }
}

async function upsertDocumentFailed(supabase, payload, errorMessage) {
  const { error } = await supabase.from("documents").upsert(
    {
      ...payload,
      status: "failed",
      error: errorMessage,
      csv_url: null,
      csv_key: null,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "custom_id" }
  );

  if (error) {
    console.error(`documents upsert (failed) also failed for ${payload.custom_id}: ${error.message}`);
  }
}

async function markDocumentProcessed(supabase, customId, csvUrl, csvKey) {
  const { error } = await supabase
    .from("documents")
    .update({
      status: "processed",
      error: null,
      csv_url: csvUrl,
      csv_key: csvKey,
      updated_at: new Date().toISOString(),
    })
    .eq("custom_id", customId);

  if (error) {
    throw new Error(`documents update (processed) failed: ${error.message}`);
  }
}

async function replaceDocumentPages(supabase, customId, pages) {
  const { error: deleteError } = await supabase.from("document_pages").delete().eq("custom_id", customId);
  if (deleteError) {
    throw new Error(`document_pages delete failed: ${deleteError.message}`);
  }

  for (let i = 0; i < pages.length; i += PAGE_CHUNK_SIZE) {
    const chunk = pages.slice(i, i + PAGE_CHUNK_SIZE).map((p) => ({
      custom_id: customId,
      page_number: p.page_number,
      text: p.text ?? "",
    }));

    if (chunk.length === 0) continue;

    const { error } = await supabase.from("document_pages").upsert(chunk, {
      onConflict: "custom_id,page_number",
    });

    if (error) {
      throw new Error(`document_pages upsert failed: ${error.message}`);
    }
  }
}

async function writeCsvRows(csvPath, pdfName, customId, pdfUrl, pages) {
  const ws = fsSync.createWriteStream(csvPath, { encoding: "utf8" });

  const writeLine = async (line) => {
    const ok = ws.write(line);
    if (!ok) {
      await once(ws, "drain");
    }
  };

  try {
    await writeLine("pdf_name,custom_id,pdf_url,page_number,text\n");

    for (const page of pages) {
      const line = [
        csvEscape(pdfName),
        csvEscape(customId),
        csvEscape(pdfUrl),
        String(page.page_number),
        csvEscape(page.text ?? ""),
      ].join(",");

      await writeLine(`${line}\n`);
    }

    ws.end();
    await once(ws, "finish");
  } catch (error) {
    ws.destroy();
    throw error;
  }
}

async function uploadCsv(utapi, csvPath, csvName, csvCustomId) {
  const csvBuffer = await fs.readFile(csvPath);

  const utFile = new UTFile([csvBuffer], csvName, {
    type: "text/csv",
    customId: csvCustomId,
    lastModified: Date.now(),
  });

  const uploadResult = await utapi.uploadFiles([utFile]);
  const first = Array.isArray(uploadResult) ? uploadResult[0] : uploadResult;

  if (!first) {
    throw new Error("UploadThing uploadFiles returned no result");
  }

  if (first.error) {
    throw new Error(`UploadThing CSV upload failed: ${shortError(first.error)}`);
  }

  const data = first.data;
  if (!data) {
    throw new Error("UploadThing CSV upload returned no data payload");
  }

  return {
    csvUrl: data.ufsUrl ?? data.url ?? null,
    csvKey: data.key ?? null,
  };
}

async function runWithConcurrency(items, concurrency, worker) {
  if (items.length === 0) return;

  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const idx = nextIndex;
      nextIndex += 1;
      if (idx >= items.length) return;
      await worker(items[idx], idx);
    }
  });

  await Promise.all(workers);
}

async function main() {
  const args = parseCliArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return;
  }

  const SUPABASE_URL = getRequiredEnv("SUPABASE_URL");
  const SUPABASE_SERVICE_ROLE_KEY = getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const UPLOADTHING_TOKEN = getRequiredEnv("UPLOADTHING_TOKEN");
  const UPLOADTHING_APP_ID = getRequiredEnv("UPLOADTHING_APP_ID");

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
  });

  const utapi = new UTApi({ token: UPLOADTHING_TOKEN });

  await fs.mkdir(TMP_DIR, { recursive: true });

  const manifestRows = await parseManifestCsv(MANIFEST_PATH);
  const pdfRows = manifestRows.filter((row) => row.relative_path.toLowerCase().endsWith(".pdf"));

  console.log(`Manifest rows: ${manifestRows.length}`);
  console.log(`Manifest PDF rows: ${pdfRows.length}`);

  const uploadedFiles = await listAllUploadThingFiles(utapi, args.verbose);
  console.log(`UploadThing files listed: ${uploadedFiles.length}`);

  const { byKey, byName } = buildFileLookups(uploadedFiles);

  /** @type {MatchedItem[]} */
  const matched = [];
  let missingCount = 0;

  for (const row of pdfRows) {
    const expectedCustomId = normalizePathForCustomId(row.relative_path);
    const expectedOcrName = expectedOcrNameFromOriginal(row.relative_path);

    const file = byKey.get(expectedCustomId) || byName.get(expectedOcrName);

    if (file) {
      matched.push({ row, expectedCustomId, expectedOcrName, file });
    } else {
      missingCount += 1;
    }
  }

  console.log(`Matched OCR PDFs: ${matched.length}`);
  console.log(`Missing OCR PDFs: ${missingCount}`);

  const safeStartAt = Math.min(args.startAt, matched.length);
  const endIndex = args.limit == null ? undefined : safeStartAt + args.limit;
  const queue = matched.slice(safeStartAt, endIndex);

  console.log(
    `Processing: ${queue.length} files (startAt=${args.startAt}, limit=${args.limit ?? "unlimited"}, concurrency=${args.concurrency})`
  );

  if (queue.length === 0) {
    console.log("No files selected to process.");
    return;
  }

  let processed = 0;
  let failed = 0;
  const startedAt = Date.now();

  await runWithConcurrency(queue, args.concurrency, async (item, idx) => {
    const { row, expectedCustomId, expectedOcrName, file } = item;
    const customId = expectedCustomId;
    const pdfName = file?.name ? String(file.name) : expectedOcrName;
    const pdfUrl = buildPdfUrl(UPLOADTHING_APP_ID, customId);

    const documentPayload = {
      original_relative_path: row.relative_path,
      custom_id: customId,
      pdf_name: pdfName,
      pdf_url: pdfUrl,
      size_bytes: row.size_bytes,
      modified_time_iso: row.modified_time_iso,
    };

    const { pdfPath, csvPath } = makeTempPaths(customId);
    const label = `[${idx + 1}/${queue.length}] ${pdfName}`;

    try {
      if (args.verbose) {
        const fileKey = file?.key ? String(file.key) : "(no-key)";
        console.log(`${label} -> key=${fileKey} customId=${customId}`);
      }

      await upsertDocumentPending(supabase, documentPayload);

      await downloadFile(pdfUrl, pdfPath);
      const pages = await runExtractor(pdfPath);

      await replaceDocumentPages(supabase, customId, pages);

      await writeCsvRows(csvPath, pdfName, customId, pdfUrl, pages);

      const csvName = `${path.basename(pdfName, path.extname(pdfName))}__pages.csv`;
      const csvCustomId = `${customId}__pages_csv`;
      const { csvUrl, csvKey } = await uploadCsv(utapi, csvPath, csvName, csvCustomId);

      await markDocumentProcessed(supabase, customId, csvUrl, csvKey);

      processed += 1;
      console.log(`${label} processed (pages=${pages.length})`);
    } catch (error) {
      failed += 1;
      const message = shortError(error);
      console.error(`${label} failed: ${message}`);
      await upsertDocumentFailed(supabase, documentPayload, message);
    } finally {
      await unlinkIfExists(pdfPath);
      await unlinkIfExists(csvPath);
    }
  });

  const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`Done in ${elapsedSec}s. processed=${processed}, failed=${failed}`);
}

main().catch((error) => {
  console.error(shortError(error));
  process.exit(1);
});
