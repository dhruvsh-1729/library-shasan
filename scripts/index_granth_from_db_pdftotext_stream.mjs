#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { createClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";

const SOURCE_TABLE = "granth_ocr_files";
const DOCS_TABLE = "documents";
const PAGES_TABLE = "document_pages";
const PAGE_CHUNK_SIZE = 200;

// Linux anonymous file flag: O_TMPFILE | O_DIRECTORY
const O_TMPFILE_FLAG = 0o20200000;
const ANON_FILE_DIRS = ["/tmp", "/dev/shm"];

function usage() {
  console.log(`Usage: node scripts/index_granth_from_db_pdftotext_stream.mjs [options]

Options:
  --limit N         Process at most N files (default: unlimited)
  --startAt N       Skip first N files after filtering (default: 0)
  --concurrency N   Number of files in parallel (default: 1)
  --collection X    Process only one collection value
  --reprocess       Reprocess even if already marked processed
  --verbose         Extra logs
  --help            Show this help
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
    throw new Error(`${name} must be an integer >= ${min}`);
  }
  return n;
}

function parseArgs(argv) {
  const args = {
    limit: null,
    startAt: 0,
    concurrency: 1,
    collection: null,
    reprocess: false,
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
    if (arg === "--reprocess") {
      args.reprocess = true;
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
    if (arg === "--collection" || arg.startsWith("--collection=")) {
      args.collection = arg.includes("=") ? arg.slice("--collection=".length) : argv[++i];
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function shortErr(error, max = 1600) {
  const msg = error instanceof Error ? error.stack || error.message : String(error);
  return msg.length > max ? `${msg.slice(0, max)}...` : msg;
}

function normalizeText(input) {
  return String(input ?? "")
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

async function runCommand(bin, args) {
  return new Promise((resolve, reject) => {
    const proc = spawn(bin, args, {
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

async function ensurePopplerInstalled() {
  await runCommand("pdfinfo", ["-v"]);
  await runCommand("pdftotext", ["-v"]);
}

function openAnonymousFileFd() {
  const errors = [];

  for (const dir of ANON_FILE_DIRS) {
    try {
      return fs.openSync(dir, O_TMPFILE_FLAG | fs.constants.O_RDWR, 0o600);
    } catch (error) {
      errors.push(`${dir}: ${shortErr(error, 200)}`);
    }
  }

  throw new Error(
    [
      "Could not create anonymous in-memory file descriptor (O_TMPFILE).",
      "Tried directories:",
      ...errors.map((entry) => `- ${entry}`),
      "This script requires Linux support for O_TMPFILE.",
    ].join("\n")
  );
}

async function withAnonymousPdfPath(pdfBuffer, fn) {
  const fd = openAnonymousFileFd();
  try {
    fs.writeFileSync(fd, pdfBuffer);
    fs.fsyncSync(fd);
    const procPath = `/proc/${process.pid}/fd/${fd}`;
    return await fn(procPath);
  } finally {
    fs.closeSync(fd);
  }
}

async function getPageCount(pdfPath) {
  const { stdout } = await runCommand("pdfinfo", [pdfPath]);
  const match = stdout.match(/^\s*Pages:\s+(\d+)/m);
  if (!match) {
    throw new Error(`Could not parse page count from pdfinfo output.`);
  }
  return Number(match[1]);
}

async function extractPageWithPdftotext(pdfPath, pageNumber) {
  const { stdout } = await runCommand("pdftotext", [
    "-f",
    String(pageNumber),
    "-l",
    String(pageNumber),
    "-enc",
    "UTF-8",
    "-layout",
    pdfPath,
    "-",
  ]);
  return normalizeText(stdout);
}

async function fetchAllSourceFiles(supabase, collection) {
  const out = [];
  let offset = 0;
  const page = 1000;

  while (true) {
    let query = supabase
      .from(SOURCE_TABLE)
      .select("id,ufs_url,ut_key,file_name,file_size,custom_id,original_rel_path,collection,subcollection")
      .order("id", { ascending: true })
      .range(offset, offset + page - 1);

    if (collection) {
      query = query.eq("collection", collection);
    }

    const { data, error } = await query;
    if (error) throw new Error(`Fetch ${SOURCE_TABLE} failed: ${error.message}`);
    if (!data || data.length === 0) break;
    out.push(...data);
    offset += page;
    if (data.length < page) break;
  }

  return out;
}

async function fetchProcessedCustomIds(supabase) {
  const ids = new Set();
  let offset = 0;
  const page = 1000;

  while (true) {
    const { data, error } = await supabase
      .from(DOCS_TABLE)
      .select("custom_id")
      .eq("status", "processed")
      .range(offset, offset + page - 1);

    if (error) throw new Error(`Fetch processed ids failed: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const row of data) {
      if (row.custom_id) ids.add(String(row.custom_id));
    }

    offset += page;
    if (data.length < page) break;
  }

  return ids;
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

async function upsertDocFailed(supabase, payload, errMsg) {
  const { error } = await supabase.from(DOCS_TABLE).upsert(
    {
      ...payload,
      status: "failed",
      error: errMsg,
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

async function replacePages(supabase, customId, pages) {
  const { error: delError } = await supabase.from(PAGES_TABLE).delete().eq("custom_id", customId);
  if (delError) throw new Error(`document_pages delete failed: ${delError.message}`);

  for (let i = 0; i < pages.length; i += PAGE_CHUNK_SIZE) {
    const chunk = pages.slice(i, i + PAGE_CHUNK_SIZE).map((p) => ({
      custom_id: customId,
      page_number: p.page_number,
      text: p.text ?? "",
    }));

    const { error } = await supabase.from(PAGES_TABLE).upsert(chunk, {
      onConflict: "custom_id,page_number",
    });
    if (error) throw new Error(`document_pages upsert failed: ${error.message}`);
  }
}

async function uploadCsv(utapi, csvBuffer, csvName) {
  const utFile = new UTFile([csvBuffer], csvName, {
    type: "text/csv",
    lastModified: Date.now(),
  });

  const result = await utapi.uploadFiles([utFile]);
  const first = Array.isArray(result) ? result[0] : result;

  if (!first) throw new Error("uploadFiles returned empty");
  if (first.error) throw new Error(`uploadFiles error: ${JSON.stringify(first.error)}`);

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

async function extractAllPagesFromBuffer(pdfBuffer, label, verbose) {
  let totalPages = 0;
  /** @type {{ page_number: number; text: string }[]} */
  const pages = [];
  let emptyPages = 0;

  await withAnonymousPdfPath(pdfBuffer, async (pdfPath) => {
    totalPages = await getPageCount(pdfPath);
    console.log(`${label} page count = ${totalPages}`);

    for (let page = 1; page <= totalPages; page += 1) {
      console.log(`${label} page ${page}/${totalPages} extracting...`);
      const text = await extractPageWithPdftotext(pdfPath, page);
      const nonSpaceChars = text.replace(/\s+/g, "").length;
      const hasText = nonSpaceChars > 0;

      if (hasText) {
        pages.push({ page_number: page, text });
      } else {
        emptyPages += 1;
      }

      const sample = verbose && hasText ? ` sample="${text.slice(0, 80).replace(/\n/g, " ")}"` : "";
      console.log(`${label} page ${page}/${totalPages} chars=${nonSpaceChars}${sample}`);
    }
  });

  return {
    pages,
    stats: {
      totalPages,
      storedPages: pages.length,
      emptyPages,
    },
  };
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

  await ensurePopplerInstalled();

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const utapi = new UTApi({ token: UPLOADTHING_TOKEN });

  console.log(`📡 Fetching OCR files from ${SOURCE_TABLE}...`);
  const sourceFiles = await fetchAllSourceFiles(supabase, args.collection);
  const withUrl = sourceFiles.filter((row) => row.ufs_url);
  console.log(`   Found ${sourceFiles.length} files (${withUrl.length} with URL).`);

  let processedIds = new Set();
  if (!args.reprocess) {
    console.log(`📡 Fetching already-processed documents...`);
    processedIds = await fetchProcessedCustomIds(supabase);
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

  console.log(`   ${withUrl.length - filtered.length} skipped (already processed), ${batch.length} to process.`);
  console.log(`Mode: pdftotext page-by-page via anonymous fd path /proc/<pid>/fd/<fd>`);
  console.log(`Processing (concurrency=${args.concurrency})\n`);

  let ok = 0;
  let failed = 0;

  await runConcurrent(batch, args.concurrency, async (file, idx) => {
    const customId = file.custom_id || file.ut_key || `granth_${file.id}`;
    const pdfName = file.file_name || `file_${file.id}.pdf`;
    const pdfUrl = file.ufs_url;
    const label = `[${idx + 1}/${batch.length}] ${pdfName}`;

    const docPayload = {
      original_relative_path: file.original_rel_path || null,
      custom_id: customId,
      pdf_name: pdfName,
      pdf_url: pdfUrl,
      size_bytes: file.file_size || null,
      modified_time_iso: null,
    };

    try {
      await upsertDocPending(supabase, docPayload);

      console.log(`${label} downloading (in-memory)...`);
      const response = await fetch(pdfUrl);
      if (!response.ok) throw new Error(`Download failed ${response.status}: ${pdfUrl}`);
      const pdfBuffer = Buffer.from(await response.arrayBuffer());
      if (args.verbose) console.log(`${label} bytes=${pdfBuffer.length}`);

      console.log(`${label} extracting with pdftotext...`);
      const { pages, stats } = await extractAllPagesFromBuffer(pdfBuffer, label, args.verbose);

      const nonEmptyPages = pages.filter((p) => hasMeaningfulText(p.text));
      if (nonEmptyPages.length === 0) {
        throw new Error(
          `No extractable text found using pdftotext text-layer extraction. stats=${JSON.stringify(stats)}`
        );
      }

      await replacePages(supabase, customId, nonEmptyPages);

      const csvName = `${path.basename(pdfName, path.extname(pdfName))}__pages.csv`;
      const header = "pdf_name,custom_id,pdf_url,page_number,text\n";
      const rows = nonEmptyPages.map(
        (page) =>
          [
            csvEscape(pdfName),
            csvEscape(customId),
            csvEscape(pdfUrl),
            String(page.page_number),
            csvEscape(page.text),
          ].join(",") + "\n"
      );
      const csvBuffer = Buffer.from(header + rows.join(""), "utf8");

      console.log(`${label} uploading CSV...`);
      const { csvUrl, csvKey } = await uploadCsv(utapi, csvBuffer, csvName);

      await markDocProcessed(supabase, customId, csvUrl, csvKey);

      ok += 1;
      console.log(
        `✅ ${label} done (storedPages=${nonEmptyPages.length}, totalPages=${stats.totalPages}, emptyPages=${stats.emptyPages})\n`
      );
    } catch (error) {
      failed += 1;
      const msg = shortErr(error);
      console.error(`❌ ${label} failed: ${msg}\n`);
      await upsertDocFailed(supabase, docPayload, msg);
    }
  });

  console.log(`🎉 Finished: processed=${ok}, failed=${failed}, skipped=${withUrl.length - filtered.length}`);
}

main().catch((error) => {
  console.error(shortErr(error));
  process.exit(1);
});
