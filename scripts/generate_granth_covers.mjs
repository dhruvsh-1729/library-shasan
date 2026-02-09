#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { spawn } from "node:child_process";
import { createClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";

const TABLE = "granth_ocr_files";
const TMP_DIR = path.join(process.cwd(), ".tmp");
const PAGE_SIZE = 500;
const DEFAULT_DPI = 180;
const DEFAULT_QUALITY = 88;

function usage() {
  console.log(`Usage: node scripts/generate_granth_covers.mjs [options]

Options:
  --limit N         Process at most N files (default: unlimited)
  --startAt N       Skip first N eligible files (default: 0)
  --concurrency N   Number of files in parallel (default: 1)
  --collection X    Only process one collection
  --force           Regenerate covers even if cover_image_url exists
  --dry-run         Show candidates, do not upload/update
  --verbose         Extra logs
  --help            Show help

Env:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  UPLOADTHING_TOKEN
  COVER_DPI (optional, default 180)
  COVER_QUALITY (optional, default 88)
`);
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
    force: false,
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
    if (arg === "--force") {
      args.force = true;
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
    if (arg === "--collection" || arg.startsWith("--collection=")) {
      args.collection = arg.includes("=") ? arg.slice("--collection=".length) : argv[++i];
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function reqEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

function shortErr(error, max = 1800) {
  const msg = error instanceof Error ? error.stack || error.message : String(error);
  return msg.length > max ? `${msg.slice(0, max)}...` : msg;
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

    proc.on("error", (err) => {
      reject(new Error(`Failed to start "${bin}": ${shortErr(err)}`));
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

async function ensureTooling() {
  await runCommand("pdftoppm", ["-v"]);
}

async function ensureCoverColumnsExist(supabase) {
  const { error } = await supabase.from(TABLE).select("id,cover_image_url,cover_image_key").limit(1);
  if (!error) return;

  if (/cover_image_url|cover_image_key/i.test(error.message)) {
    throw new Error(
      [
        "Missing required columns on granth_ocr_files: cover_image_url / cover_image_key.",
        "Run this SQL in Supabase first:",
        "ALTER TABLE public.granth_ocr_files ADD COLUMN IF NOT EXISTS cover_image_url text;",
        "ALTER TABLE public.granth_ocr_files ADD COLUMN IF NOT EXISTS cover_image_key text;",
      ].join("\n")
    );
  }

  throw new Error(`Failed to validate cover columns: ${error.message}`);
}

async function fetchRows(supabase, collection) {
  const rows = [];
  let offset = 0;

  while (true) {
    let query = supabase
      .from(TABLE)
      .select(
        "id,file_name,file_type,file_size,ufs_url,ut_key,custom_id,collection,subcollection,original_rel_path,cover_image_url,cover_image_key"
      )
      .order("id", { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);

    if (collection) query = query.eq("collection", collection);

    const { data, error } = await query;
    if (error) throw new Error(`Failed to fetch ${TABLE}: ${error.message}`);
    if (!data || data.length === 0) break;

    rows.push(...data);
    offset += PAGE_SIZE;
    if (data.length < PAGE_SIZE) break;
  }

  return rows;
}

function isPdfRow(row) {
  const name = String(row.file_name ?? "").toLowerCase();
  const type = String(row.file_type ?? "").toLowerCase();
  return name.endsWith(".pdf") || type.includes("pdf");
}

function makeTempPaths(id) {
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const safe = String(id).replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 80);
  const base = path.join(TMP_DIR, `${safe}_${suffix}`);
  return {
    pdfPath: `${base}.pdf`,
    imageBase: `${base}_cover`,
    imagePath: `${base}_cover.jpg`,
  };
}

async function unlinkSafe(filePath) {
  try {
    await fs.unlink(filePath);
  } catch {
    // ignore
  }
}

async function downloadFile(url, outPath) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Download failed (${res.status} ${res.statusText}): ${url}`);
  if (!res.body) throw new Error(`No response body while downloading: ${url}`);
  const ws = fsSync.createWriteStream(outPath);
  await pipeline(Readable.fromWeb(res.body), ws);
}

async function renderFirstPageToJpg(pdfPath, imageBase, dpi, quality) {
  await runCommand("pdftoppm", [
    "-f",
    "1",
    "-l",
    "1",
    "-singlefile",
    "-r",
    String(dpi),
    "-jpeg",
    "-jpegopt",
    `quality=${quality}`,
    pdfPath,
    imageBase,
  ]);
}

async function resolveExistingCover(utapi, coverCustomId, appId) {
  try {
    const urlResponse = await utapi.getFileUrls(coverCustomId, { keyType: "customId" });
    const hit = Array.isArray(urlResponse?.data) ? urlResponse.data[0] : null;
    if (hit?.url || hit?.key) {
      return {
        coverUrl: hit.url ?? (appId ? `https://${appId}.ufs.sh/f/${coverCustomId}` : null),
        coverKey: hit.key ?? null,
      };
    }
  } catch {
    // ignore and fall through
  }

  if (appId) {
    return {
      coverUrl: `https://${appId}.ufs.sh/f/${coverCustomId}`,
      coverKey: null,
    };
  }

  return null;
}

async function uploadCover(utapi, row, imagePath, appId) {
  const fileBuffer = await fs.readFile(imagePath);
  const pdfBaseName = path.basename(String(row.file_name || `granth_${row.id}.pdf`), path.extname(String(row.file_name || "")));
  const safeBase = pdfBaseName.replace(/\s+/g, "_");
  const coverName = `${safeBase}__cover.jpg`;
  const baseCustomId = String(row.custom_id || row.ut_key || `granth_${row.id}`);
  const coverCustomId = `${baseCustomId}__COVER`;

  const utFile = new UTFile([fileBuffer], coverName, {
    type: "image/jpeg",
    customId: coverCustomId,
    lastModified: Date.now(),
  });

  const uploadResult = await utapi.uploadFiles([utFile]);
  const first = Array.isArray(uploadResult) ? uploadResult[0] : uploadResult;

  if (!first) throw new Error("uploadFiles returned no result");
  if (first.error) {
    const errText = shortErr(first.error);
    if (/already exists|409/i.test(errText)) {
      const existing = await resolveExistingCover(utapi, coverCustomId, appId);
      if (existing?.coverUrl) {
        return {
          coverUrl: existing.coverUrl,
          coverKey: existing.coverKey,
          coverCustomId,
          reusedExisting: true,
        };
      }
    }
    throw new Error(`Cover upload failed: ${errText}`);
  }
  if (!first.data) throw new Error("Cover upload returned no data");

  return {
    coverUrl: first.data.ufsUrl ?? first.data.url ?? null,
    coverKey: first.data.key ?? null,
    coverCustomId,
    reusedExisting: false,
  };
}

async function updateCoverFields(supabase, id, coverUrl, coverKey) {
  const { error } = await supabase
    .from(TABLE)
    .update({ cover_image_url: coverUrl, cover_image_key: coverKey })
    .eq("id", id);

  if (error) throw new Error(`Failed to update ${TABLE}#${id}: ${error.message}`);
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

  const coverDpi = Number.parseInt(process.env.COVER_DPI || String(DEFAULT_DPI), 10) || DEFAULT_DPI;
  const coverQuality =
    Number.parseInt(process.env.COVER_QUALITY || String(DEFAULT_QUALITY), 10) || DEFAULT_QUALITY;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const utapi = new UTApi({ token: UPLOADTHING_TOKEN });

  await ensureTooling();
  await ensureCoverColumnsExist(supabase);
  await fs.mkdir(TMP_DIR, { recursive: true });

  const rows = await fetchRows(supabase, args.collection);
  const pdfRows = rows.filter((row) => isPdfRow(row) && row.ufs_url);
  const eligible = pdfRows.filter((row) => args.force || !row.cover_image_url);

  const start = Math.min(args.startAt, eligible.length);
  const end = args.limit == null ? eligible.length : start + args.limit;
  const queue = eligible.slice(start, end);

  console.log(`Rows fetched: ${rows.length}`);
  console.log(`PDF rows: ${pdfRows.length}`);
  console.log(`Eligible rows: ${eligible.length} (force=${args.force ? "yes" : "no"})`);
  console.log(
    `Processing: ${queue.length} (startAt=${args.startAt}, limit=${args.limit ?? "unlimited"}, concurrency=${args.concurrency})`
  );
  console.log(`Cover settings: dpi=${coverDpi}, jpegQuality=${coverQuality}`);

  if (queue.length === 0) {
    console.log("Nothing to process.");
    return;
  }

  if (args.dryRun) {
    console.log("DRY RUN sample:");
    queue.slice(0, 5).forEach((row, idx) => {
      console.log(
        `[${idx + 1}] id=${row.id} file="${row.file_name}" collection="${row.collection}/${row.subcollection}"`
      );
    });
    return;
  }

  let processed = 0;
  let failed = 0;
  const startedAt = Date.now();

  await runConcurrent(queue, args.concurrency, async (row, idx) => {
    const customId = String(row.custom_id || row.ut_key || `granth_${row.id}`);
    const label = `[${idx + 1}/${queue.length}] ${row.file_name || row.id}`;
    const { pdfPath, imageBase, imagePath } = makeTempPaths(customId);

    try {
      if (args.verbose) {
        console.log(`${label} downloading ${row.ufs_url}`);
      }

      await downloadFile(row.ufs_url, pdfPath);
      await renderFirstPageToJpg(pdfPath, imageBase, coverDpi, coverQuality);

      const { coverUrl, coverKey, coverCustomId, reusedExisting } = await uploadCover(
        utapi,
        row,
        imagePath,
        UPLOADTHING_APP_ID
      );
      await updateCoverFields(supabase, row.id, coverUrl, coverKey);

      processed += 1;
      console.log(`✅ ${label} cover ${reusedExisting ? "linked" : "saved"} (${coverCustomId})`);
    } catch (error) {
      failed += 1;
      console.error(`❌ ${label} failed: ${shortErr(error)}`);
    } finally {
      await unlinkSafe(pdfPath);
      await unlinkSafe(imagePath);
    }
  });

  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`Done in ${elapsed}s. processed=${processed}, failed=${failed}`);
}

main().catch((error) => {
  console.error(shortErr(error));
  process.exit(1);
});
