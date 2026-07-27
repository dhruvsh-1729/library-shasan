#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { createClient as createTursoClient } from "@libsql/client";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";
import { UTApi, UTFile } from "uploadthing/server";
import XLSX from "xlsx";

const require = createRequire(import.meta.url);

const DEFAULT_SOURCE_ROOT =
  process.env.LIBRARY_SOURCE_ROOT ||
  "/media/dell/KINGSTON/Library Final/Shrirang Library_5-9-2024 OCR _ed";
const DEFAULT_STATE_DIR = path.join(process.cwd(), ".library_pipeline_state");
const DEFAULT_OUTPUT_DIR = path.join(process.cwd(), ".library_pipeline_output");
const DEFAULT_TMP_DIR = path.join(process.cwd(), ".tmp_library_pipeline");
const DEFAULT_EXTRA_TESSDATA_DIRS = "/home/dell/Downloads";

const SOURCE_TABLE = "granth_ocr_files";
const DOCS_TABLE = "documents";
const PAGES_TABLE = "document_pages";
const INSERT_BATCH_SIZE = 150;
const SUPABASE_PAGE_BATCH_SIZE = Math.max(
  1,
  Math.min(Number.parseInt(process.env.LIBRARY_PIPELINE_SUPABASE_PAGE_BATCH_SIZE || "10", 10) || 10, 50)
);
const SUPABASE_WRITE_RETRIES = Math.max(
  1,
  Math.min(Number.parseInt(process.env.LIBRARY_PIPELINE_SUPABASE_WRITE_RETRIES || "4", 10) || 4, 8)
);
const UPLOADTHING_CUSTOM_ID_MAX = 128;
const GOOGLE_PRICE_PER_1000_PAGES = 1.5;
const DEFAULT_GOOGLE_PROJECT_ID = "461791694388";
const DEFAULT_GOOGLE_LOCATION = "asia-south1";
const DEFAULT_GOOGLE_PROCESSOR_ID = "c6b075985ab223a8";
const DEFAULT_GOOGLE_PROCESSOR_VERSION = "pretrained-ocr-v2.1.1-2025-01-31";
const DEFAULT_TESSERACT_LANGS = "guj+san+eng";
const DEFAULT_GOOGLE_LANGUAGE_HINTS = "gu,sa,en";
const SYSTEM_TESSDATA_DIR_CANDIDATES = [
  "/usr/share/tesseract-ocr/5/tessdata",
  "/usr/share/tesseract-ocr/4.00/tessdata",
  "/usr/share/tessdata",
  "/usr/local/share/tessdata",
];
const GOOGLE_LANGUAGE_HINT_ALIASES = new Map([
  ["guj", "gu"],
  ["gu", "gu"],
  ["san", "sa"],
  ["sa", "sa"],
  ["eng", "en"],
  ["en", "en"],
]);

function usage() {
  console.log(`Usage: node scripts/run_budgeted_library_pipeline.mjs [options]

Default mode is dry-run. Use --execute to upload files, write databases, or call Google.

Main options:
  --sourceRoot PATH        PDF library root (default: ${DEFAULT_SOURCE_ROOT})
  --phase all|catalog|text Run catalog upload, OCR text, or both (default: all)
  --execute                Enable UploadThing, Supabase/Turso writes, and Google calls
  --dry-run                Force read-only mode (default when --execute is absent)
  --limit N                Process at most N PDFs after filtering
  --startAt N              Skip first N selected PDFs (default: 0)
  --bookNumber N           Process one leading filename book number
  --reprocess              Re-OCR even if Turso already has pages and an XLSX URL

Catalog options:
  --catalogConcurrency N   Parallel PDF/cover catalog workers (default: 1)
  --skipPdfUpload          Do not upload PDFs to UploadThing
  --skipCovers             Do not render/upload first-page covers
  --coverDpi N             Cover render DPI (default: 160)

OCR options:
  --maxPages N             Debug cap per PDF
  --pageConcurrency N      Parallel local page workers per PDF (default: 2)
  --dpi N                  Page image render DPI for OCR (default: 300)
  --langs X                Tesseract langs (default: ${DEFAULT_TESSERACT_LANGS})
  --tessdataDir PATH       Combined tessdata directory (default: stateDir/tessdata)
  --extraTessdataDirs PATH Extra traineddata dirs, path-list or comma-separated (default: ${DEFAULT_EXTRA_TESSDATA_DIRS})
  --tessThreads N          OMP_THREAD_LIMIT per tesseract process (default: 1)
  --tessTimeoutMs N        Per-page tesseract timeout (default: 90000)
  --googleMode X           off|low-confidence|all (default: low-confidence)
  --googleBudgetUsd N      Hard cap for Google OCR attempts (default: 20)
  --googleLanguageHints X  Google OCR BCP-47 hints, comma-separated. Empty/auto omits hints.
  --googleLowTextPages     Allow paid OCR for very low-text pages
  --noResumePages          Ignore existing page text and process selected pages again
  --resumeMinChars N       Reuse existing page text only above this char count (default: 80)
  --resumeMinScore N       Reuse existing page text only above this quality score (default: 0.68)
  --noPageCheckpoints      Do not save per-page OCR checkpoints during processing
  --supabasePageCheckpoints Also save per-page OCR checkpoints to Supabase. Turso checkpoints stay enabled by default.
  --supabasePageSync       Also write final OCR page text to Supabase document_pages. Disabled by default.

Safety/resume options:
  --stateDir PATH          Budget/resume state directory (default: ${DEFAULT_STATE_DIR})
  --outputDir PATH         Local generated spreadsheets (default: ${DEFAULT_OUTPUT_DIR})
  --tmpDir PATH            Temp rendered pages/covers (default: ${DEFAULT_TMP_DIR})
  --minFreeMemMB N         Wait unless this much RAM is available (default: 2048)
  --memoryWaitSeconds N    Max wait for RAM before failing (default: 900)
  --noRemoteReads          Do not read Supabase/Turso/UploadThing in dry-runs
  --keepTemp               Keep rendered temp images
  --verbose                Extra logs
  --help                   Show this help
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

function parseFloatFlag(name, raw, min) {
  if (raw == null || raw === "") throw new Error(`${name} requires a numeric value`);
  const n = Number.parseFloat(String(raw));
  if (!Number.isFinite(n) || Number.isNaN(n) || n < min) {
    throw new Error(`${name} must be a number >= ${min}`);
  }
  return n;
}

function readValueArg(argv, index, longName) {
  const arg = argv[index];
  if (arg.includes("=")) return { value: arg.slice(longName.length + 1), nextIndex: index };
  return { value: argv[index + 1], nextIndex: index + 1 };
}

function parseGoogleLanguageHints(raw) {
  const value = String(raw ?? "").trim();
  if (!value || /^(auto|none|off)$/i.test(value)) return [];
  return value
    .split(/[,+]/)
    .map((x) => {
      const hint = x.trim();
      return GOOGLE_LANGUAGE_HINT_ALIASES.get(hint.toLowerCase()) || hint;
    })
    .filter(Boolean);
}

function parsePathList(raw) {
  return String(raw ?? "")
    .split(new RegExp(`[${path.delimiter},]`))
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseArgs(argv) {
  const args = {
    sourceRoot: DEFAULT_SOURCE_ROOT,
    phase: "all",
    execute: false,
    dryRun: true,
    limit: null,
    startAt: 0,
    bookNumber: null,
    reprocess: false,
    catalogConcurrency: 1,
    skipPdfUpload: false,
    skipCovers: false,
    coverDpi: 160,
    maxPages: null,
    pageConcurrency: Number.parseInt(process.env.OCR_PAGE_CONCURRENCY || "2", 10) || 2,
    dpi: Number.parseInt(process.env.OCR_DPI || "300", 10) || 300,
    langs: process.env.OCR_LANGS || DEFAULT_TESSERACT_LANGS,
    tessThreads: Number.parseInt(process.env.OCR_TESSERACT_THREADS || "1", 10) || 1,
    tessTimeoutMs: Number.parseInt(process.env.OCR_TESSERACT_TIMEOUT_MS || "90000", 10) || 90000,
    googleMode: process.env.LIBRARY_GOOGLE_OCR_MODE || "low-confidence",
    googleBudgetUsd: parseFloatFlag(
      "LIBRARY_GOOGLE_BUDGET_USD",
      process.env.LIBRARY_GOOGLE_BUDGET_USD || "20",
      0
    ),
    googlePricePer1000: parseFloatFlag(
      "LIBRARY_GOOGLE_PRICE_PER_1000",
      process.env.LIBRARY_GOOGLE_PRICE_PER_1000 || String(GOOGLE_PRICE_PER_1000_PAGES),
      0
    ),
    googleLowTextPages: false,
    googleProjectId:
      process.env.GOOGLE_DOCUMENTAI_PROJECT_ID ||
      process.env.GOOGLE_CLOUD_PROJECT ||
      DEFAULT_GOOGLE_PROJECT_ID,
    googleLocation: process.env.GOOGLE_DOCUMENTAI_LOCATION || DEFAULT_GOOGLE_LOCATION,
    googleProcessorId: process.env.GOOGLE_DOCUMENTAI_PROCESSOR_ID || DEFAULT_GOOGLE_PROCESSOR_ID,
    googleProcessorVersion:
      process.env.GOOGLE_DOCUMENTAI_PROCESSOR_VERSION || DEFAULT_GOOGLE_PROCESSOR_VERSION,
    googleLanguageHints: parseGoogleLanguageHints(
      process.env.GOOGLE_DOCUMENTAI_LANGUAGE_HINTS ?? DEFAULT_GOOGLE_LANGUAGE_HINTS
    ),
    resumePages: process.env.LIBRARY_PIPELINE_RESUME_PAGES !== "0",
    resumeMinChars: Number.parseInt(process.env.LIBRARY_PIPELINE_RESUME_MIN_CHARS || "80", 10) || 80,
    resumeMinScore: parseFloatFlag(
      "LIBRARY_PIPELINE_RESUME_MIN_SCORE",
      process.env.LIBRARY_PIPELINE_RESUME_MIN_SCORE || "0.68",
      0
    ),
    pageCheckpoints: process.env.LIBRARY_PIPELINE_PAGE_CHECKPOINTS !== "0",
    supabasePageCheckpoints: process.env.LIBRARY_PIPELINE_SUPABASE_PAGE_CHECKPOINTS === "1",
    supabasePageSync: process.env.LIBRARY_PIPELINE_SUPABASE_PAGE_SYNC === "1",
    stateDir: DEFAULT_STATE_DIR,
    outputDir: DEFAULT_OUTPUT_DIR,
    tmpDir: DEFAULT_TMP_DIR,
    tessdataDir: process.env.OCR_TESSDATA_DIR || null,
    extraTessdataDirs: parsePathList(process.env.OCR_EXTRA_TESSDATA_DIRS || DEFAULT_EXTRA_TESSDATA_DIRS),
    minFreeMemMB: Number.parseInt(process.env.LIBRARY_PIPELINE_MIN_FREE_MEM_MB || "2048", 10) || 2048,
    memoryWaitSeconds:
      Number.parseInt(process.env.LIBRARY_PIPELINE_MEMORY_WAIT_SECONDS || "900", 10) || 900,
    remoteReads: true,
    keepTemp: false,
    verbose: false,
    help: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") {
      args.help = true;
      continue;
    }
    if (arg === "--execute") {
      args.execute = true;
      args.dryRun = false;
      continue;
    }
    if (arg === "--dry-run") {
      args.execute = false;
      args.dryRun = true;
      continue;
    }
    if (arg === "--reprocess") {
      args.reprocess = true;
      continue;
    }
    if (arg === "--skipPdfUpload") {
      args.skipPdfUpload = true;
      continue;
    }
    if (arg === "--skipCovers") {
      args.skipCovers = true;
      continue;
    }
    if (arg === "--googleLowTextPages") {
      args.googleLowTextPages = true;
      continue;
    }
    if (arg === "--noResumePages") {
      args.resumePages = false;
      continue;
    }
    if (arg === "--noPageCheckpoints") {
      args.pageCheckpoints = false;
      continue;
    }
    if (arg === "--supabasePageCheckpoints") {
      args.supabasePageCheckpoints = true;
      continue;
    }
    if (arg === "--supabasePageSync") {
      args.supabasePageSync = true;
      continue;
    }
    if (arg === "--noRemoteReads") {
      args.remoteReads = false;
      continue;
    }
    if (arg === "--keepTemp") {
      args.keepTemp = true;
      continue;
    }
    if (arg === "--verbose") {
      args.verbose = true;
      continue;
    }

    const longFlags = new Set([
      "--sourceRoot",
      "--phase",
      "--limit",
      "--startAt",
      "--bookNumber",
      "--catalogConcurrency",
      "--coverDpi",
      "--maxPages",
      "--pageConcurrency",
      "--dpi",
      "--langs",
      "--tessThreads",
      "--tessTimeoutMs",
      "--googleMode",
      "--googleBudgetUsd",
      "--googlePricePer1000",
      "--googleProjectId",
      "--googleLocation",
      "--googleProcessorId",
      "--googleProcessorVersion",
      "--googleLanguageHints",
      "--resumeMinChars",
      "--resumeMinScore",
      "--stateDir",
      "--outputDir",
      "--tmpDir",
      "--tessdataDir",
      "--extraTessdataDirs",
      "--minFreeMemMB",
      "--memoryWaitSeconds",
    ]);

    const flagName = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!longFlags.has(flagName)) throw new Error(`Unknown argument: ${arg}`);

    const { value, nextIndex } = readValueArg(argv, i, flagName);
    i = nextIndex;

    if (flagName === "--sourceRoot") args.sourceRoot = value;
    else if (flagName === "--phase") args.phase = value;
    else if (flagName === "--limit") args.limit = parseIntFlag(flagName, value, 0);
    else if (flagName === "--startAt") args.startAt = parseIntFlag(flagName, value, 0);
    else if (flagName === "--bookNumber") args.bookNumber = String(value).padStart(3, "0");
    else if (flagName === "--catalogConcurrency")
      args.catalogConcurrency = parseIntFlag(flagName, value, 1);
    else if (flagName === "--coverDpi") args.coverDpi = parseIntFlag(flagName, value, 72);
    else if (flagName === "--maxPages") args.maxPages = parseIntFlag(flagName, value, 1);
    else if (flagName === "--pageConcurrency")
      args.pageConcurrency = parseIntFlag(flagName, value, 1);
    else if (flagName === "--dpi") args.dpi = parseIntFlag(flagName, value, 72);
    else if (flagName === "--langs") args.langs = value;
    else if (flagName === "--tessThreads") args.tessThreads = parseIntFlag(flagName, value, 1);
    else if (flagName === "--tessTimeoutMs")
      args.tessTimeoutMs = parseIntFlag(flagName, value, 1000);
    else if (flagName === "--googleMode") args.googleMode = value;
    else if (flagName === "--googleBudgetUsd")
      args.googleBudgetUsd = parseFloatFlag(flagName, value, 0);
    else if (flagName === "--googlePricePer1000")
      args.googlePricePer1000 = parseFloatFlag(flagName, value, 0);
    else if (flagName === "--googleProjectId") args.googleProjectId = value;
    else if (flagName === "--googleLocation") args.googleLocation = value;
    else if (flagName === "--googleProcessorId") args.googleProcessorId = value;
    else if (flagName === "--googleProcessorVersion") args.googleProcessorVersion = value;
    else if (flagName === "--googleLanguageHints") args.googleLanguageHints = parseGoogleLanguageHints(value);
    else if (flagName === "--resumeMinChars") args.resumeMinChars = parseIntFlag(flagName, value, 0);
    else if (flagName === "--resumeMinScore") args.resumeMinScore = parseFloatFlag(flagName, value, 0);
    else if (flagName === "--stateDir") args.stateDir = value;
    else if (flagName === "--outputDir") args.outputDir = value;
    else if (flagName === "--tmpDir") args.tmpDir = value;
    else if (flagName === "--tessdataDir") args.tessdataDir = value;
    else if (flagName === "--extraTessdataDirs") args.extraTessdataDirs = parsePathList(value);
    else if (flagName === "--minFreeMemMB") args.minFreeMemMB = parseIntFlag(flagName, value, 256);
    else if (flagName === "--memoryWaitSeconds")
      args.memoryWaitSeconds = parseIntFlag(flagName, value, 0);
  }

  if (!["all", "catalog", "text"].includes(args.phase)) {
    throw new Error(`--phase must be one of all, catalog, text`);
  }
  if (!["off", "low-confidence", "all"].includes(args.googleMode)) {
    throw new Error(`--googleMode must be one of off, low-confidence, all`);
  }
  if (args.catalogConcurrency > 4) {
    throw new Error(`--catalogConcurrency above 4 is intentionally blocked for large PDF uploads`);
  }
  if (args.pageConcurrency > 8) {
    throw new Error(`--pageConcurrency above 8 is intentionally blocked for memory safety`);
  }
  if (args.resumeMinScore > 1) {
    throw new Error(`--resumeMinScore must be between 0 and 1`);
  }
  if (args.dryRun) {
    args.execute = false;
  }
  args.tessdataDir = path.resolve(args.tessdataDir || path.join(args.stateDir, "tessdata"));
  return args;
}

function shortErr(error, max = 1800) {
  const text =
    error instanceof Error
      ? error.stack || error.message
      : typeof error === "string"
        ? error
        : JSON.stringify(error);
  return text.length > max ? `${text.slice(0, max)}...` : text;
}

function normalizeText(raw) {
  return String(raw ?? "")
    .replace(/\u0000/g, "")
    .replace(/\f/g, "\n")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .trim();
}

function sanitizeForCustomId(value, max = 160) {
  return String(value)
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, max);
}

function hashText(value, len = 12) {
  return crypto.createHash("sha1").update(String(value)).digest("hex").slice(0, len);
}

function uploadThingCustomId(value, uniqueHash = null) {
  const clean = sanitizeForCustomId(value, 512);
  if (clean.length <= UPLOADTHING_CUSTOM_ID_MAX) return clean;

  const tail = uniqueHash || hashText(clean);
  const headMax = UPLOADTHING_CUSTOM_ID_MAX - tail.length - 1;
  const head = sanitizeForCustomId(clean, headMax).replace(/_+$/g, "");
  return `${head}_${tail}`;
}

function bytesToHuman(bytes) {
  const n = Number(bytes || 0);
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(2)} MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(2)} KB`;
  return `${n} B`;
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableSupabaseWriteError(error) {
  return /statement timeout|canceling statement due to statement timeout|deadlock detected|could not serialize access|fetch failed|ECONNRESET|ETIMEDOUT/i.test(
    shortErr(error, 600)
  );
}

async function supabaseWriteWithRetry(label, write) {
  let lastError = null;

  for (let attempt = 1; attempt <= SUPABASE_WRITE_RETRIES; attempt += 1) {
    const { error } = await write();
    if (!error) return;

    lastError = error;
    if (attempt >= SUPABASE_WRITE_RETRIES || !isRetryableSupabaseWriteError(error)) {
      break;
    }

    const delayMs = Math.min(30000, 1500 * attempt * attempt);
    console.warn(
      `[supabase] retry ${attempt}/${SUPABASE_WRITE_RETRIES - 1} ${label}: ${error.message}; waiting ${Math.round(
        delayMs / 1000
      )}s`
    );
    await sleep(delayMs);
  }

  throw new Error(`${label}: ${lastError?.message || "unknown Supabase write error"}`);
}

async function readMemAvailableMB() {
  const raw = await fs.readFile("/proc/meminfo", "utf8");
  const match = raw.match(/^MemAvailable:\s+(\d+)\s+kB/m);
  if (!match) return Number.POSITIVE_INFINITY;
  return Math.floor(Number(match[1]) / 1024);
}

async function waitForMemory(args, label, extraMB = 0) {
  const needed = args.minFreeMemMB + extraMB;
  const started = Date.now();
  while (true) {
    const available = await readMemAvailableMB();
    if (available >= needed) {
      if (args.verbose) {
        console.log(`[mem] ${label}: available=${available}MB required=${needed}MB`);
      }
      return available;
    }

    const waitedSeconds = Math.floor((Date.now() - started) / 1000);
    if (waitedSeconds >= args.memoryWaitSeconds) {
      throw new Error(
        `Timed out waiting for memory before ${label}: available=${available}MB required=${needed}MB`
      );
    }
    console.log(
      `[mem] Waiting before ${label}: available=${available}MB required=${needed}MB waited=${waitedSeconds}s`
    );
    await sleep(5000);
  }
}

async function runCommand(bin, commandArgs, options = {}) {
  const env = options.env ? { ...process.env, ...options.env } : process.env;
  return new Promise((resolve, reject) => {
    const proc = spawn(bin, commandArgs, {
      cwd: options.cwd || process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let settled = false;
    let timeout = null;

    if (options.timeoutMs) {
      timeout = setTimeout(() => {
        if (settled) return;
        settled = true;
        proc.kill("SIGTERM");
        setTimeout(() => {
          if (!proc.killed) proc.kill("SIGKILL");
        }, 2000).unref();
        reject(new Error(`"${bin}" timed out after ${options.timeoutMs}ms`));
      }, options.timeoutMs);
      timeout.unref();
    }

    proc.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    proc.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    proc.on("error", (error) => {
      if (settled) return;
      settled = true;
      if (timeout) clearTimeout(timeout);
      reject(new Error(`Failed to start "${bin}": ${shortErr(error)}`));
    });
    proc.on("close", (code) => {
      if (settled) return;
      settled = true;
      if (timeout) clearTimeout(timeout);
      if (code !== 0) {
        reject(new Error(`"${bin}" exited ${code}: ${stderr || stdout || "(no output)"}`));
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

async function ensureTools() {
  await runCommand("pdfinfo", ["-v"]);
  await runCommand("pdftotext", ["-v"]);
  await runCommand("pdftoppm", ["-v"]);
  await runCommand("tesseract", ["--version"]);
}

let cachedTesseractLangs = null;
function parseTesseractLangSpec(spec) {
  return String(spec)
    .split("+")
    .map((x) => x.trim())
    .filter(Boolean);
}

async function pathExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function tessdataSourceDirs(extraDirs = []) {
  const dirs = [];
  const add = (dir) => {
    if (!dir) return;
    const resolved = path.resolve(dir);
    if (!dirs.includes(resolved)) dirs.push(resolved);
  };

  if (process.env.TESSDATA_PREFIX) {
    add(process.env.TESSDATA_PREFIX);
    add(path.join(process.env.TESSDATA_PREFIX, "tessdata"));
  }
  for (const dir of SYSTEM_TESSDATA_DIR_CANDIDATES) add(dir);
  for (const dir of extraDirs) add(dir);
  return dirs;
}

async function findTessdataSource(lang, sourceDirs) {
  for (const dir of sourceDirs) {
    const filePath = path.join(dir, `${lang}.traineddata`);
    if (await pathExists(filePath)) return filePath;
  }
  return null;
}

async function ensureTessdataLink(source, dest) {
  try {
    const stat = await fs.lstat(dest);
    if (!stat.isSymbolicLink()) return;
    const target = await fs.readlink(dest);
    if (path.resolve(path.dirname(dest), target) === source) return;
    await fs.unlink(dest);
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }

  try {
    await fs.symlink(source, dest);
  } catch {
    await fs.copyFile(source, dest);
  }
}

async function ensureCombinedTessdataDir(args) {
  const requested = parseTesseractLangSpec(args.langs);
  const sourceDirs = tessdataSourceDirs(args.extraTessdataDirs);
  await fs.mkdir(args.tessdataDir, { recursive: true });

  for (const lang of requested) {
    const source = await findTessdataSource(lang, sourceDirs);
    if (!source) continue;
    await ensureTessdataLink(source, path.join(args.tessdataDir, `${lang}.traineddata`));
  }
}

function addTessdataDir(commandArgs, args) {
  if (args.tessdataDir) commandArgs.push("--tessdata-dir", args.tessdataDir);
  return commandArgs;
}

async function resolveTesseractLangs(args) {
  if (!cachedTesseractLangs) {
    const { stdout, stderr } = await runCommand("tesseract", addTessdataDir(["--list-langs"], args));
    cachedTesseractLangs = new Set(
      `${stdout}\n${stderr}`
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line && !line.toLowerCase().startsWith("list of available languages"))
    );
  }

  const requested = parseTesseractLangSpec(args.langs);
  const selected = requested.filter((lang) => cachedTesseractLangs.has(lang));
  const missing = requested.filter((lang) => !cachedTesseractLangs.has(lang));

  if (selected.length === 0) {
    throw new Error(
      `None of requested tesseract langs are installed. requested=${requested.join(
        "+"
      )}, available=${[...cachedTesseractLangs].join(",")}`
    );
  }
  if (missing.length > 0) {
    console.warn(`[ocr] Missing tesseract langs skipped: ${missing.join(", ")}`);
  }
  return selected.join("+");
}

async function walkPdfs(root) {
  const out = [];

  async function walk(dir) {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(full);
      } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".pdf")) {
        out.push(full);
      }
    }
  }

  await walk(root);
  return out.sort((a, b) => {
    const aBase = path.basename(a);
    const bBase = path.basename(b);
    const aNum = aBase.match(/^(\d{1,4})(?=[_\s.-]|$)/);
    const bNum = bBase.match(/^(\d{1,4})(?=[_\s.-]|$)/);
    if (aNum && !bNum) return -1;
    if (!aNum && bNum) return 1;
    if (aNum && bNum && aNum[1] !== bNum[1]) {
      return Number.parseInt(aNum[1], 10) - Number.parseInt(bNum[1], 10);
    }
    return a.localeCompare(b, "en");
  });
}

function prettifyName(value) {
  return String(value)
    .replace(/[_]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b(?:hr6|std|ocr|ocred|needs ocr)\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function derivePdfMetadata(filePath, sourceRoot) {
  const relPath = path.relative(sourceRoot, filePath).replaceAll(path.sep, "/");
  const fileName = path.basename(filePath);
  const stem = fileName.replace(/\.pdf$/i, "");
  const hash = hashText(relPath);
  const leading = stem.match(/^(\d{1,4})(?:[_\s-]+(.+))?$/);
  const bookNumber = leading ? leading[1].padStart(3, "0") : null;
  const parts = stem.split("_");
  let libraryCode = null;
  if (parts.length >= 2 && /^[a-zA-Z]\d{4,}$/.test(parts[1])) {
    libraryCode = parts[1].toUpperCase();
  }
  let nameRaw = leading?.[2] || stem;
  if (bookNumber && parts.length > 1) {
    nameRaw = libraryCode ? parts.slice(2).join("_") : parts.slice(1).join("_");
  }
  const granthName = prettifyName(nameRaw || stem) || stem;
  const granthKey = bookNumber || `unnum_${hash}`;
  const collection = path.basename(sourceRoot);
  const relForCustomId = sanitizeForCustomId(relPath, 120);
  const pdfCustomId = uploadThingCustomId(`${collection}__${relForCustomId}__OCR_${hash}`, hash);
  const xlsxCustomId = uploadThingCustomId(`${granthKey}__${hash}__ocr_xlsx`, hash);
  const csvCustomId = uploadThingCustomId(`${granthKey}__${hash}__ocr_csv`, hash);
  const coverCustomId = uploadThingCustomId(`${granthKey}__${hash}__cover`, hash);

  return {
    filePath,
    relPath,
    fileName,
    stem,
    hash,
    bookNumber,
    libraryCode,
    granthName,
    granthKey,
    collection,
    subcollection: null,
    pdfCustomId,
    xlsxCustomId,
    csvCustomId,
    coverCustomId,
  };
}

async function pdfInfo(filePath) {
  const { stdout } = await runCommand("pdfinfo", [filePath], { timeoutMs: 60000 });
  const pages = stdout.match(/^Pages:\s+(\d+)/m);
  if (!pages) throw new Error(`pdfinfo did not return a Pages line`);
  return { pageCount: Number.parseInt(pages[1], 10), raw: stdout };
}

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let nextIndex = 0;
  let active = 0;
  let rejected = null;

  return await new Promise((resolve, reject) => {
    const pump = () => {
      if (rejected) return;
      if (nextIndex >= items.length && active === 0) {
        resolve(out);
        return;
      }
      while (active < limit && nextIndex < items.length) {
        const index = nextIndex;
        nextIndex += 1;
        active += 1;
        Promise.resolve()
          .then(() => fn(items[index], index))
          .then((result) => {
            out[index] = result;
            active -= 1;
            pump();
          })
          .catch((error) => {
            rejected = error;
            reject(error);
          });
      }
    };
    pump();
  });
}

async function readJsonIfExists(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    if (error && typeof error === "object" && error.code === "ENOENT") return fallback;
    throw error;
  }
}

async function writeJsonAtomic(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  await fs.writeFile(tmp, `${JSON.stringify(value, null, 2)}\n`);
  await fs.rename(tmp, filePath);
}

class GoogleBudget {
  constructor(args, state) {
    this.args = args;
    this.state = state;
    this.pricePerPage = args.googlePricePer1000 / 1000;
    this.maxPaidPages =
      this.pricePerPage > 0
        ? Math.floor((args.googleBudgetUsd + Number.EPSILON) / this.pricePerPage)
        : Number.POSITIVE_INFINITY;
    this.state.google = this.state.google || {};
    this.state.google.budgetUsd = args.googleBudgetUsd;
    this.state.google.pricePer1000 = args.googlePricePer1000;
    this.state.google.paidPagesAttempted = Number(this.state.google.paidPagesAttempted || 0);
  }

  attemptedPages() {
    return Number(this.state.google.paidPagesAttempted || 0);
  }

  estimatedSpendUsd() {
    return this.attemptedPages() * this.pricePerPage;
  }

  canAttemptPage() {
    return this.attemptedPages() + 1 <= this.maxPaidPages;
  }

  summary() {
    return {
      budgetUsd: this.args.googleBudgetUsd,
      pricePer1000: this.args.googlePricePer1000,
      maxPaidPages: this.maxPaidPages,
      paidPagesAttempted: this.attemptedPages(),
      estimatedSpendUsd: Number(this.estimatedSpendUsd().toFixed(4)),
      remainingPaidPages: Math.max(0, this.maxPaidPages - this.attemptedPages()),
    };
  }

  async markAttempt(statePath) {
    if (!this.canAttemptPage()) return false;
    this.state.google.paidPagesAttempted = this.attemptedPages() + 1;
    this.state.google.estimatedSpendUsd = Number(this.estimatedSpendUsd().toFixed(4));
    this.state.updatedAt = new Date().toISOString();
    await writeJsonAtomic(statePath, this.state);
    return true;
  }
}

function makeSupabase() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) return null;
  return createSupabaseClient(url, key, { auth: { persistSession: false } });
}

function makeTurso() {
  const url = process.env.TURSO_URL;
  const token = process.env.TURSO_AUTH_TOKEN;
  if (!url || !token) return null;
  return createTursoClient({ url, authToken: token });
}

function makeUploadThing() {
  if (!process.env.UPLOADTHING_TOKEN) return null;
  return new UTApi();
}

function requireClient(client, name, execute) {
  if (!client && execute) throw new Error(`Missing ${name} credentials required by --execute`);
  return client;
}

async function fetchSupabaseTableAll(supabase, table, columns) {
  if (!supabase) return [];
  const rows = [];
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from(table)
      .select(columns)
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`Supabase ${table} read failed: ${error.message}`);
    if (!data || data.length === 0) break;
    rows.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return rows;
}

async function loadRemoteState({ supabase, turso, remoteReads }) {
  const state = {
    sourceByCustomId: new Map(),
    docsByCustomId: new Map(),
    docsByRelPath: new Map(),
    tursoByGranthKey: new Map(),
  };

  if (!remoteReads) return state;

  const [sources, docs] = await Promise.all([
    fetchSupabaseTableAll(
      supabase,
      SOURCE_TABLE,
      "id,custom_id,ufs_url,ut_key,file_name,original_rel_path,cover_image_url,cover_image_key"
    ),
    fetchSupabaseTableAll(
      supabase,
      DOCS_TABLE,
      "id,custom_id,original_relative_path,pdf_name,pdf_url,csv_url,csv_key,status,error"
    ),
  ]);

  for (const row of sources) {
    if (row.custom_id) state.sourceByCustomId.set(row.custom_id, row);
  }
  for (const row of docs) {
    if (row.custom_id) state.docsByCustomId.set(row.custom_id, row);
    if (row.original_relative_path) state.docsByRelPath.set(row.original_relative_path, row);
  }

  if (turso) {
    try {
      const result = await turso.execute(
        "SELECT granth_key, page_count, text_row_count, xlsx_url, xlsx_key, updated_at FROM ocr_granths"
      );
      for (const row of result.rows || []) {
        state.tursoByGranthKey.set(String(row.granth_key), row);
      }
    } catch (error) {
      if (!/ocr_granths|no such table|SQLITE_UNKNOWN/i.test(shortErr(error))) throw error;
    }
  }

  return state;
}

async function fetchSupabasePagesForCustomId(supabase, customId) {
  if (!supabase || !customId) return [];
  const rows = [];
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const { data, error } = await supabase
      .from(PAGES_TABLE)
      .select("page_number,text")
      .eq("custom_id", customId)
      .order("page_number", { ascending: true })
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`Supabase ${PAGES_TABLE} read failed: ${error.message}`);
    if (!data || data.length === 0) break;
    rows.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return rows;
}

async function fetchTursoPagesForGranth(db, granthKey) {
  if (!db || !granthKey) return [];
  try {
    const result = await db.execute({
      sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number",
      args: [granthKey],
    });
    return result.rows || [];
  } catch (error) {
    if (/ocr_pages|no such table|SQLITE_UNKNOWN/i.test(shortErr(error))) return [];
    throw error;
  }
}

async function resolveExistingUpload(utapi, customId, appId) {
  if (!utapi) return null;
  try {
    const response = await utapi.getFileUrls(customId, { keyType: "customId" });
    const hit = Array.isArray(response?.data) ? response.data[0] : null;
    if (hit?.url || hit?.key) {
      return {
        url: hit.url || (hit.key && appId ? `https://${appId}.ufs.sh/f/${hit.key}` : null),
        key: hit.key || null,
      };
    }
  } catch {
    return null;
  }
  return null;
}

async function uploadWithUploadThing(utapi, file, options = {}) {
  const result = await utapi.uploadFiles([file], options.uploadOptions || undefined);
  const first = Array.isArray(result) ? result[0] : result;
  if (!first) throw new Error(`UploadThing uploadFiles returned no result`);
  if (first.error) throw new Error(`UploadThing upload failed: ${shortErr(first.error)}`);
  const data = first.data || {};
  return {
    url: data.ufsUrl || null,
    key: data.key || null,
    appUrl: null,
    name: data.name || file.name,
    size: data.size || file.size,
    type: data.type || file.type,
    customId: data.customId || file.customId || null,
    fileHash: data.fileHash || null,
    lastModified: data.lastModified || file.lastModified || Date.now(),
  };
}

async function uploadLocalFile(utapi, filePath, fileName, customId, mimeType, args) {
  await waitForMemory(args, `upload ${fileName}`, 256);
  const stat = await fs.stat(filePath);
  let file;
  if (typeof fsSync.openAsBlob === "function") {
    const blob = await fsSync.openAsBlob(filePath, { type: mimeType });
    file = new UTFile([blob], fileName, {
      type: mimeType,
      customId,
      lastModified: stat.mtimeMs,
    });
  } else {
    const available = await readMemAvailableMB();
    const fileMB = Math.ceil(stat.size / 1024 / 1024);
    if (available < args.minFreeMemMB + fileMB + 512) {
      throw new Error(
        `Cannot buffer ${fileName}; fs.openAsBlob unavailable and only ${available}MB available`
      );
    }
    const buffer = await fs.readFile(filePath);
    file = new UTFile([buffer], fileName, {
      type: mimeType,
      customId,
      lastModified: stat.mtimeMs,
    });
  }
  return await uploadWithUploadThing(utapi, file);
}

async function upsertByMatch(supabase, table, matchColumn, matchValue, row) {
  const { data: existing, error: selectError } = await supabase
    .from(table)
    .select("id")
    .eq(matchColumn, matchValue)
    .limit(1)
    .maybeSingle();
  if (selectError) throw new Error(`Supabase ${table} select failed: ${selectError.message}`);

  if (existing?.id != null) {
    const { error } = await supabase.from(table).update(row).eq("id", existing.id);
    if (error) throw new Error(`Supabase ${table} update failed: ${error.message}`);
    return { id: existing.id, inserted: false };
  }

  const { data, error } = await supabase.from(table).insert([row]).select("id").single();
  if (error) throw new Error(`Supabase ${table} insert failed: ${error.message}`);
  return { id: data.id, inserted: true };
}

async function renderCover(meta, args) {
  const prefix = path.join(args.tmpDir, `${meta.granthKey}_${meta.hash}_cover`);
  await fs.mkdir(args.tmpDir, { recursive: true });
  await waitForMemory(args, `render cover ${meta.fileName}`, 384);
  await runCommand(
    "pdftoppm",
    ["-f", "1", "-l", "1", "-singlefile", "-r", String(args.coverDpi), "-jpeg", meta.filePath, prefix],
    { timeoutMs: 120000 }
  );
  return `${prefix}.jpg`;
}

async function catalogOne(meta, args, clients, remoteState) {
  const stat = await fs.stat(meta.filePath);
  const sourceRow = remoteState.sourceByCustomId.get(meta.pdfCustomId);
  let upload = sourceRow?.ufs_url
    ? { url: sourceRow.ufs_url, key: sourceRow.ut_key || null, existing: true }
    : null;

  if (!upload && args.execute && !args.skipPdfUpload) {
    const existing = await resolveExistingUpload(
      clients.utapi,
      meta.pdfCustomId,
      process.env.UPLOADTHING_APP_ID || null
    );
    if (existing?.url) {
      upload = { ...existing, existing: true };
    } else {
      upload = await uploadLocalFile(
        clients.utapi,
        meta.filePath,
        meta.fileName,
        meta.pdfCustomId,
        "application/pdf",
        args
      );
      upload.existing = false;
    }
  }

  if (!upload && args.dryRun) {
    upload = { url: null, key: null, existing: false, dryRun: true };
  }

  let cover = sourceRow?.cover_image_url
    ? { url: sourceRow.cover_image_url, key: sourceRow.cover_image_key || null, existing: true }
    : null;

  if (!cover && args.execute && !args.skipCovers) {
    const existingCover = await resolveExistingUpload(
      clients.utapi,
      meta.coverCustomId,
      process.env.UPLOADTHING_APP_ID || null
    );
    if (existingCover?.url) {
      cover = { ...existingCover, existing: true };
    } else {
      let coverPath = null;
      try {
        coverPath = await renderCover(meta, args);
        cover = await uploadLocalFile(
          clients.utapi,
          coverPath,
          `${meta.stem}_cover.jpg`,
          meta.coverCustomId,
          "image/jpeg",
          args
        );
      } finally {
        if (coverPath && !args.keepTemp) {
          await fs.unlink(coverPath).catch(() => {});
        }
      }
    }
  }

  if (args.execute) {
    const row = {
      ufs_url: upload?.url || null,
      ut_key: upload?.key || null,
      ut_url: upload?.url || null,
      app_url: null,
      file_name: meta.fileName,
      file_size: stat.size,
      file_hash: upload?.fileHash || null,
      file_type: "application/pdf",
      custom_id: meta.pdfCustomId,
      original_rel_path: meta.relPath,
      collection: meta.collection,
      subcollection: meta.subcollection,
      last_modified: Math.floor(stat.mtimeMs),
      cover: cover?.url || null,
      cover_image_url: cover?.url || null,
      cover_image_key: cover?.key || null,
    };
    await upsertByMatch(clients.supabase, SOURCE_TABLE, "custom_id", meta.pdfCustomId, row);

    const existingComplete = Boolean(
      remoteState.tursoByGranthKey.get(meta.granthKey)?.xlsx_url &&
        remoteState.tursoByGranthKey.get(meta.granthKey)?.page_count
    );
    await upsertByMatch(clients.supabase, DOCS_TABLE, "custom_id", meta.pdfCustomId, {
      original_relative_path: meta.relPath,
      custom_id: meta.pdfCustomId,
      pdf_name: meta.fileName,
      pdf_url: upload?.url || null,
      size_bytes: stat.size,
      modified_time_iso: stat.mtime.toISOString(),
      status: existingComplete ? "processed_existing_turso" : "pdf_uploaded_not_searchable",
      error: existingComplete
        ? JSON.stringify({ searchableInTurso: true, granthKey: meta.granthKey })
        : JSON.stringify({ searchableInTurso: false, workInProgress: true }),
      updated_at: new Date().toISOString(),
    });
  }

  return {
    meta,
    sizeBytes: stat.size,
    pdfUrl: upload?.url || null,
    pdfKey: upload?.key || null,
    coverUrl: cover?.url || null,
    catalogStatus: upload?.url ? (upload.existing ? "existing" : "uploaded") : "planned",
  };
}

function charMetrics(text) {
  const compact = normalizeText(text).replace(/\s+/g, "");
  let indic = 0;
  let latin = 0;
  let digit = 0;
  let bad = 0;
  for (const ch of compact) {
    const cp = ch.codePointAt(0);
    if ((cp >= 0x0900 && cp <= 0x097f) || (cp >= 0x0a80 && cp <= 0x0aff)) indic += 1;
    else if ((cp >= 65 && cp <= 90) || (cp >= 97 && cp <= 122)) latin += 1;
    else if (cp >= 48 && cp <= 57) digit += 1;
    else if ("�□■●○~`^_|\\{}<>".includes(ch)) bad += 1;
  }
  const total = compact.length;
  const useful = indic + latin + digit;
  const usefulRatio = total ? useful / total : 0;
  const badRatio = total ? bad / total : 0;
  const lengthScore = Math.min(total / 900, 1);
  const usefulScore = Math.min(usefulRatio / 0.8, 1);
  const badScore = 1 - Math.min(badRatio / 0.15, 1);
  const score = Math.max(0, Math.min(1, lengthScore * 0.35 + usefulScore * 0.55 + badScore * 0.1));
  return {
    chars: total,
    indic,
    latin,
    digit,
    usefulRatio,
    badRatio,
    score,
  };
}

function pageRowFromExisting(pageNumber, text, source) {
  const clean = normalizeText(text);
  const metrics = charMetrics(clean);
  return {
    pageNumber,
    text: clean,
    method: source,
    status: "reused_existing",
    qualityScore: Number(metrics.score.toFixed(4)),
    chars: metrics.chars,
    needsGoogle: false,
    googleReason: "existing_page_reused",
    imagePath: null,
    embeddedScore: null,
    localScore: null,
    needsReview: false,
    error: "",
    reusedExisting: true,
  };
}

function isReusableExistingPage(row, args) {
  if (!args.resumePages || !row) return false;
  if (normalizeText(row.text).length === 0) return false;
  if (Number(row.chars || 0) < args.resumeMinChars) return false;
  if (Number(row.qualityScore || 0) < args.resumeMinScore) return false;
  return true;
}

async function loadExistingPageResume(meta, totalPages, args, clients) {
  const stats = {
    supabaseRows: 0,
    tursoRows: 0,
    uniqueRows: 0,
    reusable: 0,
    lowQuality: 0,
    empty: 0,
    outOfRange: 0,
  };
  const combined = new Map();
  const reusableByPage = new Map();

  if (!args.resumePages || args.reprocess) {
    return { reusableByPage, stats };
  }

  const [supabaseRows, tursoRows] = await Promise.all([
    fetchSupabasePagesForCustomId(clients.supabase, meta.pdfCustomId),
    fetchTursoPagesForGranth(clients.turso, meta.granthKey),
  ]);
  stats.supabaseRows = supabaseRows.length;
  stats.tursoRows = tursoRows.length;

  const add = (pageNumber, text, source) => {
    const page = Number(pageNumber);
    if (!Number.isFinite(page) || page < 1 || page > totalPages) {
      stats.outOfRange += 1;
      return;
    }
    combined.set(page, pageRowFromExisting(page, text, source));
  };

  for (const row of supabaseRows) add(row.page_number, row.text, "existing_supabase");
  for (const row of tursoRows) add(row.page_number, row.content, "existing_turso");

  stats.uniqueRows = combined.size;
  for (const row of combined.values()) {
    if (isReusableExistingPage(row, args)) {
      reusableByPage.set(row.pageNumber, row);
      stats.reusable += 1;
    } else if (normalizeText(row.text).length === 0) {
      stats.empty += 1;
    } else {
      stats.lowQuality += 1;
    }
  }

  return { reusableByPage, stats };
}

function compareKey(text) {
  return normalizeText(text)
    .replace(/\s+/g, "")
    .replace(/[^\u0900-\u097Fa-zA-Z0-9]/g, "")
    .slice(0, 6000);
}

function trigramDice(aText, bText) {
  const a = compareKey(aText);
  const b = compareKey(bText);
  if (a.length < 12 || b.length < 12) return null;
  const counts = new Map();
  for (let i = 0; i < a.length - 2; i += 1) {
    const gram = a.slice(i, i + 3);
    counts.set(gram, (counts.get(gram) || 0) + 1);
  }
  let overlap = 0;
  for (let i = 0; i < b.length - 2; i += 1) {
    const gram = b.slice(i, i + 3);
    const count = counts.get(gram) || 0;
    if (count > 0) {
      overlap += 1;
      counts.set(gram, count - 1);
    }
  }
  return (2 * overlap) / (Math.max(0, a.length - 2) + Math.max(0, b.length - 2));
}

function chooseBestCandidate(candidates) {
  const usable = candidates.filter((c) => c && c.text != null);
  if (usable.length === 0) {
    return {
      method: "none",
      text: "",
      metrics: charMetrics(""),
      score: 0,
      status: "empty",
    };
  }
  usable.sort((a, b) => {
    if (b.metrics.score !== a.metrics.score) return b.metrics.score - a.metrics.score;
    return b.metrics.chars - a.metrics.chars;
  });
  return usable[0];
}

function shouldAskGoogle({ args, best, embedded, local, minUsefulChars = 80 }) {
  if (args.googleMode === "off") return { needed: false, reason: "google_off" };
  if (args.googleMode === "all") return { needed: true, reason: "google_all" };

  if (!best || best.metrics.chars === 0) return { needed: true, reason: "empty_text" };
  if (best.metrics.chars < minUsefulChars) {
    return args.googleLowTextPages
      ? { needed: true, reason: "low_text" }
      : { needed: false, reason: "low_text_skipped_for_budget" };
  }
  if (best.metrics.score < 0.68) return { needed: true, reason: "low_quality_score" };

  if (embedded?.metrics?.chars >= minUsefulChars && local?.metrics?.chars >= minUsefulChars) {
    const similarity = trigramDice(embedded.text, local.text);
    if (similarity != null && similarity < 0.45) {
      return { needed: true, reason: `embedded_local_conflict_${similarity.toFixed(3)}` };
    }
  }

  return { needed: false, reason: "local_or_embedded_confident" };
}

async function extractEmbeddedPage(meta, pageNumber, args) {
  await waitForMemory(args, `pdftotext ${meta.fileName} p${pageNumber}`, 128);
  const { stdout } = await runCommand(
    "pdftotext",
    ["-f", String(pageNumber), "-l", String(pageNumber), "-layout", "-enc", "UTF-8", meta.filePath, "-"],
    { timeoutMs: 60000 }
  );
  const text = normalizeText(stdout);
  return {
    method: "embedded",
    text,
    metrics: charMetrics(text),
    score: charMetrics(text).score,
  };
}

async function renderPage(meta, pageNumber, args) {
  await fs.mkdir(args.tmpDir, { recursive: true });
  const prefix = path.join(args.tmpDir, `${meta.granthKey}_${meta.hash}_p${pageNumber}`);
  await waitForMemory(args, `render ${meta.fileName} p${pageNumber}`, Math.ceil(384 * (args.dpi / 300)));
  await runCommand(
    "pdftoppm",
    [
      "-f",
      String(pageNumber),
      "-l",
      String(pageNumber),
      "-singlefile",
      "-r",
      String(args.dpi),
      "-png",
      meta.filePath,
      prefix,
    ],
    { timeoutMs: 180000 }
  );
  return `${prefix}.png`;
}

async function tesseractPage(imagePath, pageNumber, args) {
  await waitForMemory(args, `tesseract p${pageNumber}`, 512);
  const commandArgs = addTessdataDir([imagePath, "stdout"], args);
  commandArgs.push("-l", args.resolvedLangs, "--oem", "1", "--psm", "6");
  const { stdout } = await runCommand(
    "tesseract",
    commandArgs,
    {
      timeoutMs: args.tessTimeoutMs,
      env: {
        OMP_THREAD_LIMIT: String(args.tessThreads),
      },
    }
  );
  const text = normalizeText(stdout);
  return {
    method: "local_ocr",
    text,
    metrics: charMetrics(text),
    score: charMetrics(text).score,
  };
}

async function processPageLocal(meta, pageNumber, args) {
  const candidates = [];
  let embedded = null;
  let local = null;
  let imagePath = null;
  let error = null;

  try {
    embedded = await extractEmbeddedPage(meta, pageNumber, args);
    candidates.push(embedded);
  } catch (err) {
    error = `embedded: ${shortErr(err, 400)}`;
  }

  const skipLocal =
    embedded &&
    embedded.metrics.chars >= 500 &&
    embedded.metrics.score >= 0.92 &&
    embedded.metrics.badRatio < 0.02;

  if (!skipLocal) {
    try {
      imagePath = await renderPage(meta, pageNumber, args);
      local = await tesseractPage(imagePath, pageNumber, args);
      candidates.push(local);
    } catch (err) {
      error = [error, `local_ocr: ${shortErr(err, 400)}`].filter(Boolean).join("; ");
    }
  }

  const best = chooseBestCandidate(candidates);
  const googleDecision = shouldAskGoogle({ args, best, embedded, local });
  const status =
    googleDecision.needed || best.metrics.score < 0.68
      ? "needs_google_or_review"
      : best.metrics.chars < 80
        ? "accepted_low_text"
        : "accepted";

  return {
    pageNumber,
    text: best.text,
    method: best.method,
    status,
    qualityScore: Number(best.metrics.score.toFixed(4)),
    chars: best.metrics.chars,
    needsGoogle: googleDecision.needed,
    googleReason: googleDecision.reason,
    imagePath,
    embeddedScore: embedded ? Number(embedded.metrics.score.toFixed(4)) : null,
    localScore: local ? Number(local.metrics.score.toFixed(4)) : null,
    error,
  };
}

let documentAiClient = null;
function loadDocumentAi() {
  try {
    return require("@google-cloud/documentai");
  } catch {
    return require(path.resolve(process.cwd(), "../ocr/node_modules/@google-cloud/documentai/build/src/index.js"));
  }
}

function getDocumentAiClient(args) {
  if (documentAiClient) return documentAiClient;
  const { DocumentProcessorServiceClient } = loadDocumentAi();
  documentAiClient = new DocumentProcessorServiceClient({
    apiEndpoint: `${args.googleLocation}-documentai.googleapis.com`,
  });
  return documentAiClient;
}

async function runGoogleDocumentAi(imagePath, args) {
  const client = getDocumentAiClient(args);
  const content = await fs.readFile(imagePath, { encoding: "base64" });
  const name = args.googleProcessorVersion
    ? `projects/${args.googleProjectId}/locations/${args.googleLocation}/processors/${args.googleProcessorId}/processorVersions/${args.googleProcessorVersion}`
    : `projects/${args.googleProjectId}/locations/${args.googleLocation}/processors/${args.googleProcessorId}`;
  const request = {
    name,
    rawDocument: {
      content,
      mimeType: "image/png",
    },
  };

  if (args.googleLanguageHints.length > 0) {
    request.processOptions = {
      ocrConfig: {
        hints: {
          languageHints: args.googleLanguageHints,
        },
      },
    };
  }

  const [result] = await client.processDocument(request);

  return normalizeText(result?.document?.text || "");
}

async function applyGoogleFallbacks(pageRows, meta, args, budget, statePath, checkpointWriter = null) {
  for (const row of pageRows) {
    if (!row.needsGoogle) continue;

    if (args.dryRun) {
      row.status = "would_use_google";
      row.estimatedGoogleCostUsd = Number((args.googlePricePer1000 / 1000).toFixed(4));
      if (checkpointWriter) await checkpointWriter.write(row);
      continue;
    }

    if (!budget.canAttemptPage()) {
      row.status = "budget_exhausted";
      row.needsReview = true;
      row.error = [row.error, "Google budget exhausted before this page"].filter(Boolean).join("; ");
      if (checkpointWriter) await checkpointWriter.write(row);
      continue;
    }

    if (!row.imagePath) {
      row.imagePath = await renderPage(meta, row.pageNumber, args);
    }

    const marked = await budget.markAttempt(statePath);
    if (!marked) {
      row.status = "budget_exhausted";
      row.needsReview = true;
      if (checkpointWriter) await checkpointWriter.write(row);
      continue;
    }

    try {
      await waitForMemory(args, `google ocr ${meta.fileName} p${row.pageNumber}`, 256);
      const googleText = await runGoogleDocumentAi(row.imagePath, args);
      if (googleText) {
        row.text = googleText;
        row.method = "google_document_ai";
        row.status = "accepted_google";
        row.qualityScore = Number(charMetrics(googleText).score.toFixed(4));
        row.chars = charMetrics(googleText).chars;
        row.needsReview = false;
      } else {
        row.status = "google_empty";
        row.needsReview = true;
        row.error = [row.error, "Google returned empty text"].filter(Boolean).join("; ");
      }
    } catch (err) {
      row.status = "google_failed";
      row.needsReview = true;
      row.error = [row.error, `google: ${shortErr(err, 600)}`].filter(Boolean).join("; ");
    }
    if (checkpointWriter) await checkpointWriter.write(row);
  }
}

async function cleanupPageImages(pageRows, args) {
  if (args.keepTemp) return;
  const seen = new Set();
  for (const row of pageRows) {
    if (!row.imagePath || seen.has(row.imagePath)) continue;
    seen.add(row.imagePath);
    await fs.unlink(row.imagePath).catch(() => {});
  }
}

function rowsToWorkbookRows(meta, pageRows, pdfUrl) {
  return pageRows.map((row) => ({
    granth_key: meta.granthKey,
    book_number: meta.bookNumber || "",
    library_code: meta.libraryCode || "",
    granth_name: meta.granthName,
    source_rel_path: meta.relPath,
    pdf_url: pdfUrl || "",
    page_number: row.pageNumber,
    content: row.text || "",
    method: row.method,
    status: row.status,
    quality_score: row.qualityScore,
    chars: row.chars,
    google_reason: row.googleReason || "",
    needs_review: row.needsReview || row.status.includes("failed") || row.status.includes("exhausted"),
    embedded_score: row.embeddedScore ?? "",
    local_score: row.localScore ?? "",
    error: row.error || "",
  }));
}

function toCsv(rows) {
  if (rows.length === 0) return "";
  const headers = Object.keys(rows[0]);
  const escape = (value) => {
    const s = value == null ? "" : String(value);
    return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  return `${headers.join(",")}\n${rows
    .map((row) => headers.map((header) => escape(row[header])).join(","))
    .join("\n")}\n`;
}

function spreadsheetBase(meta) {
  return sanitizeForCustomId(`${meta.granthKey}_${meta.hash}_${meta.stem}`, 120);
}

async function writeSpreadsheetFiles(meta, pageRows, pdfUrl, args) {
  await fs.mkdir(args.outputDir, { recursive: true });
  const rows = rowsToWorkbookRows(meta, pageRows, pdfUrl);
  const base = spreadsheetBase(meta);
  const xlsxPath = path.join(args.outputDir, `${base}.xlsx`);
  const csvPath = path.join(args.outputDir, `${base}.csv`);
  const sheet = XLSX.utils.json_to_sheet(rows);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, sheet, "OCR Pages");
  XLSX.writeFile(workbook, xlsxPath);
  await fs.writeFile(csvPath, toCsv(rows));
  return { xlsxPath, csvPath, rows };
}

async function uploadSmallGeneratedFile(utapi, filePath, customId, mimeType, args) {
  await waitForMemory(args, `upload generated ${path.basename(filePath)}`, 128);
  const buffer = await fs.readFile(filePath);
  const stat = await fs.stat(filePath);
  const file = new UTFile([buffer], path.basename(filePath), {
    type: mimeType,
    customId,
    lastModified: stat.mtimeMs,
  });
  return await uploadWithUploadThing(utapi, file);
}

async function ensureTursoSchema(db) {
  const statements = [
    "PRAGMA foreign_keys = ON;",
    `CREATE TABLE IF NOT EXISTS ocr_granths (
      granth_key TEXT PRIMARY KEY,
      book_number TEXT NOT NULL,
      library_code TEXT,
      granth_name TEXT NOT NULL,
      source_rel_path TEXT NOT NULL,
      xlsx_filename TEXT NOT NULL,
      xlsx_custom_id TEXT NOT NULL,
      xlsx_url TEXT,
      xlsx_key TEXT,
      sheet_name TEXT,
      page_count INTEGER NOT NULL DEFAULT 0,
      text_row_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );`,
    `CREATE TABLE IF NOT EXISTS ocr_pages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      granth_key TEXT NOT NULL,
      page_number INTEGER NOT NULL,
      content TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (granth_key) REFERENCES ocr_granths(granth_key) ON DELETE CASCADE,
      UNIQUE (granth_key, page_number)
    );`,
    "CREATE INDEX IF NOT EXISTS idx_ocr_pages_granth_page ON ocr_pages(granth_key, page_number);",
    "CREATE INDEX IF NOT EXISTS idx_ocr_granths_book_number ON ocr_granths(book_number);",
    "CREATE INDEX IF NOT EXISTS idx_ocr_granths_name ON ocr_granths(granth_name);",
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_fts USING fts5(
      content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages',
      content_rowid='id',
      tokenize='unicode61 remove_diacritics 0'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_ai AFTER INSERT ON ocr_pages BEGIN
      INSERT INTO ocr_pages_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_ad AFTER DELETE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_fts(ocr_pages_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_au AFTER UPDATE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_fts(ocr_pages_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
  ];

  for (const sql of statements) {
    await db.execute(sql);
  }
}

async function upsertTursoPageCheckpoint(db, meta, totalPages, row) {
  const tx = await db.transaction("write");
  try {
    await tx.execute({
      sql: `INSERT INTO ocr_granths (
          granth_key,
          book_number,
          library_code,
          granth_name,
          source_rel_path,
          xlsx_filename,
          xlsx_custom_id,
          xlsx_url,
          xlsx_key,
          sheet_name,
          page_count,
          text_row_count,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 0, CURRENT_TIMESTAMP)
        ON CONFLICT(granth_key) DO UPDATE SET
          book_number = excluded.book_number,
          library_code = excluded.library_code,
          granth_name = excluded.granth_name,
          source_rel_path = excluded.source_rel_path,
          xlsx_filename = excluded.xlsx_filename,
          xlsx_custom_id = excluded.xlsx_custom_id,
          sheet_name = excluded.sheet_name,
          page_count = CASE
            WHEN ocr_granths.page_count > excluded.page_count THEN ocr_granths.page_count
            ELSE excluded.page_count
          END,
          updated_at = CURRENT_TIMESTAMP`,
      args: [
        meta.granthKey,
        meta.bookNumber || "000",
        meta.libraryCode,
        meta.granthName,
        meta.relPath,
        `${spreadsheetBase(meta)}.xlsx`,
        meta.xlsxCustomId,
        "OCR Pages",
        totalPages,
      ],
    });
    await tx.execute({
      sql: `INSERT INTO ocr_pages (granth_key, page_number, content, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(granth_key, page_number) DO UPDATE SET
              content = excluded.content,
              updated_at = CURRENT_TIMESTAMP`,
      args: [meta.granthKey, row.pageNumber, row.text || ""],
    });
    await tx.execute({
      sql: `UPDATE ocr_granths
            SET text_row_count = (
              SELECT COUNT(*) FROM ocr_pages
              WHERE granth_key = ? AND length(trim(content)) > 0
            ),
            updated_at = CURRENT_TIMESTAMP
            WHERE granth_key = ?`,
      args: [meta.granthKey, meta.granthKey],
    });
    await tx.commit();
  } catch (error) {
    try {
      if (!tx.closed) await tx.rollback();
    } catch {
      // ignore rollback failures
    }
    throw error;
  } finally {
    tx.close();
  }
}

async function upsertSupabasePageCheckpoint(supabase, customId, row) {
  await supabaseWriteWithRetry(
    `Supabase ${PAGES_TABLE} checkpoint failed`,
    () =>
      supabase.from(PAGES_TABLE).upsert(
        [
          {
            custom_id: customId,
            page_number: row.pageNumber,
            text: row.text || "",
            updated_at: new Date().toISOString(),
          },
        ],
        { onConflict: "custom_id,page_number" }
      )
  );
}

async function checkpointPageResult(clients, args, meta, totalPages, row) {
  if (!args.execute || !args.pageCheckpoints || row.reusedExisting) return;
  await Promise.all([
    clients.turso ? upsertTursoPageCheckpoint(clients.turso, meta, totalPages, row) : null,
    clients.supabase && args.supabasePageCheckpoints
      ? upsertSupabasePageCheckpoint(clients.supabase, meta.pdfCustomId, row)
      : null,
  ]);
}

function createPageCheckpointWriter(clients, args, meta, totalPages) {
  let chain = Promise.resolve();
  let checkpointed = 0;
  const warned = new Set();

  return {
    async write(row) {
      if (!args.execute || !args.pageCheckpoints || !row || row.reusedExisting) return;
      chain = chain.then(async () => {
        try {
          await checkpointPageResult(clients, args, meta, totalPages, row);
          checkpointed += 1;
        } catch (error) {
          const key = String(error instanceof Error ? error.message : error).slice(0, 160);
          if (!warned.has(key)) {
            warned.add(key);
            console.warn(`[checkpoint] failed ${meta.fileName} p${row.pageNumber}: ${shortErr(error, 500)}`);
          }
        }
      });
      await chain;
    },
    async flush() {
      await chain;
      return checkpointed;
    },
    count() {
      return checkpointed;
    },
  };
}

async function upsertTursoGranthAndPages(db, payload) {
  const tx = await db.transaction("write");
  try {
    await tx.execute({
      sql: `INSERT INTO ocr_granths (
          granth_key,
          book_number,
          library_code,
          granth_name,
          source_rel_path,
          xlsx_filename,
          xlsx_custom_id,
          xlsx_url,
          xlsx_key,
          sheet_name,
          page_count,
          text_row_count,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(granth_key) DO UPDATE SET
          book_number = excluded.book_number,
          library_code = excluded.library_code,
          granth_name = excluded.granth_name,
          source_rel_path = excluded.source_rel_path,
          xlsx_filename = excluded.xlsx_filename,
          xlsx_custom_id = excluded.xlsx_custom_id,
          xlsx_url = excluded.xlsx_url,
          xlsx_key = excluded.xlsx_key,
          sheet_name = excluded.sheet_name,
          page_count = excluded.page_count,
          text_row_count = excluded.text_row_count,
          updated_at = CURRENT_TIMESTAMP`,
      args: [
        payload.granthKey,
        payload.bookNumber,
        payload.libraryCode,
        payload.granthName,
        payload.sourceRelPath,
        payload.xlsxFilename,
        payload.xlsxCustomId,
        payload.xlsxUrl,
        payload.xlsxKey,
        payload.sheetName,
        payload.pageCount,
        payload.textRowCount,
      ],
    });

    await tx.execute({ sql: "DELETE FROM ocr_pages WHERE granth_key = ?", args: [payload.granthKey] });

    const statements = payload.pages.map((page) => ({
      sql: `INSERT INTO ocr_pages (granth_key, page_number, content, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
      args: [payload.granthKey, page.pageNumber, page.content || ""],
    }));

    for (let i = 0; i < statements.length; i += INSERT_BATCH_SIZE) {
      const chunk = statements.slice(i, i + INSERT_BATCH_SIZE);
      if (chunk.length > 0) await tx.batch(chunk);
    }

    await tx.commit();
  } catch (error) {
    try {
      if (!tx.closed) await tx.rollback();
    } catch {
      // ignore rollback failures
    }
    throw error;
  } finally {
    tx.close();
  }
}

async function upsertSupabasePages(supabase, customId, meta, pageRows) {
  const rows = pageRows.map((page) => ({
    custom_id: customId,
    page_number: page.pageNumber,
    text: page.text || "",
    updated_at: new Date().toISOString(),
  }));

  for (let i = 0; i < rows.length; i += SUPABASE_PAGE_BATCH_SIZE) {
    const chunk = rows.slice(i, i + SUPABASE_PAGE_BATCH_SIZE);
    await supabaseWriteWithRetry(
      `Supabase document_pages upsert failed for ${meta.fileName} rows ${i + 1}-${i + chunk.length}`,
      () =>
        supabase.from(PAGES_TABLE).upsert(chunk, {
          onConflict: "custom_id,page_number",
        })
    );
  }
}

function summarizeBook(pageRows, totalPages) {
  const byStatus = new Map();
  const byMethod = new Map();
  let reviewPages = 0;
  let textPages = 0;
  for (const row of pageRows) {
    byStatus.set(row.status, (byStatus.get(row.status) || 0) + 1);
    byMethod.set(row.method, (byMethod.get(row.method) || 0) + 1);
    if (row.needsReview || row.status.includes("failed") || row.status.includes("exhausted")) reviewPages += 1;
    if (normalizeText(row.text).length > 0) textPages += 1;
  }
  const budgetExhausted = byStatus.get("budget_exhausted") || 0;
  const googleFailed = (byStatus.get("google_failed") || 0) + (byStatus.get("google_empty") || 0);
  let documentStatus = "processed";
  if (budgetExhausted > 0) documentStatus = "partial_searchable_budget_exhausted";
  else if (googleFailed > 0) documentStatus = "processed_with_google_errors";
  else if (reviewPages > 0) documentStatus = "processed_with_review_pages";
  if (textPages === 0) documentStatus = "failed";
  return {
    totalPages,
    processedPages: pageRows.length,
    textPages,
    reviewPages,
    budgetExhausted,
    googleFailed,
    byStatus: Object.fromEntries(byStatus),
    byMethod: Object.fromEntries(byMethod),
    documentStatus,
  };
}

function isExistingComplete(meta, pageCount, remoteState) {
  const row = remoteState.tursoByGranthKey.get(meta.granthKey);
  if (!row?.xlsx_url) return false;
  const existingPages = Number(row.page_count || 0);
  return existingPages >= pageCount;
}

async function processTextOne(catalog, args, clients, remoteState, budget, statePath) {
  const meta = catalog.meta;
  const info = await pdfInfo(meta.filePath);
  const totalPages = args.maxPages ? Math.min(args.maxPages, info.pageCount) : info.pageCount;

  if (!args.reprocess && isExistingComplete(meta, totalPages, remoteState)) {
    return {
      meta,
      skipped: true,
      reason: "existing_turso_xlsx_and_pages",
      totalPages,
      documentStatus: "processed_existing_turso",
    };
  }

  if (args.dryRun && !args.maxPages) {
    return {
      meta,
      skipped: true,
      reason: "dry_run_without_maxPages_only_plans_text",
      totalPages,
      documentStatus: "planned",
    };
  }

  const resume = await loadExistingPageResume(meta, totalPages, args, clients);
  if (args.resumePages && !args.reprocess) {
    console.log(
      `[text] resume ${meta.fileName}: reusable=${resume.stats.reusable}/${totalPages} existingUnique=${resume.stats.uniqueRows} lowQuality=${resume.stats.lowQuality} empty=${resume.stats.empty}`
    );
  }

  const pageNumbers = Array.from({ length: totalPages }, (_, idx) => idx + 1);
  const pagesToProcess = pageNumbers.length - resume.stats.reusable;
  console.log(`[text] ${meta.fileName}: local/google pass ${pagesToProcess} pages, reused ${resume.stats.reusable}`);
  const checkpointWriter = createPageCheckpointWriter(clients, args, meta, totalPages);

  const pageRows = await mapLimit(pageNumbers, args.pageConcurrency, async (pageNumber) => {
    const existing = resume.reusableByPage.get(pageNumber);
    if (existing) return existing;

    const row = await processPageLocal(meta, pageNumber, args);
    if (args.verbose) {
      console.log(
        `[page] ${meta.granthKey} p${pageNumber} method=${row.method} status=${row.status} score=${row.qualityScore}`
      );
    }
    if (!row.needsGoogle) await checkpointWriter.write(row);
    return row;
  });

  await applyGoogleFallbacks(pageRows, meta, args, budget, statePath, checkpointWriter);
  const checkpointedPages = await checkpointWriter.flush();
  await cleanupPageImages(pageRows, args);

  const summary = summarizeBook(pageRows, totalPages);
  summary.reusedExistingPages = resume.stats.reusable;
  summary.checkpointedPages = checkpointedPages;
  const { xlsxPath, csvPath } = await writeSpreadsheetFiles(meta, pageRows, catalog.pdfUrl, args);

  let xlsxUpload = null;
  let csvUpload = null;
  if (args.execute) {
    xlsxUpload = await uploadSmallGeneratedFile(
      clients.utapi,
      xlsxPath,
      meta.xlsxCustomId,
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      args
    );
    csvUpload = await uploadSmallGeneratedFile(clients.utapi, csvPath, meta.csvCustomId, "text/csv", args);

    await ensureTursoSchema(clients.turso);
    await upsertTursoGranthAndPages(clients.turso, {
      granthKey: meta.granthKey,
      bookNumber: meta.bookNumber || "000",
      libraryCode: meta.libraryCode,
      granthName: meta.granthName,
      sourceRelPath: meta.relPath,
      xlsxFilename: path.basename(xlsxPath),
      xlsxCustomId: meta.xlsxCustomId,
      xlsxUrl: xlsxUpload?.url || null,
      xlsxKey: xlsxUpload?.key || null,
      sheetName: "OCR Pages",
      pageCount: totalPages,
      textRowCount: pageRows.filter((row) => normalizeText(row.text).length > 0).length,
      pages: pageRows.map((row) => ({ pageNumber: row.pageNumber, content: row.text || "" })),
    });

    if (args.supabasePageSync) {
      await upsertSupabasePages(clients.supabase, meta.pdfCustomId, meta, pageRows);
    }
    await upsertByMatch(clients.supabase, DOCS_TABLE, "custom_id", meta.pdfCustomId, {
      original_relative_path: meta.relPath,
      custom_id: meta.pdfCustomId,
      pdf_name: meta.fileName,
      pdf_url: catalog.pdfUrl || null,
      size_bytes: catalog.sizeBytes || null,
      modified_time_iso: (await fs.stat(meta.filePath)).mtime.toISOString(),
      csv_url: csvUpload?.url || null,
      csv_key: csvUpload?.key || null,
      status: summary.documentStatus,
      error: summary.documentStatus === "processed" ? null : JSON.stringify(summary),
      updated_at: new Date().toISOString(),
    });
  }

  return {
    meta,
    skipped: false,
    totalPages,
    documentStatus: summary.documentStatus,
    summary,
    xlsxPath,
    csvPath,
    xlsxUrl: xlsxUpload?.url || null,
    csvUrl: csvUpload?.url || null,
  };
}

async function buildCatalogPlan(args) {
  await waitForMemory(args, "scan source PDFs", 0);
  const allPdfs = await walkPdfs(args.sourceRoot);
  let selected = allPdfs.map((filePath) => derivePdfMetadata(filePath, args.sourceRoot));
  if (args.bookNumber) {
    selected = selected.filter((meta) => meta.bookNumber === args.bookNumber);
  }
  if (args.startAt > 0) selected = selected.slice(args.startAt);
  if (args.limit != null) selected = selected.slice(0, args.limit);
  return { allCount: allPdfs.length, selected };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    usage();
    return;
  }

  await waitForMemory(args, "startup", 0);
  await ensureTools();
  await ensureCombinedTessdataDir(args);
  args.resolvedLangs = await resolveTesseractLangs(args);
  console.log(
    `[ocr] Tesseract langs requested=${args.langs} resolved=${args.resolvedLangs} tessdataDir=${args.tessdataDir}`
  );
  console.log(
    `[ocr] Google language hints=${args.googleLanguageHints.length ? args.googleLanguageHints.join(",") : "auto"}`
  );

  const sourceStat = await fs.stat(args.sourceRoot).catch((error) => {
    throw new Error(`Cannot read --sourceRoot ${args.sourceRoot}: ${error.message}`);
  });
  if (!sourceStat.isDirectory()) throw new Error(`--sourceRoot is not a directory: ${args.sourceRoot}`);

  const supabase = makeSupabase();
  const turso = makeTurso();
  const utapi = makeUploadThing();
  const needsCatalog = args.phase === "all" || args.phase === "catalog";
  const needsText = args.phase === "all" || args.phase === "text";
  const needsUploads = args.execute && (needsCatalog || needsText);
  const clients = {
    supabase: requireClient(supabase, "Supabase", args.execute),
    turso: requireClient(turso, "Turso", args.execute && needsText),
    utapi: requireClient(utapi, "UploadThing", needsUploads),
  };
  if (args.execute && needsText && clients.turso) {
    await ensureTursoSchema(clients.turso);
  }

  if (!args.execute) {
    console.log("[mode] dry-run: no UploadThing writes, database writes, or Google OCR calls will run");
  }

  const statePath = path.join(args.stateDir, "state.json");
  const state = await readJsonIfExists(statePath, {
    createdAt: new Date().toISOString(),
    books: {},
    google: {},
  });
  const budget = new GoogleBudget(args, state);
  await writeJsonAtomic(statePath, state);

  console.log(
    `[budget] Google cap=$${args.googleBudgetUsd.toFixed(2)} price=$${args.googlePricePer1000}/1000 pages maxPaidPages=${budget.maxPaidPages} alreadyAttempted=${budget.attemptedPages()}`
  );

  const plan = await buildCatalogPlan(args);
  const totalSize = plan.selected.reduce((acc, meta) => acc + fsSync.statSync(meta.filePath).size, 0);
  console.log(
    `[plan] Found ${plan.allCount} PDFs, selected ${plan.selected.length}, selected size ${bytesToHuman(totalSize)}`
  );
  if (plan.selected.length === 0) return;

  const remoteState = await loadRemoteState({
    supabase: clients.supabase,
    turso: clients.turso,
    remoteReads: args.remoteReads && Boolean(clients.supabase || clients.turso),
  });

  const catalogResults = [];
  if (needsCatalog) {
    console.log(`[catalog] Starting catalog phase for ${plan.selected.length} PDFs`);
    const results = await mapLimit(plan.selected, args.catalogConcurrency, async (meta, index) => {
      console.log(`[catalog] ${index + 1}/${plan.selected.length} ${meta.fileName}`);
      try {
        return await catalogOne(meta, args, clients, remoteState);
      } catch (error) {
        console.error(`[catalog] failed ${meta.fileName}: ${shortErr(error, 600)}`);
        return { meta, catalogStatus: "failed", error: shortErr(error) };
      }
    });
    catalogResults.push(...results);
  } else {
    for (const meta of plan.selected) {
      const stat = await fs.stat(meta.filePath);
      const source = remoteState.sourceByCustomId.get(meta.pdfCustomId);
      catalogResults.push({
        meta,
        sizeBytes: stat.size,
        pdfUrl: source?.ufs_url || null,
        pdfKey: source?.ut_key || null,
        coverUrl: source?.cover_image_url || null,
        catalogStatus: source?.ufs_url ? "existing" : "not_cataloged_in_this_run",
      });
    }
  }

  const textResults = [];
  if (needsText) {
    console.log(`[text] Starting text phase for ${catalogResults.length} PDFs`);
    for (let i = 0; i < catalogResults.length; i += 1) {
      const catalog = catalogResults[i];
      if (!catalog || catalog.catalogStatus === "failed") {
        textResults.push({
          meta: catalog?.meta,
          skipped: true,
          reason: "catalog_failed",
          documentStatus: "failed",
        });
        continue;
      }

      console.log(`[text] ${i + 1}/${catalogResults.length} ${catalog.meta.fileName}`);
      try {
        const result = await processTextOne(catalog, args, clients, remoteState, budget, statePath);
        textResults.push(result);
        state.books[catalog.meta.granthKey] = {
          relPath: catalog.meta.relPath,
          status: result.documentStatus,
          totalPages: result.totalPages,
          skipped: result.skipped,
          updatedAt: new Date().toISOString(),
        };
        state.google = { ...state.google, ...budget.summary() };
        state.updatedAt = new Date().toISOString();
        await writeJsonAtomic(statePath, state);
        console.log(
          `[text] done ${catalog.meta.fileName}: status=${result.documentStatus} paidPages=${budget.attemptedPages()} estGoogle=$${budget.estimatedSpendUsd().toFixed(4)}`
        );
      } catch (error) {
        console.error(`[text] failed ${catalog.meta.fileName}: ${shortErr(error, 800)}`);
        textResults.push({
          meta: catalog.meta,
          skipped: false,
          documentStatus: "failed",
          error: shortErr(error),
        });
      }
    }
  }

  const catalogCounts = catalogResults.reduce((acc, row) => {
    const key = row.catalogStatus || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const textCounts = textResults.reduce((acc, row) => {
    const key = row.documentStatus || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  const runSummary = {
    dryRun: args.dryRun,
    phase: args.phase,
    sourceRoot: args.sourceRoot,
    selectedPdfs: plan.selected.length,
    selectedSizeBytes: totalSize,
    catalogCounts,
    textCounts,
    googleBudget: budget.summary(),
    generatedAt: new Date().toISOString(),
  };
  await writeJsonAtomic(path.join(args.stateDir, "last_summary.json"), runSummary);

  console.log("[summary]");
  console.log(JSON.stringify(runSummary, null, 2));
}

main().catch((error) => {
  console.error(shortErr(error));
  process.exit(1);
});
