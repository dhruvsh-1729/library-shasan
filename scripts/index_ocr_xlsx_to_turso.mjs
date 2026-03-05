#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import path from "node:path";
import { createClient as createTursoClient } from "@libsql/client";
import { UTApi, UTFile } from "uploadthing/server";
import XLSX from "xlsx";

const DATA_ROOT = path.join(process.cwd(), "data");
const DATA_BUCKETS = ["1", "2", "3"].map((name) => path.join(DATA_ROOT, name));
const INSERT_BATCH_SIZE = 150;

function usage() {
  console.log(`Usage: node scripts/index_ocr_xlsx_to_turso.mjs [options]

Options:
  --limit N         Process at most N selected granths (default: all)
  --startAt N       Skip first N selected granths (default: 0)
  --dry-run         Parse and plan only; no upload and no DB writes
  --verbose         Print detailed logs
  --help            Show help
`);
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function parseIntFlag(flagName, raw, min) {
  if (raw == null || raw === "") throw new Error(`${flagName} requires a numeric value`);
  const value = Number.parseInt(String(raw), 10);
  if (!Number.isFinite(value) || Number.isNaN(value) || value < min) {
    throw new Error(`${flagName} must be an integer >= ${min}`);
  }
  return value;
}

function parseArgs(argv) {
  const args = {
    limit: null,
    startAt: 0,
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

    throw new Error(`Unknown argument: ${arg}`);
  }

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

function sanitizeForCustomId(value) {
  return String(value)
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 160);
}

function normalizeStem(fileName) {
  let stem = String(fileName).replace(/\.xlsx$/i, "");
  stem = stem.replace(/\.pdf$/i, "");
  stem = stem.replace(/\(\d+\)$/i, "");
  return stem.trim();
}

function prettifyName(value) {
  return String(value)
    .replace(/[_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function deriveMetadata(filePath) {
  const baseName = path.basename(filePath);
  const stem = normalizeStem(baseName);
  const relPath = path.relative(DATA_ROOT, filePath).replaceAll(path.sep, "/");

  const prefixed = stem.match(/^(\d{2,4})[_\s-]+(.+)$/);
  const firstNumeric = stem.match(/^(\d{2,4})\b/);
  const bookNumber = prefixed?.[1] ?? firstNumeric?.[1] ?? "000";

  const underscoreParts = stem.split("_");
  let libraryCode = null;
  let granthNameRaw = stem;
  let granthKey = bookNumber;

  if (underscoreParts.length >= 2 && /^\d{2,4}$/.test(underscoreParts[0])) {
    const maybeCode = underscoreParts[1];
    const codeLooksReal = /^[a-zA-Z]\d{4,}$/.test(maybeCode);

    if (codeLooksReal) {
      libraryCode = maybeCode.toUpperCase();
      granthKey = `${underscoreParts[0]}_${libraryCode}`;
      granthNameRaw = underscoreParts.slice(2).join("_") || maybeCode;
    } else {
      granthKey = underscoreParts[0];
      granthNameRaw = underscoreParts.slice(1).join("_") || stem;
    }
  } else if (prefixed) {
    granthKey = prefixed[1];
    granthNameRaw = prefixed[2];
  }

  const granthName = prettifyName(granthNameRaw || stem) || stem;

  return {
    relPath,
    filePath,
    fileName: baseName,
    stem,
    bookNumber,
    libraryCode,
    granthName,
    granthKey,
  };
}

async function findAllXlsxFiles() {
  const out = [];

  async function walk(dirPath) {
    let entries = [];
    try {
      entries = await fs.readdir(dirPath, { withFileTypes: true });
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        return;
      }
      throw error;
    }

    for (const entry of entries) {
      const fullPath = path.join(dirPath, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".xlsx")) {
        out.push(fullPath);
      }
    }
  }

  for (const dirPath of DATA_BUCKETS) {
    await walk(dirPath);
  }

  return out.sort((a, b) => a.localeCompare(b, "en"));
}

function normalizeHeader(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

function parsePageNumber(value) {
  const raw = String(value ?? "").trim().replaceAll(",", "");
  if (!raw) return null;
  const exact = raw.match(/^(\d+)(?:\.0+)?$/);
  if (exact) return Number.parseInt(exact[1], 10);
  const lead = raw.match(/^(\d+)/);
  if (lead) return Number.parseInt(lead[1], 10);
  return null;
}

function normalizeCellText(value) {
  return String(value ?? "")
    .replace(/\u0000/g, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
}

function chooseColumns(headerRow) {
  const normalized = headerRow.map((v) => normalizeHeader(v));
  const pageCandidates = new Set(["page number", "page no", "page", "column 1", "pg no"]);
  const textCandidates = new Set(["content", "text", "column 2"]);

  let pageCol = null;
  let textCol = null;

  for (let idx = 0; idx < normalized.length; idx += 1) {
    const header = normalized[idx];
    if (pageCol == null && pageCandidates.has(header)) pageCol = idx;
    if (textCol == null && textCandidates.has(header)) textCol = idx;
  }

  const nonEmptyCols = [];
  for (let idx = 0; idx < headerRow.length; idx += 1) {
    if (String(headerRow[idx] ?? "").trim() !== "") {
      nonEmptyCols.push(idx);
    }
  }

  if (pageCol == null) pageCol = nonEmptyCols[0] ?? 0;
  if (textCol == null) textCol = nonEmptyCols[1] ?? 1;

  return { pageCol, textCol };
}

function sheetRows(sheet) {
  return XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    raw: false,
    blankrows: false,
    defval: "",
  });
}

function analyzeWorkbook(filePath, includePages) {
  const workbook = XLSX.readFile(filePath, { raw: false, cellText: true, dense: false });
  if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
    return {
      sheetName: null,
      textRowCount: 0,
      pageCount: 0,
      missingPageTextRows: 0,
      pages: includePages ? [] : null,
    };
  }

  let bestSheetName = workbook.SheetNames[0];
  let bestRows = [];
  let bestNonEmpty = -1;

  for (const sheetName of workbook.SheetNames) {
    const sheet = workbook.Sheets[sheetName];
    const rows = sheetRows(sheet);
    const nonEmpty = rows.reduce((acc, row) => {
      if (!Array.isArray(row)) return acc;
      return row.some((cell) => String(cell ?? "").trim() !== "") ? acc + 1 : acc;
    }, 0);

    if (nonEmpty > bestNonEmpty || (nonEmpty === bestNonEmpty && rows.length > bestRows.length)) {
      bestSheetName = sheetName;
      bestRows = rows;
      bestNonEmpty = nonEmpty;
    }
  }

  if (bestRows.length === 0) {
    return {
      sheetName: bestSheetName,
      textRowCount: 0,
      pageCount: 0,
      missingPageTextRows: 0,
      pages: includePages ? [] : null,
    };
  }

  let headerIndex = -1;
  for (let i = 0; i < bestRows.length; i += 1) {
    const row = bestRows[i];
    if (Array.isArray(row) && row.some((cell) => String(cell ?? "").trim() !== "")) {
      headerIndex = i;
      break;
    }
  }

  if (headerIndex < 0) {
    return {
      sheetName: bestSheetName,
      textRowCount: 0,
      pageCount: 0,
      missingPageTextRows: 0,
      pages: includePages ? [] : null,
    };
  }

  const headerRow = Array.isArray(bestRows[headerIndex]) ? bestRows[headerIndex] : [];
  const { pageCol, textCol } = chooseColumns(headerRow);

  let textRowCount = 0;
  let missingPageTextRows = 0;
  const pageBucket = new Map();

  for (let i = headerIndex + 1; i < bestRows.length; i += 1) {
    const row = Array.isArray(bestRows[i]) ? bestRows[i] : [];
    const text = normalizeCellText(row[textCol]);
    if (!text) continue;
    textRowCount += 1;

    const pageNumber = parsePageNumber(row[pageCol]);
    if (pageNumber == null) {
      missingPageTextRows += 1;
      continue;
    }

    const existing = pageBucket.get(pageNumber);
    if (existing) {
      existing.push(text);
    } else {
      pageBucket.set(pageNumber, [text]);
    }
  }

  const sortedPages = [...pageBucket.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([pageNumber, parts]) => ({
      pageNumber,
      content: parts.join("\n"),
    }));

  return {
    sheetName: bestSheetName,
    textRowCount,
    pageCount: sortedPages.length,
    missingPageTextRows,
    pages: includePages ? sortedPages : null,
  };
}

function pickBestCandidates(candidatesByKey, verbose) {
  const selected = [];
  const duplicateGroups = [];

  for (const [granthKey, candidates] of candidatesByKey.entries()) {
    candidates.sort((a, b) => {
      if (b.analysis.textRowCount !== a.analysis.textRowCount) {
        return b.analysis.textRowCount - a.analysis.textRowCount;
      }
      if (b.analysis.pageCount !== a.analysis.pageCount) {
        return b.analysis.pageCount - a.analysis.pageCount;
      }
      if (b.sizeBytes !== a.sizeBytes) {
        return b.sizeBytes - a.sizeBytes;
      }
      return a.meta.relPath.localeCompare(b.meta.relPath, "en");
    });

    const [best, ...rest] = candidates;
    selected.push(best);
    if (rest.length > 0) {
      duplicateGroups.push({ granthKey, best, rest });
    }
  }

  selected.sort((a, b) => {
    const aNum = Number.parseInt(a.meta.bookNumber, 10);
    const bNum = Number.parseInt(b.meta.bookNumber, 10);
    if (Number.isFinite(aNum) && Number.isFinite(bNum) && aNum !== bNum) return aNum - bNum;
    return a.meta.granthKey.localeCompare(b.meta.granthKey, "en");
  });

  if (verbose && duplicateGroups.length > 0) {
    console.log(`Found ${duplicateGroups.length} duplicate key groups; selected highest-yield candidate in each.`);
    for (const group of duplicateGroups) {
      console.log(`  • ${group.granthKey}`);
      console.log(
        `    selected: ${group.best.meta.relPath} (textRows=${group.best.analysis.textRowCount}, pages=${group.best.analysis.pageCount})`
      );
      for (const drop of group.rest) {
        console.log(
          `    skipped : ${drop.meta.relPath} (textRows=${drop.analysis.textRowCount}, pages=${drop.analysis.pageCount})`
        );
      }
    }
  }

  return { selected, duplicateGroups };
}

async function resolveExistingUpload(utapi, customId, appId) {
  try {
    const response = await utapi.getFileUrls(customId, { keyType: "customId" });
    const hit = Array.isArray(response?.data) ? response.data[0] : null;
    if (hit?.url || hit?.key) {
      return {
        url: hit.url ?? (appId ? `https://${appId}.ufs.sh/f/${customId}` : null),
        key: hit.key ?? null,
      };
    }
  } catch {
    // ignore lookup failures
  }

  if (!appId) return null;
  return { url: `https://${appId}.ufs.sh/f/${customId}`, key: null };
}

async function uploadXlsx(utapi, filePath, customId, appId) {
  const buffer = await fs.readFile(filePath);
  const fileName = path.basename(filePath);
  const utFile = new UTFile([buffer], fileName, {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    customId,
    lastModified: Date.now(),
  });

  const result = await utapi.uploadFiles([utFile]);
  const first = Array.isArray(result) ? result[0] : result;
  if (!first) throw new Error(`uploadFiles returned no response`);

  if (first.error) {
    const message = shortErr(first.error);
    const existing = await resolveExistingUpload(utapi, customId, appId);
    if (existing && existing.url) {
      return { url: existing.url, key: existing.key };
    }
    throw new Error(`UploadThing XLSX upload failed: ${message}`);
  }

  const data = first.data || {};
  return {
    url: data.ufsUrl ?? data.url ?? null,
    key: data.key ?? null,
  };
}

async function ensureSchema(db) {
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

async function upsertGranthAndPages(db, payload) {
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
        payload.fileName,
        payload.xlsxCustomId,
        payload.xlsxUrl,
        payload.xlsxKey,
        payload.sheetName,
        payload.pageCount,
        payload.textRowCount,
      ],
    });

    await tx.execute({
      sql: "DELETE FROM ocr_pages WHERE granth_key = ?",
      args: [payload.granthKey],
    });

    const statements = [];
    for (const page of payload.pages) {
      statements.push({
        sql: `INSERT INTO ocr_pages (granth_key, page_number, content, updated_at)
              VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
        args: [payload.granthKey, page.pageNumber, page.content],
      });
    }

    for (let i = 0; i < statements.length; i += INSERT_BATCH_SIZE) {
      const chunk = statements.slice(i, i + INSERT_BATCH_SIZE);
      if (chunk.length > 0) {
        await tx.batch(chunk);
      }
    }

    await tx.commit();
  } catch (error) {
    try {
      if (!tx.closed) {
        await tx.rollback();
      }
    } catch {
      // ignore rollback failures when transaction is already closed
    }
    throw error;
  } finally {
    tx.close();
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    usage();
    return;
  }

  const TURSO_URL = requireEnv("TURSO_URL");
  const TURSO_AUTH_TOKEN = requireEnv("TURSO_AUTH_TOKEN");
  const UPLOADTHING_TOKEN = requireEnv("UPLOADTHING_TOKEN");
  const UPLOADTHING_APP_ID = process.env.UPLOADTHING_APP_ID || null;

  const allXlsx = await findAllXlsxFiles();
  if (allXlsx.length === 0) {
    console.log(`No .xlsx files found under data/1, data/2, data/3`);
    return;
  }

  console.log(`Found ${allXlsx.length} XLSX files under data/1..3`);
  console.log(`Analyzing files and selecting unique granths...`);

  const byKey = new Map();

  for (const filePath of allXlsx) {
    const meta = deriveMetadata(filePath);
    const analysis = analyzeWorkbook(filePath, false);
    const fileStat = await fs.stat(filePath);

    const candidate = {
      meta,
      analysis,
      sizeBytes: fileStat.size,
    };

    const existing = byKey.get(meta.granthKey);
    if (existing) {
      existing.push(candidate);
    } else {
      byKey.set(meta.granthKey, [candidate]);
    }
  }

  const { selected, duplicateGroups } = pickBestCandidates(byKey, args.verbose);
  console.log(
    `Selected ${selected.length} unique granth files (raw=${allXlsx.length}, duplicatesResolved=${duplicateGroups.length})`
  );

  const start = Math.min(args.startAt, selected.length);
  const end = args.limit == null ? selected.length : Math.min(selected.length, start + args.limit);
  const queue = selected.slice(start, end);
  console.log(`Queue size=${queue.length} (startAt=${args.startAt}, limit=${args.limit ?? "all"})`);

  if (queue.length === 0) {
    console.log(`Nothing to process.`);
    return;
  }

  if (args.dryRun) {
    console.log(`DRY RUN sample:`);
    for (const [idx, item] of queue.slice(0, 12).entries()) {
      console.log(
        `[${idx + 1}] key=${item.meta.granthKey} book=${item.meta.bookNumber} code=${item.meta.libraryCode ?? "-"} name="${item.meta.granthName}" file="${item.meta.relPath}"`
      );
    }
    return;
  }

  const db = createTursoClient({
    url: TURSO_URL,
    authToken: TURSO_AUTH_TOKEN,
  });
  const utapi = new UTApi({ token: UPLOADTHING_TOKEN });

  await ensureSchema(db);
  console.log(`Turso schema verified.`);

  let success = 0;
  let failed = 0;

  for (let i = 0; i < queue.length; i += 1) {
    const item = queue[i];
    const { meta } = item;
    const label = `[${i + 1}/${queue.length}] ${meta.granthKey} ${meta.granthName}`;

    try {
      const full = analyzeWorkbook(meta.filePath, true);
      const pages = Array.isArray(full.pages) ? full.pages : [];
      const xlsxCustomId = sanitizeForCustomId(`${meta.granthKey}__ocr_xlsx`);

      if (args.verbose) {
        console.log(
          `${label} parsed sheet="${full.sheetName ?? "-"}" pages=${full.pageCount} textRows=${full.textRowCount} missingPageTextRows=${full.missingPageTextRows}`
        );
      } else {
        console.log(`${label} uploading + storing...`);
      }

      const uploaded = await uploadXlsx(utapi, meta.filePath, xlsxCustomId, UPLOADTHING_APP_ID);

      await upsertGranthAndPages(db, {
        granthKey: meta.granthKey,
        bookNumber: meta.bookNumber,
        libraryCode: meta.libraryCode,
        granthName: meta.granthName,
        sourceRelPath: meta.relPath,
        fileName: meta.fileName,
        xlsxCustomId,
        xlsxUrl: uploaded.url,
        xlsxKey: uploaded.key,
        sheetName: full.sheetName,
        pageCount: full.pageCount,
        textRowCount: full.textRowCount,
        pages,
      });

      success += 1;
      console.log(`${label} done (pages=${full.pageCount})`);
    } catch (error) {
      failed += 1;
      console.error(`${label} failed: ${shortErr(error)}`);
    }
  }

  console.log(`Finished. success=${success}, failed=${failed}, total=${queue.length}`);
}

main().catch((error) => {
  console.error(shortErr(error));
  process.exit(1);
});
