#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import { createClient as createTursoClient } from "@libsql/client";

const DEFAULT_BATCH_SIZE = 75;
const DEFAULT_MIN_FREE_MEMORY_MB = 1024;
const OCR_WORD_TOKEN_PATTERN = /[\p{L}\p{N}\p{M}_]+/gu;
const OCR_GRAPHEME_SEGMENTER =
  typeof Intl !== "undefined" && typeof Intl.Segmenter === "function"
    ? new Intl.Segmenter(undefined, { granularity: "grapheme" })
    : null;

function usage() {
  console.log(`Usage: node scripts/ensure_turso_search_indexes.mjs [options]

Options:
  --execute                Create/rebuild Turso search indexes
  --batchSize N            Pages per suffix-index batch (default: ${DEFAULT_BATCH_SIZE})
  --minFreeMemoryMb N      Stop if local MemAvailable drops below N MB (default: ${DEFAULT_MIN_FREE_MEMORY_MB})
  --skipFtsRebuild         Skip normal/trigram FTS rebuilds
  --skipSuffix             Skip suffix-index population
  --help                   Show help
`);
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
    execute: false,
    batchSize: DEFAULT_BATCH_SIZE,
    minFreeMemoryMb: DEFAULT_MIN_FREE_MEMORY_MB,
    skipFtsRebuild: false,
    skipSuffix: false,
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
      continue;
    }
    if (arg === "--skipFtsRebuild") {
      args.skipFtsRebuild = true;
      continue;
    }
    if (arg === "--skipSuffix") {
      args.skipSuffix = true;
      continue;
    }
    if (arg === "--batchSize" || arg.startsWith("--batchSize=")) {
      const raw = arg.includes("=") ? arg.slice("--batchSize=".length) : argv[++i];
      args.batchSize = parseIntFlag("--batchSize", raw, 1);
      continue;
    }
    if (arg === "--minFreeMemoryMb" || arg.startsWith("--minFreeMemoryMb=")) {
      const raw = arg.includes("=") ? arg.slice("--minFreeMemoryMb=".length) : argv[++i];
      args.minFreeMemoryMb = parseIntFlag("--minFreeMemoryMb", raw, 128);
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

async function readAvailableMemoryMb() {
  const meminfo = await fs.readFile("/proc/meminfo", "utf8");
  const match = meminfo.match(/^MemAvailable:\s+(\d+)\s+kB/m);
  if (!match) return null;
  return Math.floor(Number(match[1]) / 1024);
}

async function assertMemory(label, minFreeMemoryMb) {
  const available = await readAvailableMemoryMb();
  if (available == null) {
    console.log(`[memory] ${label}: MemAvailable unavailable`);
    return;
  }
  console.log(`[memory] ${label}: ${available} MB available`);
  if (available < minFreeMemoryMb) {
    throw new Error(
      `Stopping before ${label}: available memory ${available} MB is below required ${minFreeMemoryMb} MB`
    );
  }
}

function reverseOCRGraphemes(input) {
  const value = String(input || "");
  if (!value) return "";
  if (!OCR_GRAPHEME_SEGMENTER) return Array.from(value).reverse().join("");
  return Array.from(OCR_GRAPHEME_SEGMENTER.segment(value), (segment) => segment.segment).reverse().join("");
}

function buildOCRSuffixIndexContent(content) {
  const tokens = String(content || "").match(OCR_WORD_TOKEN_PATTERN) || [];
  return tokens.map(reverseOCRGraphemes).join(" ");
}

async function ensureSchema(db) {
  const statements = [
    "PRAGMA foreign_keys = ON;",
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_search_fts USING fts5(
      content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages',
      content_rowid='id',
      tokenize="unicode61 remove_diacritics 0 categories 'L* N* Co M*'",
      prefix='2 3 4'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_search_ai AFTER INSERT ON ocr_pages BEGIN
      INSERT INTO ocr_pages_search_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_search_ad AFTER DELETE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_search_fts(ocr_pages_search_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_search_au AFTER UPDATE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_search_fts(ocr_pages_search_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_search_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_trigram_fts USING fts5(
      content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages',
      content_rowid='id',
      tokenize='trigram'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_trigram_ai AFTER INSERT ON ocr_pages BEGIN
      INSERT INTO ocr_pages_trigram_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_trigram_ad AFTER DELETE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_trigram_fts(ocr_pages_trigram_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_trigram_au AFTER UPDATE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_trigram_fts(ocr_pages_trigram_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_trigram_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TABLE IF NOT EXISTS ocr_pages_suffix (
      page_id INTEGER PRIMARY KEY,
      granth_key TEXT NOT NULL,
      page_number INTEGER NOT NULL,
      reversed_content TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (page_id) REFERENCES ocr_pages(id) ON DELETE CASCADE,
      UNIQUE (granth_key, page_number)
    );`,
    "CREATE INDEX IF NOT EXISTS idx_ocr_pages_suffix_granth_page ON ocr_pages_suffix(granth_key, page_number);",
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_suffix_fts USING fts5(
      reversed_content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages_suffix',
      content_rowid='page_id',
      tokenize="unicode61 remove_diacritics 0 categories 'L* N* Co M*'",
      prefix='2 3 4'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_ai AFTER INSERT ON ocr_pages_suffix BEGIN
      INSERT INTO ocr_pages_suffix_fts(rowid, reversed_content, granth_key, page_number)
      VALUES (new.page_id, new.reversed_content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_ad AFTER DELETE ON ocr_pages_suffix BEGIN
      INSERT INTO ocr_pages_suffix_fts(ocr_pages_suffix_fts, rowid, reversed_content, granth_key, page_number)
      VALUES ('delete', old.page_id, old.reversed_content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_au AFTER UPDATE ON ocr_pages_suffix BEGIN
      INSERT INTO ocr_pages_suffix_fts(ocr_pages_suffix_fts, rowid, reversed_content, granth_key, page_number)
      VALUES ('delete', old.page_id, old.reversed_content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_suffix_fts(rowid, reversed_content, granth_key, page_number)
      VALUES (new.page_id, new.reversed_content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_cleanup AFTER DELETE ON ocr_pages BEGIN
      DELETE FROM ocr_pages_suffix WHERE page_id = old.id;
    END;`,
  ];

  for (const sql of statements) {
    await db.execute(sql);
  }
}

async function rebuildFts(db, tableName, minFreeMemoryMb) {
  await assertMemory(`before rebuilding ${tableName}`, minFreeMemoryMb);
  const started = Date.now();
  console.log(`[fts] rebuilding ${tableName}`);
  await db.execute(`INSERT INTO ${tableName}(${tableName}) VALUES('rebuild')`);
  console.log(`[fts] rebuilt ${tableName} in ${Math.round((Date.now() - started) / 1000)}s`);
}

function makeSuffixStatement(row) {
  return {
    sql: `INSERT INTO ocr_pages_suffix (page_id, granth_key, page_number, reversed_content, updated_at)
          VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
          ON CONFLICT(page_id) DO UPDATE SET
            granth_key = excluded.granth_key,
            page_number = excluded.page_number,
            reversed_content = excluded.reversed_content,
            updated_at = CURRENT_TIMESTAMP`,
    args: [row.id, row.granth_key, row.page_number, buildOCRSuffixIndexContent(row.content)],
  };
}

async function populateSuffixIndex(db, args) {
  const countResult = await db.execute("SELECT COUNT(*) AS total, MAX(id) AS max_id FROM ocr_pages");
  const total = Number(countResult.rows[0]?.total || 0);
  const maxId = Number(countResult.rows[0]?.max_id || 0);
  console.log(`[suffix] syncing ${total} page rows up to id ${maxId}`);

  let lastId = 0;
  let synced = 0;
  const started = Date.now();

  while (lastId < maxId) {
    await assertMemory(`before suffix batch after id ${lastId}`, args.minFreeMemoryMb);
    const pageResult = await db.execute({
      sql: `SELECT id, granth_key, page_number, content
            FROM ocr_pages
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?`,
      args: [lastId, args.batchSize],
    });

    const rows = pageResult.rows;
    if (rows.length === 0) break;

    const tx = await db.transaction("write");
    try {
      await tx.batch(rows.map(makeSuffixStatement));
      await tx.commit();
    } catch (error) {
      try {
        if (!tx.closed) await tx.rollback();
      } catch {
        // ignore rollback close races
      }
      throw error;
    } finally {
      tx.close();
    }

    lastId = Number(rows[rows.length - 1].id);
    synced += rows.length;
    console.log(`[suffix] synced ${synced}/${total}; last id ${lastId}`);
  }

  console.log(`[suffix] finished in ${Math.round((Date.now() - started) / 1000)}s`);
}

async function printCounts(db) {
  const tables = ["ocr_pages", "ocr_pages_search_fts", "ocr_pages_trigram_fts", "ocr_pages_suffix", "ocr_pages_suffix_fts"];
  for (const table of tables) {
    const result = await db.execute(`SELECT COUNT(*) AS total FROM ${table}`);
    console.log(`[count] ${table}: ${result.rows[0]?.total ?? 0}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    usage();
    return;
  }

  await assertMemory("startup", args.minFreeMemoryMb);

  const db = createTursoClient({
    url: requireEnv("TURSO_URL"),
    authToken: requireEnv("TURSO_AUTH_TOKEN"),
  });

  if (!args.execute) {
    console.log("[dry-run] pass --execute to create and populate Turso search indexes");
    await printCounts(db).catch((error) => {
      console.log(`[dry-run] existing count check skipped: ${error.message}`);
    });
    return;
  }

  await assertMemory("before schema", args.minFreeMemoryMb);
  await ensureSchema(db);

  if (!args.skipFtsRebuild) {
    await rebuildFts(db, "ocr_pages_search_fts", args.minFreeMemoryMb);
    await rebuildFts(db, "ocr_pages_trigram_fts", args.minFreeMemoryMb);
  }

  if (!args.skipSuffix) {
    await populateSuffixIndex(db, args);
  }

  await printCounts(db);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
