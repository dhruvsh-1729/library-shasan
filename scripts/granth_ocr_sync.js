#!/usr/bin/env node
/**
 * sync-ocr-to-supabase.js
 *
 * Scans the _ocr_work/logs directory for *.upload.json files,
 * parses each one, and upserts into the granth_ocr_files Supabase table.
 * Skips any record whose ufs_url already exists in the table.
 *
 * Usage:
 *   node sync-ocr-to-supabase.js [--dry-run] [--logs-dir /path/to/logs]
 *
 * Environment variables (from .env):
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 */

const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

// Replace the existing dotenv try/catch block with:
try {
  require("dotenv").config();
} catch {
  // dotenv not installed
}

// ─── Config ─────────────────────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error(
    "ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env or environment."
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ─── CLI Args ───────────────────────────────────────────────────────
const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const logsIdx = args.indexOf("--logs-dir");
const DEFAULT_LOGS_DIR = "/media/dell/HP USB20FD/_ocr_work/logs";
const LOGS_DIR = logsIdx !== -1 ? args[logsIdx + 1] : DEFAULT_LOGS_DIR;

const TABLE = "granth_ocr_files";
const BATCH_SIZE = 50; // upsert in batches

// ─── Helpers ────────────────────────────────────────────────────────

/**
 * Recursively find all files matching a pattern.
 */
function walkDir(dir, pattern, results = []) {
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkDir(full, pattern, results);
    } else if (pattern.test(entry.name)) {
      results.push(full);
    }
  }
  return results;
}

/**
 * Parse a single upload.json and return a row object.
 */
function parseUploadJson(filePath) {
  try {
    const raw = fs.readFileSync(filePath, "utf-8").trim();
    if (!raw) return null;

    const data = JSON.parse(raw);

    // The customId encodes the path: "00.ACROBAT__Daanpradeep__filename.pdf__OCR"
    const customId = data.customId || null;

    // Derive collection/subcollection from customId
    let collection = null;
    let subcollection = null;
    let originalRelPath = null;

    if (customId) {
      const parts = customId.split("__");
      if (parts.length >= 1) collection = parts[0];
      if (parts.length >= 2) subcollection = parts[1];
      // Reconstruct original relative path: folder/subfolder/filename.pdf
      if (parts.length >= 3) {
        // Remove the trailing "__OCR" marker
        const fileParts = parts.slice(0, -1); // drop "OCR"
        // Replace underscores-that-were-spaces back isn't reliable,
        // so we use the folder structure from the log file path instead
        originalRelPath = fileParts.slice(0, -1).join("/") + "/" + fileParts[fileParts.length - 1];
      }
    }

    // Also try to derive original path from the log file's position in the tree
    // e.g. logs/00.ACROBAT/Daanpradeep/filename.upload.json
    // This is more reliable for the relative path
    const relFromLogs = path.relative(LOGS_DIR, filePath);
    // Remove .upload.json → get the original base name
    const relBase = relFromLogs.replace(/\.upload\.json$/, "");
    // This gives us e.g. "00.ACROBAT/Daanpradeep/159_B008812_दानप्रदीप (गु.) अनुवाद_ocred"
    // Add .pdf back
    const derivedRelPath = relBase + ".pdf";

    // Prefer derived path from directory structure
    if (!collection) {
      const derivedParts = relFromLogs.split(path.sep);
      if (derivedParts.length >= 1) collection = derivedParts[0];
      if (derivedParts.length >= 2) subcollection = derivedParts[1];
    }

    return {
      ufs_url: data.ufsUrl,
      ut_key: data.key || null,
      ut_url: data.url || null,
      app_url: data.appUrl || null,
      file_name: data.name || null,
      file_size: data.size || null,
      file_hash: data.fileHash || null,
      file_type: data.type || "application/pdf",
      custom_id: customId,
      original_rel_path: originalRelPath || derivedRelPath,
      collection: collection,
      subcollection: subcollection,
      last_modified: data.lastModified || null,
    };
  } catch (err) {
    console.error(`  WARN: Failed to parse ${filePath}: ${err.message}`);
    return null;
  }
}

// ─── Main ───────────────────────────────────────────────────────────
async function main() {
  console.log(`\n🔍 Scanning for .upload.json files in: ${LOGS_DIR}`);
  if (DRY_RUN) console.log("   (DRY RUN — no database writes)\n");

  // Skip non-upload files like pdf_map.run.json etc.
  const jsonFiles = walkDir(LOGS_DIR, /\.upload\.json$/);

  // Filter out manifest uploads (pdf_map.txt.upload.json etc.)
  const uploadFiles = jsonFiles.filter((f) => {
    const base = path.basename(f);
    return !base.startsWith("pdf_map");
  });

  console.log(`   Found ${uploadFiles.length} upload log files.\n`);

  if (uploadFiles.length === 0) {
    console.log("Nothing to process.");
    return;
  }

  // Parse all files
  const rows = [];
  for (const f of uploadFiles) {
    const row = parseUploadJson(f);
    if (row && row.ufs_url) {
      rows.push(row);
    }
  }

  console.log(`   Parsed ${rows.length} valid records.\n`);

  if (DRY_RUN) {
    console.log("Sample records:");
    rows.slice(0, 3).forEach((r, i) => {
      console.log(`\n  [${i + 1}]`);
      console.log(`    file_name:    ${r.file_name}`);
      console.log(`    ufs_url:      ${r.ufs_url}`);
      console.log(`    collection:   ${r.collection}`);
      console.log(`    subcollection:${r.subcollection}`);
      console.log(`    original_rel: ${r.original_rel_path}`);
      console.log(`    file_size:    ${r.file_size ? (r.file_size / 1024 / 1024).toFixed(1) + " MB" : "?"}`);
    });
    console.log(`\n✅ Dry run complete. ${rows.length} records would be synced.`);
    return;
  }

  // ─── Fetch existing ufs_urls to skip duplicates ─────────────────
  console.log("📡 Fetching existing records from Supabase...");

  const existingUrls = new Set();
  let offset = 0;
  const PAGE = 1000;

  while (true) {
    const { data, error } = await supabase
      .from(TABLE)
      .select("ufs_url")
      .range(offset, offset + PAGE - 1);

    if (error) {
      console.error(`ERROR fetching existing records: ${error.message}`);
      process.exit(1);
    }

    if (!data || data.length === 0) break;
    data.forEach((r) => existingUrls.add(r.ufs_url));
    offset += PAGE;
    if (data.length < PAGE) break;
  }

  console.log(`   ${existingUrls.size} existing records in table.\n`);

  // Filter out already-existing
  const newRows = rows.filter((r) => !existingUrls.has(r.ufs_url));

  console.log(
    `   ${rows.length - newRows.length} skipped (already exist), ${newRows.length} to insert.\n`
  );

  if (newRows.length === 0) {
    console.log("✅ Everything is already synced!");
    return;
  }

  // ─── Insert in batches ──────────────────────────────────────────
  let inserted = 0;
  let failed = 0;

  for (let i = 0; i < newRows.length; i += BATCH_SIZE) {
    const batch = newRows.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase.from(TABLE).insert(batch).select("id");

    if (error) {
      console.error(
        `  ❌ Batch ${Math.floor(i / BATCH_SIZE) + 1} failed: ${error.message}`
      );
      // Try one-by-one for this batch
      for (const row of batch) {
        const { error: singleErr } = await supabase.from(TABLE).insert([row]);
        if (singleErr) {
          // Could be a duplicate that slipped through (race condition) or other error
          if (singleErr.code === "23505") {
            console.log(`  SKIP (dup): ${row.file_name}`);
          } else {
            console.error(`  ❌ ${row.file_name}: ${singleErr.message}`);
            failed++;
          }
        } else {
          inserted++;
        }
      }
    } else {
      inserted += batch.length;
      console.log(
        `  ✅ Batch ${Math.floor(i / BATCH_SIZE) + 1}: inserted ${batch.length} records (${inserted}/${newRows.length})`
      );
    }
  }

  console.log(`\n🎉 Done! Inserted: ${inserted}, Failed: ${failed}, Skipped: ${rows.length - newRows.length}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});