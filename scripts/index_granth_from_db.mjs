#!/usr/bin/env node
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import { UTApi, UTFile } from "uploadthing/server";
import path from "node:path";

const SOURCE_TABLE = "granth_ocr_files";
const DOCS_TABLE = "documents";
const PAGES_TABLE = "document_pages";
const PAGE_CHUNK_SIZE = 200;

function reqEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function parseArgs(argv) {
  const args = { limit: null, startAt: 0, concurrency: 1, reprocess: false, verbose: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--verbose") args.verbose = true;
    else if (a === "--reprocess") args.reprocess = true;
    else if (a === "--limit" || a.startsWith("--limit=")) {
      const v = a.includes("=") ? a.split("=")[1] : argv[++i];
      args.limit = Number(v);
    } else if (a === "--startAt" || a.startsWith("--startAt=")) {
      const v = a.includes("=") ? a.split("=")[1] : argv[++i];
      args.startAt = Number(v);
    } else if (a === "--concurrency" || a.startsWith("--concurrency=")) {
      const v = a.includes("=") ? a.split("=")[1] : argv[++i];
      args.concurrency = Math.max(1, Number(v));
    } else {
      throw new Error(`Unknown arg: ${a}`);
    }
  }
  return args;
}

function shortErr(e, max = 1600) {
  const s = e?.stack || e?.message || String(e);
  return s.length > max ? s.slice(0, max) + "..." : s;
}

function hasMeaningfulText(t) {
  return String(t ?? "").replace(/\s+/g, "").length > 0;
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}

async function fetchAll(supabase, table, selectCols) {
  const out = [];
  let offset = 0;
  const PAGE = 1000;
  while (true) {
    const { data, error } = await supabase
      .from(table)
      .select(selectCols)
      .order("id", { ascending: true })
      .range(offset, offset + PAGE - 1);

    if (error) throw new Error(`Fetch ${table} failed: ${error.message}`);
    if (!data?.length) break;
    out.push(...data);
    offset += PAGE;
    if (data.length < PAGE) break;
  }
  return out;
}

async function fetchProcessedIds(supabase) {
  const ids = new Set();
  let offset = 0;
  const PAGE = 1000;
  while (true) {
    const { data, error } = await supabase
      .from(DOCS_TABLE)
      .select("custom_id")
      .eq("status", "processed")
      .range(offset, offset + PAGE - 1);

    if (error) throw new Error(`Fetch processed ids failed: ${error.message}`);
    if (!data?.length) break;
    for (const r of data) ids.add(r.custom_id);
    offset += PAGE;
    if (data.length < PAGE) break;
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
  await supabase.from(DOCS_TABLE).upsert(
    {
      ...payload,
      status: "failed",
      error: errMsg,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "custom_id" }
  );
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
  const { error: delErr } = await supabase.from(PAGES_TABLE).delete().eq("custom_id", customId);
  if (delErr) throw new Error(`document_pages delete failed: ${delErr.message}`);

  for (let i = 0; i < pages.length; i += PAGE_CHUNK_SIZE) {
    const chunk = pages.slice(i, i + PAGE_CHUNK_SIZE).map((p) => ({
      custom_id: customId,
      page_number: p.page_number,
      text: p.text ?? "",
    }));
    const { error } = await supabase.from(PAGES_TABLE).upsert(chunk, { onConflict: "custom_id,page_number" });
    if (error) throw new Error(`document_pages upsert failed: ${error.message}`);
  }
}

async function extractViaPython(pdfBuffer, env, verbose) {
  return new Promise((resolve, reject) => {
    const proc = spawn("python3", ["scripts/extract_pages_mem.py"], {
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, ...env },
    });

    let out = "";
    let err = "";

    proc.stdout.on("data", (c) => (out += String(c)));
    proc.stderr.on("data", (c) => (err += String(c)));
    proc.on("error", (e) => reject(new Error(`Failed to start python: ${shortErr(e)}`)));

    proc.stdin.write(pdfBuffer);
    proc.stdin.end();

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`python exited ${code}: ${err || out || "(no output)"}`));
        return;
      }
      try {
        const parsed = JSON.parse(out);
        if (parsed?.error) reject(new Error(parsed.error));
        else resolve(parsed);
      } catch (e) {
        reject(new Error(`Failed to parse python JSON. err=${shortErr(e)} out=${out.slice(0, 500)}`));
      }
    });

    if (verbose) {
      proc.on("spawn", () => console.log("   python extractor spawned"));
    }
  });
}

async function uploadCsv(utapi, csvBuf, csvName) {
  const utFile = new UTFile([csvBuf], csvName, {
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
  let next = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const idx = next++;
      if (idx >= items.length) return;
      await worker(items[idx], idx);
    }
  });
  await Promise.all(workers);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const SUPABASE_URL = reqEnv("SUPABASE_URL");
  const SUPABASE_KEY = reqEnv("SUPABASE_SERVICE_ROLE_KEY");
  const UT_TOKEN = reqEnv("UPLOADTHING_TOKEN");

  // OCR control (in-memory)
  const OCR_LANGS = process.env.OCR_LANGS || "guj+hin+san";
  const OCR_DPI = process.env.OCR_DPI || "300";
  const USE_OCR_FALLBACK = process.env.USE_OCR_FALLBACK || "1";

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const utapi = new UTApi({ token: UT_TOKEN });

  console.log(`📡 Fetching OCR files from ${SOURCE_TABLE}...`);
  const sourceFiles = await fetchAll(
    supabase,
    SOURCE_TABLE,
    "id, ufs_url, ut_key, file_name, file_size, custom_id, original_rel_path, collection, subcollection"
  );
  console.log(`   Found ${sourceFiles.length} files.`);

  let processedIds = new Set();
  if (!args.reprocess) {
    console.log(`📡 Fetching already-processed documents...`);
    processedIds = await fetchProcessedIds(supabase);
    console.log(`   ${processedIds.size} already processed.`);
  }

  const queue = sourceFiles.filter((f) => args.reprocess || !processedIds.has(f.custom_id || ""));
  const start = Math.min(args.startAt, queue.length);
  const end = args.limit != null ? Math.min(queue.length, start + args.limit) : queue.length;
  const batch = queue.slice(start, end);

  console.log(`   ${sourceFiles.length - queue.length} skipped, ${batch.length} to process.`);
  console.log(`OCR in-memory: langs=${OCR_LANGS}, dpi=${OCR_DPI}, fallback=${USE_OCR_FALLBACK}`);
  console.log(`Processing (concurrency=${args.concurrency})\n`);

  let ok = 0, failed = 0;

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
      const res = await fetch(pdfUrl);
      if (!res.ok) throw new Error(`Download failed ${res.status}: ${pdfUrl}`);
      const pdfBuf = Buffer.from(await res.arrayBuffer());
      if (args.verbose) console.log(`   bytes=${pdfBuf.length}`);

      console.log(`${label} extracting (PyMuPDF in-memory)...`);
      const parsed = await extractViaPython(
        pdfBuf,
        { OCR_LANGS, OCR_DPI, USE_OCR_FALLBACK },
        args.verbose
      );

      const pagesAll = parsed.pages || [];
      const stats = parsed.stats || {};

      const pages = pagesAll
        .map((p) => ({ page_number: p.page_number, text: p.text || "" }))
        .filter((p) => hasMeaningfulText(p.text));

      if (args.verbose) {
        console.log(`   stats=${JSON.stringify(stats)}`);
        const sample = pagesAll.slice(0, 3).map((p) => ({ p: p.page_number, src: p.source, chars: p.chars }));
        console.log(`   firstPages=${JSON.stringify(sample)}`);
      }

      if (pages.length === 0) {
        throw new Error(`No extractable text after text-layer + OCR fallback. stats=${JSON.stringify(stats)}`);
      }

      await replacePages(supabase, customId, pages);

      // Build CSV in memory
      const csvName = `${path.basename(pdfName, path.extname(pdfName))}__pages.csv`;
      const header = "pdf_name,custom_id,pdf_url,page_number,text\n";
      const rows = pages.map(
        (p) =>
          [
            csvEscape(pdfName),
            csvEscape(customId),
            csvEscape(pdfUrl),
            String(p.page_number),
            csvEscape(p.text),
          ].join(",") + "\n"
      );
      const csvBuf = Buffer.from(header + rows.join(""), "utf8");

      console.log(`${label} uploading CSV (in-memory)...`);
      const { csvUrl, csvKey } = await uploadCsv(utapi, csvBuf, csvName);

      await markDocProcessed(supabase, customId, csvUrl, csvKey);

      ok++;
      console.log(`✅ ${label} done (pagesStored=${pages.length}, totalPages=${stats.totalPages ?? "?"})\n`);
    } catch (e) {
      failed++;
      const msg = shortErr(e);
      console.error(`❌ ${label} failed: ${msg}\n`);
      await upsertDocFailed(supabase, docPayload, msg);
    }
  });

  console.log(`🎉 Finished: processed=${ok}, failed=${failed}, skipped=${sourceFiles.length - queue.length}`);
}

main().catch((e) => {
  console.error(e?.stack || e?.message || String(e));
  process.exit(1);
});
