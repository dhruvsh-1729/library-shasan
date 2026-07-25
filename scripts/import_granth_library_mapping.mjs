#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { createClient } from "@supabase/supabase-js";

const DEFAULT_ROOT = "/media/dell/KINGSTON/Library Final/GRANTH-LIBRARY";
const INDEX_REL = "index.html";
const DOWNLOADS_REL = "granth downloads";
const BATCH_SIZE = 500;

function usage() {
  console.log(`Usage: node scripts/import_granth_library_mapping.mjs [options]

Options:
  --root PATH        GRANTH-LIBRARY root (default: ${DEFAULT_ROOT})
  --execute          Write to Supabase. Default is dry-run.
  --noPdfInfo        Do not call pdfinfo to calculate last-gatha page_end.
  --limit N          Parse at most N index rows, for testing.
  --verbose          Print detailed unmatched paths.
  --help             Show help
`);
}

function parseIntFlag(name, raw, min) {
  const n = Number.parseInt(String(raw ?? ""), 10);
  if (!Number.isFinite(n) || Number.isNaN(n) || n < min) {
    throw new Error(`${name} must be an integer >= ${min}`);
  }
  return n;
}

function parseArgs(argv) {
  const args = {
    root: DEFAULT_ROOT,
    execute: false,
    pdfInfo: true,
    limit: null,
    verbose: false,
    help: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") args.help = true;
    else if (arg === "--execute") args.execute = true;
    else if (arg === "--noPdfInfo") args.pdfInfo = false;
    else if (arg === "--verbose") args.verbose = true;
    else if (arg === "--root" || arg.startsWith("--root=")) {
      args.root = arg.includes("=") ? arg.slice("--root=".length) : argv[++i];
    } else if (arg === "--limit" || arg.startsWith("--limit=")) {
      args.limit = parseIntFlag("--limit", arg.includes("=") ? arg.slice("--limit=".length) : argv[++i], 1);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return args;
}

function hashText(value, len = 16) {
  return crypto.createHash("sha1").update(String(value)).digest("hex").slice(0, len);
}

function decodeHtml(value) {
  return String(value ?? "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(Number.parseInt(n, 16)));
}

function textFromHtml(html) {
  return decodeHtml(
    String(html ?? "")
      .replace(/<br\s*\/?\s*>/gi, "\n")
      .replace(/<[^>]*>/g, " ")
      .replace(/[ \t]+/g, " ")
      .replace(/\s*\n\s*/g, "\n")
      .replace(/\n{2,}/g, "\n")
      .trim()
  );
}

function compactText(html) {
  return textFromHtml(html).replace(/\s+/g, " ").trim();
}

function attrValue(tag, attr) {
  const re = new RegExp(`${attr}\\s*=\\s*["']([^"']+)["']`, "i");
  return tag.match(re)?.[1] || null;
}

function extractTags(html, tagName) {
  const re = new RegExp(`<${tagName}\\b[\\s\\S]*?<\\/${tagName}>`, "gi");
  return [...String(html).matchAll(re)].map((m) => m[0]);
}

function extractHrefs(html) {
  return [...String(html).matchAll(/<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>/gi)].map((m) =>
    decodeHtml(m[1])
  );
}

function extractFirstImgSrc(html) {
  return decodeHtml(String(html).match(/<img\b[^>]*src\s*=\s*["']([^"']+)["']/i)?.[1] || "");
}

function normalizeRel(value) {
  return decodeURIComponent(String(value || ""))
    .replace(/\\/g, "/")
    .replace(/^\.\//, "")
    .replace(/^\/+/, "");
}

function withoutHash(value) {
  return normalizeRel(value).split("#")[0];
}

function resolveHref(baseRel, href) {
  const clean = withoutHash(href);
  if (!clean) return "";
  if (/^[a-z][a-z0-9+.-]*:/i.test(clean)) return clean;
  const baseDir = baseRel ? path.posix.dirname(normalizeRel(baseRel)) : "";
  return path.posix.normalize(path.posix.join(baseDir, clean));
}

function basenameLower(rel) {
  return normalizeRel(rel).split("/").pop()?.toLowerCase() || "";
}

function canonicalPdfName(value) {
  let name = basenameLower(value)
    .normalize("NFKC")
    .replace(/\.pdf$/i, "")
    .replace(/\s+-\s+copy$/i, "");

  for (let i = 0; i < 6; i += 1) {
    name = name
      .replace(/[_\s.-]+(?:ocr|ocred|std|hr\d*)$/i, "")
      .replace(/[_\s.-]+\d{4,}$/i, "");
  }

  return name
    .replace(/[^\p{L}\p{N}]+/gu, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function pdfNameAliases(value) {
  const canonical = canonicalPdfName(value);
  const aliases = new Set([basenameLower(value), canonical]);
  aliases.add(canonical.replace(/^\d{1,4}_+/, ""));
  aliases.add(canonical.replace(/^\d{1,4}_+b\d{4,}_+/i, ""));
  aliases.add(canonical.replace(/^b\d{4,}_+/i, ""));
  return [...aliases].filter(Boolean);
}

function leadingBookCode(fileName) {
  const match = String(fileName || "").match(/^(\d{1,4})(?=[_\s.-]|$)/);
  return match ? match[1].padStart(3, "0") : null;
}

function parseCodes(firstCellHtml) {
  const withMarker = String(firstCellHtml || "").replace(/<br\s*\/?\s*>/i, "|||");
  const [namePart, codesPart = ""] = withMarker.split("|||");
  const titleEnglish = compactText(namePart);
  const bookCodes = [...compactText(codesPart).matchAll(/\d{1,4}/g)].map((m) => m[0].padStart(3, "0"));
  return { titleEnglish, bookCodes: [...new Set(bookCodes)] };
}

function parseIndexRows(indexHtml) {
  const rows = extractTags(indexHtml, "tr");
  const parsed = [];

  for (const rowHtml of rows) {
    const cells = extractTags(rowHtml, "td");
    if (cells.length < 2) continue;

    const { titleEnglish, bookCodes } = parseCodes(cells[0]);
    if (!titleEnglish || bookCodes.length === 0) continue;

    const hrefs = extractHrefs(rowHtml);
    const pdfHrefs = hrefs.filter((href) => /\.pdf(?:#|$)/i.test(withoutHash(href)));
    const htmlHrefs = hrefs.filter((href) => /\.html?$/i.test(withoutHash(href)));
    const indexHref = htmlHrefs[0] || pdfHrefs[0] || hrefs[0] || null;
    const titleDisplay = compactText(cells[1]);
    const authorText = compactText(cells[2] || "");
    const detailsText = compactText(cells[3] || "");
    const coverRelPath = extractFirstImgSrc(cells[4] || rowHtml) || null;
    const rawText = compactText(rowHtml);
    const rowHash = hashText(`${titleEnglish}|${bookCodes.join(",")}|${rawText}`, 20);

    parsed.push({
      source_row_hash: rowHash,
      title_english: titleEnglish || null,
      title_display: titleDisplay || null,
      author_text: authorText || null,
      details_text: detailsText || null,
      book_codes: bookCodes,
      index_href: indexHref ? normalizeRel(indexHref) : null,
      index_href_type: indexHref
        ? /\.html?$/i.test(withoutHash(indexHref))
          ? "html"
          : /\.pdf$/i.test(withoutHash(indexHref))
            ? "pdf"
            : "other"
        : null,
      cover_rel_path: coverRelPath ? normalizeRel(coverRelPath) : null,
      raw_text: rawText || null,
      pdfHrefs,
      htmlHrefs,
    });
  }

  return parsed;
}

function parseGathaFromAnchorText(anchorText) {
  const beforeColon = String(anchorText || "").split(":")[0] || "";
  let adhikar = null;
  let gatha = null;

  const slashMatches = [...beforeColon.matchAll(/(\d+)\s*\/\s*(\d+)/g)];
  if (slashMatches.length) {
    const match = slashMatches[slashMatches.length - 1];
    adhikar = Number.parseInt(match[1], 10);
    gatha = Number.parseInt(match[2], 10);
  } else {
    const nums = [...beforeColon.matchAll(/(\d+)/g)].map((m) => Number.parseInt(m[1], 10));
    if (nums.length >= 2) {
      adhikar = nums[0];
      gatha = nums[nums.length - 1];
    } else if (nums.length === 1) {
      gatha = nums[0];
    }
  }

  if (!Number.isFinite(gatha)) return null;
  return {
    adhikar: Number.isFinite(adhikar) ? adhikar : null,
    gatha,
    anchorLabel: String(anchorText || "").match(/\[([^\]]+)\]/)?.[1] || null,
  };
}

function parseGathaAnchors(htmlText, htmlRel) {
  const title = compactText(String(htmlText).match(/<h[1-3][^>]*>([\s\S]*?)<\/h[1-3]>/i)?.[1] || "");
  const re = /<a\b[^>]*href\s*=\s*["']([^"']*#page=(\d+)[^"']*)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const out = [];
  let sequence = 0;

  for (const match of String(htmlText).matchAll(re)) {
    const href = decodeHtml(match[1]);
    const pageStart = Number.parseInt(match[2], 10);
    const anchorText = compactText(match[3]);
    const parsed = parseGathaFromAnchorText(anchorText);
    if (!parsed || !Number.isFinite(pageStart)) continue;

    const pdfRelPath = resolveHref(htmlRel, href);
    out.push({
      source_html_rel_path: htmlRel,
      source_html_title: title || null,
      pdf_file_name: basenameLower(pdfRelPath),
      pdf_rel_path: pdfRelPath,
      adhikar: parsed.adhikar,
      gatha: parsed.gatha,
      anchor_text: anchorText,
      anchor_label: parsed.anchorLabel,
      href: normalizeRel(href),
      page_start: pageStart,
      sequence_index: sequence,
    });
    sequence += 1;
  }

  return out;
}

async function walkFiles(root, predicate, out = []) {
  const entries = await fs.readdir(root, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(root, entry.name);
    if (entry.isDirectory()) await walkFiles(full, predicate, out);
    else if (entry.isFile() && predicate(full)) out.push(full);
  }
  return out;
}

async function runCommand(bin, commandArgs) {
  return await new Promise((resolve, reject) => {
    const proc = spawn(bin, commandArgs, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    proc.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    proc.on("error", reject);
    proc.on("close", (code) => {
      if (code !== 0) reject(new Error(stderr || stdout || `${bin} exited ${code}`));
      else resolve({ stdout, stderr });
    });
  });
}

async function getPdfPageCount(filePath, cache) {
  if (!filePath) return null;
  if (cache.has(filePath)) return cache.get(filePath);
  try {
    const { stdout } = await runCommand("pdfinfo", [filePath]);
    const pages = stdout.match(/^Pages:\s+(\d+)/m);
    const count = pages ? Number.parseInt(pages[1], 10) : null;
    cache.set(filePath, count);
    return count;
  } catch {
    cache.set(filePath, null);
    return null;
  }
}

async function fetchAllSupabase(supabase, table, select) {
  const rows = [];
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const { data, error } = await supabase.from(table).select(select).range(from, from + pageSize - 1);
    if (error) throw error;
    if (!data || data.length === 0) break;
    rows.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return rows;
}

async function tableExists(supabase) {
  const { error } = await supabase.from("granth_library_books").select("id").limit(1);
  return !error;
}

function buildSupabasePdfIndex(rows) {
  const exact = new Map();
  const aliases = new Map();
  const addAlias = (key, row) => {
    if (!key) return;
    const existing = aliases.get(key);
    if (existing && existing.id !== row.id) aliases.set(key, null);
    else aliases.set(key, row);
  };

  for (const row of rows) {
    const names = [row.file_name, row.original_rel_path, row.custom_id].filter(Boolean);
    for (const name of names) {
      const base = basenameLower(name);
      if (!base) continue;
      if (!exact.has(base)) exact.set(base, row);
      for (const alias of pdfNameAliases(name)) addAlias(alias, row);
    }
  }
  return { exact, aliases };
}

function findSupabasePdf(index, name) {
  const exact = basenameLower(name);
  const exactRow = index.exact.get(exact);
  if (exactRow) return exactRow;

  for (const alias of pdfNameAliases(name)) {
    const row = index.aliases.get(alias);
    if (row) return row;
  }

  return null;
}

function computePageEnds(anchors, pageCountByRel) {
  const byGroup = new Map();
  for (const anchor of anchors) {
    const key = `${anchor.source_html_rel_path}|${anchor.pdf_rel_path}|${anchor.adhikar ?? "none"}`;
    if (!byGroup.has(key)) byGroup.set(key, []);
    byGroup.get(key).push(anchor);
  }

  for (const group of byGroup.values()) {
    group.sort((a, b) => a.page_start - b.page_start || a.sequence_index - b.sequence_index);
    const starts = [...new Set(group.map((item) => item.page_start))].sort((a, b) => a - b);
    const total = pageCountByRel.get(group[0].pdf_rel_path) || null;

    for (const item of group) {
      const next = starts.find((page) => page > item.page_start) || null;
      item.next_page_start = next;
      item.page_count = total;
      item.page_end = next ? Math.max(item.page_start, next - 1) : total || item.page_start;
    }
  }
}

async function upsertReturning(supabase, table, rows, onConflict, select = "*") {
  const out = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase
      .from(table)
      .upsert(chunk, { onConflict })
      .select(select);
    if (error) throw new Error(`${table} upsert failed: ${error.message}`);
    out.push(...(data || []));
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    usage();
    return;
  }

  const indexPath = path.join(args.root, INDEX_REL);
  const downloadsRoot = path.join(args.root, DOWNLOADS_REL);
  const indexHtml = await fs.readFile(indexPath, "utf8");
  let books = parseIndexRows(indexHtml);
  if (args.limit != null) books = books.slice(0, args.limit);

  const localPdfs = await walkFiles(downloadsRoot, (file) => file.toLowerCase().endsWith(".pdf"));
  const localPdfByRel = new Map();
  const localPdfByName = new Map();
  for (const filePath of localPdfs) {
    const rel = normalizeRel(path.relative(args.root, filePath));
    localPdfByRel.set(rel.toLowerCase(), filePath);
    localPdfByName.set(path.basename(filePath).toLowerCase(), filePath);
  }

  const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
  const supabasePdfs = await fetchAllSupabase(
    supabase,
    "granth_ocr_files",
    "id,custom_id,file_name,original_rel_path,ufs_url"
  );
  const supabaseByName = buildSupabasePdfIndex(supabasePdfs);

  const allFiles = new Map();
  const anchors = [];
  const pageCountCache = new Map();
  const pageCountByRel = new Map();
  const unmatchedHtml = [];

  for (const book of books) {
    const pdfHrefs = new Set(book.pdfHrefs);
    const htmlHrefs = new Set(book.htmlHrefs);

    for (const pdfHref of pdfHrefs) {
      const pdfRel = resolveHref("", pdfHref);
      const pdfName = basenameLower(pdfRel);
      const bookCode = leadingBookCode(pdfName) || book.book_codes[0] || null;
      const sourceFileKey = hashText(`${book.source_row_hash}|${bookCode}|${pdfRel}`, 24);
      const supabaseRow = findSupabasePdf(supabaseByName, pdfName);
      const localPath = localPdfByRel.get(pdfRel.toLowerCase()) || localPdfByName.get(pdfName) || null;
      const pageCount = args.pdfInfo ? await getPdfPageCount(localPath, pageCountCache) : null;
      pageCountByRel.set(pdfRel, pageCount);
      allFiles.set(sourceFileKey, {
        source_file_key: sourceFileKey,
        source_row_hash: book.source_row_hash,
        book_code: bookCode,
        granth_ocr_file_id: supabaseRow?.id || null,
        custom_id: supabaseRow?.custom_id || null,
        pdf_file_name: pdfName,
        pdf_rel_path: pdfRel,
        pdf_url: supabaseRow?.ufs_url || null,
        page_count: pageCount,
        source_href: normalizeRel(pdfHref),
        source_kind: "index",
      });
    }

    for (const htmlHref of htmlHrefs) {
      const htmlRel = resolveHref("", htmlHref);
      const htmlPath = path.join(args.root, htmlRel);
      let htmlText = null;
      try {
        htmlText = await fs.readFile(htmlPath, "utf8");
      } catch {
        unmatchedHtml.push(htmlRel);
        continue;
      }

      const parsedAnchors = parseGathaAnchors(htmlText, htmlRel);
      for (const anchor of parsedAnchors) {
        const pdfName = anchor.pdf_file_name;
        const pdfRel = anchor.pdf_rel_path;
        const bookCode = leadingBookCode(pdfName) || book.book_codes.find((code) => pdfName.startsWith(`${code}_`)) || null;
        const sourceFileKey = hashText(`${book.source_row_hash}|${bookCode || "none"}|${pdfRel}`, 24);
        const supabaseRow = findSupabasePdf(supabaseByName, pdfName);
        const localPath = localPdfByRel.get(pdfRel.toLowerCase()) || localPdfByName.get(pdfName) || null;
        const pageCount = args.pdfInfo ? await getPdfPageCount(localPath, pageCountCache) : null;
        pageCountByRel.set(pdfRel, pageCount);

        allFiles.set(sourceFileKey, {
          source_file_key: sourceFileKey,
          source_row_hash: book.source_row_hash,
          book_code: bookCode,
          granth_ocr_file_id: supabaseRow?.id || null,
          custom_id: supabaseRow?.custom_id || null,
          pdf_file_name: pdfName,
          pdf_rel_path: pdfRel,
          pdf_url: supabaseRow?.ufs_url || null,
          page_count: pageCount,
          source_href: anchor.href,
          source_kind: "html-anchor",
        });

        anchors.push({
          ...anchor,
          source_anchor_key: hashText(
            `${htmlRel}|${anchor.href}|${anchor.anchor_text}|${anchor.sequence_index}`,
            28
          ),
          source_row_hash: book.source_row_hash,
          source_file_key: sourceFileKey,
          book_code: bookCode,
          granth_ocr_file_id: supabaseRow?.id || null,
          custom_id: supabaseRow?.custom_id || null,
          pdf_url: supabaseRow?.ufs_url || null,
          page_count: pageCount,
        });
      }
    }
  }

  computePageEnds(anchors, pageCountByRel);

  const files = [...allFiles.values()];
  const matchedFiles = files.filter((file) => file.pdf_url).length;
  const matchedAnchors = anchors.filter((anchor) => anchor.pdf_url).length;
  const summary = {
    dryRun: !args.execute,
    books: books.length,
    files: files.length,
    gathaAnchors: anchors.length,
    matchedFiles,
    unmatchedFiles: files.length - matchedFiles,
    matchedAnchors,
    unmatchedAnchors: anchors.length - matchedAnchors,
    unmatchedHtml: unmatchedHtml.length,
  };

  console.log(JSON.stringify(summary, null, 2));
  if (args.verbose) {
    console.log("Unmatched HTML", unmatchedHtml.slice(0, 50));
    console.log(
      "Unmatched files",
      files
        .filter((file) => !file.pdf_url)
        .slice(0, 50)
        .map((file) => file.pdf_file_name)
    );
  }

  if (!args.execute) return;

  if (!(await tableExists(supabase))) {
    throw new Error(
      "Mapping tables are missing. Run supabase/migrations/20260725_granth_library_mapping.sql in Supabase SQL first."
    );
  }

  const bookRows = books.map((book) => ({
    source_row_hash: book.source_row_hash,
    title_english: book.title_english,
    title_display: book.title_display,
    author_text: book.author_text,
    details_text: book.details_text,
    book_codes: book.book_codes,
    index_href: book.index_href,
    index_href_type: book.index_href_type,
    cover_rel_path: book.cover_rel_path,
    raw_text: book.raw_text,
    updated_at: new Date().toISOString(),
  }));

  const writtenBooks = await upsertReturning(
    supabase,
    "granth_library_books",
    bookRows,
    "source_row_hash",
    "id,source_row_hash"
  );
  const bookIdByHash = new Map(writtenBooks.map((row) => [row.source_row_hash, row.id]));

  const fileRows = files.map((file) => ({
    source_file_key: file.source_file_key,
    book_id: bookIdByHash.get(file.source_row_hash) || null,
    book_code: file.book_code,
    granth_ocr_file_id: file.granth_ocr_file_id,
    custom_id: file.custom_id,
    pdf_file_name: file.pdf_file_name,
    pdf_rel_path: file.pdf_rel_path,
    pdf_url: file.pdf_url,
    page_count: file.page_count,
    source_href: file.source_href,
    source_kind: file.source_kind,
    updated_at: new Date().toISOString(),
  }));

  const writtenFiles = await upsertReturning(
    supabase,
    "granth_library_files",
    fileRows,
    "source_file_key",
    "id,source_file_key"
  );
  const fileIdByKey = new Map(writtenFiles.map((row) => [row.source_file_key, row.id]));

  const anchorRows = anchors.map((anchor) => ({
    source_anchor_key: anchor.source_anchor_key,
    book_id: bookIdByHash.get(anchor.source_row_hash) || null,
    library_file_id: fileIdByKey.get(anchor.source_file_key) || null,
    granth_ocr_file_id: anchor.granth_ocr_file_id,
    custom_id: anchor.custom_id,
    book_code: anchor.book_code,
    source_html_rel_path: anchor.source_html_rel_path,
    source_html_title: anchor.source_html_title,
    pdf_file_name: anchor.pdf_file_name,
    pdf_rel_path: anchor.pdf_rel_path,
    pdf_url: anchor.pdf_url,
    adhikar: anchor.adhikar,
    gatha: anchor.gatha,
    anchor_text: anchor.anchor_text,
    anchor_label: anchor.anchor_label,
    href: anchor.href,
    page_start: anchor.page_start,
    next_page_start: anchor.next_page_start,
    page_end: anchor.page_end,
    page_count: anchor.page_count,
    sequence_index: anchor.sequence_index,
    updated_at: new Date().toISOString(),
  }));

  await upsertReturning(supabase, "granth_gatha_map", anchorRows, "source_anchor_key", "id");
  console.log("Import complete.");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exit(1);
});
