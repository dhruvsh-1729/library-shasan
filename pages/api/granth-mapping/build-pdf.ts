import type { NextApiRequest, NextApiResponse } from "next";
import { createHash, randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";
import { PDFDocument } from "pdf-lib";
import { GranthResolveError, resolveGranthSelection } from "@/lib/granth-resolver";
import type { MappingRange, MappingSegment } from "@/lib/granth-mapping";
import { expandPagesWithContext, normalizeContextPageRadius } from "@/lib/page-context";
import { DownloadEmailError, getDownloadRecipientClientKey, sendDownloadEmail } from "@/lib/download-email";

export const config = {
  api: {
    responseLimit: false,
  },
};

type BuildMode = "combined" | "separate";

type BuildBody = {
  bookId?: number | string | null;
  bookCode?: string | null;
  rawFileId?: number | string | null;
  kind?: string;
  spec?: string;
  adhikar?: number | string | null;
  includeCover?: boolean;
  includeAllIdentifiers?: boolean;
  contextPages?: number | string | null;
  delivery?: string | null;
  email?: string | null;
  mode?: BuildMode;
  title?: string | null;
  selection?: {
    pagesByPdf?: Array<{ pdfUrl?: string | null; pages?: Array<number | string> | null }> | null;
    rangeIndexesByPdf?: Array<{ pdfUrl?: string | null; rangeIndexes?: Array<number | string> | null }> | null;
  } | null;
};

type BuildSelection = {
  pagesByPdf: Map<string, Set<number>>;
  rangeIndexesByPdf: Map<string, Set<number>>;
};

const SOURCE_CACHE_DIR = path.join(tmpdir(), "ndms-library-pdf-source-cache");
const MAX_PAGES_PER_BUILD = 900;
const MIN_AVAILABLE_MEMORY_MB = 768;

function toInt(value: unknown, fallback: number | null = null) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function safeFileName(value: string, fallback = "granth") {
  const cleaned = String(value || "")
    .replace(/\.pdf$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return cleaned || fallback;
}

function contentDisposition(filename: string) {
  const ascii = filename.replace(/[^\x20-\x7e]+/g, "_").replace(/["\\]/g, "_") || "download";
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}

function hashText(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

async function availableMemoryMB() {
  try {
    const meminfo = await readFile("/proc/meminfo", "utf8");
    const match = meminfo.match(/^MemAvailable:\s+(\d+)\s+kB/m);
    if (!match) return null;
    return Number(match[1]) / 1024;
  } catch {
    return null;
  }
}

async function ensureFreeMemory(label: string) {
  const available = await availableMemoryMB();
  if (available != null && available < MIN_AVAILABLE_MEMORY_MB) {
    throw new Error(
      `Not enough free memory to ${label}. Available ${Math.round(available)} MB, need ${MIN_AVAILABLE_MEMORY_MB} MB.`
    );
  }
}

async function fileExists(filePath: string) {
  try {
    const info = await stat(filePath);
    return info.size > 0;
  } catch {
    return false;
  }
}

async function runCommand(command: string, args: string[], cwd?: string) {
  await ensureFreeMemory(command);

  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ["ignore", "ignore", "pipe"],
    });

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} failed (${code}): ${stderr.trim()}`));
    });
  });
}

async function downloadSourcePdf(pdfUrl: string) {
  await mkdir(SOURCE_CACHE_DIR, { recursive: true });
  const cachePath = path.join(SOURCE_CACHE_DIR, `${hashText(pdfUrl)}.pdf`);
  if (await fileExists(cachePath)) return cachePath;

  await ensureFreeMemory("download PDF");
  const tempPath = `${cachePath}.${process.pid}.${randomUUID()}.tmp`;
  const response = await fetch(pdfUrl);
  if (!response.ok || !response.body) {
    throw new Error(`Could not fetch source PDF (${response.status} ${response.statusText})`);
  }

  await pipeline(
    Readable.fromWeb(response.body as unknown as NodeReadableStream<Uint8Array>),
    createWriteStream(tempPath)
  );
  await rename(tempPath, cachePath);
  return cachePath;
}

function uniqueSortedPages(pages: number[]) {
  return [...new Set(pages.map((page) => Math.floor(Number(page))).filter((page) => page > 0))].sort((a, b) => a - b);
}

function parseBuildSelection(selection: BuildBody["selection"]): BuildSelection {
  const pagesByPdf = new Map<string, Set<number>>();
  const rangeIndexesByPdf = new Map<string, Set<number>>();

  for (const item of selection?.pagesByPdf || []) {
    const pdfUrl = String(item?.pdfUrl || "").trim();
    if (!pdfUrl) continue;
    const pages = uniqueSortedPages((item?.pages || []).map((page) => Number(page)));
    if (pages.length > 0) pagesByPdf.set(pdfUrl, new Set(pages));
  }

  for (const item of selection?.rangeIndexesByPdf || []) {
    const pdfUrl = String(item?.pdfUrl || "").trim();
    if (!pdfUrl) continue;
    const rangeIndexes = uniqueSortedPages((item?.rangeIndexes || []).map((rangeIndex) => Number(rangeIndex)))
      .map((rangeIndex) => rangeIndex - 1)
      .filter((rangeIndex) => rangeIndex >= 0);
    if (rangeIndexes.length > 0) rangeIndexesByPdf.set(pdfUrl, new Set(rangeIndexes));
  }

  return { pagesByPdf, rangeIndexesByPdf };
}

function rangeIsSelected(segment: MappingSegment, rangeIndex: number, selection: BuildSelection) {
  if (selection.rangeIndexesByPdf.size === 0) return true;
  return selection.rangeIndexesByPdf.get(segment.pdfUrl)?.has(rangeIndex) ?? false;
}

function filterSelectedPages(segment: MappingSegment, pages: number[], selection: BuildSelection) {
  if (selection.pagesByPdf.size === 0) return pages;
  const selectedPages = selection.pagesByPdf.get(segment.pdfUrl);
  if (!selectedPages) return [];
  return pages.filter((page) => selectedPages.has(page));
}

async function loadSourcePdf(sourcePath: string) {
  await ensureFreeMemory("load source PDF");
  return PDFDocument.load(await readFile(sourcePath), { ignoreEncryption: true });
}

async function copyPagesInto(outputDoc: PDFDocument, sourcePath: string, pages: number[]) {
  const sourceDoc = await loadSourcePdf(sourcePath);
  const pageCount = sourceDoc.getPageCount();
  const validPages = uniqueSortedPages(pages).filter((page) => page <= pageCount);
  if (validPages.length === 0) return 0;

  const copiedPages = await outputDoc.copyPages(
    sourceDoc,
    validPages.map((page) => page - 1)
  );
  for (const page of copiedPages) outputDoc.addPage(page);
  return copiedPages.length;
}

async function makePdfFromPages(sourcePath: string, pages: number[], outPath: string) {
  const outputDoc = await PDFDocument.create();
  const copied = await copyPagesInto(outputDoc, sourcePath, pages);
  if (copied === 0) throw new Error("No valid pages selected");
  await ensureFreeMemory("save PDF");
  await writeFile(outPath, await outputDoc.save());
}

function pagesForRange(range: MappingRange) {
  const start = Math.max(1, Math.floor(Number(range.pageStart)));
  const end = Math.max(start, Math.floor(Number(range.pageEnd || range.pageStart)));
  const pages: number[] = [];
  for (let page = start; page <= end; page += 1) pages.push(page);
  return pages;
}

function pagesForDownloadSegment(
  segment: MappingSegment,
  contextPages: number,
  includeCover: boolean,
  selection: BuildSelection
) {
  const selectedRanges = segment.ranges.filter((_, index) => rangeIsSelected(segment, index, selection));
  if (selectedRanges.length === 0) return [];
  const rangePages = selectedRanges.flatMap((range) => pagesForRange(range));
  return uniqueSortedPages([
    ...(includeCover ? [1] : []),
    ...expandPagesWithContext(rangePages, contextPages),
  ]);
}

function pagesForDownloadRange(
  segment: MappingSegment,
  range: MappingRange,
  rangeIndex: number,
  contextPages: number,
  includeCover: boolean,
  selection: BuildSelection
) {
  if (!rangeIsSelected(segment, rangeIndex, selection)) return [];
  return uniqueSortedPages([
    ...(includeCover ? [1] : []),
    ...expandPagesWithContext(pagesForRange(range), contextPages),
  ]);
}

async function buildCombined(
  segments: MappingSegment[],
  workDir: string,
  contextPages: number,
  includeCover: boolean,
  selection: BuildSelection
) {
  const outputDoc = await PDFDocument.create();
  const seenPagesByPdf = new Map<string, Set<number>>();
  let copied = 0;

  for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex += 1) {
    const segment = segments[segmentIndex];
    const sourcePath = await downloadSourcePdf(segment.pdfUrl);
    const seenPages = seenPagesByPdf.get(segment.pdfUrl) ?? new Set<number>();
    const pages = filterSelectedPages(segment, pagesForDownloadSegment(segment, contextPages, includeCover, selection), selection).filter((page) => {
      if (seenPages.has(page)) return false;
      seenPages.add(page);
      return true;
    });
    seenPagesByPdf.set(segment.pdfUrl, seenPages);
    if (pages.length === 0) continue;
    copied += await copyPagesInto(outputDoc, sourcePath, pages);
  }

  if (copied === 0) throw new Error("No pages selected");

  const outPath = path.join(workDir, "combined.pdf");
  await ensureFreeMemory("save combined PDF");
  await writeFile(outPath, await outputDoc.save());

  return outPath;
}

async function buildSeparateZip(
  segments: MappingSegment[],
  workDir: string,
  contextPages: number,
  includeCover: boolean,
  selection: BuildSelection
) {
  const outDir = path.join(workDir, "separate");
  await mkdir(outDir, { recursive: true });
  const builtFiles: string[] = [];

  for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex += 1) {
    const segment = segments[segmentIndex];
    const sourcePath = await downloadSourcePdf(segment.pdfUrl);
    for (let rangeIndex = 0; rangeIndex < segment.ranges.length; rangeIndex += 1) {
      const range = segment.ranges[rangeIndex];
      const label = range.gatha
        ? `id-${range.adhikar ?? "NA"}_gatha-${range.gatha}`
        : `pages-${range.pageStart}-${range.pageEnd}`;
      const outPath = path.join(
        outDir,
        `${safeFileName(segment.pdfFileName, `segment-${segmentIndex + 1}`)}_${safeFileName(label, `range-${rangeIndex + 1}`)}.pdf`
      );
      const pages = filterSelectedPages(
        segment,
        pagesForDownloadRange(segment, range, rangeIndex, contextPages, includeCover, selection),
        selection
      );
      if (pages.length === 0) continue;
      await makePdfFromPages(sourcePath, pages, outPath);
      builtFiles.push(outPath);
    }
  }

  if (builtFiles.length === 0) throw new Error("No ranges selected");

  const zipPath = path.join(workDir, "separate.zip");
  await runCommand("zip", ["-q", "-j", zipPath, ...builtFiles], workDir);
  return zipPath;
}

function countCombinedPages(
  segments: MappingSegment[],
  contextPages: number,
  includeCover: boolean,
  selection: BuildSelection
) {
  const seenPagesByPdf = new Map<string, Set<number>>();

  for (const segment of segments) {
    const seenPages = seenPagesByPdf.get(segment.pdfUrl) ?? new Set<number>();
    for (const page of filterSelectedPages(segment, pagesForDownloadSegment(segment, contextPages, includeCover, selection), selection)) {
      seenPages.add(page);
    }
    seenPagesByPdf.set(segment.pdfUrl, seenPages);
  }

  return [...seenPagesByPdf.values()].reduce((sum, pages) => sum + pages.size, 0);
}

function countSeparatePages(
  segments: MappingSegment[],
  contextPages: number,
  includeCover: boolean,
  selection: BuildSelection
) {
  return segments.reduce(
    (sum, segment) =>
      sum +
      segment.ranges.reduce((rangeSum, range, rangeIndex) => {
        const pages = pagesForDownloadRange(segment, range, rangeIndex, contextPages, includeCover, selection);
        return rangeSum + filterSelectedPages(segment, pages, selection).length;
      }, 0),
    0
  );
}

function streamFile(res: NextApiResponse, filePath: string, contentType: string, filename: string, cleanupDir: string) {
  res.setHeader("Content-Type", contentType);
  res.setHeader("Content-Disposition", contentDisposition(filename));
  res.setHeader("Cache-Control", "no-store");
  res.on("finish", () => {
    void rm(cleanupDir, { recursive: true, force: true });
  });
  createReadStream(filePath).pipe(res);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  let workDir = "";

  try {
    await ensureFreeMemory("start PDF build");

    const body = (req.body || {}) as BuildBody;
    const mode: BuildMode = body.mode === "separate" ? "separate" : "combined";
    const delivery = body.delivery === "email" ? "email" : "download";
    const adhikar = body.adhikar == null || body.adhikar === "" ? null : toInt(body.adhikar, null);
    const includeCover = Boolean(body.includeCover);
    const contextPages = normalizeContextPageRadius(body.contextPages);
    const selection = parseBuildSelection(body.selection);

    const resolved = await resolveGranthSelection({
      bookId: toInt(body.bookId, null),
      bookCode: body.bookCode || "",
      rawFileId: toInt(body.rawFileId, null),
      kind: body.kind || "gathas",
      spec: String(body.spec || ""),
      adhikar,
      includeCover,
      includeAllIdentifiers: Boolean(body.includeAllIdentifiers),
    });

    const pageTotal =
      mode === "separate"
        ? countSeparatePages(resolved.segments, contextPages, includeCover, selection)
        : countCombinedPages(resolved.segments, contextPages, includeCover, selection);
    if (pageTotal <= 0) {
      return res.status(400).json({ error: "Select at least one mapped page before building the PDF." });
    }
    if (pageTotal > MAX_PAGES_PER_BUILD) {
      return res.status(413).json({
        error: `Selection contains ${pageTotal} pages after nearby pages are added. Narrow it below ${MAX_PAGES_PER_BUILD} pages for a browser download.`,
      });
    }

    workDir = await mkdtemp(path.join(tmpdir(), "ndms-granth-build-"));
    const title = safeFileName(String(body.title || "granth_selection"), "granth_selection");

    if (mode === "separate") {
      const zipPath = await buildSeparateZip(resolved.segments, workDir, contextPages, includeCover, selection);
      const filename = `${title}_separate.zip`;
      if (delivery === "email") {
        const recipientClientKey = getDownloadRecipientClientKey(req, res);
        const sent = await sendDownloadEmail({
          to: String(body.email || ""),
          filePath: zipPath,
          filename,
          contentType: "application/zip",
          title: `${title} separate files`,
          recipientClientKey,
        });
        await rm(workDir, { recursive: true, force: true });
        workDir = "";
        return res.status(200).json({
          emailed: true,
          email: sent.email,
          size_bytes: sent.sizeBytes,
          file_name: filename,
        });
      }
      streamFile(res, zipPath, "application/zip", filename, workDir);
      return;
    }

    const pdfPath = await buildCombined(resolved.segments, workDir, contextPages, includeCover, selection);
    const filename = `${title}_combined.pdf`;
    if (delivery === "email") {
      const recipientClientKey = getDownloadRecipientClientKey(req, res);
      const sent = await sendDownloadEmail({
        to: String(body.email || ""),
        filePath: pdfPath,
        filename,
        contentType: "application/pdf",
        title: `${title} combined PDF`,
        recipientClientKey,
      });
      await rm(workDir, { recursive: true, force: true });
      workDir = "";
      return res.status(200).json({
        emailed: true,
        email: sent.email,
        size_bytes: sent.sizeBytes,
        file_name: filename,
      });
    }

    streamFile(res, pdfPath, "application/pdf", filename, workDir);
  } catch (error) {
    if (workDir) await rm(workDir, { recursive: true, force: true });
    if (error instanceof DownloadEmailError) {
      return res.status(error.status).json({ error: error.message });
    }
    if (error instanceof GranthResolveError) {
      return res.status(error.status).json(error.payload);
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
