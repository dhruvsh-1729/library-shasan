import { createHash, randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, mkdtemp, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";
import { PDFDocument } from "pdf-lib";
import type { OCRSearchMode } from "@/lib/ocr-search";

type HighlightBuildOptions = {
  pdfUrl: string;
  pages: number[];
  query?: string;
  queries?: string[];
  matchMode?: OCRSearchMode;
};

export type CombinedPdfSource = {
  pdfUrl: string;
  pages: number[];
  label: string;
};

export type CombinedPdfSourceResult = {
  label: string;
  pdfUrl: string;
  included_pages: number[];
  skipped_pages: number[];
};

const SOURCE_CACHE_DIR = path.join(tmpdir(), "ndms-library-pdf-source-cache");
const MIN_AVAILABLE_MEMORY_MB = 768;
/** Keeps the shared source-PDF cache from filling the disk during large exports. */
const MAX_SOURCE_CACHE_BYTES = 4 * 1024 * 1024 * 1024;

function hashText(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

function uniqueSortedPages(pages: number[]) {
  return [...new Set(pages.map((page) => Math.floor(Number(page))).filter((page) => page > 0))].sort((a, b) => a - b);
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

export async function ensureFreeMemory(label: string) {
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

async function pruneSourceCache(keepPath: string) {
  try {
    const names = await readdir(SOURCE_CACHE_DIR);
    const files: Array<{ filePath: string; size: number; mtimeMs: number }> = [];
    let total = 0;

    for (const name of names) {
      if (!name.endsWith(".pdf")) continue;
      const filePath = path.join(SOURCE_CACHE_DIR, name);
      try {
        const info = await stat(filePath);
        files.push({ filePath, size: info.size, mtimeMs: info.mtimeMs });
        total += info.size;
      } catch {
        // Another request may have pruned this entry already.
      }
    }

    if (total <= MAX_SOURCE_CACHE_BYTES) return;

    files.sort((a, b) => a.mtimeMs - b.mtimeMs);
    for (const file of files) {
      if (total <= MAX_SOURCE_CACHE_BYTES) break;
      if (file.filePath === keepPath) continue;
      await rm(file.filePath, { force: true });
      total -= file.size;
    }
  } catch {
    // Cache pruning is best effort; a full cache never blocks a build.
  }
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
  await pruneSourceCache(cachePath);
  return cachePath;
}

export async function buildHighlightedSearchPdf(options: HighlightBuildOptions) {
  await ensureFreeMemory("start selected-page PDF build");

  const pages = uniqueSortedPages(options.pages);
  if (pages.length === 0) throw new Error("No pages selected.");

  const workDir = await mkdtemp(path.join(tmpdir(), "ndms-search-pages-"));

  try {
    const sourcePath = await downloadSourcePdf(options.pdfUrl);

    await ensureFreeMemory("load source PDF");
    const sourceDoc = await PDFDocument.load(await readFile(sourcePath), { ignoreEncryption: true });
    const pageCount = sourceDoc.getPageCount();
    const validPages = pages.filter((page) => page <= pageCount);
    if (validPages.length === 0) throw new Error("Selected pages are outside the PDF page range.");

    const outputDoc = await PDFDocument.create();
    const copiedPages = await outputDoc.copyPages(
      sourceDoc,
      validPages.map((page) => page - 1)
    );
    for (const page of copiedPages) outputDoc.addPage(page);

    const outPath = path.join(workDir, "selected-pages.pdf");
    await ensureFreeMemory("save selected-page PDF");
    await writeFile(outPath, await outputDoc.save());

    return { filePath: outPath, cleanupDir: workDir };
  } catch (error) {
    await rm(workDir, { recursive: true, force: true });
    throw error;
  }
}

/**
 * Merges matched pages from many granths into one PDF, keeping each granth's
 * pages together and in order. Sources are processed one at a time so a large
 * multi-granth export does not hold every source PDF in memory at once.
 */
export async function buildCombinedSearchPdf(options: { sources: CombinedPdfSource[] }) {
  await ensureFreeMemory("start combined granth PDF build");

  const workDir = await mkdtemp(path.join(tmpdir(), "ndms-search-combined-"));
  const included: CombinedPdfSourceResult[] = [];
  const failed: Array<{ label: string; error: string }> = [];

  try {
    const outputDoc = await PDFDocument.create();

    for (const source of options.sources) {
      const pages = uniqueSortedPages(source.pages);
      if (pages.length === 0) continue;

      try {
        const sourcePath = await downloadSourcePdf(source.pdfUrl);
        await ensureFreeMemory(`load source PDF for ${source.label}`);
        const sourceDoc = await PDFDocument.load(await readFile(sourcePath), { ignoreEncryption: true });
        const pageCount = sourceDoc.getPageCount();
        const validPages = pages.filter((page) => page <= pageCount);
        const skippedPages = pages.filter((page) => page > pageCount);

        if (validPages.length === 0) {
          failed.push({ label: source.label, error: "Matched pages are outside the PDF page range." });
          continue;
        }

        const copiedPages = await outputDoc.copyPages(
          sourceDoc,
          validPages.map((page) => page - 1)
        );
        for (const page of copiedPages) outputDoc.addPage(page);

        included.push({
          label: source.label,
          pdfUrl: source.pdfUrl,
          included_pages: validPages,
          skipped_pages: skippedPages,
        });
      } catch (sourceError) {
        failed.push({
          label: source.label,
          error: sourceError instanceof Error ? sourceError.message : String(sourceError),
        });
      }
    }

    if (outputDoc.getPageCount() === 0) {
      const detail = failed.length > 0 ? ` ${failed[0].error}` : "";
      throw new Error(`No pages could be copied from the selected granths.${detail}`);
    }

    const outPath = path.join(workDir, "combined-pages.pdf");
    await ensureFreeMemory("save combined granth PDF");
    await writeFile(outPath, await outputDoc.save());

    return {
      filePath: outPath,
      cleanupDir: workDir,
      included,
      failed,
      totalPages: included.reduce((sum, entry) => sum + entry.included_pages.length, 0),
    };
  } catch (error) {
    await rm(workDir, { recursive: true, force: true });
    throw error;
  }
}
