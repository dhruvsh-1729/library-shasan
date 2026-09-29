import { createHash, randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, mkdtemp, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";
import { PDFDict, PDFDocument, PDFName, type PDFPage, PDFRawStream, PDFStream, degrees, rgb } from "pdf-lib";
import { extractPdfPagesByRange } from "@/lib/pdf-range-subset.mjs";
import { findGlyphWordBoxes } from "@/lib/pdf-glyph-boxes";
import { type PdfTextItem, type WordBox, findWordBoxes } from "@/lib/pdf-word-boxes";
import type { OCRSearchMode, OCRSearchScripts } from "@/lib/ocr-search";
import { availableMemoryMB } from "@/lib/available-memory";

type HighlightBuildOptions = {
  pdfUrl: string;
  pages: number[];
  query?: string;
  queries?: string[];
  matchMode?: OCRSearchMode;
  scripts?: OCRSearchScripts;
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

/** Red of the grease-pencil ring used on the search page. */
const RING_COLOR = rgb(0.776, 0.184, 0.11);
/** Books read at once in a combined export. */
const SOURCE_PARALLEL = 4;

/**
 * The wanted pages of a source PDF as a small standalone PDF. Normally only
 * those pages' bytes are fetched (HTTP ranges); a file the range reader cannot
 * handle is downloaded whole (disk-cached) and its pages copied, as before.
 */
async function sourcePagesPdf(pdfUrl: string, pages: number[]) {
  try {
    const subset = await extractPdfPagesByRange(pdfUrl, pages);
    return { bytes: subset.bytes, pages: subset.pages };
  } catch (error) {
    console.warn("range subset fell back to a full download", error instanceof Error ? error.message : error);
    const sourcePath = await downloadSourcePdf(pdfUrl);
    await ensureFreeMemory("load source PDF");
    const sourceDoc = await PDFDocument.load(await readFile(sourcePath), { ignoreEncryption: true, updateMetadata: false });
    const valid = pages.filter((page) => page <= sourceDoc.getPageCount());
    const out = await PDFDocument.create();
    for (const page of await out.copyPages(sourceDoc, valid.map((page) => page - 1))) out.addPage(page);
    return { bytes: await out.save(), pages: valid };
  }
}

type MarkOptions = { queries: string[]; matchMode: OCRSearchMode; scripts?: OCRSearchScripts };

/**
 * The same pages without their images. Finding words only needs the text
 * operators, and pdf.js would otherwise decode every full-page scan while
 * building the operator list, which is most of the time spent.
 */
async function withoutImages(bytes: Uint8Array) {
  const doc = await PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false });
  for (const page of doc.getPages()) {
    const resources = page.node.Resources();
    const xobjects = resources?.lookupMaybe(PDFName.of("XObject"), PDFDict);
    if (!xobjects) continue;
    for (const key of xobjects.keys()) {
      const target = xobjects.lookup(key);
      const subtype = target instanceof PDFRawStream || target instanceof PDFStream ? target.dict.get(PDFName.of("Subtype")) : null;
      if (subtype === PDFName.of("Image")) xobjects.delete(key);
    }
  }
  return doc.save({ useObjectStreams: false });
}

/** Where the searched word is on each page of a PDF, from its text layer. */
async function wordBoxesPerPage(bytes: Uint8Array, mark: MarkOptions): Promise<WordBox[][]> {
  const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await pdfjs.getDocument({
    data: await withoutImages(bytes),
    disableFontFace: true,
    isEvalSupported: false,
    useSystemFonts: false,
    fontExtraProperties: true,
    verbosity: 0,
  }).promise;
  try {
    const boxes: WordBox[][] = [];
    for (let i = 1; i <= doc.numPages; i += 1) {
      const page = await doc.getPage(i);
      // Exact glyph positions first; the text-content estimate if the page cannot be replayed.
      const exact = await findGlyphWordBoxes(page, pdfjs.OPS, mark.queries, mark.matchMode, mark.scripts ?? null);
      if (exact) {
        boxes.push(exact);
        continue;
      }
      const content = await page.getTextContent();
      boxes.push(findWordBoxes(content.items as PdfTextItem[], mark.queries, mark.matchMode, mark.scripts ?? null));
    }
    return boxes;
  } finally {
    await doc.destroy();
  }
}

/** Draws a red ellipse around each box; the ring becomes part of the page. */
function drawRings(page: PDFPage, boxes: WordBox[]) {
  for (const box of boxes) {
    page.drawEllipse({
      x: box.x + box.width / 2,
      y: box.y + box.height / 2,
      xScale: box.width / 2 + box.fontSize * 0.35,
      yScale: box.height / 2 + box.fontSize * 0.12,
      borderColor: RING_COLOR,
      borderWidth: Math.max(1.1, box.fontSize * 0.09),
      borderOpacity: 0.95,
      rotate: degrees(-2),
    });
  }
}

/** Fetches, rings and returns one source's pages, ready to append. */
async function markedSource(source: CombinedPdfSource, mark: MarkOptions | null) {
  const { bytes, pages } = await sourcePagesPdf(source.pdfUrl, uniqueSortedPagesKeepOrder(source.pages));
  const doc = await PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false });
  let rings = 0;
  if (mark?.queries.length) {
    try {
      const boxes = await wordBoxesPerPage(bytes, mark);
      doc.getPages().forEach((page, i) => {
        drawRings(page, boxes[i] ?? []);
        rings += boxes[i]?.length ?? 0;
      });
    } catch (error) {
      // A page that cannot be read for positions still exports, unmarked.
      console.warn("word rings skipped", error instanceof Error ? error.message : error);
    }
  }
  return { doc, pages, rings };
}

/** Pages in the order asked for, duplicates dropped (the cover page 1 comes first). */
function uniqueSortedPagesKeepOrder(pages: number[]) {
  return [...new Set(pages.map((page) => Math.floor(Number(page))).filter((page) => page > 0))];
}

async function writeOutput(doc: PDFDocument, name: string) {
  const workDir = await mkdtemp(path.join(tmpdir(), "ndms-search-pages-"));
  const outPath = path.join(workDir, name);
  await writeFile(outPath, await doc.save({ useObjectStreams: true }));
  return { filePath: outPath, cleanupDir: workDir };
}

export async function buildHighlightedSearchPdf(options: HighlightBuildOptions) {
  await ensureFreeMemory("start selected-page PDF build");
  const pages = uniqueSortedPagesKeepOrder(options.pages);
  if (pages.length === 0) throw new Error("No pages selected.");
  const queries = options.queries?.length ? options.queries : options.query ? [options.query] : [];
  const { doc, pages: kept } = await markedSource(
    { pdfUrl: options.pdfUrl, pages, label: "" },
    { queries, matchMode: options.matchMode ?? "sanskrit_forms", scripts: options.scripts ?? null }
  );
  if (kept.length === 0) throw new Error("Selected pages are outside the PDF page range.");
  return writeOutput(doc, "selected-pages.pdf");
}

/**
 * Merges matched pages from many granths into one PDF, keeping each granth's
 * pages together and in order, with the searched word ringed in red. Books are
 * fetched a few at a time, and only the pages needed.
 */
export async function buildCombinedSearchPdf(options: {
  sources: CombinedPdfSource[];
  queries?: string[];
  matchMode?: OCRSearchMode;
  scripts?: OCRSearchScripts;
}) {
  await ensureFreeMemory("start combined granth PDF build");
  const mark = options.queries?.length
    ? { queries: options.queries, matchMode: options.matchMode ?? "sanskrit_forms", scripts: options.scripts ?? null }
    : null;

  const results: Array<Awaited<ReturnType<typeof markedSource>> | { error: string }> = new Array(options.sources.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(SOURCE_PARALLEL, options.sources.length) }, async () => {
      while (next < options.sources.length) {
        const index = next++;
        try {
          results[index] = await markedSource(options.sources[index], mark);
        } catch (error) {
          results[index] = { error: error instanceof Error ? error.message : String(error) };
        }
      }
    })
  );

  const outputDoc = await PDFDocument.create();
  const included: CombinedPdfSourceResult[] = [];
  const failed: Array<{ label: string; error: string }> = [];
  for (const [index, source] of options.sources.entries()) {
    const result = results[index];
    if (!result || "error" in result) {
      failed.push({ label: source.label, error: result?.error ?? "Not built." });
      continue;
    }
    if (result.pages.length === 0) {
      failed.push({ label: source.label, error: "Matched pages are outside the PDF page range." });
      continue;
    }
    for (const page of await outputDoc.copyPages(result.doc, result.doc.getPageIndices())) outputDoc.addPage(page);
    const requested = uniqueSortedPagesKeepOrder(source.pages);
    included.push({
      label: source.label,
      pdfUrl: source.pdfUrl,
      included_pages: result.pages,
      skipped_pages: requested.filter((page) => !result.pages.includes(page)),
    });
  }

  if (outputDoc.getPageCount() === 0) {
    const detail = failed.length > 0 ? ` ${failed[0].error}` : "";
    throw new Error(`No pages could be copied from the selected granths.${detail}`);
  }
  const written = await writeOutput(outputDoc, "combined-pages.pdf");
  return {
    ...written,
    included,
    failed,
    totalPages: included.reduce((sum, entry) => sum + entry.included_pages.length, 0),
  };
}

/**
 * Starts reading a PDF's cross-reference data in the background, so a
 * download pressed a moment later only fetches its pages. Errors are ignored.
 */
export function warmPdfIndex(pdfUrl: string) {
  if (!pdfUrl) return;
  void extractPdfPagesByRange(pdfUrl, [1]).catch(() => undefined);
}
