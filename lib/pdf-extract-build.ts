// Builds one PDF from exact page lists of library PDFs. The caller has already
// chosen every page (the extractor shows them before export), so nothing is
// resolved or widened here: what was previewed is what is built.
import { createHash, randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";
import { PDFDocument } from "pdf-lib";
import { availableMemoryMB } from "@/lib/available-memory";

export const MAX_EXTRACT_PAGES = 900;
const MIN_AVAILABLE_MEMORY_MB = 768;
const SOURCE_CACHE_DIR = path.join(tmpdir(), "ndms-library-pdf-source-cache");

export type ExtractPart = { pdfUrl: string; pages: number[] };

export class ExtractBuildError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export async function ensureFreeMemory(label: string) {
  const available = await availableMemoryMB();
  if (available != null && available < MIN_AVAILABLE_MEMORY_MB) {
    throw new ExtractBuildError(503, `The server is busy (${Math.round(available)} MB free, ${label} needs ${MIN_AVAILABLE_MEMORY_MB} MB). Try again in a minute.`);
  }
}

async function exists(file: string) {
  try {
    return (await stat(file)).size > 0;
  } catch {
    return false;
  }
}

/** A local copy of a library PDF, kept between builds. */
export async function sourcePdf(pdfUrl: string) {
  await mkdir(SOURCE_CACHE_DIR, { recursive: true });
  const cachePath = path.join(SOURCE_CACHE_DIR, `${createHash("sha256").update(pdfUrl).digest("hex")}.pdf`);
  if (await exists(cachePath)) return cachePath;
  await ensureFreeMemory("downloading the PDF");
  const tempPath = `${cachePath}.${process.pid}.${randomUUID()}.tmp`;
  const response = await fetch(pdfUrl);
  if (!response.ok || !response.body) throw new ExtractBuildError(502, `Could not fetch the source PDF (${response.status}).`);
  await pipeline(Readable.fromWeb(response.body as unknown as NodeReadableStream<Uint8Array>), createWriteStream(tempPath));
  await rename(tempPath, cachePath);
  return cachePath;
}

/** Copies the pages, part after part, in the order given; returns the file path and page count. */
export async function buildExtractPdf(parts: ExtractPart[], outPath: string) {
  const out = await PDFDocument.create();
  let copied = 0;
  for (const part of parts) {
    if (!part.pages.length) continue;
    await ensureFreeMemory("building the PDF");
    const source = await PDFDocument.load(await readFile(await sourcePdf(part.pdfUrl)), { ignoreEncryption: true });
    const last = source.getPageCount();
    const pages = part.pages.filter((p) => p >= 1 && p <= last);
    if (!pages.length) continue;
    for (const page of await out.copyPages(source, pages.map((p) => p - 1))) out.addPage(page);
    copied += pages.length;
  }
  if (!copied) throw new ExtractBuildError(400, "None of the chosen pages exist in these PDFs.");
  await ensureFreeMemory("saving the PDF");
  await writeFile(outPath, await out.save());
  return { path: outPath, pages: copied, sizeBytes: (await stat(outPath)).size };
}
