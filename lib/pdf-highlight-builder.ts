import { createHash, randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";
import { PDFDocument, rgb } from "pdf-lib";
import { findOCRSearchMatches, type OCRSearchMode } from "@/lib/ocr-search";

type WordBox = {
  text: string;
  xMin: number;
  yMin: number;
  xMax: number;
  yMax: number;
};

type HighlightBuildOptions = {
  pdfUrl: string;
  pages: number[];
  query: string;
  matchMode: OCRSearchMode;
};

const SOURCE_CACHE_DIR = path.join(tmpdir(), "ndms-library-pdf-source-cache");
const MIN_AVAILABLE_MEMORY_MB = 768;

function hashText(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

function uniqueSortedPages(pages: number[]) {
  return [...new Set(pages.map((page) => Math.floor(Number(page))).filter((page) => page > 0))].sort((a, b) => a - b);
}

function contiguousGroups(pages: number[]) {
  const sorted = uniqueSortedPages(pages);
  const groups: Array<{ start: number; end: number }> = [];

  for (const page of sorted) {
    const last = groups[groups.length - 1];
    if (last && page === last.end + 1) {
      last.end = page;
    } else {
      groups.push({ start: page, end: page });
    }
  }

  return groups;
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

async function runCommandOutput(command: string, args: string[], cwd?: string) {
  await ensureFreeMemory(command);

  return new Promise<string>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve(stdout);
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

async function extractPagesToMap(sourcePath: string, pages: number[], workDir: string) {
  const pageMap = new Map<number, string>();

  for (const group of contiguousGroups(pages)) {
    const pattern = path.join(workDir, `source-%d.pdf`);
    await runCommand("pdfseparate", ["-f", String(group.start), "-l", String(group.end), sourcePath, pattern], workDir);
    for (let page = group.start; page <= group.end; page += 1) {
      const pagePath = path.join(workDir, `source-${page}.pdf`);
      if (!(await fileExists(pagePath))) throw new Error(`Could not extract page ${page}.`);
      pageMap.set(page, pagePath);
    }
  }

  return pageMap;
}

function decodeXmlEntities(value: string) {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_, hex: string) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, code: string) => String.fromCodePoint(Number.parseInt(code, 10)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function attrNumber(attrs: string, name: string) {
  const match = attrs.match(new RegExp(`${name}="([^"]+)"`));
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function parseWordBoxes(xml: string) {
  const boxes: WordBox[] = [];
  const wordPattern = /<word\b([^>]*)>([\s\S]*?)<\/word>/g;
  let match: RegExpExecArray | null;

  while ((match = wordPattern.exec(xml)) !== null) {
    const attrs = match[1] || "";
    const text = decodeXmlEntities(String(match[2] || "").replace(/<[^>]+>/g, "")).trim();
    const xMin = attrNumber(attrs, "xMin");
    const yMin = attrNumber(attrs, "yMin");
    const xMax = attrNumber(attrs, "xMax");
    const yMax = attrNumber(attrs, "yMax");

    if (!text || xMin == null || yMin == null || xMax == null || yMax == null) continue;
    if (xMax <= xMin || yMax <= yMin) continue;

    boxes.push({ text, xMin, yMin, xMax, yMax });
  }

  return boxes;
}

async function extractWordBoxes(pagePath: string) {
  try {
    const xml = await runCommandOutput("pdftotext", ["-enc", "UTF-8", "-bbox", pagePath, "-"]);
    return parseWordBoxes(xml);
  } catch {
    return [];
  }
}

async function highlightSinglePage(
  inputPath: string,
  outputPath: string,
  query: string,
  matchMode: OCRSearchMode
) {
  const boxes = await extractWordBoxes(inputPath);
  const matchingBoxes = boxes
    .map((box) => ({ box, matches: findOCRSearchMatches(box.text, query, matchMode) }))
    .filter((entry) => entry.matches.length > 0);

  if (matchingBoxes.length === 0) {
    await copyFile(inputPath, outputPath);
    return;
  }

  await ensureFreeMemory("highlight PDF page");
  const pdfDoc = await PDFDocument.load(await readFile(inputPath), { ignoreEncryption: true });
  const page = pdfDoc.getPages()[0];
  if (!page) {
    await copyFile(inputPath, outputPath);
    return;
  }

  const pageHeight = page.getHeight();

  for (const { box, matches } of matchingBoxes) {
    const boxWidth = box.xMax - box.xMin;
    const boxHeight = box.yMax - box.yMin;
    const textLength = Math.max(1, box.text.length);

    for (const match of matches) {
      const startRatio = Math.max(0, Math.min(1, match.start / textLength));
      const endRatio = Math.max(startRatio, Math.min(1, match.end / textLength));
      const x = box.xMin + boxWidth * startRatio;
      const width = Math.max(1.5, boxWidth * (endRatio - startRatio));
      const y = pageHeight - box.yMax;

      page.drawRectangle({
        x: Math.max(0, x - 0.7),
        y: Math.max(0, y - 0.6),
        width: width + 1.4,
        height: boxHeight + 1.2,
        color: rgb(1, 0.92, 0),
        opacity: 0.45,
        borderWidth: 0,
      });
    }
  }

  await writeFile(outputPath, await pdfDoc.save());
}

export async function buildHighlightedSearchPdf(options: HighlightBuildOptions) {
  await ensureFreeMemory("start highlighted PDF build");

  const pages = uniqueSortedPages(options.pages);
  if (pages.length === 0) throw new Error("No pages selected.");

  const workDir = await mkdtemp(path.join(tmpdir(), "ndms-search-highlight-"));

  try {
    const sourcePath = await downloadSourcePdf(options.pdfUrl);
    const extracted = await extractPagesToMap(sourcePath, pages, workDir);
    const highlightedPaths: string[] = [];

    for (let index = 0; index < pages.length; index += 1) {
      const pageNumber = pages[index];
      const inputPath = extracted.get(pageNumber);
      if (!inputPath) throw new Error(`Could not prepare page ${pageNumber}.`);

      const highlightedPath = path.join(workDir, `highlighted-${index + 1}.pdf`);
      await highlightSinglePage(inputPath, highlightedPath, options.query, options.matchMode);
      highlightedPaths.push(highlightedPath);
    }

    const outPath = path.join(workDir, "highlighted-selection.pdf");
    if (highlightedPaths.length === 1) {
      await copyFile(highlightedPaths[0], outPath);
    } else {
      await runCommand("pdfunite", [...highlightedPaths, outPath], workDir);
    }

    return { filePath: outPath, cleanupDir: workDir };
  } catch (error) {
    await rm(workDir, { recursive: true, force: true });
    throw error;
  }
}
