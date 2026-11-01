// OCR of uploaded box pages, through Sarvam's Doc AI "digitise" jobs: one job
// per 10 pages (the service's limit), run side by side, each giving every
// page as Markdown (tables as HTML). Measured on the box scans of 29 Oct 2026:
// about 4 seconds for 2 pages, tables read whole.
//
// Nothing is kept on our side: the pages live only in the request.

import { PDFDocument } from "pdf-lib";
import { unzip } from "@/lib/anuvaad/unzip";

const BASE = "https://api.sarvam.ai/doc-ai/v1";
const PAGES_PER_JOB = 10;
const PARALLEL_JOBS = 3;
const POLL_MS = 2500;
const JOB_TIMEOUT_MS = 8 * 60_000;

export class OcrError extends Error {
  constructor(message: string, readonly status = 502) {
    super(message);
  }
}

function key() {
  const k = process.env.SARVAM_API_KEY;
  if (!k) throw new OcrError("Reading the pages is not set up on this server.", 503);
  return k;
}

async function call(path: string, init?: RequestInit) {
  const res = await fetch(`${BASE}${path}`, { ...init, headers: { "api-subscription-key": key(), ...(init?.headers ?? {}) } });
  const text = await res.text();
  let body: Record<string, unknown> = {};
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!res.ok) {
    console.error("ocr call failed", path, res.status, text.slice(0, 300));
    throw new OcrError(res.status === 429 ? "Too many pages are being read right now. Try again in a minute." : "The pages could not be read just now. Please try again.");
  }
  return body;
}

/** An image (JPEG or PNG) as a one-page PDF, the page the size of the image. */
export async function imageToPdf(bytes: Uint8Array, type: string) {
  const doc = await PDFDocument.create();
  const image = type.includes("png") ? await doc.embedPng(bytes) : await doc.embedJpg(bytes);
  const page = doc.addPage([image.width, image.height]);
  page.drawImage(image, { x: 0, y: 0, width: image.width, height: image.height });
  return doc.save();
}

async function splitPdf(bytes: Uint8Array) {
  const src = await PDFDocument.load(bytes, { ignoreEncryption: true });
  const total = src.getPageCount();
  const chunks: Array<{ first: number; bytes: Uint8Array }> = [];
  for (let start = 0; start < total; start += PAGES_PER_JOB) {
    const out = await PDFDocument.create();
    const indices = Array.from({ length: Math.min(PAGES_PER_JOB, total - start) }, (_, i) => start + i);
    for (const page of await out.copyPages(src, indices)) out.addPage(page);
    chunks.push({ first: start, bytes: await out.save() });
  }
  return { total, chunks };
}

type Block = { text?: string; reading_order?: number; layout_tag?: string };

async function digitise(pdf: Uint8Array, language: string): Promise<string[]> {
  const form = new FormData();
  form.append("file", new Blob([pdf as BlobPart], { type: "application/pdf" }), "pages.pdf");
  form.append("output_format", "md");
  form.append("language", language);
  const job = await call("/job/digitise", { method: "POST", body: form });
  const jobId = String(job.job_id ?? "");
  if (!jobId) throw new OcrError("The pages could not be read just now. Please try again.");

  const started = Date.now();
  for (;;) {
    await new Promise((r) => setTimeout(r, POLL_MS));
    const status = await call(`/job/${jobId}/status`);
    const state = String(status.status ?? status.job_status ?? "").toLowerCase();
    if (state === "completed" || state === "partially_completed") break;
    if (state === "failed" || state === "rejected") throw new OcrError("These pages could not be read. Check that the scan is clear and try again.");
    if (Date.now() - started > JOB_TIMEOUT_MS) throw new OcrError("Reading the pages took too long. Try fewer pages at a time.");
  }

  const download = await call(`/job/${jobId}/download-url`);
  const res = await fetch(String(download.url ?? download.download_url ?? ""));
  if (!res.ok) throw new OcrError("The pages could not be read just now. Please try again.");
  const files = unzip(new Uint8Array(await res.arrayBuffer()));

  // Each page's blocks in reading order; the whole-document Markdown is the fallback.
  const pages: Array<{ n: number; text: string }> = [];
  for (const [name, data] of files) {
    const m = name.match(/page_(\d+)\.json$/);
    if (!m) continue;
    const json = JSON.parse(data.toString("utf8")) as { blocks?: Block[] };
    const text = (json.blocks ?? [])
      .slice()
      .sort((a, b) => (a.reading_order ?? 0) - (b.reading_order ?? 0))
      .map((b) => b.text ?? "")
      .filter(Boolean)
      .join("\n\n");
    pages.push({ n: Number(m[1]), text });
  }
  if (pages.length) return pages.sort((a, b) => a.n - b.n).map((p) => p.text);
  const md = [...files].find(([name]) => name.endsWith(".md"))?.[1].toString("utf8") ?? "";
  return md.split(/\n---\n/);
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>) {
  const out = new Array<R>(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i]);
      }
    })
  );
  return out;
}

export const MAX_OCR_PAGES = 120;

/** Every page of a PDF as Markdown, in order. */
export async function ocrPdf(bytes: Uint8Array, language = "hi-IN", onProgress?: (done: number, total: number) => void) {
  let split: Awaited<ReturnType<typeof splitPdf>>;
  try {
    split = await splitPdf(bytes);
  } catch {
    throw new OcrError("This file could not be opened as a PDF.", 400);
  }
  if (!split.total) throw new OcrError("This PDF has no pages.", 400);
  if (split.total > MAX_OCR_PAGES) throw new OcrError(`This PDF has ${split.total} pages; send at most ${MAX_OCR_PAGES} at a time.`, 400);
  let done = 0;
  const results = await mapLimit(split.chunks, PARALLEL_JOBS, async (chunk) => {
    const pages = await digitise(chunk.bytes, language);
    done += Math.min(PAGES_PER_JOB, split.total - chunk.first);
    onProgress?.(done, split.total);
    return pages;
  });
  return results.flat();
}
