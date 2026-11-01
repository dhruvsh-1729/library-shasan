import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { OcrError, imageToPdf, ocrPdf } from "@/lib/anuvaad/ocr";

// One uploaded box file (a PDF, or a photo of a page) read page by page,
// streamed as NDJSON: {type:"progress", done, total} while the pages are read,
// then {type:"done", pages: [markdown…]} or {type:"error"}. The file is sent as
// the raw request body and kept nowhere.

export const config = { api: { bodyParser: false, responseLimit: false }, maxDuration: 300 };

const MAX_BYTES = 60 * 1024 * 1024;

async function readBody(req: NextApiRequest) {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BYTES) throw new OcrError("This file is over 60 MB. Send it in parts.", 413);
    chunks.push(chunk as Buffer);
  }
  return new Uint8Array(Buffer.concat(chunks));
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const type = String(req.headers["content-type"] ?? "").toLowerCase();
  if (!/application\/pdf|image\/(jpeg|jpg|png)/.test(type)) {
    return res.status(415).json({ error: "Send a PDF, JPG or PNG file." });
  }
  let bytes: Uint8Array;
  try {
    bytes = await readBody(req);
  } catch (error) {
    return res.status(error instanceof OcrError ? error.status : 400).json({ error: error instanceof OcrError ? error.message : "The file did not arrive whole." });
  }
  if (!bytes.length) return res.status(400).json({ error: "The file is empty." });

  res.writeHead(200, {
    "Content-Type": "application/x-ndjson; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Content-Encoding": "none",
    "X-Accel-Buffering": "no",
  });
  res.flushHeaders?.();
  const emit = (event: Record<string, unknown>) => {
    if (!res.writableEnded) res.write(`${JSON.stringify(event)}\n`);
  };
  try {
    const pdf = type.includes("pdf") ? bytes : await imageToPdf(bytes, type);
    const pages = await ocrPdf(pdf, "hi-IN", (done, total) => emit({ type: "progress", done, total }));
    emit({ type: "done", pages });
  } catch (error) {
    if (!(error instanceof OcrError)) console.error("anuvaad ocr failed:", error instanceof Error ? error.message : error);
    emit({ type: "error", error: error instanceof OcrError ? error.message : "The pages could not be read. Please try again." });
  }
  res.end();
}

export default protectApi(handler, PERMISSIONS.libraryRead);
