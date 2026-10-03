import type { NextApiRequest, NextApiResponse } from "next";
import { createReadStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import {
  DownloadEmailError,
  MAX_EMAIL_ATTACHMENT_BYTES,
  getDownloadRecipientClientKey,
  sendDownloadEmail,
} from "@/lib/download-email";
import { libraryPdfUrls } from "@/lib/extract-data";
import { ExtractBuildError, MAX_EXTRACT_PAGES, buildExtractPdf, ensureFreeMemory, type ExtractPart } from "@/lib/pdf-extract-build";

export const config = { api: { responseLimit: false } };

type Body = {
  title?: string;
  parts?: Array<{ pdfUrl?: string; pages?: Array<number | string> }>;
  delivery?: "download" | "email";
  email?: string;
};

function safeName(value: string) {
  return (
    String(value || "")
      .replace(/\.pdf$/i, "")
      .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
      .replace(/^_+|_+$/g, "")
      .slice(0, 120) || "granth_pages"
  );
}

function disposition(filename: string) {
  const ascii = filename.replace(/[^\x20-\x7e]+/g, "_").replace(/["\\]/g, "_") || "download.pdf";
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}

/**
 * Builds the pages the extractor showed, from library PDFs only, and returns
 * the file or emails it. Email refuses a file over the attachment limit; the
 * page asks for smaller parts before it gets here.
 */
async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  let workDir = "";
  try {
    const body = (req.body ?? {}) as Body;
    const known = await libraryPdfUrls();
    const parts: ExtractPart[] = [];
    for (const raw of body.parts ?? []) {
      const pdfUrl = String(raw?.pdfUrl ?? "");
      const info = known.get(pdfUrl);
      if (!info) throw new ExtractBuildError(400, "One of the PDFs is not a library PDF.");
      const pages = [...new Set((raw?.pages ?? []).map((p) => Math.floor(Number(p))))].filter(
        (p) => Number.isFinite(p) && p >= 1 && (info.pageCount == null || p <= info.pageCount)
      );
      if (pages.length) parts.push({ pdfUrl, pages });
    }
    const total = parts.reduce((n, p) => n + p.pages.length, 0);
    if (!total) throw new ExtractBuildError(400, "Choose at least one page.");
    if (total > MAX_EXTRACT_PAGES) throw new ExtractBuildError(413, `That is ${total} pages; the limit is ${MAX_EXTRACT_PAGES}. Choose fewer gathas or pages.`);

    await ensureFreeMemory("building the PDF");
    workDir = await mkdtemp(path.join(tmpdir(), "ndms-extract-"));
    const filename = `${safeName(String(body.title ?? ""))}.pdf`;
    const built = await buildExtractPdf(parts, path.join(workDir, "out.pdf"));

    if (body.delivery === "email") {
      if (built.sizeBytes > MAX_EMAIL_ATTACHMENT_BYTES) {
        throw new ExtractBuildError(
          413,
          `This file is ${(built.sizeBytes / 1048576).toFixed(1)} MB, over the ${MAX_EMAIL_ATTACHMENT_BYTES / 1048576} MB email limit. Send it in smaller parts or download it.`
        );
      }
      const sent = await sendDownloadEmail({
        to: String(body.email ?? ""),
        filePath: built.path,
        filename,
        contentType: "application/pdf",
        title: String(body.title ?? "Granth pages"),
        recipientClientKey: getDownloadRecipientClientKey(req, res),
      });
      await rm(workDir, { recursive: true, force: true });
      workDir = "";
      return res.status(200).json({ emailed: true, email: sent.email, pages: built.pages, sizeBytes: sent.sizeBytes, fileName: filename });
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", disposition(filename));
    res.setHeader("Content-Length", String(built.sizeBytes));
    res.setHeader("X-Extract-Pages", String(built.pages));
    res.setHeader("Cache-Control", "no-store");
    const dir = workDir;
    workDir = "";
    res.on("close", () => void rm(dir, { recursive: true, force: true }));
    createReadStream(built.path).pipe(res);
  } catch (error) {
    if (workDir) await rm(workDir, { recursive: true, force: true });
    if (error instanceof ExtractBuildError || error instanceof DownloadEmailError) {
      return res.status(error.status).json({ error: error.message });
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.pdfBuild);
