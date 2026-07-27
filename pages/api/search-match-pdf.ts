import type { NextApiRequest, NextApiResponse } from "next";
import { createReadStream } from "node:fs";
import { rm } from "node:fs/promises";
import { parseOCRSearchMode } from "@/lib/ocr-search";
import { buildHighlightedSearchPdf } from "@/lib/pdf-highlight-builder";
import { setNoStore } from "@/lib/api-cache";
import {
  MAX_MATCH_PAGE_DOWNLOAD,
  SearchMatchError,
  loadSearchMatchPages,
  resolveSearchPdfSource,
  validateSearchDownloadQuery,
} from "@/lib/search-match-pages";

export const config = {
  api: {
    responseLimit: false,
  },
};

type DownloadBody = {
  customId?: string | null;
  sourceRelPath?: string | null;
  q?: string | null;
  matchMode?: string | null;
  pages?: unknown;
  title?: string | null;
};

function safeFileName(value: string, fallback = "matched_pages") {
  const cleaned = String(value || "")
    .replace(/\.pdf$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return cleaned || fallback;
}

function parseSelectedPages(value: unknown) {
  if (!Array.isArray(value)) return null;
  return [...new Set(
    value
      .map((page) => Math.floor(Number(page)))
      .filter((page) => Number.isFinite(page) && page > 0)
  )].sort((a, b) => a - b);
}

function streamFile(res: NextApiResponse, filePath: string, filename: string, cleanupDir: string) {
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.setHeader("Cache-Control", "no-store");

  const cleanup = () => {
    void rm(cleanupDir, { recursive: true, force: true });
  };

  res.on("finish", cleanup);
  res.on("close", cleanup);
  createReadStream(filePath).pipe(res);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const body = (req.body || {}) as DownloadBody;
    const customId = String(body.customId || "").trim();
    const sourceRelPath = String(body.sourceRelPath || "").trim();
    const q = validateSearchDownloadQuery(String(body.q || "").trim(), parseOCRSearchMode(body.matchMode));
    const matchMode = parseOCRSearchMode(body.matchMode);
    const requestedPages = parseSelectedPages(body.pages);

    const source = await resolveSearchPdfSource(customId);
    const { pages: matchingPages } = await loadSearchMatchPages(sourceRelPath || source.sourceRelPath, q, matchMode);
    const matchingPageSet = new Set(matchingPages.map((page) => page.page_number));
    const requested = requestedPages ?? matchingPages.map((page) => page.page_number);
    const selectedPages = requested.filter((page) => matchingPageSet.has(page));

    if (selectedPages.length === 0) {
      throw new SearchMatchError(400, "Select at least one matching page before downloading.");
    }
    if (selectedPages.length > MAX_MATCH_PAGE_DOWNLOAD) {
      throw new SearchMatchError(
        413,
        `Selection contains ${selectedPages.length} pages. Keep it at ${MAX_MATCH_PAGE_DOWNLOAD} pages or fewer.`
      );
    }

    const orderedPages = [1, ...selectedPages.filter((page) => page !== 1)];
    const built = await buildHighlightedSearchPdf({
      pdfUrl: source.pdfUrl,
      pages: orderedPages,
      query: q,
      matchMode,
    });
    const title = safeFileName(String(body.title || source.pdfName || customId), "matched_pages");
    const queryLabel = safeFileName(q, "search");

    streamFile(res, built.filePath, `${title}_${queryLabel}_matched_pages.pdf`, built.cleanupDir);
  } catch (error) {
    setNoStore(res);
    if (error instanceof SearchMatchError) {
      return res.status(error.status).json({ error: error.message });
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
