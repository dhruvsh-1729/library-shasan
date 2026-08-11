import type { NextApiRequest, NextApiResponse } from "next";
import { createReadStream } from "node:fs";
import { rm } from "node:fs/promises";
import { parseOCRSearchMode } from "@/lib/ocr-search";
import {
  type CombinedPdfSource,
  buildCombinedSearchPdf,
  buildHighlightedSearchPdf,
} from "@/lib/pdf-highlight-builder";
import { expandPagesWithContext, normalizeContextPageRadius } from "@/lib/page-context";
import { setNoStore } from "@/lib/api-cache";
import { DownloadEmailError, getDownloadRecipientClientKey, sendDownloadEmail } from "@/lib/download-email";
import {
  MAX_COMBINED_PDF_GRANTHS,
  MAX_MATCH_PAGE_DOWNLOAD,
  SearchMatchError,
  loadSearchMatchPages,
  resolveSearchPdfSource,
  resolveSearchPdfSources,
  validateSearchDownloadQueries,
} from "@/lib/search-match-pages";

export const config = {
  api: {
    responseLimit: false,
  },
};

type GranthSelection = {
  customId?: string | null;
  sourceRelPath?: string | null;
  pages?: unknown;
};

type DownloadBody = {
  customId?: string | null;
  sourceRelPath?: string | null;
  q?: string | null;
  queryVariants?: unknown;
  matchMode?: string | null;
  pages?: unknown;
  contextPages?: unknown;
  delivery?: string | null;
  email?: string | null;
  title?: string | null;
  granths?: unknown;
  maxPagesPerGranth?: unknown;
};

/** 0 (or absent) keeps every matched page of every selected granth. */
function parseMaxPagesPerGranth(value: unknown) {
  const parsed = Math.floor(Number(value ?? 0));
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return Math.min(parsed, MAX_MATCH_PAGE_DOWNLOAD);
}

function safeFileName(value: string, fallback = "matched_pages") {
  const cleaned = String(value || "")
    .replace(/\.pdf$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return cleaned || fallback;
}

function contentDisposition(filename: string) {
  const ascii = filename.replace(/[^\x20-\x7e]+/g, "_").replace(/["\\]/g, "_") || "download.pdf";
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}

function parseQueryVariants(value: unknown) {
  if (Array.isArray(value)) return value.map((item) => String(item || ""));
  if (value == null) return [];
  return [String(value || "")];
}

function parseSelectedPages(value: unknown) {
  if (!Array.isArray(value)) return null;
  return [...new Set(
    value
      .map((page) => Math.floor(Number(page)))
      .filter((page) => Number.isFinite(page) && page > 0)
  )].sort((a, b) => a - b);
}

function parseGranthSelections(value: unknown): GranthSelection[] {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const selections: GranthSelection[] = [];

  for (const entry of value) {
    if (!entry || typeof entry !== "object") continue;
    const candidate = entry as GranthSelection;
    const customId = String(candidate.customId || "").trim();
    const sourceRelPath = String(candidate.sourceRelPath || "").trim();
    if (!customId) continue;
    const key = `${customId}\n${sourceRelPath}`;
    if (seen.has(key)) continue;
    seen.add(key);
    selections.push({ customId, sourceRelPath, pages: candidate.pages });
  }

  return selections;
}

function streamFile(res: NextApiResponse, filePath: string, filename: string, cleanupDir: string) {
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", contentDisposition(filename));
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
    const delivery = body.delivery === "email" ? "email" : "download";
    const matchMode = parseOCRSearchMode(body.matchMode);
    const queries = validateSearchDownloadQueries(String(body.q || "").trim(), parseQueryVariants(body.queryVariants), matchMode);
    const contextPages = normalizeContextPageRadius(body.contextPages);
    const granthSelections = parseGranthSelections(body.granths);
    const queryLabel = safeFileName(queries.join("_"), "search");

    let built: { filePath: string; cleanupDir: string };
    let filename: string;
    let emailTitle: string;

    if (granthSelections.length > 0) {
      if (granthSelections.length > MAX_COMBINED_PDF_GRANTHS) {
        throw new SearchMatchError(
          413,
          `Select up to ${MAX_COMBINED_PDF_GRANTHS} granths for one combined PDF. ${granthSelections.length} were selected.`
        );
      }

      const sources = await resolveSearchPdfSources(
        granthSelections.map((selection) => ({
          customId: String(selection.customId || ""),
          sourceRelPath: String(selection.sourceRelPath || ""),
        }))
      );

      const maxPagesPerGranth = parseMaxPagesPerGranth(body.maxPagesPerGranth);
      const combinedSources: CombinedPdfSource[] = [];
      const skipped: Array<{ granth: string; reason: string }> = [];
      let totalPages = 0;

      for (const [index, selection] of granthSelections.entries()) {
        const resolved = sources[index];
        if (!resolved.ok) {
          skipped.push({ granth: String(selection.customId || ""), reason: resolved.error });
          continue;
        }

        const source = resolved.source;
        const requestedPages = parseSelectedPages(selection.pages);
        const { pages: matchingPages } = await loadSearchMatchPages(
          String(selection.sourceRelPath || "") || source.sourceRelPath,
          queries,
          matchMode
        );

        if (matchingPages.length === 0) {
          skipped.push({ granth: source.pdfName, reason: "No matching pages found." });
          continue;
        }

        const matchingPageSet = new Set(matchingPages.map((page) => page.page_number));
        const matchedSelection = (requestedPages ?? matchingPages.map((page) => page.page_number)).filter((page) =>
          matchingPageSet.has(page)
        );
        const selectedPages = maxPagesPerGranth > 0 ? matchedSelection.slice(0, maxPagesPerGranth) : matchedSelection;
        if (selectedPages.length === 0) {
          skipped.push({ granth: source.pdfName, reason: "No matching pages selected." });
          continue;
        }

        const expandedPages = expandPagesWithContext(selectedPages, contextPages);
        const orderedPages = [1, ...expandedPages.filter((page) => page !== 1)];
        totalPages += orderedPages.length;

        if (totalPages > MAX_MATCH_PAGE_DOWNLOAD) {
          throw new SearchMatchError(
            413,
            `This selection needs more than ${MAX_MATCH_PAGE_DOWNLOAD} PDF pages once nearby pages and covers are added. Deselect some granths or reduce the nearby-page count.`
          );
        }

        combinedSources.push({ pdfUrl: source.pdfUrl, pages: orderedPages, label: source.pdfName });
      }

      if (combinedSources.length === 0) {
        const detail = skipped.length > 0 ? ` ${skipped[0].reason}` : "";
        throw new SearchMatchError(404, `None of the selected granths could be exported.${detail}`);
      }

      const combined = await buildCombinedSearchPdf({ sources: combinedSources });
      built = { filePath: combined.filePath, cleanupDir: combined.cleanupDir };
      const granthLabel = combinedSources.length === 1 ? safeFileName(combinedSources[0].label, "granth") : `${combinedSources.length}_granths`;
      filename = `granth_search_${queryLabel}_${granthLabel}_matched_pages.pdf`;
      emailTitle = `${queries.join(", ")} matched pages from ${combinedSources.length} granth(s)`;
    } else {
      const customId = String(body.customId || "").trim();
      const sourceRelPath = String(body.sourceRelPath || "").trim();
      const requestedPages = parseSelectedPages(body.pages);

      const source = await resolveSearchPdfSource(customId, sourceRelPath);
      const { pages: matchingPages } = await loadSearchMatchPages(sourceRelPath || source.sourceRelPath, queries, matchMode);
      const matchingPageSet = new Set(matchingPages.map((page) => page.page_number));
      const requested = requestedPages ?? matchingPages.map((page) => page.page_number);
      const selectedPages = requested.filter((page) => matchingPageSet.has(page));

      if (selectedPages.length === 0) {
        throw new SearchMatchError(400, "Select at least one matching page before downloading.");
      }

      const expandedPages = expandPagesWithContext(selectedPages, contextPages);
      const orderedPages = [1, ...expandedPages.filter((page) => page !== 1)];

      if (orderedPages.length > MAX_MATCH_PAGE_DOWNLOAD) {
        throw new SearchMatchError(
          413,
          `Selection contains ${orderedPages.length} pages after nearby pages are added. Keep it at ${MAX_MATCH_PAGE_DOWNLOAD} pages or fewer.`
        );
      }

      built = await buildHighlightedSearchPdf({
        pdfUrl: source.pdfUrl,
        pages: orderedPages,
        queries,
        matchMode,
      });
      const title = safeFileName(String(body.title || source.pdfName || customId), "matched_pages");
      filename = `${title}_${queryLabel}_matched_pages.pdf`;
      emailTitle = `${title} matched pages`;
    }

    if (delivery === "email") {
      try {
        const recipientClientKey = getDownloadRecipientClientKey(req, res);
        const sent = await sendDownloadEmail({
          to: String(body.email || ""),
          filePath: built.filePath,
          filename,
          contentType: "application/pdf",
          title: emailTitle,
          recipientClientKey,
        });
        await rm(built.cleanupDir, { recursive: true, force: true });
        return res.status(200).json({
          emailed: true,
          email: sent.email,
          size_bytes: sent.sizeBytes,
          file_name: filename,
        });
      } catch (emailError) {
        await rm(built.cleanupDir, { recursive: true, force: true });
        throw emailError;
      }
    }

    streamFile(res, built.filePath, filename, built.cleanupDir);
  } catch (error) {
    setNoStore(res);
    if (error instanceof DownloadEmailError) {
      return res.status(error.status).json({ error: error.message });
    }
    if (error instanceof SearchMatchError) {
      return res.status(error.status).json({ error: error.message });
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
