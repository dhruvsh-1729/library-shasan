import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { parseOCRSearchMode } from "@/lib/ocr-search";
import {
  MAX_COMBINED_PDF_GRANTHS,
  MAX_CSV_GRANTHS,
  MAX_CSV_ROWS,
  MAX_EXPORT_GRANTH_PREVIEW,
  MAX_MATCH_PAGE_DOWNLOAD,
  SearchMatchError,
  loadSearchMatchGranths,
  resolveSearchPdfSources,
  validateSearchDownloadQueries,
} from "@/lib/search-match-pages";
import { resolveGranthPdfTargets, resolveRelPathsForCustomIds } from "@/lib/search-pdf-resolver";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";

function firstQueryValue(raw: string | string[] | undefined) {
  return Array.isArray(raw) ? raw[0] : raw;
}

function parseGranthIds(raw: string | string[] | undefined) {
  if (!raw) return [];
  const values = Array.isArray(raw) ? raw : String(raw).split(",");
  return values.map((value) => String(value).trim()).filter(Boolean);
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const q = String(firstQueryValue(req.query.q) || "").trim();
    const queryVariants = req.query.queryVariant ?? req.query.queryVariants;
    const matchMode = parseOCRSearchMode(firstQueryValue(req.query.matchMode));
    const queries = validateSearchDownloadQueries(q, queryVariants, matchMode);
    const selectedGranths = parseGranthIds(req.query.granths).slice(0, 500);

    const cacheKey = buildCacheKey(req, "search-match-granths");
    const { value: payload, status } = await getCachedJson(cacheKey, 60, async () => {
      const scopedRelPaths = selectedGranths.length
        ? (await resolveRelPathsForCustomIds(selectedGranths)).relPaths
        : null;

      if (scopedRelPaths && scopedRelPaths.length === 0) {
        return {
          granths: [],
          total_granths: 0,
          exportable_granths: 0,
          total_matched_pages: 0,
          truncated: false,
          match_mode: matchMode,
          queries,
          max_export_granth_preview: MAX_EXPORT_GRANTH_PREVIEW,
          max_combined_pdf_granths: MAX_COMBINED_PDF_GRANTHS,
          max_download_pages: MAX_MATCH_PAGE_DOWNLOAD,
          max_csv_granths: MAX_CSV_GRANTHS,
          max_csv_rows: MAX_CSV_ROWS,
        };
      }

      const { granths, truncated } = await loadSearchMatchGranths(scopedRelPaths, queries, matchMode);
      const targets = await resolveGranthPdfTargets(
        granths.map((granth) => ({
          granth_key: granth.granth_key,
          source_rel_path: granth.source_rel_path,
          pdf_name: granth.granth_name,
          page_number: granth.first_page,
        }))
      );
      const sources = await resolveSearchPdfSources(
        targets.map((target) => ({ customId: target.custom_id, sourceRelPath: target.source_rel_path }))
      );

      const items = granths.map((granth, index) => {
        const target = targets[index];
        const source = sources[index];
        return {
          granth_key: granth.granth_key,
          source_rel_path: granth.source_rel_path,
          granth_name: granth.granth_name || target.pdf_name,
          custom_id: target.custom_id,
          pdf_name: source.ok ? source.source.pdfName : target.pdf_name,
          matched_pages: granth.matched_pages,
          exportable: source.ok,
          unavailable_reason: source.ok ? null : source.error,
        };
      });

      return {
        granths: items,
        total_granths: items.length,
        exportable_granths: items.filter((item) => item.exportable).length,
        total_matched_pages: items.reduce((sum, item) => sum + item.matched_pages, 0),
        truncated,
        match_mode: matchMode,
        queries,
        max_export_granth_preview: MAX_EXPORT_GRANTH_PREVIEW,
        max_combined_pdf_granths: MAX_COMBINED_PDF_GRANTHS,
        max_download_pages: MAX_MATCH_PAGE_DOWNLOAD,
        max_csv_granths: MAX_CSV_GRANTHS,
        max_csv_rows: MAX_CSV_ROWS,
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    if (error instanceof SearchMatchError) {
      return res.status(error.status).json({ error: error.message });
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
