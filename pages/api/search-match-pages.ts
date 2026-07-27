import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { parseOCRSearchMode } from "@/lib/ocr-search";
import {
  MAX_MATCH_PAGE_DOWNLOAD,
  SearchMatchError,
  loadSearchMatchPages,
  resolveSearchPdfSource,
  validateSearchDownloadQueries,
} from "@/lib/search-match-pages";

function firstQueryValue(raw: string | string[] | undefined) {
  return Array.isArray(raw) ? raw[0] : raw;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const customId = String(firstQueryValue(req.query.customId) || "").trim();
    const sourceRelPath = String(firstQueryValue(req.query.sourceRelPath) || "").trim();
    const q = String(firstQueryValue(req.query.q) || "").trim();
    const queryVariants = req.query.queryVariant ?? req.query.queryVariants;
    const matchMode = parseOCRSearchMode(firstQueryValue(req.query.matchMode));
    const queries = validateSearchDownloadQueries(q, queryVariants, matchMode);

    const cacheKey = buildCacheKey(req, "search-match-pages");
    const { value: payload, status } = await getCachedJson(cacheKey, 60, async () => {
      const source = await resolveSearchPdfSource(customId, sourceRelPath);
      const { pages, truncated } = await loadSearchMatchPages(sourceRelPath || source.sourceRelPath, queries, matchMode);

      return {
        custom_id: source.customId,
        pdf_name: source.pdfName,
        pdf_url: source.pdfUrl,
        cover_image_url: source.coverImageUrl,
        cover_page: 1,
        pages,
        total_matched_pages: pages.length,
        truncated,
        max_download_pages: MAX_MATCH_PAGE_DOWNLOAD,
        match_mode: matchMode,
        queries,
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
