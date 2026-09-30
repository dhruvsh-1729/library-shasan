import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { getGranthCatalog, relPathsForIds } from "@/lib/granth-catalog";
import { MAX_SEARCH_QUERIES, normalizeOCRSearchQueries, parseOCRSearchMode, parseOCRSearchScripts } from "@/lib/ocr-search";
import { countSearch } from "@/lib/search-count";

// Exact totals for a search whose /api/search totals carry a "+" (more hit
// pages than it checks at once). Same parameters as /api/search.

function list(raw: string | string[] | undefined) {
  return (Array.isArray(raw) ? raw : raw ? [raw] : []).flatMap((value) => String(value).split(/\r?\n|\|/g));
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  try {
    const matchMode = parseOCRSearchMode(req.query.matchMode);
    const scripts = parseOCRSearchScripts(req.query.scripts);
    const queries = normalizeOCRSearchQueries(String(req.query.q ?? "").trim(), list(req.query.queryVariant ?? req.query.queryVariants))
      .filter((query) => Array.from(query).length >= 2)
      .slice(0, MAX_SEARCH_QUERIES);
    if (!queries.length) return res.status(400).json({ error: "Enter a word to count." });
    const granths = String(req.query.granths ?? "").split(",").map((v) => v.trim()).filter(Boolean).slice(0, 250);
    const relPaths = granths.length ? relPathsForIds(await getGranthCatalog(), granths).relPaths : [];
    if (granths.length && !relPaths.length) return res.status(200).json({ pages: 0, occurrences: 0, formCounts: [], indexPages: 0 });
    const count = await countSearch({ queries, matchMode, scripts, relPaths });
    return res.status(200).json(count);
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
