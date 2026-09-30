import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { lineBoxRingsForPdf } from "@/lib/line-boxes";
import { MAX_SEARCH_QUERIES, normalizeOCRSearchQueries, parseOCRSearchMode, parseOCRSearchScripts } from "@/lib/ocr-search";

// Where to ring the searched word on one page of a PDF, from our OCR's line
// boxes (lib/line-boxes). Boxes are fractions of the page, lower-left origin.
// { rings: null } when the page has no usable boxes: the viewer then uses the
// PDF's own text layer.

function list(raw: string | string[] | undefined) {
  return (Array.isArray(raw) ? raw : raw ? [raw] : []).map(String);
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const pdfUrl = String(req.query.pdfUrl ?? "").trim();
  const page = Math.floor(Number(req.query.page));
  const queries = normalizeOCRSearchQueries(String(req.query.q ?? "").trim(), list(req.query.queryVariant), MAX_SEARCH_QUERIES);
  if (!pdfUrl || !(page > 0) || !queries.length) return res.status(400).json({ error: "pdfUrl, page and q are required" });
  try {
    const rings = await lineBoxRingsForPdf(pdfUrl, [{ page, width: 1, height: 1 }], {
      queries,
      matchMode: parseOCRSearchMode(req.query.matchMode),
      scripts: parseOCRSearchScripts(req.query.scripts),
    });
    const boxes = rings.get(page);
    return res.status(200).json({
      rings: boxes ? boxes.map(({ x, y, width, height }) => ({ x, y, width, height })) : null,
    });
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
