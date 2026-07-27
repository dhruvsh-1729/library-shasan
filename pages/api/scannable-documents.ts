import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import {
  READY_DOCUMENT_STATUSES,
  REVIEW_DOCUMENT_STATUSES,
  SEARCHABLE_DOCUMENT_STATUSES,
  buildRemainingStatusFilter,
  buildStatusInFilter,
  getDocumentScanState,
} from "@/lib/document-scan-state";
import { getSupabaseAdmin } from "@/lib/supabase-server";

type ScannableRow = {
  custom_id: string;
  pdf_name: string | null;
  pdf_url: string | null;
  csv_url: string | null;
  status: string | null;
  updated_at: string | null;
};

type ScanView = "remaining" | "ready" | "review" | "searchable" | "all";

function parseIntQuery(
  raw: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number
) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function displayName(pdfName: string | null, customId: string) {
  const raw = pdfName && pdfName.trim() ? pdfName : customId;
  return raw.replace(/\s+OCR\.pdf$/i, "").replace(/\.pdf$/i, "");
}

function parseView(raw: string | string[] | undefined): ScanView {
  const value = String(Array.isArray(raw) ? raw[0] : raw ?? "remaining").trim().toLowerCase();
  if (value === "ready" || value === "review" || value === "searchable" || value === "all") return value;
  return "remaining";
}

function applyScanViewFilter<T extends { or: (filter: string) => T }>(query: T, view: ScanView) {
  if (view === "all") return query;
  if (view === "ready") return query.or(buildStatusInFilter(READY_DOCUMENT_STATUSES));
  if (view === "review") return query.or(buildStatusInFilter(REVIEW_DOCUMENT_STATUSES));
  if (view === "searchable") return query.or(buildStatusInFilter(SEARCHABLE_DOCUMENT_STATUSES));
  return query.or(buildRemainingStatusFilter());
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const limit = parseIntQuery(req.query.limit, 300, 1, 1000);
    const offset = parseIntQuery(req.query.offset, 0, 0, 1000000);
    const view = parseView(req.query.view);

    const cacheKey = buildCacheKey(req, "scannable-documents");
    const { value: payload, status } = await getCachedJson(cacheKey, 180, async () => {
      const supabase = getSupabaseAdmin();

      const totalReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null);

      const readyReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null)
        .or(buildStatusInFilter(READY_DOCUMENT_STATUSES));

      const reviewReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null)
        .or(buildStatusInFilter(REVIEW_DOCUMENT_STATUSES));

      const remainingReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null)
        .or(buildRemainingStatusFilter());

      const searchableReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null)
        .or(buildStatusInFilter(SEARCHABLE_DOCUMENT_STATUSES));

      const viewCountReq = applyScanViewFilter(
        supabase
          .from("documents")
          .select("custom_id", { count: "exact", head: true })
          .not("custom_id", "is", null),
        view
      );

      const listReq = applyScanViewFilter(
        supabase
          .from("documents")
          .select("custom_id,pdf_name,pdf_url,csv_url,status,updated_at")
          .not("custom_id", "is", null),
        view
      )
        .order("pdf_name", { ascending: true, nullsFirst: false })
        .range(offset, offset + limit - 1);

      const [
        { count: totalCount, error: totalErr },
        { count: readyCount, error: readyErr },
        { count: reviewCount, error: reviewErr },
        { count: remainingCount, error: remainingErr },
        { count: searchableCount, error: searchableErr },
        { count: viewCount, error: viewCountErr },
        { data, error: listErr },
      ] = await Promise.all([
        totalReq,
        readyReq,
        reviewReq,
        remainingReq,
        searchableReq,
        viewCountReq,
        listReq,
      ]);

      if (totalErr) throw new Error(totalErr.message);
      if (readyErr) throw new Error(readyErr.message);
      if (reviewErr) throw new Error(reviewErr.message);
      if (remainingErr) throw new Error(remainingErr.message);
      if (searchableErr) throw new Error(searchableErr.message);
      if (viewCountErr) throw new Error(viewCountErr.message);
      if (listErr) throw new Error(listErr.message);

      const items = ((data ?? []) as ScannableRow[])
        .filter((row) => String(row.custom_id ?? "").trim().length > 0)
        .map((row) => {
          const customId = String(row.custom_id).trim();
          return {
            custom_id: customId,
            pdf_name: row.pdf_name,
            display_name: displayName(row.pdf_name, customId),
            pdf_url: row.pdf_url,
            csv_url: row.csv_url,
            status: row.status ?? null,
            scan_state: getDocumentScanState(row.status),
            updated_at: row.updated_at,
          };
        });

      return {
        items,
        meta: {
          total_documents: totalCount ?? 0,
          total_processed: searchableCount ?? 0,
          ready_documents: readyCount ?? 0,
          review_documents: reviewCount ?? 0,
          remaining_documents: remainingCount ?? 0,
          total_for_view: viewCount ?? 0,
          pageCount: items.length,
          limit,
          offset,
          view,
        },
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 180, staleWhileRevalidateSeconds: 900 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
