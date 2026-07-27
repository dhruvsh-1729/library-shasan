import type { NextApiRequest, NextApiResponse } from "next";
import {
  READY_DOCUMENT_STATUSES,
  REVIEW_DOCUMENT_STATUSES,
  SEARCHABLE_DOCUMENT_STATUSES,
  buildRemainingStatusFilter,
  buildStatusInFilter,
} from "@/lib/document-scan-state";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const cacheKey = buildCacheKey(req, "document-stats");
    const { value: payload, status } = await getCachedJson(cacheKey, 300, async () => {
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

      const searchableReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null)
        .or(buildStatusInFilter(SEARCHABLE_DOCUMENT_STATUSES));

      const remainingReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .not("custom_id", "is", null)
        .or(buildRemainingStatusFilter());

      const [
        { count: totalCount, error: totalErr },
        { count: readyCount, error: readyErr },
        { count: reviewCount, error: reviewErr },
        { count: searchableCount, error: searchableErr },
        { count: remainingCount, error: remainingErr },
      ] = await Promise.all([totalReq, readyReq, reviewReq, searchableReq, remainingReq]);

      if (totalErr) throw new Error(totalErr.message);
      if (readyErr) throw new Error(readyErr.message);
      if (reviewErr) throw new Error(reviewErr.message);
      if (searchableErr) throw new Error(searchableErr.message);
      if (remainingErr) throw new Error(remainingErr.message);

      return {
        total_documents: totalCount ?? 0,
        processed_documents: searchableCount ?? 0,
        ready_documents: readyCount ?? 0,
        review_documents: reviewCount ?? 0,
        searchable_documents: searchableCount ?? 0,
        remaining_documents: remainingCount ?? 0,
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 1800 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
