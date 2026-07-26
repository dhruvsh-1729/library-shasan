import type { NextApiRequest, NextApiResponse } from "next";
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
        .select("custom_id", { count: "exact", head: true });

      const processedReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .eq("status", "processed");

      const [{ count: totalCount, error: totalErr }, { count: processedCount, error: processedErr }] =
        await Promise.all([totalReq, processedReq]);

      if (totalErr) throw new Error(totalErr.message);
      if (processedErr) throw new Error(processedErr.message);

      return {
        total_documents: totalCount ?? 0,
        processed_documents: processedCount ?? 0,
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 1800 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
