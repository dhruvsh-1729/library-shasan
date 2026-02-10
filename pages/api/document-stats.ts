import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const totalReq = supabase
    .from("documents")
    .select("custom_id", { count: "exact", head: true });

  const processedReq = supabase
    .from("documents")
    .select("custom_id", { count: "exact", head: true })
    .eq("status", "processed");

  const [{ count: totalCount, error: totalErr }, { count: processedCount, error: processedErr }] =
    await Promise.all([totalReq, processedReq]);

  if (totalErr) return res.status(500).json({ error: totalErr.message });
  if (processedErr) return res.status(500).json({ error: processedErr.message });

  return res.status(200).json({
    total_documents: totalCount ?? 0,
    processed_documents: processedCount ?? 0,
  });
}
