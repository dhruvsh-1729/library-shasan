import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const q = String(req.query.q ?? "").trim();
  const limit = Math.min(Number(req.query.limit ?? 50) || 50, 100);

  if (!q || q.length < 2) return res.status(200).json({ results: [] });

  const { data, error } = await supabase.rpc("search_pages", { q, max_rows: limit });

  if (error) return res.status(500).json({ error: error.message });

  const results = (data ?? []).map((r: any) => ({
    ...r,
    open_pdf_url: `${r.pdf_url}#page=${r.page_number}`,
  }));

  res.status(200).json({ results });
}
