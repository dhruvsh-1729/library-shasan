import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

type SearchRow = {
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  page_number: number;
  snippet: string;
  score?: number;
};

function parseLimit(raw: unknown) {
  const value = Number(raw ?? 50);
  if (!Number.isFinite(value) || value <= 0) return 50;
  return Math.min(Math.floor(value), 100);
}

function parseGranthIds(raw: string | string[] | undefined) {
  if (!raw) return [];
  const values = Array.isArray(raw) ? raw : String(raw).split(",");
  return values
    .map((v) => String(v).trim())
    .filter(Boolean);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const q = String(req.query.q ?? "").trim();
  const limit = parseLimit(req.query.limit);
  const selectedGranths = parseGranthIds(req.query.granths);

  if (!q || q.length < 2) return res.status(200).json({ results: [], total: 0 });

  // When user filters by granth IDs, fetch a larger candidate window before filtering.
  const maxRows = selectedGranths.length
    ? Math.min(Math.max(limit * 20, 500), 4000)
    : limit;

  const { data, error } = await supabase.rpc("search_pages", { q, max_rows: maxRows });

  if (error) return res.status(500).json({ error: error.message });

  const baseResults = ((data ?? []) as SearchRow[]).map((r) => ({
    ...r,
    open_pdf_url: `${r.pdf_url}#page=${r.page_number}`,
  }));

  const granthSet = new Set(selectedGranths);
  const filtered = selectedGranths.length
    ? baseResults.filter((r) => granthSet.has(String(r.custom_id ?? "")))
    : baseResults;

  const results = filtered.slice(0, limit);
  const customIds = Array.from(
    new Set(results.map((r) => String(r.custom_id ?? "").trim()).filter(Boolean))
  );

  let csvByCustomId = new Map<string, string | null>();
  if (customIds.length > 0) {
    const { data: docsData, error: docsErr } = await supabase
      .from("documents")
      .select("custom_id,csv_url")
      .in("custom_id", customIds);

    if (docsErr) return res.status(500).json({ error: docsErr.message });

    csvByCustomId = new Map(
      (docsData ?? []).map((row) => [String(row.custom_id ?? ""), row.csv_url ?? null])
    );
  }

  const enrichedResults = results.map((r) => ({
    ...r,
    csv_url: csvByCustomId.get(String(r.custom_id ?? "")) ?? null,
  }));
  res.status(200).json({
    results: enrichedResults,
    total: filtered.length,
    selected_granth_count: selectedGranths.length,
  });
}
