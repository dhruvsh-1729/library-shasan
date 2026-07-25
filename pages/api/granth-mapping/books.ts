import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

function parseLimit(raw: string | string[] | undefined) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value || "500"), 10);
  if (!Number.isFinite(parsed)) return 500;
  return Math.max(1, Math.min(parsed, 1000));
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const q = String(req.query.q || "").trim().toLowerCase();
  const limit = parseLimit(req.query.limit);

  const { data, error } = await supabase
    .from("granth_library_books")
    .select(
      "id,source_row_hash,title_english,title_display,author_text,details_text,book_codes,index_href,index_href_type,cover_rel_path"
    )
    .order("title_english", { ascending: true })
    .limit(1000);

  if (error) {
    if (/granth_library_books|schema cache/i.test(error.message)) {
      return res.status(503).json({
        error:
          "Mapping tables are not available yet. Run supabase/migrations/20260725_granth_library_mapping.sql and import the mapping data.",
      });
    }
    return res.status(500).json({ error: error.message });
  }

  const filtered = (data || []).filter((row) => {
    if (!q) return true;
    const haystack = [
      row.title_english,
      row.title_display,
      row.author_text,
      row.details_text,
      ...(Array.isArray(row.book_codes) ? row.book_codes : []),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    return haystack.includes(q);
  });

  return res.status(200).json({
    items: filtered.slice(0, limit),
    meta: {
      count: filtered.length,
      limit,
    },
  });
}
