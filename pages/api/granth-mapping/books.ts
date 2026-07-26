import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";

function parseLimit(raw: string | string[] | undefined) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value || "500"), 10);
  if (!Number.isFinite(parsed)) return 500;
  return Math.max(1, Math.min(parsed, 1000));
}

function firstQueryValue(raw: string | string[] | undefined) {
  return Array.isArray(raw) ? raw[0] : raw;
}

function parseOffset(raw: string | string[] | undefined) {
  const parsed = Number.parseInt(String(firstQueryValue(raw) || "0"), 10);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.min(parsed, 1000000));
}

function escapeIlikeTerm(value: string) {
  return value.replace(/[\\%_]/g, "\\$&").replace(/[(),]/g, " ");
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const q = String(firstQueryValue(req.query.q) || "").trim().slice(0, 120);
  const limit = parseLimit(req.query.limit);
  const offset = parseOffset(req.query.offset);

  try {
    const cacheKey = buildCacheKey(req, "granth-mapping-books");
    const { value: payload, status } = await getCachedJson(cacheKey, 300, async () => {
      const supabase = getSupabaseAdmin();
      let query = supabase
        .from("granth_library_books")
        .select(
          "id,source_row_hash,title_english,title_display,author_text,details_text,book_codes,index_href,index_href_type,cover_rel_path",
          { count: "exact" }
        )
        .order("title_english", { ascending: true })
        .range(offset, offset + limit - 1);

      if (q) {
        const pattern = `%${escapeIlikeTerm(q)}%`;
        query = query.or(
          [
            `title_english.ilike.${pattern}`,
            `title_display.ilike.${pattern}`,
            `author_text.ilike.${pattern}`,
            `details_text.ilike.${pattern}`,
          ].join(",")
        );
      }

      const { data, error, count } = await query;
      if (error) throw new Error(error.message);
      const total = count ?? data?.length ?? 0;

      return {
        items: data ?? [],
        meta: {
          count: total,
          total,
          pageCount: data?.length ?? 0,
          limit,
          offset,
          q: q || null,
        },
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 1800 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    setNoStore(res);
    if (/granth_library_books|schema cache/i.test(message)) {
      return res.status(503).json({
        error:
          "Mapping tables are not available yet. Run supabase/migrations/20260725_granth_library_mapping.sql and import the mapping data.",
      });
    }
    return res.status(500).json({ error: message });
  }
}
