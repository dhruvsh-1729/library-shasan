import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";

type GranthOption = {
  custom_id: string;
  pdf_name: string | null;
  display_name: string;
};

function displayName(pdfName: string | null, customId: string) {
  const raw = pdfName && pdfName.trim() ? pdfName : customId;
  return raw.replace(/\s+OCR\.pdf$/i, "").replace(/\.pdf$/i, "");
}

function firstQueryValue(raw: string | string[] | undefined) {
  return Array.isArray(raw) ? raw[0] : raw;
}

function parseIntQuery(raw: string | string[] | undefined, fallback: number, min: number, max: number) {
  const parsed = Number.parseInt(String(firstQueryValue(raw) ?? fallback), 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function escapeIlikeTerm(value: string) {
  return value.replace(/[\\%_]/g, "\\$&").replace(/[(),]/g, " ");
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const page = parseIntQuery(req.query.page, 1, 1, 100000);
    const limit = parseIntQuery(req.query.limit, 200, 1, 5000);
    const offset = parseIntQuery(req.query.offset, (page - 1) * limit, 0, 1000000);
    const q = String(firstQueryValue(req.query.q) ?? "").trim().slice(0, 120);

    const cacheKey = buildCacheKey(req, "search-granths");
    const { value: payload, status } = await getCachedJson(cacheKey, 300, async () => {
      const supabase = getSupabaseAdmin();
      let query = supabase
        .from("documents")
        .select("custom_id,pdf_name,status", { count: "exact" })
        .not("custom_id", "is", null)
        .eq("status", "processed")
        .order("pdf_name", { ascending: true, nullsFirst: false })
        .range(offset, offset + limit - 1);

      if (q) {
        const pattern = `%${escapeIlikeTerm(q)}%`;
        query = query.or(`custom_id.ilike.${pattern},pdf_name.ilike.${pattern}`);
      }

      const { data, error, count } = await query;
      if (error) throw new Error(error.message);

      const seen = new Set<string>();
      const items: GranthOption[] = [];

      for (const row of data ?? []) {
        const customId = String(row.custom_id ?? "").trim();
        if (!customId || seen.has(customId)) continue;
        seen.add(customId);
        items.push({
          custom_id: customId,
          pdf_name: row.pdf_name ?? null,
          display_name: displayName(row.pdf_name ?? null, customId),
        });
      }

      const total = count ?? items.length;
      return {
        items,
        total,
        meta: {
          total,
          pageCount: items.length,
          page: Math.floor(offset / limit) + 1,
          limit,
          offset,
          totalPages: Math.max(1, Math.ceil(total / limit)),
          hasNextPage: offset + items.length < total,
          hasPreviousPage: offset > 0,
          q: q || null,
        },
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 1800 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
