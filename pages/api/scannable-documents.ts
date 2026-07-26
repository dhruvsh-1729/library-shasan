import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";

type ScannableRow = {
  custom_id: string;
  pdf_name: string | null;
  pdf_url: string | null;
  csv_url: string | null;
  status: string | null;
  updated_at: string | null;
};

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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const limit = parseIntQuery(req.query.limit, 300, 1, 1000);
    const offset = parseIntQuery(req.query.offset, 0, 0, 1000000);

    const cacheKey = buildCacheKey(req, "scannable-documents");
    const { value: payload, status } = await getCachedJson(cacheKey, 180, async () => {
      const supabase = getSupabaseAdmin();
      const countReq = supabase
        .from("documents")
        .select("custom_id", { count: "exact", head: true })
        .eq("status", "processed");

      const listReq = supabase
        .from("documents")
        .select("custom_id,pdf_name,pdf_url,csv_url,status,updated_at")
        .eq("status", "processed")
        .not("custom_id", "is", null)
        .order("pdf_name", { ascending: true, nullsFirst: false })
        .range(offset, offset + limit - 1);

      const [{ count, error: countErr }, { data, error: listErr }] = await Promise.all([
        countReq,
        listReq,
      ]);

      if (countErr) throw new Error(countErr.message);
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
            status: row.status ?? "processed",
            updated_at: row.updated_at,
          };
        });

      return {
        items,
        meta: {
          total_processed: count ?? 0,
          pageCount: items.length,
          limit,
          offset,
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
