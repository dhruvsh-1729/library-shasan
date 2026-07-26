import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";

type SearchRow = {
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  page_number: number;
  snippet: string;
  score?: number;
  csv_url?: string | null;
  total_count?: number;
};

function parseLimit(raw: unknown) {
  const value = Number(raw ?? 20);
  if (!Number.isFinite(value) || value <= 0) return 20;
  return Math.min(Math.floor(value), 100);
}

function parsePage(raw: unknown) {
  const value = Number(raw ?? 1);
  if (!Number.isFinite(value) || value <= 0) return 1;
  return Math.floor(value);
}

function parseGranthIds(raw: string | string[] | undefined) {
  if (!raw) return [];
  const values = Array.isArray(raw) ? raw : String(raw).split(",");
  return values
    .map((v) => String(v).trim())
    .filter(Boolean);
}

function isMissingPaginatedSearchRpc(message: string) {
  return /search_pages_paginated|schema cache|function .* does not exist|could not find the function/i.test(message);
}

async function fallbackSearch(
  q: string,
  limit: number,
  offset: number,
  selectedGranths: string[]
) {
  const supabase = getSupabaseAdmin();
  const maxRows = selectedGranths.length
    ? Math.min(Math.max((offset + limit) * 20, 500), 4000)
    : Math.min(Math.max(offset + limit, limit), 1000);

  const { data, error } = await supabase.rpc("search_pages", { q, max_rows: maxRows });
  if (error) throw new Error(error.message);

  const baseResults = ((data ?? []) as SearchRow[]).map((r) => ({
    ...r,
    open_pdf_url: `${r.pdf_url}#page=${encodeURIComponent(String(r.page_number))}`,
  }));

  const granthSet = new Set(selectedGranths);
  const filtered = selectedGranths.length
    ? baseResults.filter((r) => granthSet.has(String(r.custom_id ?? "")))
    : baseResults;

  const results = filtered.slice(offset, offset + limit);
  const customIds = Array.from(
    new Set(results.map((r) => String(r.custom_id ?? "").trim()).filter(Boolean))
  );

  let csvByCustomId = new Map<string, string | null>();
  if (customIds.length > 0) {
    const { data: docsData, error: docsErr } = await supabase
      .from("documents")
      .select("custom_id,csv_url")
      .in("custom_id", customIds);

    if (docsErr) throw new Error(docsErr.message);
    csvByCustomId = new Map(
      (docsData ?? []).map((row) => [String(row.custom_id ?? ""), row.csv_url ?? null])
    );
  }

  return {
    results: results.map((r) => ({
      ...r,
      csv_url: csvByCustomId.get(String(r.custom_id ?? "")) ?? null,
    })),
    total: filtered.length,
    totalIsExact: false,
  };
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const q = String(req.query.q ?? "").trim();
    const limit = parseLimit(req.query.limit);
    const page = parsePage(req.query.page);
    const offset = Math.max(0, (page - 1) * limit);
    const selectedGranths = parseGranthIds(req.query.granths).slice(0, 250);

    if (!q || q.length < 2) {
      setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 });
      return res.status(200).json({
        results: [],
        total: 0,
        page,
        per_page: limit,
        total_pages: 1,
        total_is_exact: true,
      });
    }

    const cacheKey = buildCacheKey(req, "pdf-search");
    const { value: payload, status } = await getCachedJson(cacheKey, 60, async () => {
      const supabase = getSupabaseAdmin();
      const { data, error } = await supabase.rpc("search_pages_paginated", {
        p_q: q,
        p_limit: limit,
        p_offset: offset,
        p_granths: selectedGranths.length ? selectedGranths : null,
      });

      let results: Array<SearchRow & { open_pdf_url: string }> = [];
      let total = 0;
      let totalIsExact = true;

      if (error && isMissingPaginatedSearchRpc(error.message)) {
        const fallback = await fallbackSearch(q, limit, offset, selectedGranths);
        results = fallback.results;
        total = fallback.total;
        totalIsExact = fallback.totalIsExact;
      } else if (error) {
        throw new Error(error.message);
      } else {
        results = ((data ?? []) as SearchRow[]).map((r) => ({
          ...r,
          csv_url: r.csv_url ?? null,
          open_pdf_url: `${r.pdf_url}#page=${encodeURIComponent(String(r.page_number))}`,
        }));
        total = Number(((data ?? []) as SearchRow[])[0]?.total_count ?? 0);
      }

      return {
        results,
        total,
        selected_granth_count: selectedGranths.length,
        page,
        per_page: limit,
        total_pages: Math.max(1, Math.ceil(total / limit)),
        total_is_exact: totalIsExact,
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
