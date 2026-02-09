import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

type GranthItem = {
  id: number;
  file_name: string | null;
  ufs_url: string | null;
  file_size: number | null;
  custom_id: string | null;
  collection: string | null;
  subcollection: string | null;
  original_rel_path: string | null;
  cover_image_url: string | null;
  cover_image_key: string | null;
};

function parseIntQuery(raw: string | string[] | undefined, fallback: number, min: number, max: number) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function isMissingCoverColumns(message: string) {
  return /cover_image_url|cover_image_key/i.test(message);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const limit = parseIntQuery(req.query.limit, 120, 1, 500);
  const offset = parseIntQuery(req.query.offset, 0, 0, 1000000);
  const collection = String(req.query.collection ?? "").trim();

  const baseSelect =
    "id,file_name,ufs_url,file_size,custom_id,collection,subcollection,original_rel_path";
  const withCoverSelect = `${baseSelect},cover_image_url,cover_image_key`;

  const buildQuery = (selectCols: string) => {
    let query = supabase
      .from("granth_ocr_files")
      .select(selectCols)
      .order("id", { ascending: true })
      .range(offset, offset + limit - 1);

    if (collection) query = query.eq("collection", collection);
    return query;
  };

  let coverColumnAvailable = true;
  let data: Record<string, unknown>[] | null = null;

  {
    const response = await buildQuery(withCoverSelect);
    if (response.error && isMissingCoverColumns(response.error.message)) {
      coverColumnAvailable = false;
    } else if (response.error) {
      return res.status(500).json({ error: response.error.message });
    } else {
      data = (response.data as unknown as Record<string, unknown>[] | null) ?? [];
    }
  }

  if (!coverColumnAvailable) {
    const response = await buildQuery(baseSelect);
    if (response.error) return res.status(500).json({ error: response.error.message });
    data = (response.data as unknown as Record<string, unknown>[] | null) ?? [];
  }

  const items: GranthItem[] = (data ?? []).map((row) => ({
    id: Number(row.id ?? 0),
    file_name: (row.file_name as string | null) ?? null,
    ufs_url: (row.ufs_url as string | null) ?? null,
    file_size: row.file_size == null ? null : Number(row.file_size),
    custom_id: (row.custom_id as string | null) ?? null,
    collection: (row.collection as string | null) ?? null,
    subcollection: (row.subcollection as string | null) ?? null,
    original_rel_path: (row.original_rel_path as string | null) ?? null,
    cover_image_url: coverColumnAvailable ? ((row.cover_image_url as string | null) ?? null) : null,
    cover_image_key: coverColumnAvailable ? ((row.cover_image_key as string | null) ?? null) : null,
  }));

  return res.status(200).json({
    items,
    meta: {
      count: items.length,
      limit,
      offset,
      collection: collection || null,
      coverColumnAvailable,
    },
  });
}
