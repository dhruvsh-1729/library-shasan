import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { type DocumentScanState, getDocumentScanState } from "@/lib/document-scan-state";
import { getSupabaseAdmin } from "@/lib/supabase-server";

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
  document_status: string | null;
  scan_state: DocumentScanState;
  mapping_book_id: number | null;
  mapping_book_code: string | null;
};

type DocumentScanRow = {
  custom_id: string | null;
  status: string | null;
};

function parseIntQuery(raw: string | string[] | undefined, fallback: number, min: number, max: number) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function firstQueryValue(raw: string | string[] | undefined) {
  return Array.isArray(raw) ? raw[0] : raw;
}

function parseSearch(raw: string | string[] | undefined) {
  return String(firstQueryValue(raw) ?? "").trim().slice(0, 120);
}

function escapeIlikeTerm(value: string) {
  return value.replace(/[\\%_]/g, "\\$&").replace(/[(),]/g, " ");
}

function isMissingCoverColumns(message: string) {
  return /cover_image_url|cover_image_key/i.test(message);
}

function isMissingMappingTables(message: string) {
  return /granth_library_files|schema cache/i.test(message);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const page = parseIntQuery(req.query.page, 1, 1, 100000);
    const limit = parseIntQuery(req.query.limit, 10, 1, 50);
    const offset = parseIntQuery(req.query.offset, (page - 1) * limit, 0, 1000000);
    const collection = String(firstQueryValue(req.query.collection) ?? "").trim();
    const q = parseSearch(req.query.q);

    const cacheKey = buildCacheKey(req, "granths");
    const { value: payload, status } = await getCachedJson(cacheKey, 120, async () => {
      const supabase = getSupabaseAdmin();

      const baseSelect =
        "id,file_name,ufs_url,file_size,custom_id,collection,subcollection,original_rel_path";
      const withCoverSelect = `${baseSelect},cover_image_url,cover_image_key`;

      const buildQuery = (selectCols: string) => {
        let query = supabase
          .from("granth_ocr_files")
          .select(selectCols, { count: "exact" })
          .order("id", { ascending: true })
          .range(offset, offset + limit - 1);

        if (collection) query = query.eq("collection", collection);
        if (q) {
          const pattern = `%${escapeIlikeTerm(q)}%`;
          query = query.or(
            [
              `file_name.ilike.${pattern}`,
              `original_rel_path.ilike.${pattern}`,
              `custom_id.ilike.${pattern}`,
              `collection.ilike.${pattern}`,
              `subcollection.ilike.${pattern}`,
            ].join(",")
          );
        }
        return query;
      };

      let coverColumnAvailable = true;
      let data: Record<string, unknown>[] | null = null;
      let total = 0;

      {
        const response = await buildQuery(withCoverSelect);
        if (response.error && isMissingCoverColumns(response.error.message)) {
          coverColumnAvailable = false;
        } else if (response.error) {
          throw new Error(response.error.message);
        } else {
          data = (response.data as unknown as Record<string, unknown>[] | null) ?? [];
          total = response.count ?? data.length;
        }
      }

      if (!coverColumnAvailable) {
        const response = await buildQuery(baseSelect);
        if (response.error) throw new Error(response.error.message);
        data = (response.data as unknown as Record<string, unknown>[] | null) ?? [];
        total = response.count ?? data.length;
      }

      const scanByCustomId = new Map<string, DocumentScanRow>();
      const customIds = Array.from(
        new Set(
          (data ?? [])
            .map((row) => String(row.custom_id ?? "").trim())
            .filter(Boolean)
        )
      );

      if (customIds.length > 0) {
        const { data: scanRows, error: scanErr } = await supabase
          .from("documents")
          .select("custom_id,status")
          .in("custom_id", customIds);

        if (scanErr) throw new Error(scanErr.message);

        for (const row of (scanRows ?? []) as DocumentScanRow[]) {
          const customId = String(row.custom_id ?? "").trim();
          if (customId) scanByCustomId.set(customId, row);
        }
      }

      const mappingByFileId = new Map<number, { book_id: number | null; book_code: string | null }>();
      const fileIds = (data ?? [])
        .map((row) => Number(row.id ?? 0))
        .filter((id) => Number.isFinite(id) && id > 0);

      if (fileIds.length > 0) {
        const { data: mappingRows, error: mappingError } = await supabase
          .from("granth_library_files")
          .select("granth_ocr_file_id,book_id,book_code")
          .in("granth_ocr_file_id", fileIds)
          .order("book_code", { ascending: true });

        if (mappingError && !isMissingMappingTables(mappingError.message)) {
          throw new Error(mappingError.message);
        }

        for (const row of (mappingRows ?? []) as Array<{
          granth_ocr_file_id: number | null;
          book_id: number | null;
          book_code: string | null;
        }>) {
          const fileId = Number(row.granth_ocr_file_id ?? 0);
          if (fileId > 0 && !mappingByFileId.has(fileId)) {
            mappingByFileId.set(fileId, {
              book_id: row.book_id == null ? null : Number(row.book_id),
              book_code: row.book_code ?? null,
            });
          }
        }
      }

      const items: GranthItem[] = (data ?? []).map((row) => {
        const customId = (row.custom_id as string | null) ?? null;
        const scanRow = customId ? scanByCustomId.get(customId) : undefined;
        const documentStatus = scanRow?.status ?? null;
        const mapping = mappingByFileId.get(Number(row.id ?? 0));

        return {
          id: Number(row.id ?? 0),
          file_name: (row.file_name as string | null) ?? null,
          ufs_url: (row.ufs_url as string | null) ?? null,
          file_size: row.file_size == null ? null : Number(row.file_size),
          custom_id: customId,
          collection: (row.collection as string | null) ?? null,
          subcollection: (row.subcollection as string | null) ?? null,
          original_rel_path: (row.original_rel_path as string | null) ?? null,
          cover_image_url: coverColumnAvailable ? ((row.cover_image_url as string | null) ?? null) : null,
          cover_image_key: coverColumnAvailable ? ((row.cover_image_key as string | null) ?? null) : null,
          document_status: documentStatus,
          scan_state: getDocumentScanState(documentStatus),
          mapping_book_id: mapping?.book_id ?? null,
          mapping_book_code: mapping?.book_code ?? null,
        };
      });

      const normalizedPage = Math.floor(offset / limit) + 1;
      const totalPages = Math.max(1, Math.ceil(total / limit));

      return {
        items,
        meta: {
          count: total,
          total,
          pageCount: items.length,
          limit,
          offset,
          page: normalizedPage,
          totalPages,
          hasNextPage: offset + items.length < total,
          hasPreviousPage: offset > 0,
          q: q || null,
          collection: collection || null,
          coverColumnAvailable,
        },
      };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 120, staleWhileRevalidateSeconds: 600 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
