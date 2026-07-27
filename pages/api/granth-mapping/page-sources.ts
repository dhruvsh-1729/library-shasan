import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import { getTursoClient } from "@/lib/turso";

type SourceRow = {
  id: number;
  file_name: string | null;
  file_type: string | null;
  ufs_url: string | null;
  file_size: number | null;
  custom_id: string | null;
  collection: string | null;
  subcollection: string | null;
  original_rel_path: string | null;
  cover_image_url?: string | null;
  cover_image_key?: string | null;
};

type MappingRow = {
  granth_ocr_file_id: number | null;
  book_id: number | null;
  book_code: string | null;
  page_count: number | null;
};

type OcrGranthRow = {
  granth_key: string;
  book_number: string;
  library_code: string | null;
  granth_name: string;
  source_rel_path: string;
  xlsx_url: string | null;
  page_count: number;
  text_row_count: number;
};

function parseLimit(raw: string | string[] | undefined) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value || "1000"), 10);
  if (!Number.isFinite(parsed)) return 1000;
  return Math.max(1, Math.min(parsed, 5000));
}

function parseOffset(raw: string | string[] | undefined) {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const parsed = Number.parseInt(String(value || "0"), 10);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.min(parsed, 1000000));
}

function firstQueryValue(raw: string | string[] | undefined) {
  return Array.isArray(raw) ? raw[0] : raw;
}

function escapeIlikeTerm(value: string) {
  return value.replace(/[\\%_]/g, "\\$&").replace(/[(),]/g, " ");
}

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toStr(value: unknown, fallback = "") {
  if (value == null) return fallback;
  return String(value);
}

function looksLikePdf(row: Pick<SourceRow, "file_name" | "file_type" | "ufs_url">) {
  const fileName = String(row.file_name || "").trim().toLowerCase();
  const fileType = String(row.file_type || "").trim().toLowerCase();
  const url = String(row.ufs_url || "").trim().toLowerCase();
  return fileName.endsWith(".pdf") || fileType.includes("pdf") || /\.pdf(?:[?#]|$)/i.test(url);
}

function isDownloadablePdf(row: SourceRow) {
  return Boolean(String(row.ufs_url || "").trim()) && looksLikePdf(row);
}

function isMissingCoverColumns(message: string) {
  return /cover_image_url|cover_image_key/i.test(message);
}

function displayName(row: SourceRow, ocr: OcrGranthRow | undefined) {
  if (ocr) {
    const code = ocr.library_code ? ` ${ocr.library_code}` : "";
    return `${ocr.book_number}${code} ${ocr.granth_name}`.replace(/\s+/g, " ").trim();
  }
  return String(row.file_name || row.original_rel_path || row.custom_id || `PDF ${row.id}`).trim();
}

async function loadOcrRows(relPaths: string[]) {
  const byRelPath = new Map<string, OcrGranthRow>();
  if (relPaths.length === 0) return byRelPath;

  try {
    const client = getTursoClient();
    for (let index = 0; index < relPaths.length; index += 400) {
      const chunk = relPaths.slice(index, index + 400);
      const result = await client.execute({
        sql: `SELECT granth_key, book_number, library_code, granth_name, source_rel_path, xlsx_url, page_count, text_row_count
              FROM ocr_granths
              WHERE source_rel_path IN (${chunk.map(() => "?").join(",")})`,
        args: chunk,
      });
      for (const row of result.rows) {
        const sourceRelPath = toStr(row.source_rel_path);
        if (!sourceRelPath) continue;
        byRelPath.set(sourceRelPath, {
          granth_key: toStr(row.granth_key),
          book_number: toStr(row.book_number),
          library_code: row.library_code == null ? null : toStr(row.library_code),
          granth_name: toStr(row.granth_name),
          source_rel_path: sourceRelPath,
          xlsx_url: row.xlsx_url == null ? null : toStr(row.xlsx_url),
          page_count: toInt(row.page_count),
          text_row_count: toInt(row.text_row_count),
        });
      }
    }
  } catch {
    return byRelPath;
  }

  return byRelPath;
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
    const cacheKey = buildCacheKey(req, "granth-page-sources");
    const { value: payload, status } = await getCachedJson(cacheKey, 300, async () => {
      const supabase = getSupabaseAdmin();
      const baseSelect = "id,file_name,file_type,ufs_url,file_size,custom_id,collection,subcollection,original_rel_path";
      const coverSelect = `${baseSelect},cover_image_url,cover_image_key`;

      const buildQuery = (selectCols: string) => {
        let query = supabase
          .from("granth_ocr_files")
          .select(selectCols, { count: "exact" })
          .not("ufs_url", "is", null)
          .order("id", { ascending: true })
          .range(offset, offset + limit - 1);

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
      let data: SourceRow[] = [];
      let total = 0;

      const coverResponse = await buildQuery(coverSelect);
      if (coverResponse.error && isMissingCoverColumns(coverResponse.error.message)) {
        coverColumnAvailable = false;
      } else if (coverResponse.error) {
        throw new Error(coverResponse.error.message);
      } else {
        data = ((coverResponse.data ?? []) as unknown as SourceRow[]).filter(isDownloadablePdf);
        total = coverResponse.count ?? data.length;
      }

      if (!coverColumnAvailable) {
        const baseResponse = await buildQuery(baseSelect);
        if (baseResponse.error) throw new Error(baseResponse.error.message);
        data = ((baseResponse.data ?? []) as unknown as SourceRow[]).filter(isDownloadablePdf);
        total = baseResponse.count ?? data.length;
      }

      const fileIds = data.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
      const mappingByFileId = new Map<number, MappingRow>();
      if (fileIds.length > 0) {
        const { data: mappingRows, error: mappingError } = await supabase
          .from("granth_library_files")
          .select("granth_ocr_file_id,book_id,book_code,page_count")
          .in("granth_ocr_file_id", fileIds)
          .order("book_code", { ascending: true });

        if (mappingError && !/granth_library_files|schema cache/i.test(mappingError.message)) {
          throw new Error(mappingError.message);
        }

        for (const row of (mappingRows ?? []) as MappingRow[]) {
          const fileId = Number(row.granth_ocr_file_id ?? 0);
          if (fileId > 0 && !mappingByFileId.has(fileId)) mappingByFileId.set(fileId, row);
        }
      }

      const relPaths = [
        ...new Set(data.map((row) => String(row.original_rel_path || "").trim()).filter(Boolean)),
      ];
      const ocrByRelPath = await loadOcrRows(relPaths);

      return {
        items: data.map((row) => {
          const ocr = ocrByRelPath.get(String(row.original_rel_path || ""));
          const mapping = mappingByFileId.get(Number(row.id));
          const pageCount = ocr?.page_count || toInt(mapping?.page_count, 0) || null;
          return {
            id: Number(row.id),
            file_name: row.file_name ?? null,
            display_name: displayName(row, ocr),
            pdf_url: row.ufs_url ?? null,
            file_size: row.file_size == null ? null : Number(row.file_size),
            custom_id: row.custom_id ?? null,
            collection: row.collection ?? null,
            subcollection: row.subcollection ?? null,
            original_rel_path: row.original_rel_path ?? null,
            cover_image_url: coverColumnAvailable ? (row.cover_image_url ?? null) : null,
            page_count: pageCount,
            ocr_granth_key: ocr?.granth_key ?? null,
            text_row_count: ocr?.text_row_count ?? null,
            xlsx_url: ocr?.xlsx_url ?? null,
            mapping_book_id: mapping?.book_id == null ? null : Number(mapping.book_id),
            mapping_book_code: mapping?.book_code ?? null,
          };
        }),
        meta: {
          count: total,
          total,
          pageCount: data.length,
          limit,
          offset,
          q: q || null,
          coverColumnAvailable,
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
