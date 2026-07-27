import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";

type FileRow = {
  id: number;
  book_code: string | null;
  pdf_file_name: string | null;
  pdf_url: string | null;
  custom_id: string | null;
  page_count: number | null;
  granth_ocr_file_id: number | null;
};

type OcrFileRow = {
  id: number;
  file_name: string | null;
  file_size: number | null;
  collection: string | null;
  subcollection: string | null;
  cover_image_url?: string | null;
  cover_image_key?: string | null;
};

type MapRow = {
  book_code: string | null;
  pdf_file_name: string | null;
  pdf_url: string | null;
  custom_id: string | null;
  adhikar: number | null;
  gatha: number | null;
  page_start: number;
  page_end: number | null;
  anchor_text: string | null;
};

function firstString(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function parseBookId(value: string | string[] | undefined) {
  const parsed = Number.parseInt(String(firstString(value) || ""), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

async function fetchAll(build: (from: number, to: number) => unknown) {
  const rows: Record<string, unknown>[] = [];
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const { data, error } = (await build(from, from + pageSize - 1)) as {
      data?: Record<string, unknown>[] | null;
      error?: { message?: string } | null;
    };
    if (error) throw error;
    if (!data || data.length === 0) break;
    rows.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return rows;
}

function codeLabel(value: string | null | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return "---";
  return /^\d+$/.test(raw) ? raw.padStart(3, "0") : raw;
}

function mergePageRanges(rows: MapRow[]) {
  const sorted = [...rows].sort((a, b) => {
    const fileCmp = String(a.pdf_file_name || "").localeCompare(String(b.pdf_file_name || ""), "en");
    if (fileCmp !== 0) return fileCmp;
    return Number(a.page_start || 0) - Number(b.page_start || 0);
  });

  const ranges: Array<{
    book_code: string | null;
    code_label: string;
    pdf_file_name: string;
    page_start: number;
    page_end: number;
    min_gatha: number | null;
    max_gatha: number | null;
    count: number;
  }> = [];

  for (const row of sorted) {
    const start = Number(row.page_start);
    const end = Math.max(start, Number(row.page_end || row.page_start));
    if (!Number.isFinite(start) || !Number.isFinite(end)) continue;

    const last = ranges[ranges.length - 1];
    const sameFile = last && last.pdf_file_name === String(row.pdf_file_name || "") && last.book_code === row.book_code;
    if (sameFile && start <= last.page_end + 1) {
      last.page_end = Math.max(last.page_end, end);
      last.count += 1;
      if (Number.isFinite(Number(row.gatha))) {
        const gatha = Number(row.gatha);
        last.min_gatha = last.min_gatha == null ? gatha : Math.min(last.min_gatha, gatha);
        last.max_gatha = last.max_gatha == null ? gatha : Math.max(last.max_gatha, gatha);
      }
      continue;
    }

    const gatha = Number(row.gatha);
    ranges.push({
      book_code: row.book_code,
      code_label: codeLabel(row.book_code),
      pdf_file_name: String(row.pdf_file_name || "PDF"),
      page_start: start,
      page_end: end,
      min_gatha: Number.isFinite(gatha) ? gatha : null,
      max_gatha: Number.isFinite(gatha) ? gatha : null,
      count: 1,
    });
  }

  return ranges;
}

function buildIdentifierSummaries(rows: MapRow[]) {
  const byId = new Map<string, MapRow[]>();

  for (const row of rows) {
    const key = row.adhikar == null ? "none" : String(row.adhikar);
    const existing = byId.get(key) || [];
    existing.push(row);
    byId.set(key, existing);
  }

  return [...byId.entries()]
    .map(([key, group]) => {
      const gathas = group
        .map((row) => Number(row.gatha))
        .filter((value) => Number.isFinite(value));
      const uniqueGathas = [...new Set(gathas)].sort((a, b) => a - b);
      const pageRanges = mergePageRanges(group);
      return {
        adhikar: key === "none" ? null : Number(key),
        label: key === "none" ? "No identifier" : `Identifier ${key}`,
        total_gathas: uniqueGathas.length,
        min_gatha: uniqueGathas[0] ?? null,
        max_gatha: uniqueGathas[uniqueGathas.length - 1] ?? null,
        book_codes: [...new Set(group.map((row) => codeLabel(row.book_code)))],
        page_ranges: pageRanges,
      };
    })
    .sort((a, b) => {
      if (a.adhikar == null && b.adhikar != null) return 1;
      if (a.adhikar != null && b.adhikar == null) return -1;
      return Number(a.adhikar || 0) - Number(b.adhikar || 0);
    });
}

function isMissingCoverColumns(message: string) {
  return /cover_image_url|cover_image_key/i.test(message);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const bookId = parseBookId(req.query.bookId);
  const bookCode = String(firstString(req.query.bookCode) || "").trim();
  if (!bookId) return res.status(400).json({ error: "bookId is required" });

  try {
    const cacheKey = buildCacheKey(req, "granth-mapping-context");
    const { value: payload, status } = await getCachedJson(cacheKey, 300, async () => {
      const supabase = getSupabaseAdmin();

      const { data: bookRows, error: bookError } = await supabase
        .from("granth_library_books")
        .select("id,title_english,title_display,author_text,details_text,book_codes,cover_rel_path,index_href,index_href_type")
        .eq("id", bookId)
        .limit(1);

      if (bookError) throw new Error(bookError.message);
      const book = bookRows?.[0];
      if (!book) return { error: "Book not found" };

      const files = (await fetchAll((from, to) => {
        let query = supabase
          .from("granth_library_files")
          .select("id,book_code,pdf_file_name,pdf_url,custom_id,page_count,granth_ocr_file_id")
          .eq("book_id", bookId)
          .range(from, to);
        if (bookCode) query = query.eq("book_code", bookCode);
        return query.order("book_code", { ascending: true }).order("pdf_file_name", { ascending: true });
      })) as FileRow[];

      const ocrIds = [...new Set(files.map((row) => Number(row.granth_ocr_file_id)).filter(Number.isFinite))];
      const ocrById = new Map<number, OcrFileRow>();

      if (ocrIds.length > 0) {
        const baseSelect = "id,file_name,file_size,collection,subcollection";
        const coverSelect = `${baseSelect},cover_image_url,cover_image_key`;
        const coverResponse = await supabase.from("granth_ocr_files").select(coverSelect).in("id", ocrIds);
        let ocrRows: OcrFileRow[] = [];
        if (coverResponse.error && isMissingCoverColumns(coverResponse.error.message)) {
          const baseResponse = await supabase.from("granth_ocr_files").select(baseSelect).in("id", ocrIds);
          if (baseResponse.error) throw new Error(baseResponse.error.message);
          ocrRows = (baseResponse.data ?? []) as OcrFileRow[];
        } else {
          if (coverResponse.error) throw new Error(coverResponse.error.message);
          ocrRows = (coverResponse.data ?? []) as OcrFileRow[];
        }
        for (const row of ocrRows) {
          ocrById.set(Number(row.id), row);
        }
      }

      const mapRows = (await fetchAll((from, to) => {
        let query = supabase
          .from("granth_gatha_map")
          .select("book_code,pdf_file_name,pdf_url,custom_id,adhikar,gatha,page_start,page_end,anchor_text")
          .eq("book_id", bookId)
          .range(from, to);
        if (bookCode) query = query.eq("book_code", bookCode);
        return query.order("adhikar", { ascending: true }).order("gatha", { ascending: true });
      })) as MapRow[];

      const identifiers = buildIdentifierSummaries(mapRows);
      const allPageRanges = mergePageRanges(mapRows);
      const totalGathas = new Set(mapRows.map((row) => Number(row.gatha)).filter(Number.isFinite)).size;

      return {
        book,
        files: files.map((file) => {
          const ocr = file.granth_ocr_file_id ? ocrById.get(Number(file.granth_ocr_file_id)) : undefined;
          return {
            id: file.id,
            book_code: file.book_code,
            code_label: codeLabel(file.book_code),
            pdf_file_name: file.pdf_file_name,
            pdf_url: file.pdf_url,
            custom_id: file.custom_id,
            page_count: file.page_count,
            cover_image_url: ocr?.cover_image_url ?? null,
            file_size: ocr?.file_size ?? null,
            collection: ocr?.collection ?? null,
            subcollection: ocr?.subcollection ?? null,
          };
        }),
        identifiers,
        page_ranges: allPageRanges,
        meta: {
          bookCode: bookCode || null,
          file_count: files.length,
          identifier_count: identifiers.length,
          mapped_row_count: mapRows.length,
          total_gathas: totalGathas,
        },
      };
    });

    if ("error" in (payload as Record<string, unknown>)) {
      setNoStore(res);
      return res.status(404).json(payload);
    }

    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 1800 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    setNoStore(res);
    if (/granth_library_books|granth_library_files|granth_gatha_map|schema cache/i.test(message)) {
      return res.status(503).json({
        error:
          "Mapping tables are not available yet. Run supabase/migrations/20260725_granth_library_mapping.sql and import the mapping data.",
      });
    }
    return res.status(500).json({ error: message });
  }
}
