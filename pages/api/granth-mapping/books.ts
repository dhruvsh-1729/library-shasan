import type { NextApiRequest, NextApiResponse } from "next";
import overrides from "@/data/granth-catalog-overrides.json";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";

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


const BOOK_OVERRIDES = (overrides as { books?: Record<string, Record<string, unknown>> }).books ?? {};

/**
 * The work's name from a library title that lists every part
 * ("शब्दरत्नमहोदधि ભાગ 1 શબ્દરત્નમહોદધિ ભાગ 2 …" -> "શબ્દરત્નમહોદધિ"): the
 * parts are chosen by their book codes in the extractor anyway.
 */
export function workTitle(display: string | null | undefined) {
  const text = String(display ?? "").trim();
  const partMark = /\s*(?:ભાગ|भाग)[-\s]*[\d૦-૯०-९]+(?:\s*-\s*[\d૦-૯०-९]+)?(?:\s*\([A-Z]\d+\))?\s*/u;
  const pieces = text.split(partMark).map((piece) => piece.trim()).filter(Boolean);
  return pieces.length > 1 ? pieces[0] : text;
}

/** Gatha rows in the map for each of a book's codes; a code with 0 has only pages. */
async function mappedGathaCounts(supabase: ReturnType<typeof getSupabaseAdmin>, books: Array<{ id: number; codes: string[] }>) {
  const jobs = books.flatMap((book) => book.codes.map((code) => ({ id: book.id, code })));
  const counts = new Map<number, Record<string, number>>();
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(16, jobs.length) }, async () => {
      while (next < jobs.length) {
        const { id, code } = jobs[next++];
        const { count } = await supabase
          .from("granth_gatha_map")
          .select("book_id", { count: "exact", head: true })
          .eq("book_id", id)
          .eq("book_code", code);
        const entry = counts.get(id) ?? {};
        entry[code] = count ?? 0;
        counts.set(id, entry);
      }
    })
  );
  return counts;
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
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

      const rows = (data ?? []).map((row) => ({ ...row, ...(BOOK_OVERRIDES[String(row.id)] ?? {}) }));
      const counts = await mappedGathaCounts(
        supabase,
        rows.map((row) => ({ id: Number(row.id), codes: ((row.book_codes as string[] | null) ?? []).map(String) }))
      );
      return {
        items: rows.map(({ _why: _unused, ...row }: Record<string, unknown>) => ({
          ...row,
          title_display: workTitle(row.title_display as string | null),
          mapped_gathas: Object.values(counts.get(Number(row.id)) ?? {}).reduce((sum, n) => sum + n, 0),
          mapped_by_code: counts.get(Number(row.id)) ?? {},
        })),
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

export default protectApi(handler, PERMISSIONS.libraryRead);
