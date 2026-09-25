import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { SEARCHABLE_DOCUMENT_STATUSES } from "@/lib/document-scan-state";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { fetchTextOnlyGranths } from "@/lib/text-only-granths";
import { assignShortKeys } from "@/lib/granth-short-keys";

type GranthOption = {
  custom_id: string;
  /** Short id used in /search URLs, e.g. "215". */
  key: string;
  /** "pdf": has an uploaded PDF; "text": OCR text only. */
  kind: "pdf" | "text";
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

async function handler(req: NextApiRequest, res: NextApiResponse) {
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
      // The whole searchable catalog is small (a few hundred granths), so it is
      // loaded in full: short keys are assigned over all of it, and the name
      // filter and paging are applied afterwards.
      const supabase = getSupabaseAdmin();
      const docs: { custom_id: string | null; pdf_name: string | null; original_relative_path: string | null }[] = [];
      for (let from = 0; ; from += 1000) {
        const { data, error } = await supabase
          .from("documents")
          .select("custom_id,pdf_name,original_relative_path")
          .not("custom_id", "is", null)
          .in("status", [...SEARCHABLE_DOCUMENT_STATUSES])
          .order("pdf_name", { ascending: true, nullsFirst: false })
          .range(from, from + 999);
        if (error) throw new Error(error.message);
        docs.push(...(data ?? []));
        if (!data || data.length < 1000) break;
      }

      const seen = new Set<string>();
      const all: (GranthOption & { source: string })[] = [];
      for (const row of docs) {
        const customId = String(row.custom_id ?? "").trim();
        if (!customId || seen.has(customId)) continue;
        seen.add(customId);
        all.push({
          custom_id: customId,
          key: "",
          kind: "pdf",
          pdf_name: row.pdf_name ?? null,
          display_name: displayName(row.pdf_name ?? null, customId),
          source: row.original_relative_path || row.pdf_name || customId,
        });
      }
      // Granths with OCR text but no uploaded PDF are searchable too.
      for (const row of await fetchTextOnlyGranths()) {
        all.push({
          custom_id: row.customId,
          key: "",
          kind: "text",
          pdf_name: null,
          display_name: row.name,
          source: row.granthKey,
        });
      }
      const keys = assignShortKeys(all.map((row) => ({ id: row.custom_id, source: row.source })));

      const term = q.toLowerCase();
      const matching = all
        .filter((row) => !term || `${keys.get(row.custom_id)} ${row.display_name} ${row.source}`.toLowerCase().includes(term))
        .map(({ source, ...row }) => ({ ...row, key: keys.get(row.custom_id) ?? source }));
      const items = matching.slice(offset, offset + limit);
      const total = matching.length;
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

export default protectApi(handler, PERMISSIONS.libraryRead);
