import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { granthDisplayName, getGranthCatalog, isSearchable } from "@/lib/granth-catalog";
import { assignShortKeys } from "@/lib/granth-short-keys";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";

export type SearchGranthOption = {
  custom_id: string;
  /** Short id used in search URLs, e.g. "215". */
  key: string;
  granth_key: string;
  /** "pdf": has an uploaded PDF; "text": OCR text only. */
  kind: "pdf" | "text";
  /** "Adhyatmasar Shabdasha Vivechan · Part 3" */
  display_name: string;
  title: string;
  native_title: string;
  part: string | null;
  series: string | null;
  author: string | null;
  book_number: string | null;
  page_count: number | null;
  /** The file name as stored, for matching and for anyone checking the source. */
  source_name: string;
};

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const cacheKey = buildCacheKey(req, "search-granths-v2");
    const { value: payload, status } = await getCachedJson(cacheKey, 300, async () => {
      // Every searchable granth is listed, from the catalog: exactly the granths
      // the index holds, less the duplicates it keeps out of search. Picking a
      // granth that has no indexed pages (or listing one twice) is not possible.
      const catalog = await getGranthCatalog();
      const listed = catalog.entries.filter((entry) => isSearchable(catalog, entry));
      const sourceOf = (entry: (typeof listed)[number]) => entry.document_rel_path || entry.granth_key;
      const keys = assignShortKeys(listed.map((entry) => ({ id: entry.custom_id, source: sourceOf(entry) })));

      const items: SearchGranthOption[] = listed.map((entry) => ({
        custom_id: entry.custom_id,
        key: keys.get(entry.custom_id) ?? entry.granth_key,
        granth_key: entry.granth_key,
        kind: entry.kind,
        display_name: granthDisplayName(entry),
        title: entry.title,
        native_title: entry.native_title,
        part: entry.part,
        series: entry.series,
        author: entry.author,
        book_number: entry.book_number,
        page_count: entry.page_count,
        source_name: entry.source_name,
      }));

      // Links keep working when a key changes or a granth turns out to be a
      // duplicate: its granth key, text-only id and old key lead to the granth
      // that is searched in its place.
      const aliases: Record<string, string> = {};
      for (const entry of catalog.entries) {
        const target = isSearchable(catalog, entry) ? entry : catalog.byGranthKey.get(entry.duplicate_of ?? "");
        if (!target || !isSearchable(catalog, target)) continue;
        for (const alias of [entry.granth_key, entry.custom_id, `text:${entry.granth_key}`]) {
          if (alias && alias !== target.custom_id) aliases[alias] = target.custom_id;
        }
      }

      return { items, total: items.length, aliases };
    });

    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 1800 }, status);
    return res.status(200).json(payload);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
