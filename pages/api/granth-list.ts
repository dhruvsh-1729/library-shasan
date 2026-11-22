import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { describeGranth, getGranthCatalog, isSearchable } from "@/lib/granth-catalog";
import list from "@/data/shrirang-granth-list.json";

// Maharaj Saheb's list of the Shrirang library's books (13 Nov 2026: "granth
// list add kiya?"), from data/shrirang-granth-list.json, each with the book of
// ours that has the same Lib No. A book with no such match is not guessed by
// name (the catalog is the single truth); the page offers a name search.

type ListRow = { sr: number; libNo: string; pdf: string; name: string };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  const catalog = await getGranthCatalog();
  const byCode = new Map<string, { granthKey: string; customId: string; name: string; bookNumber: string }>();
  for (const entry of catalog.entries) {
    if (!isSearchable(catalog, entry) || !entry.library_code) continue;
    const code = entry.library_code.toUpperCase();
    if (byCode.has(code)) continue;
    byCode.set(code, { granthKey: entry.granth_key, customId: entry.custom_id, name: describeGranth(catalog, entry.granth_key)?.displayName ?? entry.granth_key, bookNumber: entry.book_number ?? "" });
  }
  const rows = (list.rows as ListRow[]).map((row) => ({ ...row, ours: byCode.get(row.libNo) ?? null }));
  return res.status(200).json({ rows });
}

export default protectApi(handler, PERMISSIONS.libraryRead);
