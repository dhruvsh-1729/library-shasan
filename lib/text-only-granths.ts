import { getTursoClient } from "@/lib/turso";

// Granths whose OCR text is in Turso but whose PDF was never uploaded (the
// "GG 76 Prat" set came in as spreadsheets only). They have no Supabase
// documents/granth_ocr_files rows, so the library and search pickers list them
// from Turso under a synthetic id instead.
export const TEXT_ONLY_ID_PREFIX = "text:";
export const TEXT_ONLY_COLLECTION = "GG 76 Prat (text only)";
const TEXT_ONLY_PATH_PATTERN = "%GG 76 Prat OCR_ed/%";
// Spreadsheet copies of granths that are already in the library with a PDF.
const DUPLICATE_KEYS = ["414_B053915"];

export type TextOnlyGranth = {
  granthKey: string;
  customId: string;
  name: string;
  sourceRelPath: string;
  pageCount: number;
};

export const isTextOnlyId = (id: string) => id.startsWith(TEXT_ONLY_ID_PREFIX);
export const textOnlyGranthKey = (id: string) => id.slice(TEXT_ONLY_ID_PREFIX.length);

let cache: { at: number; rows: TextOnlyGranth[] } | null = null;

export async function fetchTextOnlyGranths(): Promise<TextOnlyGranth[]> {
  if (cache && Date.now() - cache.at < 5 * 60_000) return cache.rows;
  const result = await getTursoClient().execute({
    sql: `SELECT granth_key, granth_name, source_rel_path, page_count
          FROM ocr_granths
          WHERE source_rel_path LIKE ?
            AND granth_key NOT IN (${DUPLICATE_KEYS.map(() => "?").join(",")})
          ORDER BY granth_key`,
    args: [TEXT_ONLY_PATH_PATTERN, ...DUPLICATE_KEYS],
  });
  const rows = result.rows.map((row) => {
    const granthKey = String(row.granth_key ?? "");
    return {
      granthKey,
      customId: `${TEXT_ONLY_ID_PREFIX}${granthKey}`,
      name: String(row.granth_name ?? granthKey),
      sourceRelPath: String(row.source_rel_path ?? ""),
      pageCount: Number(row.page_count ?? 0),
    };
  });
  cache = { at: Date.now(), rows };
  return rows;
}

/** Text-only granths whose name, key or path contains the search term. */
export function filterTextOnlyGranths(rows: TextOnlyGranth[], q: string) {
  const term = q.trim().toLowerCase();
  if (!term) return rows;
  return rows.filter((row) => `${row.granthKey} ${row.name} ${row.sourceRelPath}`.toLowerCase().includes(term));
}
