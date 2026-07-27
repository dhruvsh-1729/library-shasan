import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";
import { buildOCRSearchExcerpt, findOCRSearchMatches, parseOCRSearchMode } from "@/lib/ocr-search";
import { buildOCRSuffixQuery, escapeFtsPhrase, escapeFtsToken } from "@/lib/ocr-search-index";

function parseLimit(raw: unknown) {
  const value = Number(raw ?? 48);
  if (!Number.isFinite(value) || value <= 0) return 48;
  return Math.min(Math.floor(value), 100);
}

function parsePage(raw: unknown) {
  const value = Number(raw ?? 1);
  if (!Number.isFinite(value) || value <= 0) return 1;
  return Math.floor(value);
}

function parseGranthKeys(raw: string | string[] | undefined) {
  if (!raw) return [];
  const values = Array.isArray(raw) ? raw : String(raw).split(",");
  return values
    .map((v) => String(v).trim())
    .filter(Boolean);
}

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toFloat(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function toStr(value: unknown, fallback = "") {
  if (value == null) return fallback;
  return String(value);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const q = String(req.query.q ?? "").trim();
  const limit = parseLimit(req.query.limit);
  const page = parsePage(req.query.page);
  const offset = (page - 1) * limit;
  const selectedGranths = parseGranthKeys(req.query.granths);
  const matchMode = parseOCRSearchMode(req.query.matchMode);

  if (!q || q.length < 2) {
    return res.status(200).json({ results: [], total: 0, match_mode: matchMode });
  }

  if (matchMode === "contains" && Array.from(q).length < 3) {
    return res.status(400).json({
      error: "Contains search requires at least 3 characters so it can use the trigram index.",
      results: [],
      total: 0,
      match_mode: matchMode,
    });
  }

  const granthFilterSql = selectedGranths.length
    ? ` AND p.granth_key IN (${selectedGranths.map(() => "?").join(",")})`
    : "";

  try {
    const client = getTursoClient();
    let total = 0;
    let results: Array<{
      granth_key: string;
      book_number: string;
      library_code: string | null;
      granth_name: string;
      page_number: number;
      snippet: string;
      score: number;
      xlsx_url: string | null;
    }> = [];

    const ftsTable =
      matchMode === "contains"
        ? "ocr_pages_trigram_fts"
        : matchMode === "ends_with"
          ? "ocr_pages_suffix_fts"
          : "ocr_pages_search_fts";
    const ftsColumn = matchMode === "ends_with" ? "reversed_content" : "content";
    const matchQuery =
      matchMode === "begins_with"
        ? `${escapeFtsToken(q)}*`
        : matchMode === "ends_with"
          ? buildOCRSuffixQuery(q)
          : escapeFtsPhrase(q);

    const countResult = await client.execute({
      sql: `SELECT COUNT(*) AS total
            FROM ${ftsTable}
            JOIN ocr_pages p ON p.id = ${ftsTable}.rowid
            WHERE ${ftsTable} MATCH ?${granthFilterSql}`,
      args: [matchQuery, ...selectedGranths],
    });

    const listResult = await client.execute({
      sql: `SELECT
              p.granth_key,
              g.book_number,
              g.library_code,
              g.granth_name,
              g.xlsx_url,
              p.page_number,
              p.content,
              0 AS rank
            FROM ${ftsTable}
            JOIN ocr_pages p ON p.id = ${ftsTable}.rowid
            JOIN ocr_granths g ON g.granth_key = p.granth_key
            WHERE ${ftsTable} MATCH ?${granthFilterSql}
            ORDER BY ${ftsTable}.rowid ASC
            LIMIT ? OFFSET ?`,
      args: [matchQuery, ...selectedGranths, limit, offset],
    });

    total = toInt(countResult.rows[0]?.total);
    results = listResult.rows.map((row) => {
      const content = toStr(row.content);
      return {
        granth_key: toStr(row.granth_key),
        book_number: toStr(row.book_number),
        library_code: row.library_code == null ? null : toStr(row.library_code),
        granth_name: toStr(row.granth_name),
        page_number: toInt(row.page_number),
        snippet: buildOCRSearchExcerpt(content, q, matchMode),
        score: toFloat(row.rank),
        occurrence_count: findOCRSearchMatches(content, q, matchMode).length,
        xlsx_url: row.xlsx_url == null ? null : toStr(row.xlsx_url),
      };
    });

    return res.status(200).json({
      results,
      total,
      selected_granth_count: selectedGranths.length,
      page,
      per_page: limit,
      match_mode: matchMode,
      search_table: ftsTable,
      search_column: ftsColumn,
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
