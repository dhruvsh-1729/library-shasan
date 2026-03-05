import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";
import { buildOCRSearchExcerpt, findOCRSearchMatches, parseOCRSearchMode } from "@/lib/ocr-search";

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

function escapeLikePattern(input: string) {
  return input.replace(/[\\%_]/g, "\\$&");
}

function buildFtsPhrase(query: string) {
  return `"${String(query ?? "").replace(/"/g, "\"\"")}"`;
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

    if (matchMode === "exact_word") {
      const candidateResult = await client.execute({
        sql: `SELECT
                p.granth_key,
                g.book_number,
                g.library_code,
                g.granth_name,
                g.xlsx_url,
                p.page_number,
                p.content,
                bm25(ocr_pages_fts) AS rank
              FROM ocr_pages_fts
              JOIN ocr_pages p ON p.id = ocr_pages_fts.rowid
              JOIN ocr_granths g ON g.granth_key = p.granth_key
              WHERE ocr_pages_fts MATCH ?${granthFilterSql}
              ORDER BY rank ASC, CAST(g.book_number AS INTEGER) ASC, p.page_number ASC`,
        args: [buildFtsPhrase(q), ...selectedGranths],
      });

      const filtered = candidateResult.rows.filter((row) =>
        findOCRSearchMatches(toStr(row.content), q, matchMode).length > 0
      );
      total = filtered.length;
      const pageRows = filtered.slice(offset, offset + limit);

      results = pageRows.map((row) => ({
        granth_key: toStr(row.granth_key),
        book_number: toStr(row.book_number),
        library_code: row.library_code == null ? null : toStr(row.library_code),
        granth_name: toStr(row.granth_name),
        page_number: toInt(row.page_number),
        snippet: buildOCRSearchExcerpt(toStr(row.content), q, matchMode),
        score: toFloat(row.rank),
        xlsx_url: row.xlsx_url == null ? null : toStr(row.xlsx_url),
      }));
    } else {
      const likePattern = `%${escapeLikePattern(q)}%`;
      const baseArgs = [likePattern, ...selectedGranths];

      const countResult = await client.execute({
        sql: `SELECT COUNT(*) AS total
              FROM ocr_pages p
              WHERE p.content LIKE ? ESCAPE '\\'${granthFilterSql}`,
        args: baseArgs,
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
              FROM ocr_pages p
              JOIN ocr_granths g ON g.granth_key = p.granth_key
              WHERE p.content LIKE ? ESCAPE '\\'${granthFilterSql}
              ORDER BY INSTR(LOWER(p.content), LOWER(?)) ASC, CAST(g.book_number AS INTEGER) ASC, p.page_number ASC
              LIMIT ? OFFSET ?`,
        args: [...baseArgs, q, limit, offset],
      });

      total = toInt(countResult.rows[0]?.total);
      results = listResult.rows.map((row) => ({
        granth_key: toStr(row.granth_key),
        book_number: toStr(row.book_number),
        library_code: row.library_code == null ? null : toStr(row.library_code),
        granth_name: toStr(row.granth_name),
        page_number: toInt(row.page_number),
        snippet: buildOCRSearchExcerpt(toStr(row.content), q, matchMode),
        score: toFloat(row.rank),
        xlsx_url: row.xlsx_url == null ? null : toStr(row.xlsx_url),
      }));
    }

    return res.status(200).json({
      results,
      total,
      selected_granth_count: selectedGranths.length,
      page,
      per_page: limit,
      match_mode: matchMode,
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
