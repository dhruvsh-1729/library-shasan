import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";

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

function normalizeQueryTerms(query: string) {
  return Array.from(
    new Set(
      query
        .trim()
        .split(/\s+/)
        .map((term) => term.replace(/^["'(]+|[)"',.:;!?]+$/g, "").trim().toLowerCase())
        .filter(Boolean)
    )
  ).sort((a, b) => b.length - a.length);
}

function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildExcerpt(content: string, query: string, maxChars = 180) {
  const cleanContent = String(content ?? "").replace(/\s+/g, " ").trim();
  if (!cleanContent) return "";

  const terms = normalizeQueryTerms(query);
  let matchIndex = -1;
  let matchLength = 0;

  for (const term of terms) {
    const pattern = new RegExp(escapeRegExp(term), "i");
    const match = pattern.exec(cleanContent);
    if (match && typeof match.index === "number") {
      matchIndex = match.index;
      matchLength = match[0]?.length ?? term.length;
      break;
    }
  }

  if (matchIndex < 0) {
    if (cleanContent.length <= maxChars) return cleanContent;
    return `${cleanContent.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
  }

  const sidePadding = Math.max(45, Math.floor((maxChars - matchLength) / 2));
  const start = Math.max(0, matchIndex - sidePadding);
  const end = Math.min(cleanContent.length, matchIndex + matchLength + sidePadding);
  let excerpt = cleanContent.slice(start, end).trim();

  if (start > 0) excerpt = `…${excerpt}`;
  if (end < cleanContent.length) excerpt = `${excerpt}…`;
  return excerpt;
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

  if (!q || q.length < 2) {
    return res.status(200).json({ results: [], total: 0 });
  }

  const granthFilterSql = selectedGranths.length
    ? ` AND p.granth_key IN (${selectedGranths.map(() => "?").join(",")})`
    : "";
  const baseArgs = [q, ...selectedGranths];

  try {
    const client = getTursoClient();

    const countResult = await client.execute({
      sql: `SELECT COUNT(*) AS total
            FROM ocr_pages_fts
            JOIN ocr_pages p ON p.id = ocr_pages_fts.rowid
            WHERE ocr_pages_fts MATCH ?${granthFilterSql}`,
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
              p.content AS snippet,
              bm25(ocr_pages_fts) AS rank
            FROM ocr_pages_fts
            JOIN ocr_pages p ON p.id = ocr_pages_fts.rowid
            JOIN ocr_granths g ON g.granth_key = p.granth_key
            WHERE ocr_pages_fts MATCH ?${granthFilterSql}
            ORDER BY rank ASC, CAST(g.book_number AS INTEGER) ASC, p.page_number ASC
            LIMIT ? OFFSET ?`,
      args: [...baseArgs, limit, offset],
    });

    const results = listResult.rows.map((row) => ({
      granth_key: toStr(row.granth_key),
      book_number: toStr(row.book_number),
      library_code: row.library_code == null ? null : toStr(row.library_code),
      granth_name: toStr(row.granth_name),
      page_number: toInt(row.page_number),
      snippet: buildExcerpt(toStr(row.snippet), q),
      score: toFloat(row.rank),
      xlsx_url: row.xlsx_url == null ? null : toStr(row.xlsx_url),
    }));

    return res.status(200).json({
      results,
      total: toInt(countResult.rows[0]?.total),
      selected_granth_count: selectedGranths.length,
      page,
      per_page: limit,
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
