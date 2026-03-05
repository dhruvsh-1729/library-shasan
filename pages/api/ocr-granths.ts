import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";

type GranthRow = {
  granth_key: string;
  book_number: string;
  library_code: string | null;
  granth_name: string;
  xlsx_url: string | null;
  page_count: number;
};

function parseLimit(raw: unknown, fallback: number, min: number, max: number) {
  const value = Number(raw ?? fallback);
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return Math.max(min, Math.min(max, Math.floor(value)));
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

function displayName(bookNumber: string, libraryCode: string | null, granthName: string) {
  const code = libraryCode ? ` ${libraryCode}` : "";
  return `${bookNumber}${code} ${granthName}`.replace(/\s+/g, " ").trim();
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const client = getTursoClient();
    const limit = parseLimit(req.query.limit, 5000, 1, 10000);

    const [listResult, countResult] = await Promise.all([
      client.execute({
        sql: `SELECT granth_key, book_number, library_code, granth_name, xlsx_url, page_count
              FROM ocr_granths
              ORDER BY CAST(book_number AS INTEGER) ASC, granth_name ASC
              LIMIT ?`,
        args: [limit],
      }),
      client.execute({
        sql: `SELECT COUNT(*) AS total FROM ocr_granths`,
      }),
    ]);

    const items: GranthRow[] = listResult.rows.map((row) => {
      const bookNumber = toStr(row.book_number);
      const granthName = toStr(row.granth_name);
      const libraryCodeRaw = row.library_code == null ? null : toStr(row.library_code);
      return {
        granth_key: toStr(row.granth_key),
        book_number: bookNumber,
        library_code: libraryCodeRaw,
        granth_name: granthName,
        xlsx_url: row.xlsx_url == null ? null : toStr(row.xlsx_url),
        page_count: toInt(row.page_count),
      };
    });

    res.status(200).json({
      items: items.map((row) => ({
        ...row,
        display_name: displayName(row.book_number, row.library_code, row.granth_name),
      })),
      total: toInt(countResult.rows[0]?.total),
      limit,
    });
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
