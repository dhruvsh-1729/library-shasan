import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
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

  const granthKey = String(req.query.granthKey ?? "").trim();
  if (!granthKey) {
    return res.status(400).json({ error: "Missing granthKey" });
  }

  try {
    const client = getTursoClient();
    const [granthResult, pagesResult] = await Promise.all([
      client.execute({
        sql: `SELECT granth_key, book_number, library_code, granth_name, xlsx_url
              FROM ocr_granths
              WHERE granth_key = ?`,
        args: [granthKey],
      }),
      client.execute({
        sql: `SELECT page_number, content
              FROM ocr_pages
              WHERE granth_key = ?
              ORDER BY page_number ASC`,
        args: [granthKey],
      }),
    ]);

    const granth = granthResult.rows[0];
    if (!granth) {
      return res.status(404).json({ error: "Granth not found" });
    }

    return res.status(200).json({
      granth: {
        granth_key: toStr(granth.granth_key),
        book_number: toStr(granth.book_number),
        library_code: granth.library_code == null ? null : toStr(granth.library_code),
        granth_name: toStr(granth.granth_name),
        xlsx_url: granth.xlsx_url == null ? null : toStr(granth.xlsx_url),
      },
      rows: pagesResult.rows.map((row) => ({
        page_number: toInt(row.page_number),
        content: toStr(row.content),
      })),
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
