import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { granthKeyForPdf } from "@/lib/line-boxes";
import { getTursoClient } from "@/lib/turso";

// The printed (book) page number of each page of a granth PDF, for the page
// viewer: [[pdfPage, "262"], …]. Pages whose text and image readings disagree
// are left out, like everywhere else printed pages are shown.

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const pdfUrl = String(req.query.pdfUrl ?? "").trim();
  if (!pdfUrl) return res.status(400).json({ error: "pdfUrl is required" });
  try {
    const granthKey = await granthKeyForPdf(pdfUrl);
    if (!granthKey) return res.status(200).json({ pages: [] });
    const rows = (
      await getTursoClient().execute({
        sql: `SELECT page_number, printed_page FROM ocr_printed_pages
              WHERE granth_key = ? AND (verified IS NULL OR verified <> 'conflict')
              ORDER BY page_number`,
        args: [granthKey],
      })
    ).rows;
    const pages: Array<[number, string]> = [];
    for (const row of rows) {
      const printed = String(row.printed_page ?? "").trim();
      const page = Number(row.page_number);
      if (printed && page > 0) pages.push([page, printed]);
    }
    res.setHeader("Cache-Control", "private, max-age=3600");
    return res.status(200).json({ pages });
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
