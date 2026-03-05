import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const client = getTursoClient();
    const [granthCount, pageCount] = await Promise.all([
      client.execute(`SELECT COUNT(*) AS total_granths FROM ocr_granths`),
      client.execute(`SELECT COUNT(*) AS total_pages FROM ocr_pages`),
    ]);

    return res.status(200).json({
      total_granths: toInt(granthCount.rows[0]?.total_granths),
      total_pages: toInt(pageCount.rows[0]?.total_pages),
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
