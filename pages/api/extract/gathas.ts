import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { chapterGathas, getWork } from "@/lib/extract-data";

/** The gathas of one chapter with the PDF pages each one is on. */
async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  try {
    const work = await getWork(String(req.query.work ?? ""));
    if (!work) return res.status(404).json({ error: "That granth was not found." });
    const gathas = await chapterGathas(work, String(req.query.chapter ?? ""));
    res.setHeader("Cache-Control", "private, max-age=120");
    return res.status(200).json({ gathas });
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
