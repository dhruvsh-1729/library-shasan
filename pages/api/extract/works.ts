import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { listWorks } from "@/lib/extract-data";

/** Every granth with a PDF, its volumes, and how many gathas are mapped. */
async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  try {
    const works = await listWorks();
    res.setHeader("Cache-Control", "private, max-age=120");
    return res.status(200).json({ works });
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
