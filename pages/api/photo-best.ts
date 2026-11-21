import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { bestReading } from "@/lib/photo-reading";

// Of a photo's readings (the photo turned three ways), which one is the right
// way up: lib/photo-reading.

export const config = { api: { bodyParser: { sizeLimit: "200kb" } } };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const texts = Array.isArray(req.body?.texts) ? (req.body.texts as unknown[]).slice(0, 4).map((t) => String(t ?? "").normalize("NFC").slice(0, 20_000)) : [];
  if (!texts.length) return res.status(400).json({ error: "No readings." });
  return res.status(200).json({ best: bestReading(texts) });
}

export default protectApi(handler, PERMISSIONS.libraryRead);
