import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { AnuvaadError, translatePage } from "@/lib/anuvaad/translate";

// The Gujarati anuvaad of one page of any PDF ("Any PDF pages" on /anuvaad),
// from the page's text as read by /api/anuvaad/ocr. One page per request, so
// each page finishes well inside the time a request may run; nothing is
// stored.

export const config = { api: { bodyParser: { sizeLimit: "200kb" } }, maxDuration: 300 };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const body = (req.body ?? {}) as Record<string, unknown>;
  try {
    const result = await translatePage({
      text: String(body.text ?? "").normalize("NFC").slice(0, 20_000),
      source: String(body.source ?? "").normalize("NFC").slice(0, 200),
    });
    return res.status(200).json({ markup: result.markup, literal: result.literal });
  } catch (error) {
    const status = error instanceof AnuvaadError ? error.status : 500;
    if (!(error instanceof AnuvaadError)) console.error("anuvaad page failed:", error instanceof Error ? error.message : error);
    return res.status(status).json({ error: error instanceof AnuvaadError ? error.message : "The anuvaad could not be prepared. Please try again." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
