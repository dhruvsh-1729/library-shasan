import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { AnuvaadError, translatePassage } from "@/lib/anuvaad/translate";

// The Gujarati anuvaad of one box passage. Built from the request alone;
// nothing is stored.

export const config = { api: { bodyParser: { sizeLimit: "200kb" } }, maxDuration: 300 };

const text = (value: unknown, max: number) => String(value ?? "").normalize("NFC").slice(0, max);

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const body = (req.body ?? {}) as Record<string, unknown>;
  try {
    const result = await translatePassage({
      vishay: text(body.vishay, 300),
      granth: text(body.granth, 600),
      passage: text(body.passage, 20_000),
    });
    return res.status(200).json(result);
  } catch (error) {
    const status = error instanceof AnuvaadError ? error.status : 500;
    if (!(error instanceof AnuvaadError)) console.error("anuvaad translate failed:", error instanceof Error ? error.message : error);
    return res.status(status).json({ error: error instanceof AnuvaadError ? error.message : "The anuvaad could not be prepared. Please try again." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
