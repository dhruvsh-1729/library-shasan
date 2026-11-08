import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { AnuvaadError } from "@/lib/anuvaad/translate";
import { translateVyutpatti } from "@/lib/anuvaad/vyutpatti";

// The Gujarati anuvaad of one vishay's vyutpatti, one per entry (after the
// reader's edits): { vishay, entries: [{ head, body }], fresh? } →
// { entries: string[], closing, failed }. Built from the request alone;
// nothing is stored ("fresh" asks again instead of reusing an earlier anuvaad).

export const config = { api: { bodyParser: { sizeLimit: "200kb" } }, maxDuration: 300 };

const text = (value: unknown, max: number) => String(value ?? "").normalize("NFC").slice(0, max);

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const body = (req.body ?? {}) as Record<string, unknown>;
  const raw = Array.isArray(body.entries) ? body.entries : Array.isArray(body.lines) ? body.lines : [];
  const entries = raw.slice(0, 80).map((l) => {
    const line = (l ?? {}) as Record<string, unknown>;
    return { head: text(line.head, 200), body: text(line.body, 4000) };
  });
  try {
    return res.status(200).json(await translateVyutpatti(text(body.vishay, 300), entries, { fresh: body.fresh === true }));
  } catch (error) {
    const status = error instanceof AnuvaadError ? error.status : 500;
    if (!(error instanceof AnuvaadError)) console.error("vyutpatti anuvaad failed:", error instanceof Error ? error.message : error);
    return res.status(status).json({ error: error instanceof AnuvaadError ? error.message : "The Gujarati anuvaad could not be prepared. Please try again." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
