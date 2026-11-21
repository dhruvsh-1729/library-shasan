import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { locatePassage } from "@/lib/anuvaad/locate";

// Where a box passage is printed in the library's granths, with the page and
// four pages on either side (their text, and which are the book's own
// Gujarati). See lib/anuvaad/locate.

export const config = { api: { bodyParser: { sizeLimit: "100kb" } }, maxDuration: 60 };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const passage = String((req.body ?? {}).passage ?? "").normalize("NFC").slice(0, 20_000);
  // the box's Granth column: the book it names is looked in first
  const granth = String((req.body ?? {}).granth ?? "").normalize("NFC").slice(0, 1_000);
  if ((passage.match(/[ऀ-ॿ]/g)?.length ?? 0) < 20) return res.status(400).json({ error: "The passage is too short to find." });
  try {
    return res.status(200).json({ places: await locatePassage(passage, 3, granth) });
  } catch (error) {
    console.error("anuvaad locate failed:", error instanceof Error ? error.message : error);
    return res.status(502).json({ error: "The library could not be searched just now. Please try again." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
