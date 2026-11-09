import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { learnWords } from "@/lib/anuvaad/saved-words";

// The words a box file's Gujarati anuvaad explains, for a person to check
// before saving: { pages: [text of each page, box and anuvaad, any order] } →
// { proposals: [{ word, gujarati, inBox, onPage }], boxPages, anuvaadPages }.
// Nothing is saved here.

export const config = { api: { bodyParser: { sizeLimit: "4mb" } }, maxDuration: 300 };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const body = (req.body ?? {}) as Record<string, unknown>;
  const pages = (Array.isArray(body.pages) ? body.pages : []).slice(0, 80).map((p) => String(p ?? "").normalize("NFC").slice(0, 30000));
  if (!pages.some((p) => p.trim())) return res.status(400).json({ error: "No pages were read." });
  try {
    const learnt = await learnWords(pages);
    if (!learnt.anuvaadPages) return res.status(400).json({ error: "No Gujarati anuvaad page was found. Add the anuvaad pages with the box pages." });
    return res.status(200).json(learnt);
  } catch (error) {
    console.error("learn words failed:", error instanceof Error ? error.message : error);
    return res.status(500).json({ error: "The words could not be read from the anuvaad just now. Please try again." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
