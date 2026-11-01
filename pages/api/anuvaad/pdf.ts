import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { type AnuvaadPdfSection, buildAnuvaadPdf } from "@/lib/anuvaad/pdf";
import { parseAnuvaad } from "@/lib/anuvaad/markup";

// The anuvaad pages as a PDF, from what the page shows (after the reader's
// edits, sent as markup). The page puts the uploaded box pages in front of
// them; nothing is stored.

export const config = { api: { bodyParser: { sizeLimit: "3mb" }, responseLimit: false } };

const MAX_SECTIONS = 200;

function readSections(raw: unknown): AnuvaadPdfSection[] | null {
  if (!Array.isArray(raw) || !raw.length || raw.length > MAX_SECTIONS) return null;
  const sections = raw
    .map((s) => {
      const section = (s ?? {}) as Record<string, unknown>;
      return {
        caption: String(section.caption ?? "").normalize("NFC").slice(0, 300),
        blocks: parseAnuvaad(String(section.markup ?? "").normalize("NFC").slice(0, 60_000)),
      };
    })
    .filter((s) => s.blocks.length);
  return sections.length ? sections : null;
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const sections = readSections((req.body ?? {}).sections);
  if (!sections) return res.status(400).json({ error: "There is no anuvaad to put in the PDF." });
  const bytes = await buildAnuvaadPdf(sections);
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="anuvaad.pdf"`);
  res.setHeader("Content-Length", String(bytes.length));
  res.status(200).send(Buffer.from(bytes));
}

export default protectApi(handler, PERMISSIONS.pdfBuild);
