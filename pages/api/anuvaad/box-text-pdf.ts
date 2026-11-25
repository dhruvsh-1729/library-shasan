import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { buildTablePdf, type TableRow } from "@/lib/word-list-pdf";
import { BOX_TEXT_WIDTHS } from "@/lib/anuvaad/box-text";

// The box text (Sr. no. | Granth | ShastraPath) as a PDF table with no header
// row, page header or footer (lib/anuvaad/box-text). Nothing is stored.

export const config = { api: { bodyParser: { sizeLimit: "8mb" }, responseLimit: false }, maxDuration: 120 };

const MAX_ROWS = 3000;

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const raw = (req.body ?? {}).rows;
  if (!Array.isArray(raw) || !raw.length || raw.length > MAX_ROWS) return res.status(400).json({ error: "There are no passages to put in the PDF." });
  const rows: TableRow[] = raw.map((r: unknown) =>
    Array.isArray(r)
      ? r.slice(0, 3).map((c) => String(c ?? "").normalize("NFC").slice(0, 40_000))
      : { band: String((r as { band?: unknown })?.band ?? "").normalize("NFC").slice(0, 300) }
  );
  const bytes = await buildTablePdf({
    title: String((req.body ?? {}).title ?? "Box text").slice(0, 200),
    headers: ["Sr.", "Granth", "ShastraPath"],
    rows,
    showHeaders: false,
    widths: BOX_TEXT_WIDTHS,
    numbers: false,
  });
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="box-text.pdf"`);
  res.setHeader("Content-Length", String(bytes.length));
  res.status(200).send(Buffer.from(bytes));
}

export default protectApi(handler, PERMISSIONS.pdfBuild);
