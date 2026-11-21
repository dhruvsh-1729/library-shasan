import type { NextApiRequest, NextApiResponse } from "next";
import { PDFDocument } from "pdf-lib";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { describeGranth, getGranthCatalog } from "@/lib/granth-catalog";
import { appendRemotePages } from "@/lib/pdf-remote-pages";
import { stampLabels, stampPages } from "@/lib/pdf-stamp";

// The pages a box passage was found on, with its neighbours, as one PDF cut
// from the library's own file: ?granth=<granth key>&from=<page>&to=<page>
// (at most 15 pages). Only the catalog's PDF is read, never a URL sent in.

export const config = { maxDuration: 120, api: { responseLimit: false } };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  const granth = String(req.query.granth ?? "");
  const from = Math.max(1, Number(req.query.from) || 0);
  const to = Number(req.query.to) || 0;
  if (!granth || !from || to < from || to - from > 14) return res.status(400).json({ error: "Choose up to 15 pages." });
  const described = describeGranth(await getGranthCatalog(), granth);
  if (!described?.pdfUrl) return res.status(404).json({ error: "This granth has no PDF to cut pages from." });
  try {
    const out = await PDFDocument.create();
    const pages = Array.from({ length: to - from + 1 }, (_, i) => from + i);
    await appendRemotePages(out, described.pdfUrl, pages);
    const labels = await stampLabels(granth, pages).catch(() => new Map<number, string>());
    await stampPages(out, out.getPages().map((page, i) => ({ page, label: labels.get(pages[i]) ?? "" })));
    const bytes = await out.save();
    const name = `${described.displayName} p${from}-${to}.pdf`.replace(/[\\/:*?"<>|]+/g, " ");
    res.setHeader("Content-Type", "application/pdf");
    // ?inline=1 opens in the browser ("Open this page"); otherwise it is saved
    const how = req.query.inline === "1" ? "inline" : "attachment";
    res.setHeader("Content-Disposition", `${how}; filename*=UTF-8''${encodeURIComponent(name)}`);
    return res.status(200).send(Buffer.from(bytes));
  } catch (error) {
    console.error("located pdf failed:", error instanceof Error ? error.message : error);
    return res.status(502).json({ error: "The pages could not be cut from the PDF just now." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
