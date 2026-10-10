import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { type KoshPageRef, buildKoshPagesPdf } from "@/lib/vyutpatti/kosh-pages";

// The kosh pages a vyutpatti was read from, as they are printed: one or all of
// them in one PDF, in the order asked (lib/vyutpatti/kosh-pages). The PDF's
// address comes from the catalog, never from the request.

export const config = { api: { responseLimit: false }, maxDuration: 120 };

const MAX_PAGES = 80;

function readPages(raw: unknown): KoshPageRef[] | null {
  if (!Array.isArray(raw) || !raw.length) return null;
  const seen = new Set<string>();
  const out: KoshPageRef[] = [];
  for (const item of raw) {
    const value = (item ?? {}) as Record<string, unknown>;
    const granthKey = String(value.granthKey ?? "").trim().slice(0, 64);
    const page = Math.floor(Number(value.page));
    if (!granthKey || !Number.isFinite(page) || page < 1) continue;
    const id = `${granthKey}:${page}`;
    if (seen.has(id)) continue;
    seen.add(id);
    out.push({ granthKey, page });
  }
  return out.length ? out : null;
}

function contentDisposition(filename: string) {
  const ascii = filename.replace(/[^\x20-\x7e]+/g, "_").replace(/["\\]/g, "_") || "kosh_pages.pdf";
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const body = (req.body ?? {}) as { pages?: unknown; name?: unknown };
  const wanted = readPages(body.pages);
  if (!wanted) return res.status(400).json({ error: "No kosh page was chosen." });
  if (wanted.length > MAX_PAGES) return res.status(413).json({ error: `Choose at most ${MAX_PAGES} pages at once.` });

  try {
    const built = await buildKoshPagesPdf(wanted);
    if (!built) return res.status(404).json({ error: "The kosh PDF for these pages could not be opened." });

    const name = String(body.name ?? "").replace(/[\\/:*?"<>|\n\r]+/g, " ").trim().slice(0, 120) || "kosh pages";
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", contentDisposition(`${name}.pdf`));
    res.setHeader("Content-Length", String(built.bytes.length));
    res.setHeader("X-Kosh-Pages", String(built.pages));
    return res.status(200).send(Buffer.from(built.bytes));
  } catch (error) {
    console.error("kosh pages failed", error);
    return res.status(502).json({ error: "The kosh pages could not be fetched just now. Try again." });
  }
}

export default protectApi(handler, PERMISSIONS.pdfBuild);
