import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import type { EntrySource, ReaderLine, TableRow } from "@/lib/vyutpatti/format";
import { type VyutpattiPdfSection, buildVyutpattiPdf } from "@/lib/vyutpatti/pdf";

// The vyutpatti PDF from what the page shows (after the reader's edits): the
// internal tables, then the reader pages. Built from the request alone;
// nothing is stored.

export const config = { api: { bodyParser: { sizeLimit: "2mb" }, responseLimit: false } };

const MAX_SECTIONS = 60;
const MAX_ITEMS = 120;
const MAX_TEXT = 4000;
const SOURCES: EntrySource[] = ["kosh", "ai", "vigraha"];

const text = (value: unknown, max = MAX_TEXT) => String(value ?? "").normalize("NFC").slice(0, max);
const source = (value: unknown): EntrySource => (SOURCES.includes(value as EntrySource) ? (value as EntrySource) : "kosh");

function readSections(raw: unknown): VyutpattiPdfSection[] | null {
  if (!Array.isArray(raw) || !raw.length || raw.length > MAX_SECTIONS) return null;
  return raw.map((s) => {
    const section = (s ?? {}) as Record<string, unknown>;
    const rows = (Array.isArray(section.rows) ? section.rows : []).slice(0, MAX_ITEMS).map((r): TableRow => {
      const row = (r ?? {}) as Record<string, unknown>;
      return { granth: text(row.granth), shastraPath: text(row.shastraPath), pubRem: text(row.pubRem), inRem: text(row.inRem), source: source(row.source) };
    });
    const lines = (Array.isArray(section.lines) ? section.lines : []).slice(0, MAX_ITEMS).map((l): ReaderLine => {
      const line = (l ?? {}) as Record<string, unknown>;
      return { head: text(line.head, 200), body: text(line.body), source: source(line.source) };
    });
    return { number: text(section.number, 24), vishay: text(section.vishay, 200), box: text(section.box, 12), rows, lines };
  });
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const sections = readSections((req.body ?? {}).sections);
  if (!sections) return res.status(400).json({ error: "Nothing to put in the PDF." });
  const bytes = await buildVyutpattiPdf(sections);
  const name = sections.length === 1 ? `${sections[0].number ? `${sections[0].number} ` : ""}${sections[0].vishay} vyutpatti.pdf` : "vyutpatti.pdf";
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="vyutpatti.pdf"; filename*=UTF-8''${encodeURIComponent(name)}`);
  res.setHeader("Content-Length", String(bytes.length));
  res.status(200).send(Buffer.from(bytes));
}

export default protectApi(handler, PERMISSIONS.pdfBuild);
