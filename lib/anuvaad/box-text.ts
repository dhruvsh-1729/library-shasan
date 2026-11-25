// The box text alone — Sr. no., Granth, ShastraPath — for Maharaj Saheb to
// give to an AI herself (25 Nov 2026: "from the box file we give you, the
// serial number, granth name and shastra path extracted separately into a PDF,
// with no header, footer or column names — or an open Excel or Word file").
// No browser or server code here: the page and the PDF route both use it.

import type { BoxRow } from "@/lib/anuvaad/box";

export type BoxTextRow = string[] | { band: string };

/** Each vishay's band (when asked for), then its rows as [Sr., Granth, ShastraPath]. */
export function boxTextRows(rows: Array<Pick<BoxRow, "sr" | "granth" | "shastraPath" | "vishay">>, bands = true): BoxTextRow[] {
  const out: BoxTextRow[] = [];
  let last: string | undefined;
  for (const r of rows) {
    if (bands && r.vishay && r.vishay !== last) out.push({ band: r.vishay });
    last = r.vishay;
    out.push([r.sr, r.granth.replace(/\s+/g, " ").trim(), r.shastraPath.replace(/\s+/g, " ").trim()]);
  }
  return out;
}

/** The share of the page each column takes: a number, a name, the passage. */
export const BOX_TEXT_WIDTHS = [6, 24, 70];

export function boxTextFileName(rows: Array<Pick<BoxRow, "vishay">>, ext: string) {
  const vishays = [...new Set(rows.map((r) => r.vishay).filter(Boolean))].map((v) => String(v).split(/\s/)[0]);
  const head = vishays.length > 2 ? `${vishays[0]} - ${vishays[vishays.length - 1]}` : vishays.join(", ");
  return `${head ? `${head} ` : ""}box text.${ext}`;
}
