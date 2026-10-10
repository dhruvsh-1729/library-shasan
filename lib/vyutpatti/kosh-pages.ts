import { PDFDocument } from "pdf-lib";
import { describeGranth, getGranthCatalog } from "@/lib/granth-catalog";
import { sourcePagesPdf } from "@/lib/pdf-highlight-builder";
import type { VyutpattiResult } from "@/lib/vyutpatti/pipeline";

// The kosh pages a vyutpatti was read from, as they are printed: the scanned
// pages themselves in one PDF, in the order asked. Only those pages' bytes are
// fetched from each kosh PDF (HTTP ranges); the PDF's address comes from the
// catalog.

export type KoshPageRef = { granthKey: string; page: number };

/** The kosh pages a result was read from, each page once, in the order the words came (as on /vyutpatti). */
export function koshPagesOfResult(result: Pick<VyutpattiResult, "words">): KoshPageRef[] {
  const seen = new Set<string>();
  const out: KoshPageRef[] = [];
  for (const w of result.words)
    for (const e of w.entries) {
      const id = `${e.granthKey}:${e.pdfPage}`;
      if (seen.has(id)) continue;
      seen.add(id);
      out.push({ granthKey: e.granthKey, page: e.pdfPage });
    }
  return out;
}

/** The pages as one PDF; null when none of them could be opened. */
export async function buildKoshPagesPdf(wanted: KoshPageRef[]) {
  const catalog = await getGranthCatalog();
  const byGranth = new Map<string, number[]>();
  for (const w of wanted) byGranth.set(w.granthKey, [...(byGranth.get(w.granthKey) ?? []), w.page]);

  // Each kosh is read once, all its wanted pages together.
  const loaded = new Map<string, { doc: PDFDocument; index: Map<number, number> }>();
  await Promise.all(
    [...byGranth].map(async ([granthKey, pages]) => {
      const pdfUrl = describeGranth(catalog, granthKey)?.pdfUrl;
      if (!pdfUrl) return;
      const subset = await sourcePagesPdf(pdfUrl, pages);
      const doc = await PDFDocument.load(subset.bytes, { ignoreEncryption: true, updateMetadata: false });
      loaded.set(granthKey, { doc, index: new Map(subset.pages.map((page, i) => [page, i])) });
    })
  );

  const out = await PDFDocument.create();
  out.setTitle("Kosh pages");
  out.setProducer("Granth library vyutpatti");
  let added = 0;
  for (const w of wanted) {
    const source = loaded.get(w.granthKey);
    const at = source?.index.get(w.page);
    if (!source || at == null) continue;
    const [copy] = await out.copyPages(source.doc, [at]);
    out.addPage(copy);
    added += 1;
  }
  if (!added) return null;
  return { bytes: await out.save(), pages: added };
}
