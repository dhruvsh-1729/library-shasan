// PDF export pieces: range-fetched page subsets and word rings, on generated PDFs.
import assert from "node:assert/strict";
import { test } from "node:test";
import { PDFDocument, StandardFonts } from "pdf-lib";
import * as pdfjs from "pdfjs-dist/legacy/build/pdf.mjs";
import { findGlyphWordBoxes, type GlyphPage } from "@/lib/pdf-glyph-boxes";
import { extractPdfPagesByRange } from "@/lib/pdf-range-subset.mjs";
import { readingOrders } from "@/lib/pdf-word-boxes";

async function samplePdf(useObjectStreams: boolean) {
  const doc = await PDFDocument.create();
  const font = await doc.embedFont(StandardFonts.Helvetica);
  for (let i = 1; i <= 6; i += 1) {
    const page = doc.addPage([400 + i, 600]);
    page.drawText(`page ${i} hello world`, { x: 50, y: 500, size: 20, font });
  }
  return doc.save({ useObjectStreams });
}

/** A fetch that serves byte ranges of an in-memory file, like the PDF host does. */
function rangeFetch(bytes: Uint8Array): typeof fetch {
  return (async (_url: unknown, init?: { headers?: Record<string, string> }) => {
    const range = init?.headers?.Range ?? "";
    let start = 0;
    let end = bytes.length - 1;
    const suffix = range.match(/bytes=-(\d+)/);
    const span = range.match(/bytes=(\d+)-(\d+)/);
    if (suffix) start = Math.max(0, bytes.length - Number(suffix[1]));
    else if (span) {
      start = Number(span[1]);
      end = Math.min(bytes.length - 1, Number(span[2]));
    }
    const body = bytes.slice(start, end + 1);
    return new Response(body, { status: 206, headers: { "content-range": `bytes ${start}-${end}/${bytes.length}` } });
  }) as unknown as typeof fetch;
}

for (const useObjectStreams of [false, true]) {
  test(`range subset keeps the asked pages in order (object streams: ${useObjectStreams})`, async () => {
    const source = await samplePdf(useObjectStreams);
    const url = `memory://sample-${useObjectStreams}.pdf`;
    const subset = await extractPdfPagesByRange(url, [5, 1, 3], { fetchImpl: rangeFetch(source) });
    assert.deepEqual(subset.pages, [5, 1, 3]);
    const doc = await PDFDocument.load(subset.bytes);
    assert.deepEqual(doc.getPages().map((p) => p.getWidth()), [405, 401, 403]);
    const text = await pdfjs.getDocument({ data: subset.bytes.slice(), verbosity: 0 }).promise;
    const first = await (await text.getPage(1)).getTextContent();
    assert.match((first.items as Array<{ str: string }>).map((i) => i.str).join(""), /page 5 hello world/);
  });
}

test("rings go around the matched word, from the glyphs themselves", async () => {
  const source = await samplePdf(false);
  const doc = await pdfjs.getDocument({ data: source.slice(), verbosity: 0, fontExtraProperties: true }).promise;
  const page = await doc.getPage(2);
  const boxes = await findGlyphWordBoxes(page as unknown as GlyphPage, pdfjs.OPS, ["world"], "exact_word");
  assert.equal(boxes?.length, 1);
  const box = boxes![0];
  // "page 2 hello " in 20pt Helvetica starts "world" at about x = 50 + 118.
  assert.ok(box.x > 160 && box.x < 176, `x ${box.x}`);
  assert.ok(box.width > 45 && box.width < 60, `width ${box.width}`);
});

test("legacy-font glyph order is repaired without changing text length", () => {
  for (const [raw, fixed] of [
    ["િમથ્યાત્વથી", "મિથ્યાત્વથી"],
    ["પર્કારના", "પ્રકારના"],
    ["सवर्गुण", "सर्वगुण"],
    ["(स्तर्ी", "(स्त्री"],
  ]) {
    const readings = readingOrders(raw);
    assert.ok(readings.includes(fixed), `${raw} -> ${fixed}`);
    for (const r of readings) assert.equal(r.length, raw.length);
  }
});
