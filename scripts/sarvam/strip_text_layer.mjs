// Removes the existing (poor) OCR text layer from a scanned PDF by deleting
// every BT...ET text block from each page's content stream. Image XObjects are
// untouched, so there is no re-encoding and no quality or size penalty.
import { PDFDocument, PDFName, PDFRawStream, PDFArray, PDFDict, PDFStream, decodePDFRawStream } from "pdf-lib";

function streamBytes(stream) {
  try {
    return decodePDFRawStream(stream).decode();
  } catch {
    return stream.getContents();
  }
}

/** Latin-1 round-trip keeps binary operands intact while we regex the operators. */
function toLatin1(bytes) {
  let s = "";
  const CH = 0x8000;
  for (let i = 0; i < bytes.length; i += CH) {
    s += String.fromCharCode.apply(null, bytes.subarray(i, Math.min(i + CH, bytes.length)));
  }
  return s;
}
function fromLatin1(str) {
  const out = new Uint8Array(str.length);
  for (let i = 0; i < str.length; i += 1) out[i] = str.charCodeAt(i) & 0xff;
  return out;
}

export function stripTextOps(content) {
  // Non-greedy so each BT/ET pair is removed independently.
  return content.replace(/BT[\s\S]*?ET/g, "");
}

/** A page scan; typeset pages may carry small images (rules, logos: 299x22 in Hinsa Shtakama). */
const MIN_SCAN_PIXELS = 250_000;

/** True if the page draws a page-sized raster image (directly or inside a form XObject): a scan. */
function pageHasImage(page) {
  const seen = new Set();
  const visit = (resources, depth) => {
    const xobjects = resources?.lookupMaybe(PDFName.of("XObject"), PDFDict);
    if (!xobjects) return false;
    for (const key of xobjects.keys()) {
      const ref = xobjects.get(key);
      if (seen.has(ref)) continue;
      seen.add(ref);
      const target = xobjects.lookup(key);
      if (!(target instanceof PDFStream)) continue;
      const subtype = target.dict.get(PDFName.of("Subtype"));
      if (subtype === PDFName.of("Image")) {
        const size = (name) => Number(target.dict.lookup(PDFName.of(name))?.asNumber?.() ?? 0);
        if (size("Width") * size("Height") >= MIN_SCAN_PIXELS) return true;
        continue;
      }
      if (subtype === PDFName.of("Form") && depth < 3) {
        if (visit(target.dict.lookupMaybe(PDFName.of("Resources"), PDFDict), depth + 1)) return true;
      }
    }
    return false;
  };
  return visit(page.node.Resources(), 0);
}

export async function stripTextLayer(srcPath, outPath, { onPage } = {}) {
  const fs = await import("node:fs/promises");
  const doc = await PDFDocument.load(await fs.readFile(srcPath), { ignoreEncryption: true });
  const pages = doc.getPages();
  let touched = 0;
  let removedBlocks = 0;

  // A page with text but no scanned image is typeset (PageMaker etc.): its
  // text IS the print, and deleting it leaves a blank page. That is how the
  // 2026-10 rebuild blanked Hinsa Shtakama, the Gujarati Acharang/Suygadang
  // anuwads and Trishashti Parv 10, so refuse instead of stripping.
  const typeset = [];
  for (let i = 0; i < pages.length; i += 1) if (!pageHasImage(pages[i])) typeset.push(i + 1);
  if (typeset.length) {
    throw new Error(
      `${typeset.length} of ${pages.length} pages have no scanned image (e.g. page ${typeset.slice(0, 5).join(", ")}); ` +
        "their text is the printed text, so the text layer is not stripped"
    );
  }

  for (let i = 0; i < pages.length; i += 1) {
    const page = pages[i];
    const contentsRef = page.node.get(PDFName.of("Contents"));
    const contents = page.node.context.lookup(contentsRef);

    const streams = contents instanceof PDFArray
      ? contents.asArray().map((r) => page.node.context.lookup(r))
      : [contents];

    let joined = "";
    for (const s of streams) {
      if (s instanceof PDFRawStream) joined += toLatin1(streamBytes(s));
    }
    if (!joined) continue;

    const before = (joined.match(/BT[\s\S]*?ET/g) || []).length;
    if (!before) continue;

    const stripped = stripTextOps(joined);
    removedBlocks += before;

    // flateStream() compresses internally, so it must receive raw bytes.
    const newStream = page.node.context.flateStream(fromLatin1(stripped));
    page.node.set(PDFName.of("Contents"), page.node.context.register(newStream));
    touched += 1;
    if (onPage) onPage(i + 1, before);
  }

  const bytes = await doc.save({ useObjectStreams: true });
  await fs.writeFile(outPath, bytes);
  return { pages: pages.length, touched, removedBlocks, bytes: bytes.length };
}
