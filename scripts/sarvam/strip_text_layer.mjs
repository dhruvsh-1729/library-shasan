// Removes the existing (poor) OCR text layer from a scanned PDF by deleting
// every BT...ET text block from each page's content stream. Image XObjects are
// untouched, so there is no re-encoding and no quality or size penalty.
import { PDFDocument, PDFName, PDFRawStream, PDFArray, decodePDFRawStream } from "pdf-lib";

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

export async function stripTextLayer(srcPath, outPath, { onPage } = {}) {
  const fs = await import("node:fs/promises");
  const doc = await PDFDocument.load(await fs.readFile(srcPath), { ignoreEncryption: true });
  const pages = doc.getPages();
  let touched = 0;
  let removedBlocks = 0;

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
