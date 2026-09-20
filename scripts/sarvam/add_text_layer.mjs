// Rebuilds a scanned PDF with an invisible, positioned text layer from Sarvam's
// per-block OCR metadata, making the PDF itself searchable.
//
// Two details matter for correctness:
//  * fontkit shapes Indic text into VISUAL glyph order, so a naive text layer
//    extracts as "िहंसा" instead of "हिंसा" and Ctrl+F fails. Each block is
//    therefore wrapped in a marked-content span carrying /ActualText with the
//    correct logical string, which is what extractors read.
//  * @pdf-lib/fontkit is a Babel bundle that needs regenerator-runtime present
//    before it loads, or every embedFont call throws.
import "regenerator-runtime/runtime.js";
import { readFile, writeFile } from "node:fs/promises";
import { PDFDocument, PDFName, PDFHexString, PDFOperator, PDFDict } from "pdf-lib";
import fontkit from "@pdf-lib/fontkit";

const FONTS = {
  gujarati: "/usr/share/fonts/truetype/fonts-gujr-extra/Rekha.ttf",
  devanagari: "/usr/share/fonts/truetype/Sarai/Sarai.ttf",
  latin: "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
};

function scriptOf(ch) {
  const c = ch.codePointAt(0);
  if (c >= 0x0a80 && c <= 0x0aff) return "gujarati";
  if (c >= 0x0900 && c <= 0x097f) return "devanagari";
  return "latin";
}

export function scriptRuns(text) {
  const runs = [];
  let cur = null;
  for (const ch of text) {
    const s = /\s/.test(ch) ? cur?.script ?? "latin" : scriptOf(ch);
    if (cur && cur.script === s) cur.text += ch;
    else {
      cur = { script: s, text: ch };
      runs.push(cur);
    }
  }
  return runs;
}

function utf16beHex(s) {
  let hex = "FEFF";
  for (const ch of s) {
    const cp = ch.codePointAt(0);
    if (cp > 0xffff) {
      const v = cp - 0x10000;
      hex += (0xd800 | (v >> 10)).toString(16).padStart(4, "0");
      hex += (0xdc00 | (v & 0x3ff)).toString(16).padStart(4, "0");
    } else {
      hex += cp.toString(16).padStart(4, "0");
    }
  }
  return hex.toUpperCase();
}

/** Replaces characters the font has no glyph for, so encodeText never throws. */
function sanitise(font, text) {
  let out = "";
  for (const ch of text) {
    if (/\s/.test(ch)) { out += " "; continue; }
    try {
      font.widthOfTextAtSize(ch, 10);
      out += ch;
    } catch {
      out += " ";
    }
  }
  return out;
}

export async function addTextLayer({ srcPdf, metaByPage, outPdf, onPage }) {
  const doc = await PDFDocument.load(await readFile(srcPdf), { ignoreEncryption: true });
  doc.registerFontkit(fontkit);

  const fonts = {};
  for (const [name, path] of Object.entries(FONTS)) {
    fonts[name] = await doc.embedFont(await readFile(path), { subset: true });
  }

  const pages = doc.getPages();
  let blocksDrawn = 0;
  let blocksSkipped = 0;

  for (let i = 0; i < pages.length; i += 1) {
    const page = pages[i];
    const meta = metaByPage.get(i + 1);
    if (!meta?.blocks?.length) continue;

    const { width: pw, height: ph } = page.getSize();
    const sx = pw / meta.image_width;
    const sy = ph / meta.image_height;

    const blocks = [...meta.blocks].sort(
      (a, b) => (a.reading_order ?? 0) - (b.reading_order ?? 0)
    );

    for (const block of blocks) {
      // Blocks tagged "image" hold a generated description of a decorative
      // picture ("this image shows a traditional lamp"), not text on the page.
      // Sarvam's own HTML omits them, and so does the CSV, so the searchable
      // layer omits them too rather than making captions findable.
      if (block.layout_tag === "image") { blocksSkipped += 1; continue; }

      const logical = String(block.text ?? "").replace(/\s+/g, " ").trim();
      const c = block.coordinates;
      if (!logical || !c) { blocksSkipped += 1; continue; }

      const boxH = Math.max(1, (c.y2 - c.y1) * sy);
      const x0 = c.x1 * sx;
      const yTop = ph - c.y1 * sy; // Sarvam's origin is top-left, PDF's is bottom-left
      const size = Math.max(4, Math.min(boxH * 0.8, 14));

      // /ActualText overrides what extractors read for this whole span.
      const dict = PDFDict.fromMapWithContext(
        new Map([[PDFName.of("ActualText"), PDFHexString.of(utf16beHex(logical))]]),
        doc.context
      );
      page.pushOperators(PDFOperator.of("BDC", [PDFName.of("Span"), dict]));

      let x = x0;
      for (const run of scriptRuns(logical)) {
        const font = fonts[run.script];
        const safe = sanitise(font, run.text);
        if (!safe.trim()) continue;
        page.drawText(safe, { x, y: yTop - size, size, font, opacity: 0 });
        x += font.widthOfTextAtSize(safe, size);
      }

      page.pushOperators(PDFOperator.of("EMC", []));
      blocksDrawn += 1;
    }
    if (onPage) onPage(i + 1, blocks.length);
  }

  const bytes = await doc.save({ useObjectStreams: true });
  await writeFile(outPdf, bytes);
  return { pages: pages.length, blocksDrawn, blocksSkipped, bytes: bytes.length };
}
