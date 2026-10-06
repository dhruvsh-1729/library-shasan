// Client for the internal Kraken OCR service (ndms/kraken-ocr on Railway).
// The service renders every page, finds lines, reads them with our fine-tuned
// model and flags table/chart pages (needs_sarvam) that it cannot read well.
import { readFile } from "node:fs/promises";
import path from "node:path";

const URL_ = process.env.KRAKEN_OCR_URL;
const TOKEN = process.env.KRAKEN_OCR_TOKEN;

function headers() {
  if (!URL_ || !TOKEN) throw new Error("KRAKEN_OCR_URL and KRAKEN_OCR_TOKEN must be set");
  return { Authorization: `Bearer ${TOKEN}` };
}

async function getJson(p, tries = 6) {
  let last;
  for (let i = 1; i <= tries; i += 1) {
    try {
      const res = await fetch(`${URL_}${p}`, { headers: headers() });
      if (!res.ok) throw new Error(`${p} -> ${res.status}`);
      return await res.json();
    } catch (e) {
      last = e;
      await new Promise((r) => setTimeout(r, 5000 * i));
    }
  }
  throw last;
}

export async function submitPdf(pdfPath) {
  const body = new FormData();
  body.append("file", new Blob([await readFile(pdfPath)], { type: "application/pdf" }), path.basename(pdfPath));
  const res = await fetch(`${URL_}/jobs`, { method: "POST", headers: headers(), body });
  if (!res.ok) throw new Error(`kraken submit ${res.status}: ${await res.text()}`);
  return res.json(); // { id, pages }
}

/** Polls every 30 s (the app sleeps after ~10 min without traffic, so polling also keeps it awake). */
export async function waitForJob(id, onProgress) {
  for (;;) {
    const s = await getJson(`/jobs/${id}`);
    onProgress?.(s);
    if (s.status === "done") return getJson(`/jobs/${id}/result`);
    await new Promise((r) => setTimeout(r, 30000));
  }
}
