// Browser helpers shared by the two Anuvaad flows (box files, any PDF pages).

import type { PDFDocument } from "pdf-lib";

export function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

/** Reads an NDJSON response event by event (a JSON error reply counts as one "error" event). */
export async function readStream(res: Response, onEvent: (event: Record<string, unknown>) => void) {
  if (!(res.headers.get("content-type") ?? "").includes("ndjson") || !res.body) {
    const json = await res.json().catch(() => ({}));
    onEvent({ type: "error", error: json.error || `Request failed (${res.status}).` });
    return;
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (!line.trim()) continue;
      try {
        onEvent(JSON.parse(line));
      } catch {
        // a torn line; the next ones still count
      }
    }
  }
}

export function fileType(file: File) {
  if (file.type) return file.type;
  const ext = file.name.toLowerCase().split(".").pop();
  return ext === "pdf" ? "application/pdf" : ext === "png" ? "image/png" : ext === "jpg" || ext === "jpeg" ? "image/jpeg" : "";
}

/** A photo on an A4 page (turned if it is wider than tall), fitted with a small margin. */
export async function addImagePage(out: PDFDocument, bytes: ArrayBuffer, type: string) {
  const image = type === "image/png" ? await out.embedPng(bytes) : await out.embedJpg(bytes);
  const landscape = image.width > image.height;
  const [w, h] = landscape ? [841.89, 595.28] : [595.28, 841.89];
  const scale = Math.min((w - 24) / image.width, (h - 24) / image.height);
  const page = out.addPage([w, h]);
  page.drawImage(image, { x: (w - image.width * scale) / 2, y: (h - image.height * scale) / 2, width: image.width * scale, height: image.height * scale });
}
