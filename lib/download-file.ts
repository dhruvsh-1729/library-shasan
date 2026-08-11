export function fileSafe(value: string) {
  return String(value || "download")
    .replace(/\.pdf$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 110) || "download";
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  // Revoking straight away can cancel the download in some browsers.
  window.setTimeout(() => URL.revokeObjectURL(url), 2000);
}

/** Prefers the file name the API already chose over rebuilding it in the browser. */
export function filenameFromResponse(response: Response, fallback: string) {
  const header = response.headers.get("Content-Disposition") || "";
  const utf8Match = header.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match) {
    try {
      const decoded = decodeURIComponent(utf8Match[1].trim());
      if (decoded) return decoded;
    } catch {
      // Fall through to the plain filename.
    }
  }
  const plainMatch = header.match(/filename="([^"]+)"/i);
  if (plainMatch?.[1]) return plainMatch[1];
  return fallback;
}
