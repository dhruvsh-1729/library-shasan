// Opens a PDF by fetching only the byte ranges pdf.js asks for, instead of the
// whole file.
//
// UploadThing answers Range requests (206) but does not expose Accept-Ranges /
// Content-Range to browser scripts, so pdf.js's own loader concludes ranges are
// unsupported and downloads the full file (30-50 MB) before showing any page.
// Here the size comes from /api/pdf-size and each range is fetched directly from
// UploadThing. Measured on a 34.7 MB granth: page 150 went from ~33 MB / ~10 s to
// ~2 MB / ~3 s. Anything unexpected (size lookup fails, a range comes back as a
// full 200 response, a range keeps failing while opening) falls back to the
// plain full download, so no PDF opens worse than before.

type PdfJs = typeof import("pdfjs-dist/legacy/build/pdf.mjs");
type LoadingTask = import("pdfjs-dist").PDFDocumentLoadingTask;
type DocumentInit = Parameters<PdfJs["getDocument"]>[0] & object;

/** Chunk pdf.js reads in. UploadThing takes 0.3-0.9 s per request, so fewer, larger reads win. */
export const RANGE_CHUNK_SIZE = 512 * 1024;
/**
 * A PDF's cross-reference table and object index sit at its end, and pdf.js reads
 * them before any page; fetching this much of the tail up front, alongside the
 * head, saves several sequential round trips on open. Measured on library
 * granths, pdf.js read the last 2.7-3.4 MB before the first page.
 */
const TAIL_PREFETCH = 6 * RANGE_CHUNK_SIZE;
const RANGE_RETRIES = 2;

/** Hosts known to serve byte ranges for our uploads. */
export function isRangeLoadableHost(hostname: string) {
  const host = hostname.toLowerCase();
  return host.endsWith(".ufs.sh") || host === "utfs.io" || host.endsWith(".utfs.io");
}

const sizeCache = new Map<string, number>();

async function lookupSize(url: string): Promise<number | null> {
  const cached = sizeCache.get(url);
  if (cached) return cached;
  try {
    const res = await fetch(`/api/pdf-size?url=${encodeURIComponent(url)}`);
    if (!res.ok) return null;
    const size = Number(((await res.json()) as { size?: number }).size);
    if (!Number.isFinite(size) || size <= 0) return null;
    sizeCache.set(url, size);
    return size;
  } catch {
    return null;
  }
}

class RangeFetchError extends Error {}

const EOF_MARKER = [0x25, 0x25, 0x45, 0x4f, 0x46]; // "%%EOF"

/** True if "%%EOF" occurs in the last KB, as it does at the end of every PDF. */
function endsLikePdf(tail: Uint8Array) {
  const from = Math.max(0, tail.byteLength - 1024);
  for (let i = tail.byteLength - EOF_MARKER.length; i >= from; i -= 1) {
    if (EOF_MARKER.every((byte, k) => tail[i + k] === byte)) return true;
  }
  return false;
}

async function fetchRange(url: string, begin: number, end: number, signal: AbortSignal) {
  let lastError: unknown = null;
  for (let attempt = 0; attempt <= RANGE_RETRIES; attempt += 1) {
    try {
      const res = await fetch(url, { headers: { Range: `bytes=${begin}-${end - 1}` }, signal });
      if (res.status !== 206) {
        await res.body?.cancel();
        // 5xx is worth another try; anything else (a 200 is the whole file,
        // i.e. ranges are not honoured) will not change on retry.
        if (res.status >= 500) throw new Error(`range request returned ${res.status}`);
        throw new RangeFetchError(`range request returned ${res.status}`);
      }
      const bytes = new Uint8Array(await res.arrayBuffer());
      if (bytes.byteLength !== end - begin) throw new RangeFetchError(`range returned ${bytes.byteLength} of ${end - begin} bytes`);
      return bytes;
    } catch (error) {
      if (signal.aborted) throw error;
      lastError = error;
      if (error instanceof RangeFetchError) break;
      await new Promise((resolve) => setTimeout(resolve, 400 * (attempt + 1)));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

export type OpenedPdf = {
  task: LoadingTask;
  /** "range": parts fetched on demand; "full": whole-file download (fallback). */
  mode: "range" | "full";
};

/**
 * Starts loading `url` and resolves once the document has opened (await
 * `task.promise` for the document itself, as usual), or with null if
 * `isCancelled` turned true first, in which case nothing is left loading.
 * `onTask` receives each task as soon as it exists, so a caller can destroy it
 * if the viewer closes mid-load.
 */
export async function openPdf(
  pdfjs: PdfJs,
  url: string,
  options: Omit<DocumentInit, "url" | "range" | "length">,
  onTask: (task: LoadingTask) => void,
  isCancelled: () => boolean = () => false
): Promise<OpenedPdf | null> {
  const openFull = (): OpenedPdf => {
    const task = pdfjs.getDocument({ ...options, url, disableStream: true, disableAutoFetch: true, rangeChunkSize: 65536 });
    onTask(task);
    return { task, mode: "full" };
  };

  let parsed: URL | null = null;
  try {
    parsed = new URL(url);
  } catch {
    parsed = null;
  }
  if (!parsed || parsed.protocol !== "https:" || !isRangeLoadableHost(parsed.hostname)) return openFull();

  // The head goes out at once; the tail as soon as the size is known, so pdf.js
  // gets both in about one round trip instead of four. (A suffix range,
  // "bytes=-N", would not need the size, but it is not CORS-safelisted and
  // UploadThing's preflight does not allow it.)
  const controller = new AbortController();
  const headPromise = fetchRange(url, 0, RANGE_CHUNK_SIZE, controller.signal).catch(() => null);
  const length = await lookupSize(url);
  if (!length || isCancelled()) {
    controller.abort();
    return isCancelled() ? null : openFull();
  }
  if (length <= TAIL_PREFETCH + RANGE_CHUNK_SIZE) {
    // Small file: one request for all of it.
    controller.abort();
    const whole = await fetchRange(url, 0, length, new AbortController().signal).catch(() => null);
    if (isCancelled()) return null;
    if (!whole || !endsLikePdf(whole)) return openFull();
    const task = pdfjs.getDocument({ ...options, data: whole });
    onTask(task);
    return { task, mode: "range" };
  }
  // Start of the tail, on a chunk boundary so pdf.js's chunk requests fall
  // entirely inside or entirely outside it.
  const tailStart = Math.floor((length - TAIL_PREFETCH) / RANGE_CHUNK_SIZE) * RANGE_CHUNK_SIZE;
  const [head, tail] = await Promise.all([
    headPromise,
    fetchRange(url, tailStart, length, controller.signal).catch(() => null),
  ]);
  if (isCancelled()) {
    controller.abort();
    return null;
  }
  // The size may come from upload records; make sure the file really ends there
  // (a PDF ends with %%EOF) before trusting it for every later range.
  if (!head || !tail || !endsLikePdf(tail)) return openFull();
  const initialData = head;
  const cachedFrom = tailStart;
  const fromCache = (begin: number, end: number) => tail.subarray(begin - cachedFrom, end - cachedFrom);

  const transport = new pdfjs.PDFDataRangeTransport(length, initialData);
  let failure: ((error: unknown) => void) | null = null;
  const failed = new Promise<never>((_, reject) => {
    failure = reject;
  });
  // Handled below via race / catch; never an unhandled rejection.
  failed.catch(() => undefined);
  transport.requestDataRange = (begin: number, requestedEnd: number) => {
    const end = Math.min(requestedEnd, length);
    let bytes: Promise<Uint8Array>;
    if (begin >= cachedFrom) {
      bytes = Promise.resolve(fromCache(begin, end));
    } else if (end > cachedFrom) {
      // Straddles the prefetched tail: fetch only the part before it.
      bytes = fetchRange(url, begin, cachedFrom, controller.signal).then((front) => {
        const joined = new Uint8Array(end - begin);
        joined.set(front, 0);
        joined.set(fromCache(cachedFrom, end), front.byteLength);
        return joined;
      });
    } else {
      bytes = fetchRange(url, begin, end, controller.signal);
    }
    bytes.then((chunk) => transport.onDataRange(begin, chunk)).catch((error) => failure?.(error));
  };

  const task = pdfjs.getDocument({
    ...options,
    range: transport,
    length,
    rangeChunkSize: RANGE_CHUNK_SIZE,
    disableAutoFetch: true,
    disableStream: true,
  });
  onTask(task);
  // Stop fetching ranges once the document is thrown away.
  const destroy = task.destroy.bind(task);
  task.destroy = () => {
    controller.abort();
    return destroy();
  };

  try {
    await Promise.race([task.promise, failed]);
    // After opening, a range that still fails after retries surfaces as a
    // page error: the task is destroyed so pending page loads reject instead
    // of waiting forever.
    failed.catch(() => {
      void task.destroy();
    });
    return { task, mode: "range" };
  } catch {
    void task.destroy();
    return isCancelled() ? null : openFull();
  }
}
