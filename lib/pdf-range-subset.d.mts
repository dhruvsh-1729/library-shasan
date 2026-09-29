export class RangeSubsetError extends Error {}
export function extractPdfPagesByRange(
  url: string,
  pageNumbers: number[],
  options?: { fetchImpl?: typeof fetch }
): Promise<{ bytes: Uint8Array; pages: number[]; bytesFetched: number; requests: number; sourceSize: number }>;
