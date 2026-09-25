import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { isRangeLoadableHost } from "@/lib/pdf-range-source";
import { getSupabaseAdmin } from "@/lib/supabase-server";

/**
 * Byte size of an uploaded PDF, so the viewer can fetch only the parts of it a
 * page needs. UploadThing serves byte ranges but does not expose Content-Range /
 * Accept-Ranges to browser scripts, so the size is read here, server side.
 * Only UploadThing URLs are looked up; anything else is refused.
 *
 * The size recorded at upload time is used when there is one (a database read
 * is far quicker than UploadThing's HEAD, which takes 1-2 s); the viewer checks
 * the file really ends where this says, and falls back to a full download if not.
 */

const cache = new Map<string, { size: number; at: number }>();
const CACHE_MS = 6 * 60 * 60 * 1000;

async function recordedSize(url: string): Promise<number | null> {
  const supabase = getSupabaseAdmin();
  const [files, docs] = await Promise.all([
    supabase.from("granth_ocr_files").select("file_size").eq("ufs_url", url).limit(1),
    supabase.from("documents").select("size_bytes").eq("pdf_url", url).limit(1),
  ]);
  const size = Number(files.data?.[0]?.file_size ?? docs.data?.[0]?.size_bytes);
  return Number.isFinite(size) && size > 0 ? size : null;
}

async function headSize(url: string): Promise<number | null> {
  const signal = AbortSignal.timeout(8000);
  const head = await fetch(url, { method: "HEAD", signal });
  const length = Number(head.headers.get("content-length"));
  if (head.ok && Number.isFinite(length) && length > 0) return length;
  // Some edges omit the length on HEAD; a one-byte range carries the total.
  const probe = await fetch(url, { headers: { Range: "bytes=0-0" }, signal });
  const total = Number((probe.headers.get("content-range") ?? "").split("/")[1]);
  await probe.body?.cancel();
  return probe.status === 206 && Number.isFinite(total) && total > 0 ? total : null;
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const raw = String(Array.isArray(req.query.url) ? req.query.url[0] : req.query.url ?? "").trim();
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return res.status(400).json({ error: "Invalid url" });
  }
  if (url.protocol !== "https:" || !isRangeLoadableHost(url.hostname)) {
    return res.status(400).json({ error: "Only uploaded library PDFs can be looked up" });
  }

  const key = url.toString();
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < CACHE_MS) {
    res.setHeader("Cache-Control", "private, max-age=3600");
    return res.status(200).json({ size: hit.size });
  }
  try {
    const size = (await recordedSize(key).catch(() => null)) ?? (await headSize(key));
    if (!size) return res.status(404).json({ error: "Size not available" });
    cache.set(key, { size, at: Date.now() });
    if (cache.size > 5000) cache.delete(cache.keys().next().value as string);
    res.setHeader("Cache-Control", "private, max-age=3600");
    return res.status(200).json({ size });
  } catch (error) {
    return res.status(502).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
