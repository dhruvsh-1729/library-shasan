import type { NextApiRequest, NextApiResponse } from "next";

type CacheEntry<T> = {
  expiresAt: number;
  value: T;
};

type CacheStatus = "HIT" | "MISS";

type CacheHeadersOptions = {
  maxAgeSeconds: number;
  staleWhileRevalidateSeconds?: number;
};

declare global {
  // eslint-disable-next-line no-var
  var __libraryApiMemoryCache: Map<string, CacheEntry<unknown>> | undefined;
}

const MAX_CACHE_ENTRIES = 250;

function getStore() {
  if (!globalThis.__libraryApiMemoryCache) {
    globalThis.__libraryApiMemoryCache = new Map<string, CacheEntry<unknown>>();
  }
  return globalThis.__libraryApiMemoryCache;
}

function pruneStore(store: Map<string, CacheEntry<unknown>>, now: number) {
  for (const [key, entry] of store.entries()) {
    if (entry.expiresAt <= now) store.delete(key);
  }

  while (store.size >= MAX_CACHE_ENTRIES) {
    const oldestKey = store.keys().next().value as string | undefined;
    if (!oldestKey) return;
    store.delete(oldestKey);
  }
}

export async function getCachedJson<T>(
  key: string,
  ttlSeconds: number,
  loader: () => Promise<T>
): Promise<{ value: T; status: CacheStatus }> {
  const ttlMs = Math.max(0, ttlSeconds) * 1000;
  if (ttlMs === 0) return { value: await loader(), status: "MISS" };

  const now = Date.now();
  const store = getStore();
  const existing = store.get(key) as CacheEntry<T> | undefined;
  if (existing && existing.expiresAt > now) {
    return { value: existing.value, status: "HIT" };
  }

  const value = await loader();
  pruneStore(store, now);
  store.set(key, { value, expiresAt: now + ttlMs });
  return { value, status: "MISS" };
}

export function buildCacheKey(req: NextApiRequest, prefix: string) {
  const query = Object.keys(req.query)
    .sort()
    .map((key) => {
      const value = req.query[key];
      const normalized = Array.isArray(value) ? value.join(",") : String(value ?? "");
      return `${key}=${normalized}`;
    })
    .join("&");

  return `${prefix}:${query}`;
}

export function setPublicCacheHeaders(
  res: NextApiResponse,
  options: CacheHeadersOptions,
  status?: CacheStatus
) {
  const stale = Math.max(0, options.staleWhileRevalidateSeconds ?? options.maxAgeSeconds * 4);
  res.setHeader(
    "Cache-Control",
    `public, s-maxage=${Math.max(0, options.maxAgeSeconds)}, stale-while-revalidate=${stale}`
  );
  if (status) res.setHeader("X-Library-Api-Cache", status);
}

export function setNoStore(res: NextApiResponse) {
  res.setHeader("Cache-Control", "no-store");
}
