// Granth Library service worker: keeps the app usable on slow or no internet.
//
//   /_next/static, fonts, icons   cache first (file names change when the content does)
//   pages (/, /ask, /vyutpatti, /library)
//                                 network first, the last copy when offline
//   read-only lookups (search, catalog, spellings, compound parts, page text)
//                                 network first, the last answer when offline
//   everything else (sign-in, POSTs, PDFs, admin)   never cached
//
// Bump VERSION to drop every cache on the next visit.

const VERSION = "v1";
const STATIC = `granth-static-${VERSION}`;
const PAGES = `granth-pages-${VERSION}`;
const DATA = `granth-data-${VERSION}`;
const OFFLINE = "/offline.html";
const PAGE_PATHS = new Set(["/", "/ask", "/vyutpatti", "/library"]);
const DATA_PATHS = [
  "/api/search",
  "/api/search-count",
  "/api/search-granths",
  "/api/granths",
  "/api/ocr-granths",
  "/api/ocr-granth-pages",
  "/api/query-forms",
  "/api/compound-parts",
];
const MAX_DATA_ENTRIES = 300;

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(STATIC).then((cache) => cache.addAll([OFFLINE, "/icon-192.png", "/icon-512.png", "/manifest.webmanifest"])));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((k) => !k.endsWith(`-${VERSION}`)).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Signing out clears what this device kept.
self.addEventListener("message", (event) => {
  if (event.data === "clear") event.waitUntil(caches.keys().then((keys) => Promise.all(keys.map((k) => caches.delete(k)))));
});

async function trim(cacheName, max) {
  const cache = await caches.open(cacheName);
  const keys = await cache.keys();
  for (const key of keys.slice(0, Math.max(0, keys.length - max))) await cache.delete(key);
}

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  const response = await fetch(request);
  if (response.ok) (await caches.open(STATIC)).put(request, response.clone());
  return response;
}

async function networkFirst(request, cacheName, fallback) {
  try {
    const response = await fetch(request);
    // A redirect (to sign-in) or an error is not worth keeping.
    if (response.ok && !response.redirected && response.type === "basic") {
      const cache = await caches.open(cacheName);
      await cache.put(request, response.clone());
      if (cacheName === DATA) trim(DATA, MAX_DATA_ENTRIES);
    }
    return response;
  } catch (error) {
    const cached = await caches.match(request, { ignoreVary: true });
    if (cached) return cached;
    if (fallback) {
      const offline = await caches.match(fallback);
      if (offline) return offline;
    }
    throw error;
  }
}

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  const path = url.pathname;

  if (path.startsWith("/_next/static/") || /\.(?:woff2?|ttf|png|svg|ico|webmanifest)$/.test(path)) {
    event.respondWith(cacheFirst(request));
    return;
  }
  if (request.mode === "navigate") {
    if (PAGE_PATHS.has(path)) event.respondWith(networkFirst(request, PAGES, OFFLINE));
    else event.respondWith(fetch(request).catch(() => caches.match(OFFLINE)));
    return;
  }
  if (DATA_PATHS.includes(path)) {
    event.respondWith(networkFirst(request, DATA));
  }
});
