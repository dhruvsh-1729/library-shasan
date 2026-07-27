export const DEFAULT_CONTEXT_PAGE_RADIUS = 5;
export const MAX_CONTEXT_PAGE_RADIUS = 50;

export function normalizeContextPageRadius(value: unknown, fallback = DEFAULT_CONTEXT_PAGE_RADIUS) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  const base = Number.isFinite(parsed) ? parsed : fallback;
  return Math.max(0, Math.min(MAX_CONTEXT_PAGE_RADIUS, Math.floor(base)));
}

export function expandPagesWithContext(pages: number[], contextPages: unknown, maxPage?: number | null) {
  const radius = normalizeContextPageRadius(contextPages);
  const pageLimit = maxPage && Number.isFinite(maxPage) ? Math.max(1, Math.floor(maxPage)) : null;
  const out = new Set<number>();

  for (const rawPage of pages) {
    const page = Math.floor(Number(rawPage));
    if (!Number.isFinite(page) || page <= 0) continue;

    const start = Math.max(1, page - radius);
    const end = pageLimit == null ? page + radius : Math.min(pageLimit, page + radius);
    for (let next = start; next <= end; next += 1) out.add(next);
  }

  return [...out].sort((a, b) => a - b);
}
