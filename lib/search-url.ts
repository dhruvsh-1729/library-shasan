import { type OCRSearchMode, parseOCRSearchMode } from "@/lib/ocr-search";

// The /search page keeps everything a search depends on in its URL, so a link,
// a refresh or the Back button always shows the same search:
//   /search?q=સામાયિક&langs=typed,devanagari&match=contains&in=215,296&page=2
// "in" lists granths by their short keys (see lib/granth-short-keys); no "in"
// means all granths. Older links (?customId=<document id>, ?matchMode=) still work.

/** Search everywhere, or only in the granths ticked in the picker. */
export type SearchScope = "all" | "selected";

export type SearchRequest = {
  q: string;
  /** Language variants to include; null means all generated variants. */
  langs: string[] | null;
  matchMode: OCRSearchMode;
  scope: SearchScope;
  granthIds: string[];
  page: number;
};

export type ParsedSearchUrl = {
  q: string;
  langs: string[] | null;
  matchMode: OCRSearchMode;
  page: number;
  /** Granths named in the link, as written (short keys or document ids). */
  granthValues: string[];
};

type QueryValue = string | string[] | undefined;

function first(value: QueryValue) {
  return (Array.isArray(value) ? value[0] : value) ?? "";
}

function splitList(value: string) {
  return value.split(",").map((part) => part.trim()).filter(Boolean);
}

export function parseSearchUrl(query: Record<string, QueryValue>): ParsedSearchUrl {
  const legacyCustomId = first(query.customId).trim();
  const langsRaw = first(query.langs).trim();
  const inRaw = first(query.in).trim();
  const page = Number.parseInt(first(query.page) || "1", 10);
  return {
    q: first(query.q).trim(),
    langs: langsRaw ? splitList(langsRaw) : null,
    // No match param means the default Sanskrit forms search.
    matchMode: parseOCRSearchMode(first(query.match) || first(query.matchMode) || "sanskrit_forms"),
    page: Number.isFinite(page) && page > 1 ? page : 1,
    granthValues: legacyCustomId ? [legacyCustomId] : inRaw ? splitList(inRaw) : [],
  };
}

/** Canonical /search URL for a request. */
export function buildSearchUrl(request: SearchRequest, keyById: ReadonlyMap<string, string>) {
  const params = new URLSearchParams();
  if (request.q) params.set("q", request.q);
  if (request.langs) params.set("langs", request.langs.join(","));
  if (request.matchMode !== "sanskrit_forms") params.set("match", request.matchMode);
  if (request.scope === "selected") {
    params.set("in", request.granthIds.map((id) => keyById.get(id) ?? id).join(","));
  }
  if (request.page > 1) params.set("page", String(request.page));
  const query = params.toString();
  return query ? `/search?${query}` : "/search";
}

/** Maps granths named in a link (short keys, or document ids from older links) to ids. */
export function resolveGranthValues(values: string[], options: ReadonlyArray<{ key: string; custom_id: string }>) {
  const byKey = new Map(options.map((row) => [row.key, row.custom_id]));
  const ids = new Set(options.map((row) => row.custom_id));
  const found: string[] = [];
  const missing: string[] = [];
  for (const value of values) {
    let id = byKey.get(value) ?? (ids.has(value) ? value : undefined);
    if (!id) {
      // A number whose key later gained a suffix ("429" -> "429_C000391").
      const prefixed = options.filter((row) => row.key.startsWith(`${value}_`));
      if (prefixed.length === 1) id = prefixed[0].custom_id;
    }
    if (id) found.push(id);
    else missing.push(value);
  }
  return { ids: Array.from(new Set(found)), missing };
}

export function sameIds(a: readonly string[], b: readonly string[]) {
  if (a.length !== b.length) return false;
  const set = new Set(a);
  return b.every((id) => set.has(id));
}
