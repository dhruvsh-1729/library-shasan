import { type OCRSearchMode, type OCRSearchScripts, parseOCRSearchMode, parseOCRSearchScripts } from "@/lib/ocr-search";

// The search page (the home page, "/") keeps everything a search depends on in its URL, so a link,
// a refresh or the Back button always shows the same search:
//   /?q=hinsa&forms=हिंसा&scripts=devanagari&match=contains&in=215,296&page=2
// "forms" lists the Devanagari spellings chosen for a romanised query (the
// spellings /api/query-forms offered); "parts" lists the compound parts searched
// with the word (/api/compound-parts offers them; "none" searches the word alone,
// no "parts" searches the default choice); "scripts" keeps only hits written in
// those scripts; "in" lists granths by their short keys (lib/granth-short-keys),
// and no "in" means all granths. Older links (?customId=, ?matchMode=,
// ?langs=typed,devanagari) still work.

/** Search everywhere, or only in the granths ticked in the picker. */
export type SearchScope = "all" | "selected";

export type SearchRequest = {
  q: string;
  /** Spellings chosen for a romanised query; null means the page's default choice. */
  forms: string[] | null;
  /** Compound parts searched with the word; null means the page's default choice, [] none. */
  parts: string[] | null;
  /** Scripts whose hits count; null means both. */
  scripts: OCRSearchScripts;
  matchMode: OCRSearchMode;
  scope: SearchScope;
  granthIds: string[];
  page: number;
};

export type ParsedSearchUrl = Omit<SearchRequest, "scope" | "granthIds"> & {
  /** Granths named in the link, as written (short keys, granth keys or document ids). */
  granthValues: string[];
};

type QueryValue = string | string[] | undefined;

function first(value: QueryValue) {
  return (Array.isArray(value) ? value[0] : value) ?? "";
}

function splitList(value: string) {
  return value.split(",").map((part) => part.trim()).filter(Boolean);
}

const DEVANAGARI = /[ऀ-ॿ]/;
const GUJARATI = /[઀-૿]/;

/**
 * Old links stored language checkbox ids ("typed", "devanagari", "gujarati").
 * For an Indic query "typed" was its own script; for a romanised one the ids
 * were the two transliterations. Either way they name scripts.
 */
function scriptsFromLegacyLangs(langs: string[], q: string): OCRSearchScripts {
  const scripts: string[] = [];
  if (langs.includes("devanagari") || (langs.includes("typed") && DEVANAGARI.test(q))) scripts.push("devanagari");
  if (langs.includes("gujarati") || (langs.includes("typed") && GUJARATI.test(q))) scripts.push("gujarati");
  return parseOCRSearchScripts(scripts);
}

export function parseSearchUrl(query: Record<string, QueryValue>): ParsedSearchUrl {
  const q = first(query.q).trim();
  const legacyCustomId = first(query.customId).trim();
  const formsRaw = first(query.forms).trim();
  const partsRaw = first(query.parts).trim();
  const scriptsRaw = first(query.scripts).trim();
  const langsRaw = first(query.langs).trim();
  const inRaw = first(query.in).trim();
  const page = Number.parseInt(first(query.page) || "1", 10);
  return {
    q,
    forms: formsRaw ? splitList(formsRaw) : null,
    parts: partsRaw === "none" ? [] : partsRaw ? splitList(partsRaw) : null,
    scripts: scriptsRaw ? parseOCRSearchScripts(scriptsRaw) : langsRaw ? scriptsFromLegacyLangs(splitList(langsRaw), q) : null,
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
  if (request.forms?.length) params.set("forms", request.forms.join(","));
  if (request.parts) params.set("parts", request.parts.length ? request.parts.join(",") : "none");
  if (request.scripts) params.set("scripts", request.scripts.join(","));
  if (request.matchMode !== "sanskrit_forms") params.set("match", request.matchMode);
  if (request.scope === "selected") {
    params.set("in", request.granthIds.map((id) => keyById.get(id) ?? id).join(","));
  }
  if (request.page > 1) params.set("page", String(request.page));
  const query = params.toString();
  return query ? `/?${query}` : "/";
}

/**
 * Maps granths named in a link to picker ids: short keys, document ids, and
 * the aliases /api/search-granths returns (granth keys, text-only ids, and
 * duplicates, which lead to the granth searched in their place).
 */
export function resolveGranthValues(
  values: string[],
  options: ReadonlyArray<{ key: string; custom_id: string }>,
  aliases: Readonly<Record<string, string>> = {}
) {
  const byKey = new Map(options.map((row) => [row.key, row.custom_id]));
  const ids = new Set(options.map((row) => row.custom_id));
  const found: string[] = [];
  const missing: string[] = [];
  for (const value of values) {
    let id = byKey.get(value) ?? (ids.has(value) ? value : undefined) ?? aliases[value];
    if (!id) {
      // A number whose key later gained a suffix ("429" -> "429_C000391").
      const prefixed = options.filter((row) => row.key.startsWith(`${value}_`));
      if (prefixed.length === 1) id = prefixed[0].custom_id;
    }
    if (id && ids.has(id)) found.push(id);
    else missing.push(value);
  }
  return { ids: Array.from(new Set(found)), missing };
}

export function sameIds(a: readonly string[], b: readonly string[]) {
  if (a.length !== b.length) return false;
  const set = new Set(a);
  return b.every((id) => set.has(id));
}
