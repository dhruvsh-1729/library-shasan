import {
  foldSanskrit,
  foldSanskritText,
  gujaratiCaseForms,
  isGrammarLabelAt,
  sanskritQueryForms,
} from "./sanskrit-fold.mjs";

export type OCRSearchMode = "sanskrit_forms" | "exact_word" | "contains" | "begins_with" | "ends_with";

export const OCR_SEARCH_MODE_OPTIONS: Array<{
  mode: OCRSearchMode;
  label: string;
  description: string;
}> = [
  {
    mode: "sanskrit_forms",
    label: "Sanskrit forms",
    description: "The word and its Sanskrit declensions (देवः, देवम्, पर्षद्…), skipping grammar labels like स्त्री.",
  },
  {
    mode: "exact_word",
    label: "Exact word",
    description: "Matches complete words only.",
  },
  {
    mode: "begins_with",
    label: "Begins with",
    description: "Matches words that start with the query.",
  },
  {
    mode: "ends_with",
    label: "Ends with",
    description: "Matches words that end with the query.",
  },
  {
    mode: "contains",
    label: "Contains",
    description: "Matches the query anywhere in page text.",
  },
];

export type SearchMatch = {
  start: number;
  end: number;
  text: string;
  query?: string;
};

const WORD_CHAR_PATTERN = /[\p{L}\p{N}\p{M}_]/u;

export function parseOCRSearchMode(raw: unknown): OCRSearchMode {
  if (raw === "sanskrit_forms" || raw === "contains" || raw === "begins_with" || raw === "ends_with") return raw;
  return "exact_word";
}

export function getOCRSearchModeLabel(mode: OCRSearchMode) {
  return OCR_SEARCH_MODE_OPTIONS.find((option) => option.mode === mode)?.label ?? "Exact word";
}

export function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isBoundaryChar(char: string | undefined) {
  if (!char) return true;
  return !WORD_CHAR_PATTERN.test(char);
}

/**
 * The folded strings a query is looked up by. Matching happens on folded text
 * (see lib/sanskrit-fold.mjs), so Gujarati/Devanagari script, anusvara vs
 * class nasal, joiners and र्ऋ/ऋ never decide whether a word is found.
 */
export function foldedNeedlesForQuery(query: string, mode: OCRSearchMode) {
  const text = String(query ?? "").normalize("NFC").replace(/\s+/g, " ").trim();
  if (!text) return [];
  if (mode === "sanskrit_forms") return [...new Set([...sanskritQueryForms(text), ...gujaratiOnlyForms(text)])];
  const folded = foldSanskrit(text);
  return folded ? [folded] : [];
}

const GUJARATI_CHAR = /[\u0A80-\u0AFF]/;
const gujaratiOnlyCache = new Map<string, string[]>();
/** Gujarati-inflected forms that are not also Sanskrit forms of the word. */
function gujaratiOnlyForms(query: string) {
  let forms = gujaratiOnlyCache.get(query);
  if (!forms) {
    const sanskrit = new Set(sanskritQueryForms(query));
    forms = /\s/.test(query) ? [] : gujaratiCaseForms(query).filter((form) => !sanskrit.has(form));
    if (gujaratiOnlyCache.size > 500) gujaratiOnlyCache.clear();
    gujaratiOnlyCache.set(query, forms);
  }
  return forms;
}

type FoldedContent = ReturnType<typeof foldForMatching>;

function foldForMatching(content: string) {
  return foldSanskritText(String(content ?? ""), true);
}

const needlePatternCache = new Map<string, RegExp>();
function needlePattern(needles: string[]) {
  const key = needles.join("\u0000");
  let pattern = needlePatternCache.get(key);
  if (!pattern) {
    const sorted = [...needles].sort((a, b) => b.length - a.length).map(escapeRegExp);
    pattern = new RegExp(sorted.join("|"), "gu");
    if (needlePatternCache.size > 500) needlePatternCache.clear();
    needlePatternCache.set(key, pattern);
  }
  pattern.lastIndex = 0;
  return pattern;
}

function findInFolded(folded: FoldedContent, query: string, mode: OCRSearchMode): SearchMatch[] {
  const needles = foldedNeedlesForQuery(query, mode);
  if (needles.length === 0 || !folded.text) return [];
  const { text, starts, ends, source } = folded;
  const needle = String(query ?? "").normalize("NFC").trim();
  const wordMode = mode === "exact_word" || mode === "sanskrit_forms";
  const pattern = needlePattern(needles);
  const matches: SearchMatch[] = [];
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(text)) !== null) {
    const hit = match[0] ?? "";
    if (!hit) {
      pattern.lastIndex += 1;
      continue;
    }
    const fStart = match.index;
    const fEnd = fStart + hit.length;
    const leftOk = isBoundaryChar(text[fStart - 1]);
    const rightOk = isBoundaryChar(text[fEnd]);

    if (wordMode && (!leftOk || !rightOk)) continue;
    if (mode === "begins_with" && !leftOk) continue;
    if (mode === "ends_with" && !rightOk) continue;

    const start = starts[fStart];
    const end = ends[fEnd - 1];
    if (mode === "sanskrit_forms" && isGrammarLabelAt(source, start, end, hit)) continue;
    // A Gujarati case ending only makes sense on a word written in Gujarati.
    if (mode === "sanskrit_forms" && !GUJARATI_CHAR.test(source.slice(start, end)) && gujaratiOnlyForms(needle).includes(hit)) continue;
    matches.push({ start, end, text: source.slice(start, end), query: needle });
  }

  return matches;
}

export function findOCRSearchMatches(content: string, query: string, mode: OCRSearchMode): SearchMatch[] {
  // Offsets index the NFC form of `content`, as before; stored OCR text is NFC.
  return findInFolded(foldForMatching(content), query, mode);
}

function queryValues(value: string | string[] | null | undefined) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

export function normalizeOCRSearchQueries(
  primary: string | string[] | null | undefined,
  variants?: string | string[] | null,
  maxQueries = 8
) {
  const seen = new Set<string>();
  const queries: string[] = [];

  for (const value of [...queryValues(primary), ...queryValues(variants)]) {
    const query = String(value || "").normalize("NFC").replace(/\s+/g, " ").trim();
    if (!query) continue;
    // Variants that fold to the same text (the Gujarati spelling of a
    // Devanagari query, anusvara vs nasal, ...) are one search.
    const key = foldSanskrit(query);
    if (seen.has(key)) continue;
    seen.add(key);
    queries.push(query);
    if (queries.length >= maxQueries) break;
  }

  return queries;
}

function mergeSearchMatches(matches: SearchMatch[]) {
  const sorted = [...matches].sort((a, b) => {
    if (a.start !== b.start) return a.start - b.start;
    return b.end - a.end;
  });
  const merged: SearchMatch[] = [];

  for (const match of sorted) {
    const last = merged[merged.length - 1];
    if (!last || match.start >= last.end) {
      merged.push({ ...match });
      continue;
    }

    const previousEnd = last.end;
    if (match.end > previousEnd) {
      last.end = match.end;
      last.text = `${last.text}${match.text.slice(Math.max(0, previousEnd - match.start))}`;
    }
  }

  return merged;
}

export function findOCRSearchMatchesForQueries(
  content: string,
  queries: string | string[],
  mode: OCRSearchMode
) {
  const normalizedQueries = normalizeOCRSearchQueries(queries);
  if (normalizedQueries.length === 0) return [];

  const folded = foldForMatching(content);
  return mergeSearchMatches(normalizedQueries.flatMap((query) => findInFolded(folded, query, mode)));
}

export function hasOCRSearchMatch(content: string, query: string, mode: OCRSearchMode) {
  return findOCRSearchMatches(content, query, mode).length > 0;
}

/**
 * One excerpt per match, so a page with five hits can be shown as five
 * separate results instead of a single row the reader has to scan.
 * `matchStart`/`matchEnd` are offsets into the returned excerpt.
 */
export type OCRSearchOccurrence = {
  snippet: string;
  matchStart: number;
  matchEnd: number;
  text: string;
};

export function buildOCRSearchOccurrences(
  content: string,
  queries: string | string[],
  mode: OCRSearchMode,
  maxChars = 240
): OCRSearchOccurrence[] {
  const clean = String(content ?? "").replace(/\s+/g, " ").trim();
  if (!clean) return [];

  return findOCRSearchMatchesForQueries(clean, queries, mode).map((match) => {
    const matchLength = Math.max(1, match.end - match.start);
    const sidePadding = Math.max(45, Math.floor((maxChars - matchLength) / 2));
    const start = Math.max(0, match.start - sidePadding);
    const end = Math.min(clean.length, match.end + sidePadding);

    const leadingEllipsis = start > 0 ? "…" : "";
    const trailingEllipsis = end < clean.length ? "…" : "";
    const body = clean.slice(start, end);
    // Offsets shift by the leading ellipsis and by whatever trimStart removes.
    const trimmed = body.trimStart();
    const trimShift = body.length - trimmed.length;
    const snippet = `${leadingEllipsis}${trimmed.trimEnd()}${trailingEllipsis}`;
    const matchStart = leadingEllipsis.length + (match.start - start) - trimShift;

    return {
      snippet,
      matchStart: Math.max(0, matchStart),
      matchEnd: Math.max(0, matchStart) + matchLength,
      text: clean.slice(match.start, match.end),
    };
  });
}

export function buildOCRSearchExcerpt(content: string, query: string, mode: OCRSearchMode, maxChars = 180) {
  return buildOCRSearchExcerptForQueries(content, [query], mode, maxChars);
}

export function buildOCRSearchExcerptForQueries(
  content: string,
  queries: string | string[],
  mode: OCRSearchMode,
  maxChars = 180
) {
  const cleanContent = String(content ?? "").replace(/\s+/g, " ").trim();
  if (!cleanContent) return "";

  const firstMatch = findOCRSearchMatchesForQueries(cleanContent, queries, mode)[0];
  if (!firstMatch) {
    if (cleanContent.length <= maxChars) return cleanContent;
    return `${cleanContent.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
  }

  const matchLength = Math.max(1, firstMatch.end - firstMatch.start);
  const sidePadding = Math.max(45, Math.floor((maxChars - matchLength) / 2));
  const start = Math.max(0, firstMatch.start - sidePadding);
  const end = Math.min(cleanContent.length, firstMatch.end + sidePadding);
  let excerpt = cleanContent.slice(start, end).trim();

  if (start > 0) excerpt = `…${excerpt}`;
  if (end < cleanContent.length) excerpt = `${excerpt}…`;
  return excerpt;
}
