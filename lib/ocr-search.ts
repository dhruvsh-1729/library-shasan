export type OCRSearchMode = "exact_word" | "contains";

type SearchMatch = {
  start: number;
  end: number;
  text: string;
};

const WORD_CHAR_PATTERN = /[\p{L}\p{N}\p{M}_]/u;

export function parseOCRSearchMode(raw: unknown): OCRSearchMode {
  return raw === "contains" ? "contains" : "exact_word";
}

export function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isBoundaryChar(char: string | undefined) {
  if (!char) return true;
  return !WORD_CHAR_PATTERN.test(char);
}

export function findOCRSearchMatches(content: string, query: string, mode: OCRSearchMode): SearchMatch[] {
  const source = String(content ?? "");
  const needle = String(query ?? "").trim();
  if (!source || !needle) return [];

  const pattern = new RegExp(escapeRegExp(needle), "giu");
  const matches: SearchMatch[] = [];
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(source)) !== null) {
    const start = typeof match.index === "number" ? match.index : 0;
    const text = match[0] ?? "";
    const end = start + text.length;

    if (mode === "exact_word") {
      const left = source[start - 1];
      const right = source[end];
      if (!isBoundaryChar(left) || !isBoundaryChar(right)) {
        continue;
      }
    }

    matches.push({ start, end, text });

    if (text.length === 0) {
      pattern.lastIndex += 1;
    }
  }

  return matches;
}

export function hasOCRSearchMatch(content: string, query: string, mode: OCRSearchMode) {
  return findOCRSearchMatches(content, query, mode).length > 0;
}

export function buildOCRSearchExcerpt(content: string, query: string, mode: OCRSearchMode, maxChars = 180) {
  const cleanContent = String(content ?? "").replace(/\s+/g, " ").trim();
  if (!cleanContent) return "";

  const firstMatch = findOCRSearchMatches(cleanContent, query, mode)[0];
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

