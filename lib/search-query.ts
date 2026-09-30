import { normalizeOCRSearchQueries } from "@/lib/ocr-search";
import { hasRomanLetters } from "@/lib/roman-sanskrit";

// What a typed query is searched as. An Indic query is searched as written;
// the fold already makes the Devanagari and Gujarati spellings one search, and
// the script toggles decide which script's hits count. A romanised query is
// searched as the Devanagari spellings chosen for each of its words
// (/api/query-forms offers them with their page counts).

export type QueryFormsWord = { word: string; forms: Array<{ form: string; pages: number }> };

const INDIC = /[ऀ-ॿ઀-૿]/;
const MAX_QUERIES = 8;

/** True when the query is written in Roman letters and needs spellings chosen. */
export function isRomanQuery(q: string) {
  return hasRomanLetters(q) && !INDIC.test(q);
}

/** The spelling a word gets when the reader has not chosen: the most common one. */
export function defaultForms(words: QueryFormsWord[]) {
  return words.map((word) => (word.forms[0] ? [word.forms[0].form] : []));
}

/** Every phrase the chosen spellings make, word by word, at most eight. */
export function composeQueries(chosen: string[][]) {
  let phrases: string[] = [""];
  for (const forms of chosen) {
    if (forms.length === 0) continue;
    const next: string[] = [];
    for (const phrase of phrases) for (const form of forms) next.push(phrase ? `${phrase} ${form}` : form);
    phrases = next.slice(0, MAX_QUERIES);
  }
  return normalizeOCRSearchQueries(phrases.filter(Boolean), [], MAX_QUERIES);
}

/**
 * The chosen spellings as stored in a link ("forms=हिंसा,अहिंसा"): per word,
 * the listed forms that word offers, else its default.
 */
export function formsFromList(words: QueryFormsWord[], list: string[] | null) {
  if (!list?.length) return defaultForms(words);
  const wanted = new Set(list);
  return words.map((word, index) => {
    const picked = word.forms.map((f) => f.form).filter((form) => wanted.has(form));
    return picked.length ? picked : defaultForms(words)[index];
  });
}

// A Devanagari compound is also searched as the words it is made of
// (/api/compound-parts): अकर्कशप्रशस्तवचनविनय… adds प्रशस्त, वचन, विनय … Each
// chosen part is searched as its kosh spelling and, when the vishay spells it
// differently, that spelling too (मनस् and मन, कर्मन् and कर्म).

export type CompoundPart = {
  text: string;
  kind: "word" | "prefix" | "ending" | "unknown";
  term?: string;
  alias?: string;
  koshes?: string[];
  vishayOnly?: boolean;
  pages?: number;
};
export type CompoundWord = { word: string; inKosh: string[]; parts: CompoundPart[] };

/** Parts that can be searched: words, and stretches no word list knows. */
export function searchablePartTerms(words: CompoundWord[]) {
  const terms: string[] = [];
  for (const word of words) {
    for (const part of word.parts) {
      if ((part.kind === "word" || part.kind === "unknown") && part.term && !terms.includes(part.term)) terms.push(part.term);
    }
  }
  return terms;
}

/** Parts searched when the reader has not chosen: every known word. */
export function defaultParts(words: CompoundWord[]) {
  const known = new Set(words.flatMap((w) => w.parts.filter((p) => p.kind === "word").map((p) => p.term)));
  return searchablePartTerms(words).filter((term) => known.has(term));
}

/** The chosen parts from a link ("parts=वचन,विनय"; [] is none), kept to those offered. */
export function partsFromList(words: CompoundWord[], list: string[] | null) {
  if (list === null) return defaultParts(words);
  const offered = new Set(searchablePartTerms(words));
  return list.filter((term) => offered.has(term));
}

/** The query strings the chosen parts add. */
export function partQueries(words: CompoundWord[], chosen: string[]) {
  const wanted = new Set(chosen);
  const out: string[] = [];
  for (const word of words) {
    for (const part of word.parts) {
      if (!part.term || !wanted.has(part.term)) continue;
      for (const text of [part.term, part.alias]) if (text && !out.includes(text)) out.push(text);
    }
  }
  return out;
}
