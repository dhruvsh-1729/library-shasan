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
