import { rankedReadings } from "@/lib/roman-forms";
import { romanWordReadings } from "@/lib/roman-sanskrit";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { headwordTiers } from "@/lib/vyutpatti/lookup";

// The Devanagari a romanised vishay word is looked up as, and the choices
// offered beside it. Readings that are kosh headwords are ranked by how few
// guesses they need and how often the granths use them, a headword of the
// three koshes ahead ("akash" → आकाश, not आकष). A reading that differs from
// the headword only in spelling takes the kosh's spelling (देवेंद्र → देवेन्द्र).

const OPTIONS = 5;
/** A headword of the three koshes counts as this many extra guesses' worth over one of the other four. */
const TIER_ONE_BONUS = 1.5;
/** A reading spelled exactly as the kosh prints the headword, over one the index only matches loosely. */
const EXACT_HEAD_BONUS = 1.0;

export type SpellOption = { form: string; pages: number; kosh: boolean };

/** For each Roman word: the spelling to use, and up to five choices. */
export async function spellRomanWords(words: string[]) {
  // Fewer readings per word for a long list, so a whole vishay list stays quick.
  const ranked = await rankedReadings(words, words.length > 20 ? 150 : 600).catch(() => words.map(() => []));
  let known = new Map<string, { tier: number; head: string }>();
  try {
    known = await headwordTiers([...new Set(ranked.flatMap((list) => list.slice(0, 200).map((r) => r.form)))]);
  } catch {
    // no headword index: the likeliest readings still go back
  }
  const spelling = (form: string) => {
    const head = known.get(form)?.head;
    // only a Devanagari headword lends its spelling; a Gujarati one (શસ્ત્ર) never does
    return head && /^[\u0900-\u097f]+$/u.test(head) && foldSanskrit(head) === foldSanskrit(form) ? head : form;
  };
  const options = new Map<string, SpellOption[]>();
  const best = new Map<string, string>();
  words.forEach((word, index) => {
    const list = ranked[index].slice(0, 200);
    // the kosh index matches loosely (vowel length, stem forms); a reading that IS the printed headword counts more
    const exact = (r: (typeof list)[number]) => {
      const head = known.get(r.form)?.head;
      return head && foldSanskrit(head).replace(/[ःं]$/u, "") === r.folded.replace(/[ःं]$/u, "") ? EXACT_HEAD_BONUS : 0;
    };
    const score = (r: (typeof list)[number]) => r.score + (known.get(r.form)?.tier === 1 ? TIER_ONE_BONUS : 0) + exact(r);
    const inKosh = list.filter((r) => known.has(r.form)).sort((a, b) => score(b) - score(a));
    const fallback = [...list].sort((a, b) => b.score - a.score);
    const chosen = (inKosh.length ? inKosh : fallback).slice(0, OPTIONS);
    options.set(word, chosen.map((r) => ({ form: spelling(r.form), pages: r.pages, kosh: known.has(r.form) })));
    best.set(word, chosen[0] ? spelling(chosen[0].form) : romanWordReadings(word, 1)[0]?.text ?? word);
  });
  return { best, options };
}
