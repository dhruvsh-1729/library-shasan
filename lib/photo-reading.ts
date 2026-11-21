// Which reading of a photo is the right way up (Diksha, 18 Nov 2026: a
// photo of a vishay column taken sideways read "लोकोत्तरअहिंसा" as
// "अकात्तरअहिंसा"). The photo is read turned three ways; the reading whose
// words are best made of kosh words wins, then the one with more words.
// Server only: the word list is data/compound-lexicon.json.

import { readFileSync } from "node:fs";
import path from "node:path";
import { aksharaCount, canStartWord, compoundParts, createLexicon } from "@/lib/sanskrit-compound.mjs";

let lexicon: ReturnType<typeof createLexicon> | null = null;
const getLexicon = () => (lexicon ??= createLexicon(JSON.parse(readFileSync(path.join(process.cwd(), "data", "compound-lexicon.json"), "utf8"))));

const WORD = /[ऀ-ॿ]{2,}/g;

/** 0-1: share of the text's letters that fall in kosh words that can start a word. */
export function readingScore(text: string) {
  const words = [...new Set(text.match(WORD) ?? [])];
  if (!words.length) return { known: 0, words: 0 };
  const lex = getLexicon();
  let known = 0;
  let total = 0;
  for (const w of words) {
    const n = aksharaCount(w);
    total += n;
    const r = compoundParts(w, lex);
    if (r?.whole) {
      known += n;
      continue;
    }
    for (const p of r?.parts ?? []) {
      if (p.kind === "prefix" || p.kind === "ending") known += aksharaCount(p.text);
      else if (p.kind === "word" && canStartWord(p.text)) known += aksharaCount(p.text);
    }
  }
  return { known: total ? known / total : 0, words: words.length };
}

/** The index of the best reading. */
export function bestReading(texts: string[]) {
  let best = 0;
  let bestScore = { known: -1, words: 0 };
  texts.forEach((text, i) => {
    const s = readingScore(text);
    const k = Math.round(s.known * 20) / 20;
    const b = Math.round(bestScore.known * 20) / 20;
    if (k > b || (k === b && s.words > bestScore.words)) {
      best = i;
      bestScore = s;
    }
  });
  return best;
}
