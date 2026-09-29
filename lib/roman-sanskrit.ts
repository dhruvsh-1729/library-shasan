// Spellings a romanised word could stand for in Devanagari, by Sanskrit rules.
//
// People type Sanskrit in Roman letters loosely: "hinsa" for हिंसा, "atma" for
// आत्मा, "tattvartha" for तत्त्वार्थ, "dharm" for धर्म. One fixed reading
// cannot serve that (the old one read "hinsa" as हिंस and found 1 page of 87),
// so this lists the readings a careful reader would consider, cheapest first,
// and /api/query-forms keeps the ones the library actually contains.
//
// Every reading follows Sanskrit orthography, never Hindi: a consonant keeps
// its inherent अ unless the spelling says otherwise, so "karma" is कर्म and not
// कर्म्-with-a-dropped-schwa; the ambiguity allowed is only what Roman letters
// fail to say (vowel length, retroflex vs dental, ś vs ṣ, a final vowel).
// IAST (ā ī ū ṛ ṭ ḍ ṇ ś ṣ ṃ ḥ ñ ṅ) is read exactly, with no alternatives.

type Alt = { text: string; cost: number };
type Unit = { kind: "consonant" | "vowel" | "mark" | "other"; alts: Alt[]; raw: string };

const VIRAMA = "्";

// Consonants, longest spelling first. The first alternative is the usual
// reading; the others cost more.
const CONSONANTS: Array<[string, Alt[]]> = [
  ["ksh", [{ text: "क्ष", cost: 0 }]],
  ["chh", [{ text: "छ", cost: 0 }]],
  ["tth", [{ text: "त्थ", cost: 0 }, { text: "ठ", cost: 1.5 }]],
  ["ddh", [{ text: "द्ध", cost: 0 }, { text: "ढ", cost: 1.5 }]],
  ["shr", [{ text: "श्र", cost: 0 }]],
  ["jñ", [{ text: "ज्ञ", cost: 0 }]],
  ["jn", [{ text: "ज्ञ", cost: 0 }, { text: "ज्न", cost: 2 }]],
  ["gn", [{ text: "ज्ञ", cost: 0.6 }, { text: "ग्न", cost: 0.4 }]],
  ["gy", [{ text: "ज्ञ", cost: 0 }, { text: "ग्य", cost: 1 }]],
  ["kh", [{ text: "ख", cost: 0 }]],
  ["gh", [{ text: "घ", cost: 0 }]],
  ["ch", [{ text: "च", cost: 0 }, { text: "छ", cost: 1.2 }]],
  ["jh", [{ text: "झ", cost: 0 }]],
  ["ṭh", [{ text: "ठ", cost: 0 }]],
  ["ḍh", [{ text: "ढ", cost: 0 }]],
  ["tt", [{ text: "त्त", cost: 0 }, { text: "ट्ट", cost: 1.5 }, { text: "ट", cost: 1.8 }]],
  ["dd", [{ text: "द्द", cost: 0 }, { text: "ड्ड", cost: 1.5 }]],
  ["th", [{ text: "थ", cost: 0 }, { text: "ठ", cost: 1.2 }]],
  ["dh", [{ text: "ध", cost: 0 }, { text: "ढ", cost: 1.2 }]],
  ["ph", [{ text: "फ", cost: 0 }]],
  ["bh", [{ text: "भ", cost: 0 }]],
  ["sh", [{ text: "श", cost: 0 }, { text: "ष", cost: 0.5 }]],
  ["ṣ", [{ text: "ष", cost: 0 }]],
  ["ś", [{ text: "श", cost: 0 }]],
  ["ṭ", [{ text: "ट", cost: 0 }]],
  ["ḍ", [{ text: "ड", cost: 0 }]],
  ["ṇ", [{ text: "ण", cost: 0 }]],
  ["ñ", [{ text: "ञ", cost: 0 }]],
  ["ṅ", [{ text: "ङ", cost: 0 }]],
  ["k", [{ text: "क", cost: 0 }]],
  ["q", [{ text: "क", cost: 0 }]],
  ["x", [{ text: "क्ष", cost: 0 }]],
  ["g", [{ text: "ग", cost: 0 }]],
  ["c", [{ text: "च", cost: 0 }]],
  ["j", [{ text: "ज", cost: 0 }]],
  ["z", [{ text: "ज", cost: 0 }]],
  ["t", [{ text: "त", cost: 0 }, { text: "ट", cost: 1.2 }]],
  ["d", [{ text: "द", cost: 0 }, { text: "ड", cost: 1.2 }]],
  ["n", [{ text: "न", cost: 0 }, { text: "ण", cost: 0.7 }]],
  ["p", [{ text: "प", cost: 0 }]],
  ["f", [{ text: "फ", cost: 0 }]],
  ["b", [{ text: "ब", cost: 0 }]],
  ["m", [{ text: "म", cost: 0 }]],
  ["y", [{ text: "य", cost: 0 }]],
  ["r", [{ text: "र", cost: 0 }]],
  ["l", [{ text: "ल", cost: 0 }]],
  ["v", [{ text: "व", cost: 0 }]],
  ["w", [{ text: "व", cost: 0 }]],
  ["s", [{ text: "स", cost: 0 }, { text: "ष", cost: 1.4 }]],
  ["h", [{ text: "ह", cost: 0 }]],
];

// Vowels as [independent letter, sign after a consonant].
const V: Record<string, [string, string]> = {
  a: ["अ", ""],
  aa: ["आ", "ा"],
  i: ["इ", "ि"],
  ii: ["ई", "ी"],
  u: ["उ", "ु"],
  uu: ["ऊ", "ू"],
  ri: ["ऋ", "ृ"],
  e: ["ए", "े"],
  ai: ["ऐ", "ै"],
  o: ["ओ", "ो"],
  au: ["औ", "ौ"],
};

// Vowel spellings, longest first, with the vowels each can mean.
const VOWELS: Array<[string, Array<[keyof typeof V, number]>]> = [
  ["aa", [["aa", 0]]],
  ["ā", [["aa", 0]]],
  ["ai", [["ai", 0]]],
  ["au", [["au", 0]]],
  ["ee", [["ii", 0]]],
  ["ii", [["ii", 0]]],
  ["ī", [["ii", 0]]],
  ["oo", [["uu", 0]]],
  ["uu", [["uu", 0]]],
  ["ū", [["uu", 0]]],
  ["ṛ", [["ri", 0]]],
  ["a", [["a", 0], ["aa", 0.9]]],
  ["i", [["i", 0], ["ii", 0.8]]],
  ["u", [["u", 0], ["uu", 0.8]]],
  ["e", [["e", 0]]],
  ["o", [["o", 0]]],
];

function vowelAt(word: string, i: number) {
  for (const [spelling, meanings] of VOWELS) if (word.startsWith(spelling, i)) return { spelling, meanings };
  return null;
}

function consonantAt(word: string, i: number) {
  for (const [spelling, alts] of CONSONANTS) if (word.startsWith(spelling, i)) return { spelling, alts };
  return null;
}

/**
 * Splits one romanised word into units, each with its possible Devanagari.
 * A consonant unit's alternatives already include the vowel that follows it
 * (or a virama), so the units simply concatenate.
 */
function unitsOf(word: string): Unit[] {
  const units: Unit[] = [];
  let i = 0;
  while (i < word.length) {
    const ch = word[i];
    if (ch === "ṃ" || ch === "ṁ") {
      units.push({ kind: "mark", alts: [{ text: "ं", cost: 0 }], raw: ch });
      i += 1;
      continue;
    }
    if (ch === "ḥ") {
      units.push({ kind: "mark", alts: [{ text: "ः", cost: 0 }], raw: ch });
      i += 1;
      continue;
    }

    const consonant = consonantAt(word, i);
    if (consonant) {
      const after = i + consonant.spelling.length;
      const isNasal = consonant.spelling === "n" || consonant.spelling === "m";
      const nextVowel = vowelAt(word, after);
      // n/m before another consonant is the anusvara (the fold treats ं and a
      // class nasal alike, so ं covers both ways of writing it).
      if (isNasal && !nextVowel && after < word.length && consonantAt(word, after)) {
        units.push({ kind: "mark", alts: [{ text: "ं", cost: 0 }, { text: `${consonant.alts[0].text}${VIRAMA}`, cost: 0.6 }], raw: consonant.spelling });
        i = after;
        continue;
      }
      // "ri"/"ru" after a consonant: vocalic ऋ (कृष्ण, दृष्टि) or र + vowel.
      if (!nextVowel && (word.startsWith("ri", after) || word.startsWith("ru", after))) {
        const base = consonant.alts;
        const tail = word[after + 1] === "i" ? "ि" : "ु";
        units.push({
          kind: "consonant",
          alts: base.flatMap((alt) => [
            { text: `${alt.text}ृ`, cost: alt.cost + 0.2 },
            { text: `${alt.text}${VIRAMA}र${tail}`, cost: alt.cost + 0.4 },
          ]),
          raw: word.slice(i, after + 2),
        });
        i = after + 2;
        continue;
      }
      if (nextVowel) {
        units.push({
          kind: "consonant",
          alts: consonant.alts.flatMap((alt) =>
            nextVowel.meanings.map(([v, cost]) => ({ text: `${alt.text}${V[v][1]}`, cost: alt.cost + cost }))
          ),
          raw: word.slice(i, after + nextVowel.spelling.length),
        });
        i = after + nextVowel.spelling.length;
        continue;
      }
      const atEnd = after >= word.length;
      // A bare consonant joins the next one (virama). At the end of a word it
      // is either a virama stem (पर्षद्) or, typed casually, an a-stem (धर्म).
      const endings: Alt[] = atEnd
        ? [{ text: "", cost: 0.3 }, { text: VIRAMA, cost: 0.5 }, { text: "ा", cost: 1.2 }]
        : [{ text: VIRAMA, cost: 0 }];
      units.push({
        kind: "consonant",
        alts: consonant.alts.flatMap((alt) => endings.map((end) => ({ text: `${alt.text}${end.text}`, cost: alt.cost + end.cost }))),
        raw: word.slice(i, after),
      });
      i = after;
      continue;
    }

    const vowel = vowelAt(word, i);
    if (vowel) {
      units.push({
        kind: "vowel",
        alts: vowel.meanings.map(([v, cost]) => ({ text: V[v][0], cost })),
        raw: vowel.spelling,
      });
      i += vowel.spelling.length;
      continue;
    }

    units.push({ kind: "other", alts: [{ text: ch, cost: 0 }], raw: ch });
    i += 1;
  }

  // A final "a" typed casually is often a long ā (hinsa, atma, gatha).
  const last = units[units.length - 1];
  if (last?.kind === "consonant" && /a$/.test(last.raw) && !/aa$|ā$/.test(last.raw)) {
    last.alts = last.alts.map((alt) => (alt.text.endsWith("ा") ? { ...alt, cost: Math.max(0, alt.cost - 0.6) } : alt));
  }
  return units;
}

/** The cheapest `limit` readings of one word, best first. */
export function romanWordReadings(word: string, limit = 40): Array<{ text: string; cost: number }> {
  const units = unitsOf(word.normalize("NFC").toLowerCase().replace(/[’']/g, ""));
  if (units.length === 0) return [];
  // Best-first over the unit choices; the space is small (a few alternatives
  // per unit) so a sorted beam is exact enough.
  let beam: Array<{ text: string; cost: number }> = [{ text: "", cost: 0 }];
  const width = Math.max(limit * 4, 64);
  for (const unit of units) {
    const next: Array<{ text: string; cost: number }> = [];
    for (const partial of beam) for (const alt of unit.alts) next.push({ text: partial.text + alt.text, cost: partial.cost + alt.cost });
    next.sort((a, b) => a.cost - b.cost);
    beam = next.slice(0, width);
  }
  const seen = new Set<string>();
  return beam.filter((reading) => !seen.has(reading.text) && seen.add(reading.text)).slice(0, limit);
}

export function hasRomanLetters(value: string) {
  return /[a-zāīūṛṭḍṇśṣṃṁḥñṅ]/i.test(value);
}
