export type IndicQueryOptionId = "typed" | "devanagari" | "gujarati";

export type IndicQueryOption = {
  id: IndicQueryOptionId;
  label: string;
  value: string;
};

type ScriptName = "devanagari" | "gujarati";

type ScriptGlyphs = {
  independentVowels: Record<string, string>;
  vowelMarks: Record<string, string>;
  consonants: Record<string, string>;
  virama: string;
  anusvara: string;
  visarga: string;
};

const DEVANAGARI_TO_GUJARATI_OFFSET = 0x180;

const SCRIPT_GLYPHS: Record<ScriptName, ScriptGlyphs> = {
  devanagari: {
    independentVowels: {
      a: "अ",
      aa: "आ",
      i: "इ",
      ii: "ई",
      u: "उ",
      uu: "ऊ",
      ri: "ऋ",
      e: "ए",
      ai: "ऐ",
      o: "ओ",
      au: "औ",
    },
    vowelMarks: {
      a: "",
      aa: "ा",
      i: "ि",
      ii: "ी",
      u: "ु",
      uu: "ू",
      ri: "ृ",
      e: "े",
      ai: "ै",
      o: "ो",
      au: "ौ",
    },
    consonants: {
      k: "क",
      kh: "ख",
      g: "ग",
      gh: "घ",
      chh: "छ",
      ch: "च",
      c: "च",
      jh: "झ",
      j: "ज",
      z: "ज",
      tt: "ट",
      tth: "ठ",
      dd: "ड",
      ddh: "ढ",
      th: "थ",
      t: "त",
      dh: "ध",
      d: "द",
      n: "न",
      p: "प",
      ph: "फ",
      f: "फ",
      b: "ब",
      bh: "भ",
      m: "म",
      y: "य",
      r: "र",
      l: "ल",
      v: "व",
      w: "व",
      sh: "श",
      ss: "ष",
      s: "स",
      h: "ह",
      ksh: "क्ष",
      jn: "ज्ञ",
      gy: "ज्ञ",
      shr: "श्र",
    },
    virama: "्",
    anusvara: "ं",
    visarga: "ः",
  },
  gujarati: {
    independentVowels: {
      a: "અ",
      aa: "આ",
      i: "ઇ",
      ii: "ઈ",
      u: "ઉ",
      uu: "ઊ",
      ri: "ઋ",
      e: "એ",
      ai: "ઐ",
      o: "ઓ",
      au: "ઔ",
    },
    vowelMarks: {
      a: "",
      aa: "ા",
      i: "િ",
      ii: "ી",
      u: "ુ",
      uu: "ૂ",
      ri: "ૃ",
      e: "ે",
      ai: "ૈ",
      o: "ો",
      au: "ૌ",
    },
    consonants: {
      k: "ક",
      kh: "ખ",
      g: "ગ",
      gh: "ઘ",
      chh: "છ",
      ch: "ચ",
      c: "ચ",
      jh: "ઝ",
      j: "જ",
      z: "જ",
      tt: "ટ",
      tth: "ઠ",
      dd: "ડ",
      ddh: "ઢ",
      th: "થ",
      t: "ત",
      dh: "ધ",
      d: "દ",
      n: "ન",
      p: "પ",
      ph: "ફ",
      f: "ફ",
      b: "બ",
      bh: "ભ",
      m: "મ",
      y: "ય",
      r: "ર",
      l: "લ",
      v: "વ",
      w: "વ",
      sh: "શ",
      ss: "ષ",
      s: "સ",
      h: "હ",
      ksh: "ક્ષ",
      jn: "જ્ઞ",
      gy: "જ્ઞ",
      shr: "શ્ર",
    },
    virama: "્",
    anusvara: "ં",
    visarga: "ઃ",
  },
};

const VOWEL_ALIASES: Record<string, string> = {
  a: "a",
  aa: "aa",
  ā: "aa",
  i: "i",
  ee: "ii",
  ii: "ii",
  ī: "ii",
  u: "u",
  oo: "uu",
  uu: "uu",
  ū: "uu",
  r̥: "ri",
  ṛ: "ri",
  ri: "ri",
  e: "e",
  ai: "ai",
  o: "o",
  au: "au",
};

const CONSONANT_ALIASES: Record<string, string> = {
  ṭh: "tth",
  ṭ: "tt",
  ḍh: "ddh",
  ḍ: "dd",
  ṇ: "n",
  ñ: "n",
  ś: "sh",
  ṣ: "ss",
  ṅ: "n",
};

const VOWEL_TOKENS = Object.keys(VOWEL_ALIASES).sort((a, b) => b.length - a.length);
const CONSONANT_TOKENS = [
  ...Object.keys(SCRIPT_GLYPHS.devanagari.consonants),
  ...Object.keys(CONSONANT_ALIASES),
].sort((a, b) => b.length - a.length);

function matchToken(source: string, index: number, tokens: string[]) {
  for (const token of tokens) {
    if (source.startsWith(token, index)) return token;
  }
  return "";
}

function matchVowel(source: string, index: number) {
  const token = matchToken(source, index, VOWEL_TOKENS);
  if (!token) return null;
  return { raw: token, key: VOWEL_ALIASES[token] };
}

function matchConsonant(source: string, index: number) {
  const token = matchToken(source, index, CONSONANT_TOKENS);
  if (!token) return null;
  return { raw: token, key: CONSONANT_ALIASES[token] || token };
}

function transliterateWord(word: string, script: ScriptName) {
  const glyphs = SCRIPT_GLYPHS[script];
  let output = "";
  let index = 0;

  while (index < word.length) {
    const char = word[index];

    if (char === "ṃ" || char === "ṁ") {
      output += glyphs.anusvara;
      index += 1;
      continue;
    }
    if (char === "ḥ") {
      output += glyphs.visarga;
      index += 1;
      continue;
    }
    if (char === "h" && index === word.length - 1 && word[index - 1] === "a") {
      output += glyphs.visarga;
      index += 1;
      continue;
    }

    const consonant = matchConsonant(word, index);
    const nextAfterNasal = consonant?.key === "n" || consonant?.key === "m" ? index + consonant.raw.length : 0;
    if (nextAfterNasal && !matchVowel(word, nextAfterNasal) && matchConsonant(word, nextAfterNasal)) {
      output += glyphs.anusvara;
      index = nextAfterNasal;
      continue;
    }

    if (consonant) {
      index += consonant.raw.length;
      const vowel = matchVowel(word, index);
      if (vowel) {
        output += `${glyphs.consonants[consonant.key]}${glyphs.vowelMarks[vowel.key]}`;
        index += vowel.raw.length;
      } else if (matchConsonant(word, index)) {
        output += `${glyphs.consonants[consonant.key]}${glyphs.virama}`;
      } else {
        output += glyphs.consonants[consonant.key];
      }
      continue;
    }

    const vowel = matchVowel(word, index);
    if (vowel) {
      output += glyphs.independentVowels[vowel.key];
      index += vowel.raw.length;
      continue;
    }

    output += char;
    index += 1;
  }

  return output;
}

export function transliteratePhonetic(input: string, script: ScriptName) {
  return String(input || "")
    .normalize("NFC")
    .toLocaleLowerCase()
    .replace(/[’']/g, "")
    .split(/(\s+)/)
    .map((part) => (/^\s+$/.test(part) ? part : transliterateWord(part, script)))
    .join("")
    .trim();
}

function hasLatinLetters(value: string) {
  return /[a-z]/i.test(value);
}

function hasDevanagari(value: string) {
  return /[\u0900-\u097f]/.test(value);
}

function hasGujarati(value: string) {
  return /[\u0a80-\u0aff]/.test(value);
}

function devanagariToGujarati(value: string) {
  return Array.from(value)
    .map((char) => {
      const code = char.codePointAt(0) || 0;
      if ((code >= 0x0901 && code <= 0x0970) || (code >= 0x0966 && code <= 0x096f)) {
        return String.fromCodePoint(code + DEVANAGARI_TO_GUJARATI_OFFSET);
      }
      return char;
    })
    .join("");
}

function gujaratiToDevanagari(value: string) {
  return Array.from(value)
    .map((char) => {
      const code = char.codePointAt(0) || 0;
      if ((code >= 0x0a81 && code <= 0x0af0) || (code >= 0x0ae6 && code <= 0x0aef)) {
        return String.fromCodePoint(code - DEVANAGARI_TO_GUJARATI_OFFSET);
      }
      return char;
    })
    .join("");
}

function addOption(options: IndicQueryOption[], option: IndicQueryOption) {
  const normalized = option.value.trim().toLocaleLowerCase();
  if (!normalized) return;
  if (options.some((existing) => existing.value.trim().toLocaleLowerCase() === normalized)) return;
  options.push(option);
}

export function buildIndicQueryOptions(input: string): IndicQueryOption[] {
  const trimmed = String(input || "").replace(/\s+/g, " ").trim();
  if (!trimmed) return [];

  const options: IndicQueryOption[] = [{ id: "typed", label: "Typed", value: trimmed }];

  if (hasLatinLetters(trimmed)) {
    addOption(options, {
      id: "devanagari",
      label: "Sanskrit",
      value: transliteratePhonetic(trimmed, "devanagari"),
    });
    addOption(options, {
      id: "gujarati",
      label: "Gujarati",
      value: transliteratePhonetic(trimmed, "gujarati"),
    });
    return options;
  }

  if (hasDevanagari(trimmed)) {
    addOption(options, { id: "gujarati", label: "Gujarati", value: devanagariToGujarati(trimmed) });
  }
  if (hasGujarati(trimmed)) {
    addOption(options, { id: "devanagari", label: "Sanskrit", value: gujaratiToDevanagari(trimmed) });
  }

  return options;
}
