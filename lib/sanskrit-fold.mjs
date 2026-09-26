// Sanskrit-aware text folding and word forms for OCR search.
//
// Plain JS so the Next.js app and the Node pipeline scripts share one
// implementation: the folded search index is built by scripts and queried by
// the app, and the two only agree if they fold text identically.
//
// Every Devanagari run in the library is Sanskrit, so the rules here are
// Sanskrit orthography rules, not Hindi ones. Gujarati-script text is folded
// into Devanagari so a word matches whichever script the OCR emitted, including
// the words OCR split across both scripts (e.g. "कांજી").

/** Bump when foldSanskritText changes; rows built by an older version are rebuilt. */
export const SANSKRIT_FOLD_VERSION = 1;

const VIRAMA = "्";
const ANUSVARA = "ं";
const CANDRABINDU = "ँ";
const VISARGA = "ः";
const VOCALIC_R = "ऋ";
const RA = "र";
const MA = "म";

const GUJARATI_TO_DEVANAGARI_OFFSET = 0x180;
// Characters with no rendering meaning in Sanskrit: joiners, BOM, soft hyphen.
const DROPPED = new Set([0x200b, 0x200c, 0x200d, 0x2060, 0xfeff, 0x00ad]);

// Each class nasal with the stops of its own class. A nasal+virama before a
// stop of its class is the same sound as anusvara before that stop, and
// printed Sanskrit writes it both ways (व्यन्तर / व्यंतर).
const NASAL_CLASS = new Map([
  ["ङ", ["क", "ख", "ग", "घ"]], // ङ : क ख ग घ
  ["ञ", ["च", "छ", "ज", "झ"]], // ञ : च छ ज झ
  ["ण", ["ट", "ठ", "ड", "ढ"]], // ण : ट ठ ड ढ
  ["न", ["त", "थ", "द", "ध"]], // न : त थ द ध
  ["म", ["प", "फ", "ब", "भ"]], // म : प फ ब भ
]);

const WORD_CHAR = /[\p{L}\p{N}\p{M}_]/u;
export const WORD_TOKEN_PATTERN = /[\p{L}\p{N}\p{M}_]+/gu;

function isWordChar(ch) {
  return Boolean(ch) && WORD_CHAR.test(ch);
}

function mapCodePoint(ch) {
  const code = ch.codePointAt(0);
  if (DROPPED.has(code)) return "";
  if (code >= 0x0a81 && code <= 0x0aef) {
    const mapped = String.fromCodePoint(code - GUJARATI_TO_DEVANAGARI_OFFSET);
    // Only map to assigned Devanagari letters/marks/digits.
    if (/[\p{L}\p{M}\p{N}]/u.test(mapped)) return mapped === CANDRABINDU ? ANUSVARA : mapped;
    return ch;
  }
  if (ch === CANDRABINDU) return ANUSVARA;
  if (code < 0x0250) {
    const lower = ch.toLowerCase();
    return lower.length === ch.length ? lower : ch;
  }
  return ch;
}

/**
 * Folds text to the canonical form used for search. With `withMap`, also
 * returns for every UTF-16 unit of the folded text the [start, end) span of
 * the NFC input it came from, so matches found in folded text can be
 * highlighted in the original.
 *
 * Rules (all Sanskrit-safe equivalences):
 *  - Gujarati letters, marks and digits -> Devanagari
 *  - ZWJ / ZWNJ / BOM / soft hyphen removed
 *  - candrabindu -> anusvara
 *  - class nasal + virama before a stop of its class -> anusvara
 *  - word-final म् -> anusvara (अहम् = अहं)
 *  - र्ऋ -> ऋ (नैर्ऋत्य = नैऋत्य, a common print/OCR variant)
 *  - Latin lower-cased
 */
export function foldSanskritText(input, withMap = false) {
  const source = String(input ?? "").normalize("NFC");
  const units = [];
  let offset = 0;
  for (const ch of source) {
    const start = offset;
    offset += ch.length;
    const mapped = mapCodePoint(ch);
    if (!mapped) continue;
    units.push({ ch: mapped, s: start, e: offset });
  }

  const out = [];
  for (let i = 0; i < units.length; i += 1) {
    const u = units[i];
    const next = units[i + 1];
    const next2 = units[i + 2];

    if (u.ch === RA && next?.ch === VIRAMA && next2?.ch === VOCALIC_R) {
      out.push({ ch: VOCALIC_R, s: u.s, e: next2.e });
      i += 2;
      continue;
    }

    if (next?.ch === VIRAMA && NASAL_CLASS.has(u.ch)) {
      if (next2 && NASAL_CLASS.get(u.ch).includes(next2.ch)) {
        out.push({ ch: ANUSVARA, s: u.s, e: next.e });
        i += 1;
        continue;
      }
      if (u.ch === MA && !isWordChar(next2?.ch)) {
        out.push({ ch: ANUSVARA, s: u.s, e: next.e });
        i += 1;
        continue;
      }
    }

    out.push(u);
  }

  let text = "";
  const starts = withMap ? [] : null;
  const ends = withMap ? [] : null;
  for (const u of out) {
    text += u.ch;
    if (withMap) {
      for (let k = 0; k < u.ch.length; k += 1) {
        starts.push(u.s);
        ends.push(u.e);
      }
    }
  }
  return withMap ? { text, starts, ends, source } : { text };
}

export function foldSanskrit(input) {
  return foldSanskritText(input).text;
}

/** Reverses by code point; see lib/ocr-search-index.ts for why not by grapheme. */
export function reverseCodePoints(input) {
  return Array.from(String(input ?? "")).reverse().join("");
}

/** Contents of the folded search row for one page. */
export function buildFoldedIndexRow(content) {
  const folded = foldSanskrit(content);
  const tokens = folded.match(WORD_TOKEN_PATTERN) ?? [];
  return { folded, reversed: tokens.map(reverseCodePoints).join(" ") };
}

// ---------------------------------------------------------------------------
// Sanskrit word forms
// ---------------------------------------------------------------------------

const VOWEL_SIGNS = new Set(["ा", "ि", "ी", "ु", "ू", "ृ", "ॄ", "े", "ै", "ो", "ौ"]);

function isConsonant(ch) {
  const code = ch?.codePointAt(0) ?? 0;
  return (code >= 0x0915 && code <= 0x0939) || (code >= 0x0958 && code <= 0x095f);
}

// Endings appended to the stem with its final vowel sign removed. Written
// unfolded; every generated form is folded afterwards (-ाम् -> -ां etc.).
const A_STEM = ["", "ः", "म्", "ो", "ेन", "ाय", "ात्", "स्य", "े", "ौ", "ाभ्याम्", "योः", "ाः", "ान्", "ैः", "ेभ्यः", "ानाम्", "ेषु", "ानि"];
const I_STEM = ["ि", "िः", "िम्", "िना", "ये", "ेः", "ौ", "ी", "िभ्याम्", "योः", "यः", "ीन्", "िभिः", "िभ्यः", "ीनाम्", "िषु", "े", "्या", "्यै", "्याः", "्याम्", "ीः", "ीनि"];
const II_STEM = ["ी", "ीम्", "्या", "्यै", "्याः", "्याम्", "ि", "्यौ", "ीभ्याम्", "्योः", "्यः", "ीः", "ीभिः", "ीभ्यः", "ीनाम्", "ीषु"];
const U_STEM = ["ु", "ुः", "ुम्", "ुना", "ुणा", "वे", "ोः", "ौ", "ो", "ू", "ुभ्याम्", "्वोः", "वः", "ून्", "ुभिः", "ुभ्यः", "ूनाम्", "ूणाम्", "ुषु", "ूनि"];
const AA_STEM = ["ा", "ाम्", "या", "ायै", "ायाः", "ायाम्", "े", "ाभ्याम्", "योः", "ाः", "ाभिः", "ाभ्यः", "ानाम्", "ासु"];
// Consonant stems, appended to the final consonant without its virama.
const CONSONANT_VOWEL_ENDINGS = ["म्", "ा", "े", "ः", "ि", "ौ", "ोः", "ाम्"];

// Pausa: a word-final voiced stop is pronounced and written voiceless
// (पर्षद् -> पर्षत्, वाच् -> वाक्).
const PAUSA = new Map([
  ["द", "त"], ["ध", "त"], ["ग", "क"], ["घ", "क"], ["ड", "ट"], ["ढ", "ट"],
  ["ब", "प"], ["भ", "प"], ["ज", "क"],
]);

function consonantStemForms(stemWithoutVirama) {
  const last = stemWithoutVirama.slice(-1);
  const head = stemWithoutVirama.slice(0, -1);
  const voiceless = PAUSA.get(last);
  const forms = [`${stemWithoutVirama}${VIRAMA}`];
  if (voiceless) forms.push(`${head}${voiceless}${VIRAMA}`);
  for (const ending of CONSONANT_VOWEL_ENDINGS) forms.push(`${stemWithoutVirama}${ending}`);
  forms.push(`${stemWithoutVirama}${VIRAMA}भिः`, `${stemWithoutVirama}${VIRAMA}भ्याम्`, `${stemWithoutVirama}${VIRAMA}भ्यः`);
  forms.push(`${voiceless ? head + voiceless : stemWithoutVirama}${VIRAMA}सु`);
  return forms;
}

// ṇatva (Pāṇini 8.4.1-2): न in an ending becomes ण when ऋ, र or ष precede it
// in the word with nothing between but vowels, gutturals, labials, य व ह or
// anusvara (पुरुषेण, गुरुणा, but ईशानेन, भवनपतिना).
const NATVA_TRIGGERS = new Set(["र", "ष", "ऋ", "ॠ", "ृ", "ॄ"]);
function natvaApplies(stem) {
  for (const ch of Array.from(stem).reverse()) {
    if (NATVA_TRIGGERS.has(ch)) return true;
    const code = ch.codePointAt(0);
    const blocker =
      (code >= 0x091a && code <= 0x0928) || code === 0x0932 || code === 0x0933 || code === 0x0936 || code === 0x0938;
    if (blocker) return false;
  }
  return false;
}

/**
 * All Sanskrit forms of one word that a search for it should find: the word
 * itself plus its regular declension (a-, ā-, i-, ī-, u- and consonant stems).
 * Irregular stems (पति, स्त्री, ...) get their regular endings only.
 */
export function sanskritWordForms(word) {
  const folded = foldSanskrit(String(word ?? "").trim());
  if (!folded || /\s/.test(folded)) return folded ? [folded] : [];
  const forms = new Set([folded]);
  const last = folded.slice(-1);
  if (!/[ऀ-ॿ]/.test(last)) return [...forms];

  let base = folded;
  if (last === VISARGA || last === ANUSVARA) base = folded.slice(0, -1);
  if (!base) return [...forms];
  const tail = base.slice(-1);
  const stem = base.slice(0, -1);
  const add = (s, endings) =>
    endings.forEach((ending) => {
      const retroflex = ending.includes("न") && natvaApplies(s) ? ending.replace("न", "ण") : ending;
      forms.add(foldSanskrit(s + retroflex));
    });

  if (isConsonant(tail)) {
    add(base, A_STEM);
  } else if (tail === VIRAMA && isConsonant(stem.slice(-1))) {
    add("", consonantStemForms(stem));
  } else if (tail === "ि") {
    add(stem, I_STEM);
  } else if (tail === "ी") {
    add(stem, II_STEM);
  } else if (tail === "ु") {
    add(stem, U_STEM);
  } else if (tail === "ा" && isConsonant(stem.slice(-1))) {
    // -ā is either an ā-stem feminine or the instrumental of a consonant
    // stem (पर्षदा = पर्षद् + ā), so both paradigms are searched.
    add(stem, AA_STEM);
    add("", consonantStemForms(stem));
  } else if (VOWEL_SIGNS.has(tail)) {
    forms.add(base);
  }

  // Visarga sandhi with the particles printed joined to the word:
  // देवः + च = देवश्च, + तु = देवस्तु, + अपि = देवोऽपि; देवाः + अपि = देवाऽपि.
  for (const form of [...forms]) {
    if (!form.endsWith(VISARGA)) continue;
    const head = form.slice(0, -1);
    forms.add(`${head}श्च`);
    forms.add(`${head}स्तु`);
    if (head.endsWith("\u093E")) forms.add(`${head}ऽपि`);
    else if (isConsonant(head.slice(-1))) forms.add(`${head}ोऽपि`);
  }
  return [...forms].filter(Boolean);
}

// Gujarati case endings / plural as printed on a word in the Gujarati glosses
// (જ્યોતિષ્કને, શ્રમણોનો, ભવનપતિના). Folded to Devanagari like the text.
const GUJARATI_AFTER_CONSONANT = ["नो", "ना", "नी", "नुं", "ने", "थी", "मां", "मांथी", "े", "ो", "ोनो", "ोना", "ोनी", "ोनुं", "ोने", "ोमां", "ोथी"];
const GUJARATI_AFTER_VOWEL = ["नो", "ना", "नी", "नुं", "ने", "थी", "मां", "मांथी", "ए", "ओ", "ओनो", "ओना", "ओनी", "ओनुं", "ओने", "ओमां", "ओथी"];

/** Forms of a word with Gujarati case endings; only valid on Gujarati-script text. */
export function gujaratiCaseForms(word) {
  const folded = foldSanskrit(String(word ?? "").trim());
  if (!folded || /\s/.test(folded)) return [];
  const last = folded.slice(-1);
  if (isConsonant(last)) return GUJARATI_AFTER_CONSONANT.map((e) => folded + e);
  if (VOWEL_SIGNS.has(last)) return GUJARATI_AFTER_VOWEL.map((e) => folded + e);
  return [];
}

/** Forms of a query: phrases keep all but their last word fixed. */
export function sanskritQueryForms(query) {
  const folded = foldSanskrit(String(query ?? "").replace(/\s+/g, " ").trim());
  if (!folded) return [];
  const parts = folded.split(" ");
  const lastWord = parts.pop();
  const prefix = parts.length ? `${parts.join(" ")} ` : "";
  return sanskritWordForms(lastWord).map((form) => prefix + form);
}

// ---------------------------------------------------------------------------
// Grammar labels
// ---------------------------------------------------------------------------

// Gender / part-of-speech abbreviations the koshes print after a headword:
// "(स्त्री.)", "स्त्री.", "-स्त्री-", "पुं.", "नपुं.", "त्रि.", "अव्य.", "वि.".
// Folded, so the Gujarati-script labels (સ્ત્રી. પું.) are covered too.
export const GRAMMAR_LABELS = new Set(
  ["स्त्री", "स्री", "स्त्रि", "पुं", "पु", "नपुं", "न", "त्रि", "वि", "अव्य", "क्ली", "उभ", "सर्व"].map(foldSanskrit)
);

function nextNonSpaceIndex(text, index, step) {
  let i = index;
  while (i >= 0 && i < text.length && /[ \t]/.test(text[i])) i += step;
  return i >= 0 && i < text.length ? i : -1;
}

const DASHES = new Set(["-", "\u2013", "\u2014"]);
const DIGIT = /[0-9\u0966-\u096F\u0AE6-\u0AEF]/;

/**
 * True when the word at [start, end) of the (original) text is a grammar
 * label rather than the word itself. The label layouts in the koshes:
 *   Apte  "(स्त्री.)"            SRM   "अजाजि स्त्री. (…" / "स्त्री, (अजेन …"
 *   AVPK  "उपत्यका—स्त्री-१०३५-"  "ऊरु-पु—સ્ત્રી ૬૧૩–"
 */
export function isGrammarLabelAt(text, start, end, foldedWord) {
  if (!GRAMMAR_LABELS.has(foldedWord)) return false;
  const afterAt = nextNonSpaceIndex(text, end, 1);
  const beforeAt = nextNonSpaceIndex(text, start - 1, -1);
  const after = afterAt < 0 ? "" : text[afterAt];
  const before = beforeAt < 0 ? "" : text[beforeAt];
  const afterNextAt = afterAt < 0 ? -1 : nextNonSpaceIndex(text, afterAt + 1, 1);
  const afterNext = afterNextAt < 0 ? "" : text[afterNextAt];

  if (after === "." || after === ")") return true;
  // "स्त्री, (etymology" — a gloss "…સ્ત્રી, એક …" continues with a word instead.
  if (after === "," && afterNext === "(") return true;
  // Gender then the verse/entry number: "-स्त्री-१०३५-", "स्त्री ૬૧૩".
  if (DASHES.has(after) && (DIGIT.test(afterNext) || afterNext === "(")) return true;
  if (DIGIT.test(after) && (DASHES.has(before) || before === "(")) return true;
  if ((DASHES.has(before) || before === "(") && (DASHES.has(after) || after === ",")) return true;
  return false;
}
