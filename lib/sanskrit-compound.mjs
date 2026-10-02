// Splits a Sanskrit compound (samāsa) into the words it is made of, so a
// search for a vishay like अकर्कशप्रशस्तवचनविनयअभ्यंतरतप can look up
// अकर्कश, प्रशस्त, वचन, विनय, अभ्यंतर and तप in the koshes, where the whole
// compound is almost never printed.
//
// Plain JS, shared by the app (/api/compound-parts) and the lexicon script
// (scripts/build_compound_lexicon.mjs), which learns the word list from the
// kosh headwords (never from the vishay list).
//
// Every Devanagari word is Sanskrit here (see lib/sanskrit-fold.mjs), so the
// joins are undone with Sanskrit sandhi rules:
//   vowel sandhi   जिनागम = जिन + आगम, देवेन्द्र = देव + इन्द्र, महोत्सव = महा + उत्सव
//   yaṇ sandhi     अध्यात्म = अधि + आत्म, प्रत्यक्ष = प्रति + अक्ष, स्वागत = सु + आगत
//   visarga        मनोयोग = मनस् + योग, तपोधन = तपस् + धन, निर्ग्रंथ = निस् + ग्रंथ
//   consonants     सम्यग्दर्शन = सम्यक् + दर्शन, सद्धर्म = सत् + धर्म, संयम = सम् + यम
// Each part is scored by where it was found: a kosh headword beats a word
// that only appears in kosh text, which beats a word only the Agamic
// vyutpatti kosh (395) has; fewer, longer parts beat many short ones.

import { foldSanskrit } from "./sanskrit-fold.mjs";

const VIRAMA = "्";
const MARK = /[ऀ-ःऺ-ॏ॑-ॗॢॣ]/;
const CONSONANT = /[क-हक़-य़]/;
// A syllable is an independent vowel or a consonant that is not followed by a
// virama: द्रव्य has two (द्र, व्य), तप two, अ one.
const SYLLABLE = /[ऄ-औॠॡ]|[क-हक़-य़](?!्)/gu;

/** Where a lexicon word comes from; lower is more trusted. */
export const SOURCE = { head: 0, text: 1, agamic: 2 };

// A lexicon entry is [kosh spelling to search, source, kosh mask, frequency];
// the mask bits are KOSH_FAMILIES' (lib/koshes.mjs).

export function aksharaCount(text) {
  return (String(text).match(SYLLABLE) ?? []).length;
}

/** False for OCR fragments that cannot begin a word: a vowel sign, anusvara, or a repha (र्त). */
export function canStartWord(text) {
  return /^[\u0904-\u0939\u0958-\u0961]/.test(text) && !/^र्/.test(text);
}

const isConsonant = (ch) => Boolean(ch) && CONSONANT.test(ch);

// Vowel sandhi read backwards: a vowel sign at the join stands for the end of
// the left word plus the start of the right word.
//   [vowel sign]: [[left word ends with, right word starts with], ...]
const VOWEL_JOINS = {
  "ा": [["", "आ"], ["", "अ"], ["ा", "आ"], ["ा", "अ"]],
  "े": [["", "इ"], ["", "ई"], ["ा", "इ"], ["ा", "ई"]],
  "ो": [["", "उ"], ["", "ऊ"], ["ा", "उ"], ["ा", "ऊ"]],
  "ै": [["", "ए"], ["", "ऐ"], ["ा", "ए"]],
  "ौ": [["", "ओ"], ["", "औ"], ["ा", "ओ"]],
  "ी": [["ि", "इ"], ["ी", "ई"], ["ि", "ई"], ["ी", "इ"]],
  "ू": [["ु", "उ"], ["ु", "ऊ"], ["ू", "उ"]],
};
// The same joins when the left word is a bare prefix-less vowel (अ + आगम).
const INDEPENDENT_TO_SIGN = { अ: "", आ: "ा", इ: "ि", ई: "ी", उ: "ु", ऊ: "ू", ऋ: "ृ", ए: "े", ऐ: "ै", ओ: "ो", औ: "ौ" };
const SIGN_TO_INDEPENDENT = Object.fromEntries(Object.entries(INDEPENDENT_TO_SIGN).map(([v, s]) => [s, v]));

// A voiced stop at the end of a left word is its voiceless stop in the kosh
// (सम्यग् → सम्यक्, सद् → सत्, षड् → षट्, अब् → अप्).
const DEVOICE = { ग: "क", द: "त", ड: "ट", ब: "प", ज: "च" };

// Prefixes that stand before a word as a word of their own: shown as a part,
// not searched (a kosh search for "अ" finds everything).
const PREFIXES = [
  ["अ", "अ"], ["अन", "अन्"], ["कु", "कु"], ["सु", "सु"], ["स", "स"],
  ["निर्", "निस्"], ["निस्", "निस्"], ["दुर्", "दुस्"], ["दुस्", "दुस्"], ["दु", "दुस्"],
].map(([surface, label]) => [foldSanskrit(surface), label]);

// Upasargas: a kosh word with one of these in front is one word (वि + रमण =
// विरमण, प्र + व्राजक = प्रव्राजक), searched whole.
export const UPASARGAS = ["प्र", "परा", "अप", "सम्", "सं", "अनु", "अव", "निस्", "निर्", "निः", "दुस्", "दुर्", "दुः", "वि", "आ", "नि", "अधि", "अपि", "अति", "सु", "उद्", "उत्", "अभि", "प्रति", "परि", "उप"].map(foldSanskrit);

// Word-forming suffixes that never stand alone: अकर्तृत्व, प्रत्याख्यानीय are one word.
const SUFFIXES = ["त्व", "ता", "त्वं", "तां", "ीय", "ईय", "वान", "वंत", "वती", "मान"].map(foldSanskrit);
const NOT_A_PART = new Set([...SUFFIXES, ...["इक", "ईया", "ीया", "इत", "इन्"].map(foldSanskrit)]);

// Gujarati case endings on the last word of a vishay (पदार्थोमां, समकितना),
// and Sanskrit nominative marks: stripped, not searched.
const ENDINGS = [
  "ोमां", "मां", "ोना", "ोनी", "ोनो", "ोनुं", "ोने", "ना", "नी", "नो", "नुं", "ने", "थी", "ओ", "ो",
  "स्य", "ेन", "ेषु", "ानां", "ाणां", "ात्", "ाः", "ैः", "ः",
].map(foldSanskrit);

// Costs. A part costs 1; these are added to it.
const COST = {
  source: [0, 0.55, 0.8],
  oneKosh: 0.3,
  twoKoshes: 0.12,
  oneAkshara: 1.4,
  twoAksharas: 0.2,
  join: 0.12,
  // अध्यात्म-type joins are rarer than a plain cut through the same letters (द्रव्य).
  yanJoin: 0.4,
  // After -ā the next word is rarely a negated one: रौद्रा + दृष्टि, not रौद्र + अदृष्टि.
  negatedAfterJoin: 0.3,
  prefix: 0.7,
  ending: 0.9,
  unknownBase: 1.5,
  unknownPerAkshara: 0.9,
};

function popcount(n) {
  let c = 0;
  for (let x = n; x; x &= x - 1) c += 1;
  return c;
}

/**
 * The lexicon as a lookup. `data` is data/compound-lexicon.json:
 * { words: { [folded stem]: [head, source, koshMask, freq] } }.
 */
export function createLexicon(data) {
  const words = new Map(Object.entries(data?.words ?? {}));
  return {
    size: words.size,
    get(key) {
      const e = words.get(key);
      return e ? { key, head: e[0], source: e[1], koshes: e[2], freq: e[3] } : null;
    },
  };
}

/** The stems a surface slice can stand for (before its join), each with an extra cost. */
function stemReadings(lex, surface) {
  const out = [];
  const tryKey = (key, extra) => {
    const e = NOT_A_PART.has(key) ? null : lex.get(key);
    if (e) out.push({ e, extra });
  };
  tryKey(surface, 0);
  if (surface.includes("ळ")) tryKey(surface.replaceAll("ळ", "ल"), 0.05);
  const last = surface.slice(-1);
  const beforeLast = surface.slice(-2, -1);
  if (last === VIRAMA && DEVOICE[beforeLast]) tryKey(surface.slice(0, -2) + DEVOICE[beforeLast] + VIRAMA, 0.08);
  // संयम, संसार: anusvara is the final म् of the left word.
  if (last === "ं") {
    tryKey(`${surface.slice(0, -1)}म्`, 0.08);
    // सर्वं, धर्मं: the accusative/neuter ending on an a-stem.
    tryKey(surface.slice(0, -1), 0.15);
  }
  // मनो-, तपो-, यशो-: the s-stem before a voiced sound.
  if (last === "ो") tryKey(`${surface.slice(0, -1)}स्`, 0.08);
  // निर्-, दुर्-, आविर्-: s after i/u before a voiced sound.
  if (surface.endsWith("र्")) tryKey(`${surface.slice(0, -2)}स्`, 0.12);
  // तपःक्षेत्र, अंतःकरण: visarga kept before a voiceless sound.
  if (last === "ः") tryKey(`${surface.slice(0, -1)}स्`, 0.08);
  // कल्पिका, रौद्रा: the feminine of an a-stem the koshes list (कल्पिक, रौद्र).
  if (last === "ा" && isConsonant(beforeLast)) {
    const masculine = lex.get(surface.slice(0, -1));
    if (masculine && masculine.source === SOURCE.head && aksharaCount(masculine.key) >= 2) out.push({ e: masculine, extra: 0.25 });
  }
  // विरमण, प्रव्राजक: an upasarga before a kosh word.
  for (const upasarga of UPASARGAS) {
    if (!surface.startsWith(upasarga) || surface.length <= upasarga.length) continue;
    const base = lex.get(surface.slice(upasarga.length));
    if (base && base.source === SOURCE.head && aksharaCount(base.key) >= 2) {
      out.push({ e: { ...base, key: surface, head: surface, source: SOURCE.text, freq: 0 }, extra: 0.1 });
    }
  }
  // अकर्तृत्व, अनंतता: a derived word whose base is known.
  for (const suffix of SUFFIXES) {
    if (!surface.endsWith(suffix) || surface.length <= suffix.length + 1) continue;
    const stem = surface.slice(0, -suffix.length);
    // प्रत्याख्यान + ईय is printed प्रत्याख्यानीय: the stem's final a merges into the ī.
    const base = lex.get(stem);
    if (base && base.source === SOURCE.head) out.push({ e: { ...base, key: surface, head: surface }, extra: 0.1 });
  }
  return out;
}

function partCost(entry, extra) {
  let cost = 1 + extra + COST.source[entry.source];
  const koshes = popcount(entry.koshes);
  if (koshes === 1) cost += COST.oneKosh;
  else if (koshes === 2) cost += COST.twoKoshes;
  const aksharas = aksharaCount(entry.key);
  if (aksharas <= 1) cost += COST.oneAkshara;
  else if (aksharas === 2) cost += COST.twoAksharas;
  // Between equally good readings the commoner word wins (जिन + आगम, not अगम).
  cost -= Math.min(0.3, Math.log10(1 + entry.freq) * 0.08);
  return cost;
}

/**
 * The best way to cut a folded word into parts. Returns null for an empty
 * word. Every part is one of:
 *   { kind: "word",    text, stem, head, source, koshes }  a kosh word
 *   { kind: "prefix",  text, label }                       अ-, कु-, निस्- …
 *   { kind: "ending",  text }                              a Gujarati case ending
 *   { kind: "unknown", text }                              nothing known
 * `text` is the slice of the (folded) input the part covers.
 */
export function splitCompound(word, lex, options = {}) {
  const W = foldSanskrit(String(word ?? "").normalize("NFC").trim());
  const N = W.length;
  if (!N) return null;
  // Used to find the best cut into two or more words for a word that is a headword itself.
  const forbidWhole = Boolean(options.forbidWhole);

  // best[i] maps the vowel a sandhi join owes the next part ("" for none) to
  // the cheapest cut of W[0, i).
  const best = Array.from({ length: N + 1 }, () => new Map());
  best[0].set("", { cost: 0, parts: [], words: 0 });

  const offer = (at, carry, from, part, cost) => {
    if (forbidWhole && from.parts.length === 0 && at === N && !carry) return;
    const words = from.words + (part.kind === "word" || part.kind === "unknown" ? 1 : 0);
    const next = { cost: from.cost + cost, parts: [...from.parts, part], words };
    const current = best[at].get(carry);
    if (!current || next.cost < current.cost) best[at].set(carry, next);
  };
  const wordPart = (text, r) => ({
    kind: "word",
    text,
    stem: r.e.key,
    head: r.e.head,
    term: searchTerm(r.e),
    source: r.e.source,
    koshes: r.e.koshes,
  });

  for (let i = 0; i < N; i += 1) {
    for (const [carry, state] of best[i]) {
      // A prefix at the start of a word (अ-कटु, कु-गुरु, स-योगी).
      if (!carry && (i === 0 || state.parts.at(-1)?.kind !== "prefix")) {
        for (const [surface, label] of PREFIXES) {
          if (!W.startsWith(surface, i)) continue;
          const end = i + surface.length;
          if (end >= N || !canStartWord(W.slice(end))) continue;
          offer(end, "", state, { kind: "prefix", text: surface, label }, COST.prefix);
        }
      }

      for (let j = i + 1; j <= N; j += 1) {
        const body = W.slice(i, j);
        const surface = carry + body;
        const nextIsMark = j < N && MARK.test(W[j]);

        // Plain cut. A part that starts with the अ a join handed it, and that is
        // a kosh word without it too, reads as the negation only reluctantly.
        const negated = carry === "अ" && Boolean(lex.get(body)) ? COST.negatedAfterJoin : 0;
        if (!nextIsMark) for (const r of stemReadings(lex, surface)) offer(j, "", state, wordPart(body, r), partCost(r.e, r.extra + negated));

        // Vowel sandhi: the part ends in a consonant + vowel sign that the next part shares.
        const sign = W[j - 1];
        if (VOWEL_JOINS[sign] && j < N && j - 1 > i && isConsonant(W[j - 2])) {
          for (const [leftEnd, rightStart] of VOWEL_JOINS[sign]) {
            const left = carry + W.slice(i, j - 1) + leftEnd;
            for (const r of stemReadings(lex, left)) offer(j, rightStart, state, wordPart(body, r), partCost(r.e, r.extra + COST.join));
          }
        }

        // yaṇ sandhi: C्य / C्व + vowel = C-i / C-u + vowel (अध्यात्म, स्वागत).
        if (j + 1 < N && W[j - 1] === VIRAMA && (W[j] === "य" || W[j] === "व") && j - 1 > i) {
          const after = W[j + 1];
          const signAfter = after in SIGN_TO_INDEPENDENT ? after : "";
          const vowel = SIGN_TO_INDEPENDENT[signAfter];
          if (vowel && vowel !== "इ" && vowel !== "ई" && vowel !== "उ" && vowel !== "ऊ") {
            const left = carry + W.slice(i, j - 1) + (W[j] === "य" ? "ि" : "ु");
            const resume = signAfter ? j + 2 : j + 1;
            if (resume < N) {
              for (const r of stemReadings(lex, left)) {
                offer(resume, vowel, state, wordPart(W.slice(i, resume), r), partCost(r.e, r.extra + COST.yanJoin));
              }
            }
          }
        }
      }

      // Nothing known starts here: the rest up to the next known part is one unknown part.
      if (!carry) {
        for (let j = i + 1; j <= N; j += 1) {
          if (j < N && MARK.test(W[j])) continue;
          const text = W.slice(i, j);
          if (!canStartWord(text)) break;
          const aksharas = Math.max(1, aksharaCount(text));
          offer(j, "", state, { kind: "unknown", text }, COST.unknownBase + COST.unknownPerAkshara * aksharas);
        }
        // A Gujarati case ending or a nominative mark at the very end.
        for (const ending of ENDINGS) {
          if (W.endsWith(ending) && i === N - ending.length && i > 0) {
            offer(N, "", state, { kind: "ending", text: ending }, COST.ending);
          }
        }
      }
    }
  }

  const done = best[N].get("");
  return done ? { parts: done.parts, cost: done.cost, word: W } : null;
}

/**
 * What a part is searched as: the kosh spelling of the headword, without the
 * nominative mark an a-stem is printed with (संयमः → संयम; the Sanskrit-forms
 * search finds संयमः again). Consonant stems keep their kosh spelling
 * (मनस्, कर्मन्, पर्षद्): that is how the koshes list them.
 */
function searchTerm(entry) {
  const head = entry.head;
  if (/[ःं]$/.test(head) && isConsonant(head.slice(-2, -1))) return head.slice(0, -1);
  return head;
}

/** Parts that name a word (searched); prefixes and endings are shown only. */
export function searchableParts(parts) {
  return parts.filter((p) => p.kind === "word" || p.kind === "unknown");
}

/**
 * How `word` breaks into parts. When the word is itself a kosh headword
 * (देवेन्द्र), its parts (देव + इन्द्र) are still offered, if every one of
 * them is a known word.
 */
export function compoundParts(word, lex) {
  const W = foldSanskrit(String(word ?? "").normalize("NFC").trim());
  const best = splitCompound(W, lex);
  if (!best) return null;
  const whole = lex.get(W);
  let parts = best.parts;
  // A word most koshes list on its own (उपाध्याय, न्याय, द्रव्य) is not taken
  // apart; one found in a single kosh (देवेन्द्र) is, into headwords only.
  if (searchableParts(parts).length < 2 && whole?.source === SOURCE.head && popcount(whole.koshes) <= 1) {
    const inner = splitCompound(W, lex, { forbidWhole: true });
    const good = (p) =>
      p.kind === "prefix" || (p.kind === "word" && p.source === SOURCE.head && aksharaCount(p.stem) >= 2);
    if (inner && inner.parts.every(good) && searchableParts(inner.parts).length >= 2) parts = inner.parts;
  }
  return { word: W, whole: whole ? { head: whole.head, source: whole.source, koshes: whole.koshes } : null, parts };
}
