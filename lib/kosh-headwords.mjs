// The koshes a vyutpatti is looked up in, and how each one prints a headword,
// so scripts/build_kosh_headwords.mjs can index every entry's first line and
// the vyutpatti page can go straight to it.
//
// Maharaj Saheb's order: the three koshes first (Abhidhan Vyutpatti Prakriya
// Kosh, Shabda Ratna Mahodadhi, Apte); only a word none of them has is looked
// up in the other four (Agamic Vyutpatti Kosh, Abhidhan Rajendra Kosh, Paia
// Sadda Mahannavo, Alpaparichit Saiddhantik Shabdakosh).
//
// Plain JS, shared by the app and the index script.

import { foldSanskrit } from "./sanskrit-fold.mjs";

/**
 * format: how an entry's first line looks (see HEADWORD_PATTERNS).
 * language: what the meanings are written in.
 * readFromScan: the OCR read this kosh's Gujarati as Devanagari letters
 *   ("योरी ४२नार" for "ચોરી કરનાર"), so its entries are read from the page scan.
 */
export const VYUTPATTI_KOSHES = [
  { key: "370", tier: 1, family: "avpk", format: "avpk", language: "gu", title: "अभिधानव्युत्पत्तिप्रक्रियाकोश", part: "1" },
  { key: "371", tier: 1, family: "avpk", format: "avpk", language: "gu", title: "अभिधानव्युत्पत्तिप्रक्रियाकोश", part: "2" },
  { key: "380", tier: 1, family: "srm", format: "srm", language: "gu", title: "शब्दरत्नमहोदधि", part: "1", readFromScan: true },
  { key: "381", tier: 1, family: "srm", format: "srm", language: "gu", title: "शब्दरत्नमहोदधि", part: "2", readFromScan: true },
  { key: "382", tier: 1, family: "srm", format: "srm", language: "gu", title: "शब्दरत्नमहोदधि", part: "3", readFromScan: true },
  { key: "375", tier: 1, family: "apte", format: "apte", language: "hi", title: "संस्कृत-हिन्दी शब्दकोश (आप्टे)" },
  { key: "395", tier: 2, family: "agamic", format: "agamic", language: "sa", title: "आगमिकव्युत्पत्तिकोश" },
  { key: "384", tier: 2, family: "ark", format: "ark", language: "sa", title: "अभिधानराजेन्द्रकोष", part: "1" },
  { key: "385", tier: 2, family: "ark", format: "ark", language: "sa", title: "अभिधानराजेन्द्रकोष", part: "2" },
  { key: "386", tier: 2, family: "ark", format: "ark", language: "sa", title: "अभिधानराजेन्द्रकोष", part: "3" },
  { key: "387", tier: 2, family: "ark", format: "ark", language: "sa", title: "अभिधानराजेन्द्रकोष", part: "4" },
  { key: "389", tier: 2, family: "ark", format: "ark", language: "sa", title: "अभिधानराजेन्द्रकोष", part: "6" },
  { key: "390", tier: 2, family: "ark", format: "ark", language: "sa", title: "अभिधानराजेन्द्रकोष", part: "7" },
  { key: "379", tier: 2, family: "psm", format: "psm", language: "hi", title: "पाइयसद्दमहण्णवो" },
  { key: "372", tier: 2, family: "alpa", format: "alpa", language: "sa", title: "अल्पपरिचितसैद्धान्तिकशब्दकोष", part: "1" },
  { key: "373", tier: 2, family: "alpa", format: "alpa", language: "sa", title: "अल्पपरिचितसैद्धान्तिकशब्दकोष", part: "2-3" },
  { key: "374", tier: 2, family: "alpa", format: "alpa", language: "sa", title: "अल्पपरिचितसैद्धान्तिकशब्दकोष", part: "4-5" },
];

const BY_KEY = new Map(VYUTPATTI_KOSHES.map((kosh, index) => [kosh.key, { ...kosh, rank: index }]));

export function vyutpattiKosh(granthKey) {
  return BY_KEY.get(String(granthKey ?? "")) ?? null;
}

/** How a source is cited: "शब्दरत्नमहोदधि भाग-2". */
export function koshCitationName(kosh) {
  return kosh.part ? `${kosh.title} भाग-${kosh.part}` : kosh.title;
}

// ------------------------------------------------------------------ headword lines

const WORD = "[\\u0900-\\u0963\\u0970-\\u097f\\u0a80-\\u0aff]{2,}";
// A gender or part-of-speech label after a headword ("पुं", "स्त्री", "त्रि", "न",
// "अव्य", "सक" …). Gujarati-script labels appear in AVPK ("સ્ત્રી").
const LABEL =
  "(?:पुं|पु|नपुं|न|स्त्री|स्री|त्रि|वि|अ|अव्य|अव्य०|क्ली|उभ|सक|अक|क्रि|देशी|दे|ना|પું|પુ|સ્ત્રી|ન|ત્રિ|અવ્ય)";
const END = "(?=[\\s.०॰)\\-–—,:।]|$)";
const LEAD = "^[\\s\\-–—*☐•'‘’\"“”(\\[]*";

/**
 * One pattern per kosh format; group 1 is the headword as printed, group 2
 * (Prakrit koshes) its Sanskrit form.
 *   avpk    चोर-पुं-३८१-ચોરી કરનાર          'આટી'–સ્ત્રી–૧૩૩૮–
 *   srm     चोर पुं. (चोरयतीति चुर्+अच्)     देवेश, देवेश्वर पुं.
 *   apte    चौर्यम् (नपुं.) [चोर+ष्यञ्]
 *   agamic  ११९. उपासकाः - साधूनुपासते…
 *   ark     अकप्पट्ठिय-पुं० (अकल्पस्थित)
 *   psm     उव्वरिअ न. [ अपवरिका]
 *   alpa    अतिक्कीलावासो-अतिक्रीडावासः, …
 */
const HEADWORD_PATTERNS = {
  avpk: new RegExp(`${LEAD}(${WORD})['’]?\\s*[-–—]\\s*${LABEL}${END}`, "u"),
  srm: new RegExp(`${LEAD}(${WORD})(?:\\s*,\\s*${WORD})*\\s+${LABEL}\\s*[.०]`, "u"),
  apte: new RegExp(`${LEAD}(${WORD})\\s*(?:,\\s*${WORD}\\s*)?\\(\\s*(?:भू\\.\\s*क\\.\\s*कृ\\.|${LABEL})\\s*[.)]`, "u"),
  agamic: new RegExp(`^\\s*[०-९0-9]+\\s*\\.\\s*(${WORD})\\s*(?:\\([^)]*\\)\\s*)?[-–—]`, "u"),
  ark: new RegExp(`${LEAD}(${WORD})\\s*[-–—]\\s*${LABEL}\\s*[०॰.]?\\s*\\(\\s*(${WORD})`, "u"),
  psm: new RegExp(`${LEAD}(${WORD})\\s+${LABEL}\\s*\\.?\\s*\\[\\s*(${WORD})`, "u"),
  alpa: new RegExp(`${LEAD}(${WORD})\\s*[-–—]\\s*(${WORD})`, "u"),
};

/** The spellings a printed headword is looked up by: देवः → देव, चौर्यम् → चौर्य, आत्मन् → आत्मन् and आत्म. */
export function headwordKeys(head) {
  const folded = foldSanskrit(String(head ?? "").normalize("NFC"));
  const keys = new Set([folded]);
  const strip = [["ाः", ""], ["ः", ""], ["म्", ""], ["ं", ""], ["ा", ""]];
  for (const [ending, replacement] of strip) {
    if (folded.length > ending.length + 1 && folded.endsWith(ending)) {
      // ā-stems (हिंसा, कथा) keep their ā: only drop it as the nominative of a
      // masculine plural (उपासकाः) or where it was the accusative/neuter mark.
      if (ending === "ा") continue;
      keys.add(folded.slice(0, -ending.length) + replacement);
    }
  }
  if (folded.endsWith("न्")) keys.add(folded.slice(0, -2));
  if (folded.endsWith("िन्")) keys.add(`${folded.slice(0, -3)}ी`);
  if (folded.endsWith("स्")) keys.add(folded.slice(0, -2));
  return [...keys].filter((k) => k.length >= 2);
}

/**
 * Every entry's first line on a kosh page. `line` is the index in
 * content.split("\n"); `head` the headword as printed; `sanskrit` the Sanskrit
 * form a Prakrit kosh gives in brackets.
 */
export function headwordLines(format, content) {
  const pattern = HEADWORD_PATTERNS[format];
  if (!pattern) return [];
  const out = [];
  String(content ?? "")
    .normalize("NFC")
    .split("\n")
    .forEach((text, line) => {
      const m = text.match(pattern);
      if (!m) return;
      out.push({ line, head: m[1], sanskrit: m[2] ?? null });
    });
  return out;
}

/** The keys an entry is indexed under: its headword's and, for a Prakrit kosh, its Sanskrit form's. */
export function entryKeys(entry) {
  const keys = new Set(headwordKeys(entry.head));
  if (entry.sanskrit) for (const key of headwordKeys(entry.sanskrit)) keys.add(key);
  return [...keys];
}
