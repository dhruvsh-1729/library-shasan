// How a vyutpatti is written out, in the two layouts of Maharaj Saheb's
// sample (WhatsApp scan, 28 Sep 2026 17.19):
//
//   reader line   चोर - पुं. (चोरयतीति चुर्+अच्) - ચોરી કરનાર, ચોર અર્થમાં. [शब्दरत्नमहोदधि भाग-2, पृ. 868]
//                 अचौर्य - न चौर्यं इति अचौर्यम्। [समासविग्रह]
//   table row     Sr | व्यु. | शब्दरत्नमहोदधि भाग-1 | कायिक - त्रि. (कायस्येदं ठक् वा) (शब्दरत्नमहोदधि भाग-1, पृ. 574) | | 18-04-2023 18:33 શરીરથી … અર્થમાં
//
// The reader page is what goes to readers; the table is for internal use.

export type EntrySource = "kosh" | "ai" | "vigraha";

/** One kosh entry for one word, as read from the page. */
export type VyutpattiEntry = {
  id: string;
  word: string;
  granthKey: string;
  citation: string;
  tier: number;
  pdfPage: number;
  printedPage: string | null;
  pdfUrl: string | null;
  head: string;
  gender: string;
  derivation: string;
  /** The meanings as printed, in the kosh's own language. */
  meaning: string;
  /** The meaning in Gujarati (copied when printed in Gujarati). */
  meaningGu: string;
  /** Only the sense that fits the vishay, in Gujarati. */
  relevantGu: string;
  /** The entry's sense is the one the vishay uses. */
  fitsVishay: boolean;
  /** The printed derivation is that sense's. */
  derivationFits: boolean;
  /** The label printed for that sense, when senses differ in gender. */
  relevantGender: string;
  /** "scan" when read from the page image, "ocr" when from the OCR text. */
  readFrom: "scan" | "ocr";
  /** The derivation as read is also in the OCR text of the page. */
  checked: boolean;
};

/** A derivation no kosh has, written by the engine with the granth it rests on. */
export type AiDerivation = {
  word: string;
  gender: string;
  derivation: string;
  meaningGu: string;
  source: string;
};

export type ReaderLine = { head: string; body: string; source: EntrySource; note?: string };

export type TableRow = {
  granth: string;
  shastraPath: string;
  pubRem: string;
  inRem: string;
  source: EntrySource;
};

const GENDER_SCRIPT: Record<string, string> = {
  "પું": "पुं", "પુ": "पुं", "સ્ત્રી": "स्त्री", "ન": "न", "ત્રિ": "त्रि", "અવ્ય": "अव्य",
  पुंलिङ्ग: "पुं", पुंलिंग: "पुं", पुल्लिङ्ग: "पुं", स्त्रीलिङ्ग: "स्त्री", स्त्रीलिंग: "स्त्री",
  नपुंसकलिङ्ग: "न", नपुंसकलिंग: "न", त्रिलिङ्ग: "त्रि", त्रिलिंग: "त्रि", अव्यय: "अव्य",
};

/** A grammar label as the sample prints it: Devanagari, ending in a dot ("पुं.", "त्रि."). */
export function normalizeGender(value: string) {
  const raw = String(value ?? "").trim().replace(/[()[\]]/g, "").replace(/[०॰]$/, ".").replace(/म्$/, "").trim();
  if (!raw) return "";
  const bare = raw.replace(/\.+$/, "");
  const devanagari = GENDER_SCRIPT[bare] ?? bare;
  return `${devanagari}.`;
}

/** The derivation without its brackets or AVPK's leading star. */
export function normalizeDerivation(value: string) {
  let text = String(value ?? "").trim().replace(/^\*\s*/, "");
  for (;;) {
    const before = text;
    text = text.replace(/[\s।॥|.,;]+$/u, "").trim();
    if (/^[([][\s\S]*[)\]]$/u.test(text)) text = text.slice(1, -1).trim();
    if (text === before) break;
  }
  return text.replace(/\s+/g, " ");
}

function meaningPhrase(meaning: string) {
  const text = String(meaning ?? "").trim().replace(/[\s.,।;-]+$/u, "");
  if (!text) return "";
  return /અર્થમાં$/u.test(text) ? `${text}.` : `${text} અર્થમાં.`;
}

/** "पृ. 868"; a page with no printed number is cited by its PDF page. */
export function pageCitation(printedPage: string | null, pdfPage: number) {
  // A no-break space keeps "पृ. 868" on one line.
  return printedPage ? `पृ.\u00a0${printedPage}` : `PDF\u00a0पृ.\u00a0${pdfPage}`;
}

export function koshReference(entry: Pick<VyutpattiEntry, "citation" | "printedPage" | "pdfPage">) {
  return `${entry.citation}, ${pageCitation(entry.printedPage, entry.pdfPage)}`;
}

/** The headword the line starts with: the word looked up, not a case form of it. */
function lineHead(entry: VyutpattiEntry) {
  return entry.word || entry.head;
}

/** "पुं. (चोरयतीति चुर्+अच्)", or as much of it as the entry prints. */
function grammar(gender: string, derivation: string) {
  const d = normalizeDerivation(derivation);
  return [normalizeGender(gender), d ? `(${d})` : ""].filter(Boolean).join(" ");
}

export function readerLineForEntry(entry: VyutpattiEntry): ReaderLine {
  const fits = entry.fitsVishay;
  const meaning = meaningPhrase((fits && entry.relevantGu) || entry.meaningGu);
  const gender = (fits && entry.relevantGender) || entry.gender;
  const derivation = fits && !entry.derivationFits ? "" : entry.derivation;
  const body = [grammar(gender, derivation), meaning].filter(Boolean).join(" - ");
  return {
    head: lineHead(entry),
    body: ` - ${body} [${koshReference(entry)}]`,
    source: "kosh",
    note: entry.checked ? undefined : "Check against the page: the reading differs from the OCR text.",
  };
}

export function readerLineForAi(ai: AiDerivation): ReaderLine {
  const body = [grammar(ai.gender, ai.derivation), meaningPhrase(ai.meaningGu)].filter(Boolean).join(" - ");
  return {
    head: ai.word,
    body: ` - ${body}${ai.source ? ` [${ai.source}]` : ""}`,
    source: "ai",
    note: "Not in any kosh: written by AI. Verify before use.",
  };
}

export function readerLineForVigraha(vishay: string, vigraha: string): ReaderLine {
  const text = String(vigraha ?? "").trim().replace(/\s*।?\s*$/u, "");
  return { head: vishay, body: ` - ${text}। [समासविग्रह]`, source: "vigraha", note: "Samasa vigraha written by AI. Verify before use." };
}

/** "18-04-2023 18:33", India time, as the sample's In.Rem column prints it. */
export function remarkStamp(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  return `${get("day")}-${get("month")}-${get("year")} ${get("hour")}:${get("minute")}`;
}

export function tableRowForEntry(entry: VyutpattiEntry, stamp: string): TableRow {
  const head = grammar(entry.gender, entry.derivation);
  const flags = entry.checked ? "" : " (પાના સાથે ચકાસો)";
  return {
    granth: entry.citation,
    shastraPath: `${lineHead(entry)}${head ? ` - ${head}` : ""} (${koshReference(entry)})`,
    pubRem: "",
    inRem: `${stamp} ${meaningPhrase(entry.meaningGu)}${flags}`.trim(),
    source: "kosh",
  };
}

export function tableRowForAi(ai: AiDerivation, stamp: string): TableRow {
  const head = grammar(ai.gender, ai.derivation);
  return {
    granth: ai.source || "AI",
    shastraPath: `${ai.word}${head ? ` - ${head}` : ""}${ai.source ? ` (${ai.source})` : ""}`,
    pubRem: "",
    inRem: `${stamp} ${meaningPhrase(ai.meaningGu)} કોઈ કોશમાં નથી, AI દ્વારા; ચકાસવું.`.trim(),
    source: "ai",
  };
}

export function tableRowForVigraha(vishay: string, vigraha: string, samasa: string, stamp: string): TableRow {
  const text = String(vigraha ?? "").trim().replace(/\s*।?\s*$/u, "");
  return {
    granth: "समासविग्रह",
    shastraPath: `${vishay} - ${text}।${samasa ? ` (${samasa})` : ""}`,
    pubRem: "",
    inRem: `${stamp} સમાસવિગ્રહ AI દ્વારા; ચકાસવું.`,
    source: "vigraha",
  };
}

const FOLD_DROP = /[\s।॥.,;:!?'"‘’“”()[\]{}\-–—+*/|]/gu;

/** Share of the reading's letter pairs that are also in the page's OCR text (1 when there is nothing to check). */
export function agreement(reading: string, ocr: string) {
  const a = String(reading ?? "").normalize("NFC").replace(FOLD_DROP, "");
  const b = String(ocr ?? "").normalize("NFC").replace(FOLD_DROP, "");
  if (a.length < 3) return 1;
  const pairs = new Set<string>();
  for (let i = 0; i + 1 < b.length; i += 1) pairs.add(b.slice(i, i + 2));
  let hit = 0;
  let total = 0;
  for (let i = 0; i + 1 < a.length; i += 1) {
    total += 1;
    if (pairs.has(a.slice(i, i + 2))) hit += 1;
  }
  return total ? hit / total : 1;
}
