// The Gujarati anuvaad of a vyutpatti, entry by entry: every kosh entry on the
// sheet ("देव - पुं. (दीव्यतीति देवः) … - દેવતા અર્થમાં.") gets its own
// explanation in plain Gujarati — the gender, the derivation or vigraha word
// by word, the meaning — printed directly under that entry, and one closing
// line on what the vishay means as a whole.
//
// One entry, one anuvaad (Maharaj Saheb, 6 Nov 2026, on the देवपर्षदा sheet:
// "above there are 6 words, below only 2"). The anuvaad used to be made from
// the reader lines, which keep one line per word, and kept one point per
// headword, so देव from three koshes came out once. Now each entry is asked
// for on its own; identical entries (same text, same vishay) are asked once
// and kept in memory, so a second run of the same vishay costs nothing.
//
// The reader never sees which service writes it.

import { CHAT_MODELS, SarvamChatError, chat } from "@/lib/sarvam-chat";
import { AnuvaadError, parseAnuvaad } from "@/lib/anuvaad/translate";

/** One entry of the vyutpatti: its word and the rest of what the sheet prints for it. */
export type VyutpattiEntryInput = { head: string; body: string };

export type VyutpattiAnuvaad = {
  /** The anuvaad of each entry, in the order sent; "" where it could not be made. */
  entries: string[];
  /** What the vishay means, put together from the entries ("" if it could not be made). */
  closing: string;
  /** How many entries are left without an anuvaad. */
  failed: number;
};

const RULES = `You write the Gujarati anuvaad of one line of a Sanskrit vyutpatti sheet for a Jain ācārya. The line gives a word, its gender, its derivation as printed in a Sanskrit kosh (or a samāsa vigraha), and its meaning. Sanskrit grammar, never Hindi.

Write one point that explains in plain Gujarati exactly what the line says:
- the gender: पुं. = પુંલિંગ, स्त्री. = સ્ત્રીલિંગ, न. = નપુંસકલિંગ, त्रि. = ત્રણે લિંગમાં, अव्य. = અવ્યય;
- the derivation or vigraha, quoted in Devanagari in single quotes and then explained word by word in Gujarati: what it says the word means (इति = "તે", भावः = ભાવ, कर्म = કર્મ, न = નહીં), and the dhātu and pratyaya only when the line prints them with a + ("चुर्+अच्" = ચુર્ ધાતુને અચ્ પ્રત્યય) — never name a dhātu or pratyaya the line does not print;
- the meaning the line gives, after "અર્થ:". When the line lists many senses, give the sense that fits the vishay first and the rest briefly.
Say only what the line says. Leave out citations, sūtra numbers and other words. Never write that a word "comes from itself". Do not name the kosh. No synonyms stacked in brackets. The label is the word itself in Devanagari, exactly as given.
Quote the derivation exactly as printed, in Devanagari; write the explanation in Gujarati letters. Never mix the two scripts inside one word.

FORM (exactly this markup, one line, nothing before or after it):
- **<word>:** <explanation>`;

const EXAMPLE_USER = `Vishay: अचौर्य
Line: चोर - पुं. (चोरयतीति चुर्+अच्) - ચોરી કરનાર, ચોર અર્થમાં.

Write the Gujarati anuvaad of this line in the form given.`;

const EXAMPLE_ANSWER = `- **चोर:** પુંલિંગ. 'चोरयति इति चोरः' — જે ચોરી કરે છે તે ચોર; ચુર્ ધાતુને અચ્ પ્રત્યય લાગીને બને છે. અર્થ: ચોરી કરનાર, ચોર.`;

const CLOSING_RULES = `You write, in plain Gujarati, one short paragraph (one to three sentences) saying what a Sanskrit vishay means, put together only from the meanings of its words given below. Sanskrit grammar, never Hindi. Write it all in Gujarati letters, the vishay too (never mix Devanagari and Gujarati letters inside one word). No labels, no bullets, no markup, no kosh names; just the paragraph.`;

const CLOSING_EXAMPLE_USER = `Vishay: अचौर्य

चोर: ચોરી કરનાર, ચોર
चौर्य: ચોરનો ભાવ અથવા કર્મ, ચોરી
अचौर्य: જે ચોરી નથી તે`;

const CLOSING_EXAMPLE_ANSWER = `અચૌર્ય એટલે ચોરનું કર્મ — ચોરી — ન કરવી તે.`;

/** Letters of Indian scripts other than Devanagari and Gujarati. */
const OTHER_SCRIPTS = /[ঀ-੿଀-෿]/;

/** Devanagari and Gujarati letters run together in one word ("षત્વम्", "દેવપर्षદા"); the dandas are shared and do not count. */
const MIXED_WORD = /[\u0900-\u0963\u0966-\u097f][\u0a80-\u0aff]|[\u0a80-\u0aff][\u0900-\u0963\u0966-\u097f]/g;
const mixedWords = (text: string) => (text.match(MIXED_WORD) ?? []).length;

const DEV_LETTER = /[ऀ-ॣ०-ॿ]/g;
const GUJ_LETTER = /[઀-૿]/g;
const toGujarati = (word: string) => word.replace(DEV_LETTER, (c) => String.fromCharCode(c.charCodeAt(0) + 0x180));
const toDevanagari = (word: string) => word.replace(GUJ_LETTER, (c) => String.fromCharCode(c.charCodeAt(0) - 0x180));

/**
 * A word still written half in one script and half in the other is written
 * wholly in the script most of its letters are in (the two blocks run in
 * parallel); with { gujarati: true } every Devanagari letter becomes Gujarati.
 */
function unmix(text: string, options: { gujarati?: boolean } = {}) {
  if (options.gujarati) return toGujarati(text);
  return text.replace(/[^\s'"‘’“”—–,;:.()[\]!?]+/g, (word) => {
    const dev = (word.match(DEV_LETTER) ?? []).length;
    const guj = (word.match(GUJ_LETTER) ?? []).length;
    if (!dev || !guj) return word;
    return guj >= dev ? toGujarati(word) : toDevanagari(word);
  });
}

// ------------------------------------------------------------ cache

const CACHE_MAX = 2000;
const cache = new Map<string, string>();

function remember(key: string, value: string) {
  cache.delete(key);
  cache.set(key, value);
  while (cache.size > CACHE_MAX) cache.delete(cache.keys().next().value as string);
}

// ------------------------------------------------------------ input

/** The kosh reference at the end ("(शब्दरत्नमहोदधि भाग-2, पृ. 868)" or "[…]") and the check note are not translated. */
export function entryForTranslation(entry: VyutpattiEntryInput): VyutpattiEntryInput {
  const body = String(entry.body ?? "")
    .normalize("NFC")
    .replace(/^\s*-\s*/, "")
    .replace(/\s*\(પાના સાથે ચકાસો\)/g, "")
    .replace(/\s*\[[^\]]*\]\s*$/, "")
    .replace(/\s*\([^()]*(?:पृ\.|PDF)[^()]*\)(?=\s*(?:-|$))/g, "")
    // sūtra numbers are not explained (Maharaj Saheb's marks): "(उणा-८९७)", "५।१।५०॥"
    .replace(/\s*\(\s*उणा[^()]*\)/g, "")
    .replace(/\s*[०-९0-9]+(?:\s*।\s*[०-९0-9]+)+\s*॥?/g, "")
    // a sūtra quoted after the derivation (“षितोऽङ्” ॥५।३।१०७), whose number went above
    .replace(/,?\s*[“"][^”"]{1,80}[”"]\s*॥/g, "")
    // AVPK's cross-references and list marks: "[क्षान्ति शि. २७]", "द्र० अचलाशब्दः", "☐", "-૩૯૧-"
    .replace(/\s*\[[^\]]{1,60}\]/g, "")
    .replace(/\s*द्र[०.]\s*\S+/g, "")
    .replace(/[☐□■●]/g, "")
    .replace(/-[૦-૯०-९0-9]{1,4}-/g, "-")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 1200);
  return { head: String(entry.head ?? "").normalize("NFC").trim(), body };
}

// ------------------------------------------------------------ one entry

/** The model's point as plain text: the label and markup taken off, an unprinted dhātu dropped. */
function tidyPoint(content: string, entry: VyutpattiEntryInput) {
  const blocks = parseAnuvaad(content.trim());
  const block = blocks.find((b) => b.kind === "point") ?? blocks.find((b) => b.kind === "para");
  if (!block || !("text" in block)) return "";
  let text = block.text.trim();
  // a label written into the text ("देव: …") when the model left the stars out
  const rest = text.startsWith(entry.head) ? text.slice(entry.head.length) : "";
  if (/^\s*[:：]/.test(rest)) text = rest.replace(/^\s*[:：]\s*/, "");
  if (!/\+/.test(entry.body)) {
    text = text.replace(/;?\s*[^;.।]*ધાતુ(?:ઓ)?(?:ને|માંથી)[^;.।]*(?:પ્રત્યય[^;.।]*)?(?=[;.।]|$)/g, "").replace(/\s+\./g, ".");
  }
  if (/^<.*>$/.test(text)) return "";
  return keepPrintedDerivation(text, entry).trim();
}

/**
 * The derivation is quoted exactly as the kosh prints it. When the model's
 * quote differs ('क्षमते इति ક્ષમા' for the printed "क्षमणं क्षमा"), the
 * printed one is put back.
 */
export function keepPrintedDerivation(text: string, entry: VyutpattiEntryInput) {
  const printed = entry.body.match(/\(([^()]{2,200})\)/)?.[1]?.trim();
  if (!printed || !/[\u0900-\u097f]/.test(printed)) return text;
  const quote = text.match(/'([^']{1,240})'/);
  if (!quote) return text;
  const squash = (value: string) => value.replace(/[\s+,;:।॥“”"'-]/g, "");
  if (squash(quote[1]) === squash(printed)) return text;
  return text.replace(quote[0], `'${printed}'`);
}

async function translateEntry(vishay: string, entry: VyutpattiEntryInput, fresh: boolean): Promise<string> {
  const user = `Vishay: ${vishay}\nLine: ${entry.head} - ${entry.body}\n\nWrite the Gujarati anuvaad of this line in the form given.`;
  const key = `entry\u0000${user}`;
  if (!fresh && cache.has(key)) return cache.get(key)!;
  let lastError: unknown = null;
  // A reading with letters of both scripts in one word is tried again; the cleanest is kept.
  let best: { text: string; mixed: number } | null = null;
  for (let attempt = 0; attempt < 3 && !(best && best.mixed === 0); attempt += 1) {
    try {
      const { content } = await chat({
        model: CHAT_MODELS.fast,
        temperature: attempt || fresh ? 0.3 : 0,
        maxTokens: 900,
        messages: [
          { role: "system", content: RULES },
          { role: "user", content: EXAMPLE_USER },
          { role: "assistant", content: EXAMPLE_ANSWER },
          { role: "user", content: user },
        ],
      });
      // a Telugu "న" once turned up inside हिंसनं
      if (OTHER_SCRIPTS.test(content)) {
        lastError = new AnuvaadError("The anuvaad came back in mixed letters.");
        continue;
      }
      const text = tidyPoint(content, entry);
      // the example copied for another word
      if (!text || (entry.head !== "चोर" && text.startsWith("પુંલિંગ. 'चोरयति"))) {
        lastError = new AnuvaadError("The anuvaad came back incomplete.");
        continue;
      }
      const mixed = mixedWords(text);
      if (!best || mixed < best.mixed) best = { text, mixed };
    } catch (error) {
      lastError = error;
      if (error instanceof SarvamChatError && error.status >= 400 && error.status < 500 && error.status !== 429) break;
    }
  }
  if (best) {
    const text = unmix(best.text);
    remember(key, text);
    return text;
  }
  console.error("vyutpatti anuvaad of an entry failed:", lastError instanceof Error ? lastError.message : lastError);
  return "";
}

// ------------------------------------------------------------ the closing line

async function translateClosing(vishay: string, entries: VyutpattiEntryInput[], fresh: boolean): Promise<string> {
  // each word once, with its meaning (the part after the last " - ")
  const seen = new Set<string>();
  const words: string[] = [];
  for (const e of entries) {
    const meaning = e.body.split(" - ").pop()?.replace(/\s*અર્થમાં\.?$/u, "").trim() ?? "";
    const line = `${e.head}: ${meaning.slice(0, 300)}`;
    if (seen.has(line)) continue;
    seen.add(line);
    words.push(line);
  }
  const user = [`Vishay: ${vishay}`, "", ...words].join("\n");
  const key = `closing\u0000${user}`;
  if (!fresh && cache.has(key)) return cache.get(key)!;
  let best: { text: string; mixed: number } | null = null;
  for (let attempt = 0; attempt < 3 && !(best && best.mixed === 0); attempt += 1) {
    try {
      const { content } = await chat({
        model: CHAT_MODELS.fast,
        temperature: attempt || fresh ? 0.3 : 0,
        maxTokens: 500,
        messages: [
          { role: "system", content: CLOSING_RULES },
          { role: "user", content: CLOSING_EXAMPLE_USER },
          { role: "assistant", content: CLOSING_EXAMPLE_ANSWER },
          { role: "user", content: user },
        ],
      });
      const text = content
        .replace(/[*#`]/g, "")
        .replace(/^\s*[-•]\s*/gm, "")
        .split(/\n+/)
        .map((l) => l.trim())
        .filter(Boolean)
        .pop() ?? "";
      if (!text || OTHER_SCRIPTS.test(text) || !/[઀-૿]/.test(text)) continue;
      if (vishay !== "अचौर्य" && /^અચૌર્ય એટલે/.test(text)) continue;
      const mixed = mixedWords(text);
      if (!best || mixed < best.mixed) best = { text, mixed };
    } catch (error) {
      if (error instanceof SarvamChatError && error.status >= 400 && error.status < 500 && error.status !== 429) break;
    }
  }
  const text = best ? unmix(best.text, { gujarati: true }) : "";
  if (text) remember(key, text);
  return text;
}

// ------------------------------------------------------------ all of it

const WORKERS = 3;

export async function translateVyutpatti(vishay: string, input: VyutpattiEntryInput[], options: { fresh?: boolean } = {}): Promise<VyutpattiAnuvaad> {
  const entries = input.map(entryForTranslation);
  const usable = entries.filter((e) => e.head && e.body);
  if (!usable.length) throw new AnuvaadError("There is no vyutpatti to translate.", 400);
  const fresh = Boolean(options.fresh);

  // Identical entries are asked once, then shared.
  const pending = new Map<string, Promise<string>>();
  const jobs: Array<() => Promise<string>> = [];
  const results: Array<Promise<string>> = entries.map((e) => {
    if (!e.head || !e.body) return Promise.resolve("");
    const id = `${e.head}\u0000${e.body}`;
    let job = pending.get(id);
    if (!job) {
      job = new Promise<string>((resolve) => jobs.push(() => translateEntry(vishay, e, fresh).then((t) => (resolve(t), t))));
      pending.set(id, job);
    }
    return job;
  });
  // A few at a time, so a long vishay does not flood the service.
  const run = async () => {
    for (let job = jobs.shift(); job; job = jobs.shift()) await job();
  };
  const closing = translateClosing(vishay, usable, fresh);
  await Promise.all(Array.from({ length: Math.min(WORKERS, jobs.length) }, run));
  const texts = await Promise.all(results);

  const failed = entries.filter((e, i) => e.head && e.body && !texts[i]).length;
  if (failed === usable.length) throw new AnuvaadError("The Gujarati anuvaad could not be prepared just now. Please try again.");
  return { entries: texts, closing: await closing, failed };
}
