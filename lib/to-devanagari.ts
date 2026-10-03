// Text typed in Gujarati or Roman letters, written in Devanagari, for the
// inputs that need Devanagari (vyutpatti looks up Hindi-lipi words only).
//
// Gujarati maps letter for letter: Unicode laid the Gujarati block out like
// the Devanagari one, 0x180 apart (ળ → ळ, ૐ → ॐ, ૧ → १). Roman letters are
// read by Sanskrit rules (lib/roman-sanskrit), taking each word's likeliest
// reading; the reader sees it before anything is looked up and can correct it.

import { romanWordReadings } from "@/lib/roman-sanskrit";

const GUJARATI = /[઀-૿]/u;
const ROMAN = /[a-zāīūṛṝḷṭḍṇśṣṃṁḥñṅ]/i;

export function gujaratiToDevanagari(text: string) {
  return String(text ?? "").replace(/[઀-૿]/gu, (ch) => {
    const code = ch.codePointAt(0)! - 0x180;
    return String.fromCodePoint(code);
  });
}

/** Each run of Roman letters read as its likeliest Devanagari word; everything else kept. */
export function romanToDevanagari(text: string) {
  return String(text ?? "").replace(/[a-zāīūṛṝḷṭḍṇśṣṃṁḥñṅ'’]+/giu, (word) => romanWordReadings(word, 1)[0]?.text ?? word);
}

/** Whether the text has Gujarati or Roman letters that would be converted. */
export function needsDevanagari(text: string) {
  return GUJARATI.test(text) || ROMAN.test(text);
}

/** Gujarati and Roman letters written in Devanagari; Devanagari, digits and punctuation unchanged. */
export function toDevanagari(text: string) {
  return romanToDevanagari(gujaratiToDevanagari(text)).normalize("NFC");
}
