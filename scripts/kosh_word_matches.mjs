// Dictionary-style word search over the three koshes (Apte, Shabda Ratna
// Mahodadhi 1–3, Abhidhan Vyutpatti Prakriya Kosh 1–2): a page matches a word
// only where a whole word on it is the word itself or the word followed by a
// Sanskrit case ending (देव, देवः, देवम्, देवेन…). The word inside or at the
// head of a compound (देवपर्षदा, महादेव), a different word that shares its
// letters (देवता), a changed stem (पर्षद् for पर्षदा) and a grammar label
// (the "स्त्री." after a headword) do not count.
//
//   node --env-file=.env scripts/kosh_word_matches.mjs <out.json>
import { writeFileSync } from "node:fs";
import { createClient } from "@libsql/client";
import { foldSanskrit, foldSanskritText, sanskritQueryForms, isGrammarLabelAt } from "../lib/sanskrit-fold.mjs";

export const KOSHES = [
  { key: "375", label: "Shivram Apte - Sanskrit-Hindi Shabdakosh" },
  { key: "380", label: "Shabda Ratna Mahodadhi - Part 1" },
  { key: "381", label: "Shabda Ratna Mahodadhi - Part 2" },
  { key: "382", label: "Shabda Ratna Mahodadhi - Part 3" },
  { key: "370", label: "Abhidhan Vyutpatti Prakriya Kosh - Part 1" },
  { key: "371", label: "Abhidhan Vyutpatti Prakriya Kosh - Part 2" },
];

// The words of search.pdf (Kingston SSD), split where it marks them.
export const WORDS = [
  ["parshada", "पर्षदा"], ["bhavanapati", "भवनपति"], ["deva", "देव"], ["vyantara", "व्यंतर"],
  ["jyotishka", "ज्योतिष्क"], ["vaimanika", "वैमानिक"], ["devi", "देवी"], ["agni", "अग्नि"],
  ["khunamambetheli", "खूणामांबेठेली"], ["ishana", "ईशान"], ["nairritya", "नैऋत्य"], ["vayavya", "वायव्य"],
  ["shramana", "श्रमण"], ["shramani", "श्रमणी"], ["manushya", "मनुष्य"], ["purusha", "पुरुष"], ["stri", "स्त्री"],
];

/** The word and its case forms that begin with it, folded like the page text. */
export function beginsWithForms(word) {
  const head = foldSanskrit(word);
  const forms = new Set(sanskritQueryForms(word).filter((f) => f.startsWith(head)));
  forms.add(head);
  // The accusative plural ends in a dental न् (देवान्, मनुष्यान्); the form
  // generator retroflexes it after र/ष, so keep both spellings.
  for (const f of [...forms]) if (f.endsWith("ण्")) forms.add(`${f.slice(0, -2)}न्`);
  return forms;
}

const WORD_RUN = /[\p{L}\p{M}]+/gu;
const GUJARATI = /[઀-૿]/;

export function matchPage(content, forms, head) {
  const { text, starts, ends, source } = foldSanskritText(String(content ?? ""), true);
  const hits = [];
  for (const m of text.matchAll(WORD_RUN)) {
    const token = m[0];
    if (!forms.has(token)) continue;
    const start = starts[m.index];
    const end = ends[m.index + token.length - 1];
    if (isGrammarLabelAt(source, start, end, token)) continue;
    // Gujarati prose takes no Sanskrit case endings: there દેવો is "to give",
    // not देवो. A word written in Gujarati script counts only as the bare word.
    if (token !== head && GUJARATI.test(source.slice(start, end))) continue;
    hits.push({ form: token, text: source.slice(start, end) });
  }
  return hits;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
  const pages = {};
  for (const k of KOSHES) {
    const r = await turso.execute({ sql: "SELECT page_number, content FROM ocr_pages WHERE granth_key = ? ORDER BY page_number", args: [k.key] });
    pages[k.key] = r.rows.map((row) => ({ page: Number(row.page_number), content: String(row.content ?? "") }));
  }
  const out = WORDS.map(([slug, word], i) => {
    const forms = beginsWithForms(word);
    const perKosh = KOSHES.map((k) => {
      const hits = [];
      for (const p of pages[k.key]) {
        const found = matchPage(p.content, forms, foldSanskrit(word));
        if (found.length) hits.push({ page: p.page, count: found.length, forms: found.map((f) => f.form) });
      }
      return { key: k.key, label: k.label, hits };
    });
    const occ = perKosh.reduce((n, k) => n + k.hits.reduce((a, h) => a + h.count, 0), 0);
    const pg = perKosh.reduce((n, k) => n + k.hits.length, 0);
    console.log(`${String(i + 1).padStart(2)}. ${word.padEnd(14)} ${String(occ).padStart(5)} occ / ${String(pg).padStart(4)} pages  [${perKosh.map((k) => k.hits.reduce((a, h) => a + h.count, 0)).join(" | ")}]`);
    return { n: i + 1, slug, word, forms: [...forms], occurrences: occ, pages: pg, perKosh };
  });
  writeFileSync(process.argv[2], JSON.stringify(out, null, 1));
}
