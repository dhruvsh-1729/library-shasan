// The Gujarati anuvaad of a vyutpatti: each word's derivation from the koshes
// ("चोर - पुं. (चोरयतीति चुर्+अच्) - ચોરી કરનાર …") explained in plain
// Gujarati — the dhātu and pratyaya, the vigraha, the gender, the meaning —
// and a closing line on what the vishay means as a whole. It goes under the
// vyutpatti on the page and at the end of its PDF.
//
// The reader never sees which service writes it.

import { CHAT_MODELS, SarvamChatError, chat } from "@/lib/sarvam-chat";
import { AnuvaadError, type Anuvaad, type AnuvaadBlock, parseAnuvaad, toMarkup } from "@/lib/anuvaad/translate";

export type VyutpattiLineInput = { head: string; body: string };

const EXAMPLE_USER = `Vishay: अचौर्य

चोर - पुं. (चोरयतीति चुर्+अच्) - ચોરી કરનાર, ચોર અર્થમાં.
चौर्य - न. (चौरस्य भावः कर्म वा) - ચોરી અર્થમાં.
अचौर्य - न चौर्यं इति अचौर्यम्।

Write the Gujarati anuvaad in the form given.`;

const EXAMPLE_ANSWER = `# अचौर्य — વ્યુત્પત્તિનો ગુજરાતી અનુવાદ
- **चोर:** પુંલિંગ. 'चोरयति इति चोरः' — જે ચોરી કરે છે તે ચોર; ચુર્ ધાતુને અચ્ પ્રત્યય લાગીને બને છે. અર્થ: ચોરી કરનાર, ચોર.
- **चौर्य:** નપુંસકલિંગ. 'चौरस्य भावः कर्म वा' — ચોરનો ભાવ અથવા ચોરનું કર્મ તે ચૌર્ય. અર્થ: ચોરી.
- **अचौर्य:** 'न चौर्यं इति अचौर्यम्' — જે ચોરી નથી તે અચૌર્ય. અર્થ: ચોરી ન કરવી.
અચૌર્ય એટલે ચોરનું કર્મ — ચોરી — ન કરવી તે.`;

const RULES = `You write the Gujarati anuvaad of a Sanskrit vyutpatti sheet for a Jain ācārya. Each line gives a word, its gender, its derivation as printed in a Sanskrit kosh, and its meaning. Sanskrit grammar, never Hindi.

For each line write one point that explains in plain Gujarati exactly what the line says:
- the gender: पुं. = પુંલિંગ, स्त्री. = સ્ત્રીલિંગ, न. = નપુંસકલિંગ, त्रि. = ત્રણે લિંગમાં, अव्य. = અવ્યય;
- the derivation or vigraha, quoted in Devanagari in single quotes and then explained word by word in Gujarati: what it says the word means (इति = "તે", भावः = ભાવ, कर्म = કર્મ, न = નહીં), and the dhātu and pratyaya only when the line prints them with a + ("चुर्+अच्" = ચુર્ ધાતુને અચ્ પ્રત્યય) — never name a dhātu or pratyaya the line does not print;
- the meaning the line gives, after "અર્થ:".
Say only what the line says. When a line holds a long kosh entry, explain only the part about this word's derivation and meaning; leave out citations, other words and other senses. Never write that a word "comes from itself". Do not name the kosh. The label is the word itself in Devanagari, exactly as given.

Write exactly one point for each line, labelled with that line's word; never a point for any other word.
Then one closing paragraph: what this vishay means, put together from these words only.

FORM (exactly this markup, nothing before or after it):
# <the vishay in Devanagari> — વ્યુત્પત્તિનો ગુજરાતી અનુવાદ
- **<word>:** <explanation>
<closing paragraph>`;

/** Letters of Indian scripts other than Devanagari and Gujarati. */
const OTHER_SCRIPTS = /[\u0980-\u0a7f\u0b00-\u0dff]/;

/** Gujarati letters written in Devanagari (the two blocks run in parallel), to match a label the model wrote in Gujarati. */
const toDevanagariLetters = (text: string) => text.replace(/[\u0a80-\u0aff]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0x180));

/**
 * What the model may not add: a point for a word that was not sent, a second
 * point for the same word, a dhātu or pratyaya the line does not print (the
 * example's "ચુર્ ધાતુને અચ્ પ્રત્યય" turned up under सत्य), the form's
 * placeholder, or more than one closing paragraph.
 */
function tidy(blocks: AnuvaadBlock[], lines: VyutpattiLineInput[], vishay: string): AnuvaadBlock[] {
  const byHead = new Map(lines.map((l) => [l.head.normalize("NFC"), l]));
  const seen = new Set<string>();
  const out: AnuvaadBlock[] = [];
  let closing: AnuvaadBlock | null = null;
  for (const b of blocks) {
    if (b.kind === "point") {
      const head = toDevanagariLetters(b.label).normalize("NFC");
      const line = byHead.get(head);
      if (!line || seen.has(head)) continue;
      seen.add(head);
      let text = b.text;
      if (!/\+/.test(line.body)) text = text.replace(/;?\s*[^;.।]*ધાતુ(?:ને|માંથી)[^;.।]*(?:પ્રત્યય[^;.।]*)?(?=[;.।]|$)/g, "").replace(/\s+\./g, ".");
      out.push({ ...b, label: line.head, text: text.trim() });
    } else if (b.kind === "para") {
      if (/^<.*>$/.test(b.text) || /closing paragraph/i.test(b.text)) continue;
      if (vishay.normalize("NFC") !== "अचौर्य" && /^અચૌર્ય એટલે ચોરનું કર્મ/.test(b.text)) continue;
      closing = b; // the last one stands
    } else if (b.kind === "title") {
      out.push({ kind: "title", text: `${vishay} — વ્યુત્પત્તિનો ગુજરાતી અનુવાદ` });
    }
  }
  if (closing) out.push(closing);
  return out;
}

export async function translateVyutpatti(vishay: string, lines: VyutpattiLineInput[]): Promise<Anuvaad> {
  // The kosh named at the end ("[शब्दरत्नमहोदधि भाग-2, पृ. 868]") is not translated.
  const usable = lines
    .map((l) => ({ head: l.head.trim(), body: l.body.replace(/^\s*-\s*/, "").replace(/\s*\[[^\]]*\]\s*$/, "").trim().slice(0, 900) }))
    .filter((l) => l.head && l.body);
  if (!usable.length) throw new AnuvaadError("There is no vyutpatti to translate.", 400);
  const user = [`Vishay: ${vishay}`, "", ...usable.map((l) => `${l.head} - ${l.body}`), "", "Write the Gujarati anuvaad in the form given."].join("\n");

  // Up to three tries; the best is kept: every word explained, no letters of another script
  // (a Telugu "న" once turned up inside हिंसनं).
  let best: { blocks: AnuvaadBlock[]; score: number } | null = null;
  let lastError: unknown = null;
  for (let attempt = 0; attempt < 3 && !(best && best.score >= usable.length); attempt += 1) {
    try {
      const { content } = await chat({
        model: CHAT_MODELS.fast,
        temperature: attempt ? 0.2 : 0,
        maxTokens: Math.min(6000, 600 + usable.length * 500),
        messages: [
          { role: "system", content: RULES },
          { role: "user", content: EXAMPLE_USER },
          { role: "assistant", content: EXAMPLE_ANSWER },
          { role: "user", content: user },
        ],
      });
      if (OTHER_SCRIPTS.test(content)) {
        lastError = new AnuvaadError("The anuvaad came back in mixed letters.");
        continue;
      }
      const blocks = tidy(parseAnuvaad(content.trim()), usable, vishay);
      const score = blocks.filter((b) => b.kind === "point").length;
      if (score && (!best || score > best.score)) best = { blocks, score };
      else if (!score) lastError = new AnuvaadError("The anuvaad came back incomplete.");
    } catch (error) {
      lastError = error;
      if (error instanceof SarvamChatError && error.status >= 400 && error.status < 500 && error.status !== 429) break;
    }
  }
  if (best) return { blocks: best.blocks, markup: toMarkup(best.blocks), literal: "" };
  console.error("vyutpatti anuvaad failed:", lastError instanceof Error ? lastError.message : lastError);
  throw new AnuvaadError("The Gujarati anuvaad could not be prepared just now. Please try again.");
}
