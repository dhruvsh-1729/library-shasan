import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { gujaratiToDevanagari } from "@/lib/to-devanagari";
import { spellRomanWords } from "@/lib/vyutpatti/spell";

// The vishay list typed in Gujarati or Roman letters, written in Devanagari.
// Gujarati maps letter for letter; Roman words are spelled by
// lib/vyutpatti/spell, which also sends back up to five choices per word.

const MAX_TEXT = 4000;
const ROMAN_WORD = /[a-zāīūṛṝḷṭḍṇśṣṃṁḥñṅ'’]+/giu;

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const text = gujaratiToDevanagari(String((req.body ?? {}).text ?? "").normalize("NFC").slice(0, MAX_TEXT));
  const words = [...new Set(text.match(ROMAN_WORD) ?? [])].slice(0, 120);
  const { best, options } = await spellRomanWords(words);
  return res.status(200).json({
    text: text.replace(ROMAN_WORD, (w) => best.get(w) ?? w).normalize("NFC"),
    options: Object.fromEntries(options),
  });
}

export default protectApi(handler, PERMISSIONS.libraryRead);
