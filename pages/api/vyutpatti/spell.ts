import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { romanWordReadings } from "@/lib/roman-sanskrit";
import { gujaratiToDevanagari } from "@/lib/to-devanagari";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { headwordTiers } from "@/lib/vyutpatti/lookup";

// The vishay list typed in Gujarati or Roman letters, written in Devanagari.
// Gujarati maps letter for letter; for a Roman word the likeliest reading that
// is a headword of the three koshes wins ("hinsa" → हिंसा, not the other
// koshes' हिंस), then of the other koshes, else the likeliest reading. A reading
// that differs from the headword only in spelling takes the kosh's spelling
// (देवेंद्र → देवेन्द्र).

const MAX_TEXT = 4000;
const READINGS = 24;
const ROMAN_WORD = /[a-zāīūṛṝḷṭḍṇśṣṃṁḥñṅ'’]+/giu;

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const text = gujaratiToDevanagari(String((req.body ?? {}).text ?? "").normalize("NFC").slice(0, MAX_TEXT));
  const words = [...new Set(text.match(ROMAN_WORD) ?? [])].slice(0, 120);
  const readings = new Map(words.map((w) => [w, romanWordReadings(w, READINGS).map((r) => r.text)]));
  let known = new Map<string, { tier: number; head: string }>();
  try {
    known = await headwordTiers([...new Set([...readings.values()].flat())]);
  } catch {
    // no headword index: the likeliest readings still go back
  }
  const pick = (list: string[]) => {
    const hits = list.filter((r) => known.has(r));
    const top = hits.find((r) => known.get(r)!.tier === 1) ?? hits[0];
    if (!top) return list[0];
    const head = known.get(top)!.head;
    return foldSanskrit(head) === foldSanskrit(top) ? head : top;
  };
  const best = new Map([...readings].map(([w, list]) => [w, pick(list) ?? w]));
  return res.status(200).json({ text: text.replace(ROMAN_WORD, (w) => best.get(w) ?? w).normalize("NFC") });
}

export default protectApi(handler, PERMISSIONS.libraryRead);
