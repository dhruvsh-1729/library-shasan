// The Sanskrit koshes in the library, in the priority Maharaj Saheb set for
// vyutpatti: Abhidhan Vyutpatti Prakriya Kosh first, then Shabda Ratna
// Mahodadhi, then Apte. Kosh exports list them in this order too. Granth keys
// are Turso ocr_granths.granth_key.

export const KOSHES = [
  { key: "370", family: "avpk", label: "Abhidhan Vyutpatti Prakriya Kosh 1" },
  { key: "371", family: "avpk", label: "Abhidhan Vyutpatti Prakriya Kosh 2" },
  { key: "380", family: "srm", label: "Shabda Ratna Mahodadhi 1" },
  { key: "381", family: "srm", label: "Shabda Ratna Mahodadhi 2" },
  { key: "382", family: "srm", label: "Shabda Ratna Mahodadhi 3" },
  { key: "375", family: "apte", label: "Apte Sanskrit-Hindi Shabdakosh" },
];

/**
 * The kosh families, in priority order, with the bit each has in a lexicon
 * kosh mask. The bits are stored in data/compound-lexicon.json, so they never
 * change; "agamic" (395) is not one of the three and is not in KOSHES.
 */
export const KOSH_FAMILIES = [
  { family: "avpk", bit: 2, label: "Abhidhan Vyutpatti Prakriya Kosh" },
  { family: "srm", bit: 1, label: "Shabda Ratna Mahodadhi" },
  { family: "apte", bit: 4, label: "Apte" },
  { family: "agamic", bit: 8, label: "Agamic Vyutpatti Kosh" },
];

const RANK = new Map(KOSHES.map((kosh, index) => [kosh.key, index]));

/** A granth's place in a kosh export: koshes first in their order, every other granth after. */
export function koshRank(granthKey) {
  return RANK.get(String(granthKey ?? "")) ?? KOSHES.length;
}

/** The mask bit of a kosh granth, 0 for any other granth. */
export function koshBit(granthKey) {
  const kosh = KOSHES.find((k) => k.key === String(granthKey ?? ""));
  return KOSH_FAMILIES.find((f) => f.family === kosh?.family)?.bit ?? 0;
}

/** Names of the kosh families in a lexicon mask, in priority order. */
export function koshFamilyLabels(mask) {
  return KOSH_FAMILIES.filter((f) => (mask & f.bit) !== 0).map((f) => f.label);
}
