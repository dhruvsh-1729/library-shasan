// The Sanskrit koshes in the library, in the order the user wants them in a
// kosh export: Shabda Ratna Mahodadhi first, then Abhidhan Vyutpatti Prakriya
// Kosh, then Apte. Granth keys are Turso ocr_granths.granth_key.

export const KOSHES = [
  { key: "380", family: "srm", label: "Shabda Ratna Mahodadhi 1" },
  { key: "381", family: "srm", label: "Shabda Ratna Mahodadhi 2" },
  { key: "382", family: "srm", label: "Shabda Ratna Mahodadhi 3" },
  { key: "370", family: "avpk", label: "Abhidhan Vyutpatti Prakriya Kosh 1" },
  { key: "371", family: "avpk", label: "Abhidhan Vyutpatti Prakriya Kosh 2" },
  { key: "375", family: "apte", label: "Apte Sanskrit-Hindi Shabdakosh" },
];

/** The three kosh families, in priority order, with the bit each has in a lexicon kosh mask. */
export const KOSH_FAMILIES = [
  { family: "srm", bit: 1, label: "Shabda Ratna Mahodadhi" },
  { family: "avpk", bit: 2, label: "Abhidhan Vyutpatti Prakriya Kosh" },
  { family: "apte", bit: 4, label: "Apte" },
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
