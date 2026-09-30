export type Kosh = { key: string; family: "srm" | "avpk" | "apte"; label: string };
export const KOSHES: Kosh[];
export const KOSH_FAMILIES: Array<{ family: Kosh["family"]; bit: number; label: string }>;
export function koshRank(granthKey: string | null | undefined): number;
export function koshBit(granthKey: string | null | undefined): number;
export function koshFamilyLabels(mask: number): string[];
