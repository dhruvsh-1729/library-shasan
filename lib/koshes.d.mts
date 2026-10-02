export type Kosh = { key: string; family: "srm" | "avpk" | "apte"; label: string };
export type KoshFamily = Kosh["family"] | "agamic";
export const KOSHES: Kosh[];
export const KOSH_FAMILIES: Array<{ family: KoshFamily; bit: number; label: string }>;
export function koshRank(granthKey: string | null | undefined): number;
export function koshBit(granthKey: string | null | undefined): number;
export function koshFamilyLabels(mask: number): string[];
