export const SOURCE: { head: 0; text: 1; vishay: 2 };
export const UPASARGAS: string[];

export type LexiconEntry = { key: string; head: string; source: number; koshes: number; freq: number };
export type Lexicon = { size: number; get(key: string): LexiconEntry | null };

export type SplitPart =
  | { kind: "word"; text: string; stem: string; head: string; term: string; source: number; koshes: number }
  | { kind: "prefix"; text: string; label: string }
  | { kind: "ending"; text: string }
  | { kind: "unknown"; text: string };

export function aksharaCount(text: string): number;
export function canStartWord(text: string): boolean;
export function createLexicon(data: { words?: Record<string, [string, number, number, number]> }): Lexicon;
export function splitCompound(
  word: string,
  lex: Lexicon,
  options?: { forbidWhole?: boolean }
): { parts: SplitPart[]; cost: number; word: string } | null;
export function searchableParts(parts: SplitPart[]): SplitPart[];
export function compoundParts(
  word: string,
  lex: Lexicon
): { word: string; whole: { head: string; source: number; koshes: number } | null; parts: SplitPart[] } | null;
