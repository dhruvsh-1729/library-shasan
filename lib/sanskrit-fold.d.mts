export const SANSKRIT_FOLD_VERSION: number;
export const WORD_TOKEN_PATTERN: RegExp;
export const GRAMMAR_LABELS: Set<string>;

export type FoldedTextWithMap = { text: string; starts: number[]; ends: number[]; source: string };

export function foldSanskritText(input: string, withMap: true): FoldedTextWithMap;
export function foldSanskritText(input: string, withMap?: false): { text: string };
export function foldSanskrit(input: string): string;
export function reverseCodePoints(input: string): string;
export function buildFoldedIndexRow(content: string): { folded: string; reversed: string };
export function sanskritWordForms(word: string): string[];
export function sanskritQueryForms(query: string): string[];
export function isGrammarLabelAt(text: string, start: number, end: number, foldedWord: string): boolean;
export function gujaratiCaseForms(word: string): string[];
