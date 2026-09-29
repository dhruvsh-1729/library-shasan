export type ParsedGranthFileName = {
  stem: string;
  bookNumbers: string;
  libraryCode: string | null;
  archiveIds: string[];
  romanTitle: string;
  nativeTitle: string;
  /** "3", "2–3" or "9–11"; null when the file names no part. */
  part: string | null;
  /** A numbered series the file is a volume of ("Agam Satik"). */
  series: string | null;
  volume: string | null;
  tags: string[];
};

export function toAsciiDigits(value: string | null | undefined): string;
export function granthFileStem(value: string | null | undefined): string;
export function titleCase(text: string | null | undefined): string;
export function formatNumberRun(values: Array<string | number>): string | null;
export function parseGranthFileName(value: string | null | undefined): ParsedGranthFileName;
export function titleKey(value: string | null | undefined): string;
export function stripPartSuffix(value: string | null | undefined): string;
