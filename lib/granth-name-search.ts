// Ranks granths for the library's name search ("/" page).
//
// A granth's book number is the leading number of its file name, exactly as
// on the source drive: "361_sad_drushti_dwatrinshika_004684_hr.pdf" is book
// 361, "025_ashtak_prakaran…" is book 25, "183-184_…" covers 183 and 184.
// A query that is just a number returns that book, not every file that
// happens to contain the digits (the archive codes "B041361", "004684" do).
//
// Names are matched in a spelling-tolerant, script-neutral form: Devanagari
// and Gujarati are romanised, and the variations common in these file names
// (tt/t, aa/a, th/t, chh/ch/c, sh/s, w/v…) are folded away, so "tatvarth"
// finds "tattvartha" and "तत्त्वार्थ" finds both.

export type NameSearchRow = {
  /** File name or path the book number and name are read from. */
  source: string;
  /** Further text to match on (collection, title…). */
  extra?: string;
};

const LEADING_NUMBER = /^(\d{1,4})(?:-(\d{1,4}))?(?=[_\s.-])/;

/** Book numbers a file covers: [361], or [183, 184] for "183-184_…". */
export function bookNumbers(source: string): number[] {
  const base = String(source ?? "").split(/[\\/]/).pop() ?? "";
  const m = base.match(LEADING_NUMBER);
  if (!m) return [];
  const a = Number(m[1]);
  const b = m[2] ? Number(m[2]) : a;
  if (!(b >= a) || b - a > 20) return [a];
  return Array.from({ length: b - a + 1 }, (_, i) => a + i);
}

/** The query as a book number or range, if that is all it is. */
export function parseBookNumberQuery(q: string): number[] | null {
  const m = q.trim().match(/^#?\s*(\d{1,4})(?:\s*-\s*(\d{1,4}))?$/);
  if (!m) return null;
  const a = Number(m[1]);
  const b = m[2] ? Number(m[2]) : a;
  if (!(b >= a) || b - a > 20) return [a];
  return Array.from({ length: b - a + 1 }, (_, i) => a + i);
}

// ---------------------------------------------------------------- romanise
// Gujarati sits 0x180 above Devanagari, letter for letter.
const GU_OFFSET = 0x180;
const CONSONANTS: Record<string, string> = {
  क: "k", ख: "kh", ग: "g", घ: "gh", ङ: "n", च: "ch", छ: "chh", ज: "j", झ: "jh", ञ: "n",
  ट: "t", ठ: "th", ड: "d", ढ: "dh", ण: "n", त: "t", थ: "th", द: "d", ध: "dh", न: "n",
  प: "p", फ: "ph", ब: "b", भ: "bh", म: "m", य: "y", र: "r", ल: "l", ळ: "l", व: "v",
  श: "sh", ष: "sh", स: "s", ह: "h",
};
const VOWELS: Record<string, string> = {
  अ: "a", आ: "aa", इ: "i", ई: "ii", उ: "u", ऊ: "uu", ऋ: "ri", ए: "e", ऐ: "ai", ओ: "o", औ: "au", ऑ: "o",
};
const MATRAS: Record<string, string> = {
  "ा": "aa", "ि": "i", "ी": "ii", "ु": "u", "ू": "uu", "ृ": "ri", "े": "e", "ै": "ai", "ो": "o", "ौ": "au", "ॉ": "o",
};
const VIRAMA = "्";

function toDevanagari(ch: string) {
  const c = ch.codePointAt(0)!;
  return c >= 0x0a80 && c <= 0x0aff ? String.fromCodePoint(c - GU_OFFSET) : ch;
}

export function romanise(text: string): string {
  const chars = [...String(text ?? "").normalize("NFC")].map(toDevanagari);
  let out = "";
  for (let i = 0; i < chars.length; i += 1) {
    const ch = chars[i];
    const next = chars[i + 1];
    if (CONSONANTS[ch]) {
      out += CONSONANTS[ch];
      if (next === VIRAMA) i += 1;
      else if (next && MATRAS[next]) { out += MATRAS[next]; i += 1; }
      else out += "a";
    } else if (VOWELS[ch]) out += VOWELS[ch];
    else if (MATRAS[ch]) out += MATRAS[ch];
    else if (ch === "ं" || ch === "ँ") out += "n";
    else if (ch === "ः" || ch === "़" || ch === "‌" || ch === "‍") continue;
    else out += ch;
  }
  return out;
}

// ---------------------------------------------------------------- fold
/** Spelling-tolerant form: lower case, letters/digits only, variants folded. */
export function foldName(text: string): string {
  return romanise(text)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/ksh|x/g, "ks")
    .replace(/chh|ch|c/g, "c")
    .replace(/shh|sh|s/g, "s")
    .replace(/([kgtdpbj])h/g, "$1")
    .replace(/w/g, "v")
    .replace(/z/g, "j")
    .replace(/ph|f/g, "p")
    .replace(/ee|ii|y(?=[^aeiou]|$)/g, "i")
    .replace(/oo|uu/g, "u")
    .replace(/aa/g, "a")
    .replace(/ri/g, "r")
    .replace(/([a-z])\1+/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

/** Consonants only: survives inherent-vowel and vowel-length differences. */
function skeleton(folded: string) {
  return folded.replace(/[aeiou\s]/g, "").replace(/([a-z])\1+/g, "$1");
}

// ---------------------------------------------------------------- rank
type Prepared = { numbers: number[]; folded: string; words: string[]; skel: string; raw: string };

export function prepareRow(row: NameSearchRow): Prepared {
  const base = String(row.source ?? "").split(/[\\/]/).pop() ?? "";
  const text = `${base} ${row.extra ?? ""}`;
  const folded = foldName(text);
  return {
    numbers: bookNumbers(base),
    folded,
    words: folded.split(" ").filter(Boolean),
    skel: skeleton(folded),
    raw: text.toLowerCase(),
  };
}

/**
 * Scores one row for the query; 0 means no match. Higher is better:
 *   1000 book number, 400+ whole phrase at a word start, 300 phrase inside,
 *   200 every word found, 150/100 consonant skeleton match within/across words.
 */
export function scoreRow(p: Prepared, q: string): number {
  const numberQuery = parseBookNumberQuery(q);
  if (numberQuery) return numberQuery.some((n) => p.numbers.includes(n)) ? 1000 : 0;

  const raw = q.trim().toLowerCase();
  const fq = foldName(q);
  if (!fq) return 0;
  let score = 0;
  if (raw && p.raw.includes(raw)) score = Math.max(score, 450);
  if (p.folded.startsWith(fq) || p.folded.includes(` ${fq}`)) score = Math.max(score, 400);
  else if (p.folded.includes(fq)) score = Math.max(score, 300);

  const qWords = fq.split(" ").filter((w) => w.length >= 2);
  if (!score && qWords.length > 1 && qWords.every((w) => p.folded.includes(w) || (skeleton(w).length >= 3 && p.skel.includes(skeleton(w))))) {
    score = 200;
  }
  const qs = skeleton(fq);
  if (!score && qs.length >= 3 && p.skel.includes(qs)) {
    // "yogshastra" should rank the one-word "yogashastra" above "yog sutra",
    // whose consonants only line up across a word break.
    score = p.words.some((w) => skeleton(w).includes(qs)) ? 150 : 100;
  }
  return score;
}

/** Returns the indexes of matching rows, best first; ties keep catalogue order. */
export function rankRows(rows: Prepared[], q: string): number[] {
  const scored: Array<[number, number]> = [];
  rows.forEach((p, i) => {
    const s = scoreRow(p, q);
    if (s > 0) scored.push([i, s]);
  });
  scored.sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  return scored.map(([i]) => i);
}
