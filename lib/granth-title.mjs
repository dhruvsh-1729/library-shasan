// Reads the parts of a granth out of its file name, for clean display titles.
//
// Plain JS so the catalog builder and the app share one reading. The source
// drive names files like
//   "010_adhyatmasar_shabdasha_vivechan_part_03_037090_hr6.pdf"
//   "029_B035152_agam_satik_part_05_sthananga_sutra_gujarati_anuwad_1_008996_std.pdf"
//   "083_B011146_उत्तराध्ययनसूत्रम् (अध्ययन 1 थी 17) भाग 1.pdf"
// which is: book number(s), library accession code, a romanised title, an
// archive id, a scan-quality tag, and sometimes the title in its own script.
// Nothing is thrown away: every piece is returned in its own field.

const INDIC = /[ऀ-ॿ઀-૿]/;
const LEADING_NUMBERS = /^(\d{1,4}(?:-\d{1,4})?)(?=[_\s.-]|$)/;
const LIBRARY_CODE = /^[BCW]\d{6}$/i;
const ARCHIVE_ID = /^\d{6}$/;
// Scan and processing tags, not part of any title.
const NOISE = new Set([
  "hr", "hr3", "hr6", "std", "data", "ocred", "ocr", "300dpi", "original", "duplicat", "duplicate", "copy",
]);
const PART_WORD = /^(?:part|bhag|bhaag|vibhag)$/i;
const GLUED_PART = /^(?:part|bhag|vibhag)-?(\d{1,3})$/i;
// A number after these words names a division of the work, not a volume.
const INLINE_NUMBER_AFTER = new Set([
  "parv", "parva", "sarg", "adhyay", "batrishi", "dwatrishika", "dwatrinshika", "shrutskandh", "agam", "ang",
  "chhed", "chulika", "upang", "prakirnak", "mool", "gatha", "stavan",
]);
// Series whose "part" is the volume of the series, with the work named after it.
const SERIES_PREFIXES = [
  ["agam", "satik"],
  ["agam", "suttani", "satikam"],
  ["agam", "sutra", "satik"],
];
const NATIVE_PART = /\s*(?:वि)?भाग[\s-]*([०-९\d]+(?:\s*-\s*[०-९\d]+)?)\s*$|\s*(?:વિ)?ભાગ[\s-]*([૦-૯\d]+(?:\s*-\s*[૦-૯\d]+)?)\s*$/u;
const LOWER_WORDS = new Set(["ane", "va", "tatha", "sah", "and", "of", "the", "thi", "ni", "nu", "no"]);

export function toAsciiDigits(value) {
  return String(value ?? "")
    .replace(/[०-९]/g, (d) => String(d.charCodeAt(0) - 0x966))
    .replace(/[૦-૯]/g, (d) => String(d.charCodeAt(0) - 0xae6));
}

/** The file name without folders, extensions or copy markers. */
export function granthFileStem(value) {
  let base = String(value ?? "").split(/[\\/]/).filter(Boolean).pop() ?? "";
  base = base.normalize("NFC");
  for (let i = 0; i < 3; i += 1) base = base.replace(/\.(pdf|xlsx|csv|txt)$/i, "");
  return base.replace(/\(\d+\)$/, "").trim();
}

function titleCaseWord(word, index) {
  const lower = word.toLowerCase();
  if (index > 0 && LOWER_WORDS.has(lower)) return lower;
  if (/^\d/.test(word)) return word;
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

/** "adhyatmasar shabdasha VIVECHAN" -> "Adhyatmasar Shabdasha Vivechan". */
export function titleCase(text) {
  return String(text ?? "")
    .split(/\s+/)
    .filter(Boolean)
    .map(titleCaseWord)
    .join(" ");
}

/** "03" -> "3"; [2, 3] -> "2–3"; [9, 10, 11] -> "9–11"; [2, 5] -> "2, 5". */
export function formatNumberRun(values) {
  const nums = values.map((v) => Number(toAsciiDigits(v))).filter((n) => Number.isFinite(n));
  if (nums.length === 0) return null;
  if (nums.length === 1) return String(nums[0]);
  const consecutive = nums.every((n, i) => i === 0 || n === nums[i - 1] + 1);
  return consecutive ? `${nums[0]}–${nums[nums.length - 1]}` : nums.join(", ");
}

/** Tags hidden in a token ("hr6", "std-ORIGINAL", "hr6-avruti2"), or null if it is a word. */
function noiseTags(token) {
  const pieces = token.toLowerCase().split("-").filter(Boolean);
  if (pieces.length === 0) return null;
  const tags = [];
  for (const piece of pieces) {
    if (NOISE.has(piece)) tags.push(piece);
    else if (/^avruti\d$/.test(piece)) tags.push(piece.replace("avruti", "edition "));
    else return null;
  }
  return tags;
}

/**
 * Splits a granth file name (or an already-spaced name) into its parts.
 * @returns {import("./granth-title.d.mts").ParsedGranthFileName}
 */
export function parseGranthFileName(value) {
  const stem = granthFileStem(value);
  const tags = [];
  let rest = stem.replace(/\s+-\s+copy$/i, () => {
    tags.push("copy");
    return "";
  });
  let bookNumbers = "";
  const lead = rest.match(LEADING_NUMBERS);
  if (lead) {
    bookNumbers = lead[1];
    rest = rest.slice(lead[0].length).replace(/^[_\s.-]+/, "");
  }

  // The Indic title, when there is one, is the tail from its first Indic word.
  const tokens = rest.split(/[_\s]+/).filter(Boolean);
  const firstIndic = tokens.findIndex((token) => INDIC.test(token));
  const romanTokens = firstIndic >= 0 ? tokens.slice(0, firstIndic) : tokens;
  const nativeTokens = firstIndic >= 0 ? tokens.slice(firstIndic) : [];
  // "(श्राद्धजीतकल्प)" can open with a bracket before the first Indic word.
  if (firstIndic > 0 && /^[([]$/.test(romanTokens[romanTokens.length - 1] ?? "")) {
    nativeTokens.unshift(romanTokens.pop());
  }
  while (nativeTokens.length && noiseTags(nativeTokens[nativeTokens.length - 1])) {
    tags.push(...noiseTags(nativeTokens.pop()));
  }
  let nativeTitle = nativeTokens.join(" ");

  let libraryCode = null;
  const archiveIds = [];
  let words = [];
  let part = null;
  let series = null;
  let volume = null;

  for (let i = 0; i < romanTokens.length; i += 1) {
    const raw = romanTokens[i].replace(/\+/g, " ").trim();
    if (!raw) continue;
    if (!libraryCode && LIBRARY_CODE.test(raw)) {
      libraryCode = raw.toUpperCase();
      continue;
    }
    if (ARCHIVE_ID.test(raw)) {
      archiveIds.push(raw);
      continue;
    }
    const noise = noiseTags(raw);
    if (noise) {
      tags.push(...noise);
      continue;
    }
    // A lone letter ("..._part_01_c_hr6", "SURYAPRAGNAPTI_01_F") is a scan marker.
    if (/^[a-z]$/i.test(raw)) {
      tags.push(raw.toLowerCase());
      continue;
    }

    let number = null;
    const glued = raw.match(GLUED_PART);
    if (glued) number = glued[1];
    else if (PART_WORD.test(raw.replace(/-$/, "")) && /^\d{1,3}$/.test(romanTokens[i + 1] ?? "")) {
      number = romanTokens[i + 1];
      i += 1;
    }

    const prefix = SERIES_PREFIXES.find((p) => p.length === words.length && p.every((w, j) => words[j].toLowerCase() === w));
    // "agam_sutra_satik_01_aachar..." names the volume without a part word.
    if (prefix && number == null && /^\d{1,3}$/.test(raw) && i + 1 < romanTokens.length) number = raw;
    if (number != null) {
      if (prefix && i + 1 < romanTokens.length) {
        series = titleCase(words.join(" "));
        volume = formatNumberRun([number]);
        words = [];
      } else {
        part = formatNumberRun([number]);
      }
      continue;
    }
    // "Sahasstri-2", "BATRISHI-20": a number glued to the last word.
    const tail = raw.match(/^(.*[a-z])-(\d{1,3})$/i);
    if (tail) {
      words.push(tail[1], tail[2]);
      continue;
    }
    words.push(raw);
  }

  // A trailing run of numbers: "..._anuwad_1", "shastravartta_samucchaya_2_3",
  // "Trishashti_Parv_2_3_4". After a division word they stay in the title.
  let runStart = words.length;
  while (runStart > 0 && /^\d{1,3}$/.test(words[runStart - 1])) runStart -= 1;
  if (runStart < words.length && runStart > 0) {
    const run = words.slice(runStart);
    const before = words[runStart - 1].toLowerCase();
    if (INLINE_NUMBER_AFTER.has(before)) {
      words = [...words.slice(0, runStart), formatNumberRun(run)];
    } else if (part == null) {
      part = formatNumberRun(run);
      words = words.slice(0, runStart);
    }
  }
  // Leading zeros inside a title read as noise: "Chulika 02" -> "Chulika 2".
  words = words.map((word) => (/^0\d+$/.test(word) ? String(Number(word)) : word));

  if (nativeTitle) {
    const m = nativeTitle.match(NATIVE_PART);
    if (m) {
      const nativePart = formatNumberRun(toAsciiDigits(m[1] ?? m[2]).split(/\s*-\s*/));
      if (part == null) part = nativePart;
      if (part === nativePart) nativeTitle = nativeTitle.slice(0, m.index).trim();
    }
  }

  return {
    stem,
    bookNumbers,
    libraryCode,
    archiveIds,
    romanTitle: titleCase(words.join(" ")),
    nativeTitle: nativeTitle.replace(/\s+/g, " ").trim(),
    part,
    series,
    volume,
    tags: [...new Set(tags)],
  };
}

/** Loose comparison key for two romanised titles ("Tattvartha" ~ "tatvarth"). */
export function titleKey(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .replace(/(.)\1+/g, "$1")
    .replace(/aa/g, "a")
    .replace(/w/g, "v")
    .replace(/sh/g, "s")
    .replace(/chh/g, "ch")
    .replace(/th/g, "t")
    .replace(/dh/g, "d")
    .replace(/a(?=\b|$)/g, "")
    .replace(/m$/, "");
}

/** A book-list title without the part it was copied from ("Tattvarthadhigam Sutra bhag 4"). */
export function stripPartSuffix(value) {
  return String(value ?? "")
    .replace(/\s+(?:part|bhag|vibhag)\s*[-\d]+\s*$/i, "")
    .trim();
}
