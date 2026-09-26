// Mechanical fixes for Google Document AI's repeatable misreads of old
// Devanagari letterpress. Measured 2026-09-26 on 20 pages across five
// Sanskrit/Prakrit granths, against the scans:
//
//   - ष read as प in the old type (त्रिषु -> त्रिपु, विशेष -> विशेप)
//   - the इ ligature read as ह in Prakrit (राइणा -> राहणा)
//   - ञ् read as न् (घञ् -> घन्), and long/short u, i swapped
//   - the anusvara over a reph dropped (तीर्थं -> तीर्थ)
//
// A word is only changed when it is NOT in the clean-text lexicon and the
// corrected form is (seen at least MIN_COUNT times). So धर्म is never turned
// into धर्मं, even where the page has धर्मं: both are real words, and choosing
// between them needs grammar, not a word list.
//
// One rule works without the lexicon: Sanskrit's ruki rule makes a locative
// plural after i/u/e/ai/o/au/r always -षु, so an unknown word ending -इपु/-एपु…
// is a misread -षु. Real words ending that way (रिपु) are in the lexicon and
// are skipped by the first check.

const MIN_COUNT = 20;
const SWAPS = [["प", "ष"], ["ह", "इ"], ["ु", "ू"], ["ि", "ी"]];
const RUKI_PU = /([िीुूेैोौ]|र्)पु$/u;
const RUKI_PAM = /([िीुूेैोौ]|र्)पा(ं|म्)$/u;

function swapVariants(w) {
  const chars = [...w];
  const pos = [];
  chars.forEach((ch, i) => {
    for (const [a, b] of SWAPS) {
      if (ch === a) pos.push([i, b]);
      else if (ch === b && a !== "ह") pos.push([i, a]); // इ is never misread as ह
    }
  });
  const out = new Set();
  const n = Math.min(2, pos.length);
  const rec = (start, chosen) => {
    if (chosen.length) {
      const arr = [...chars];
      for (const [i, c] of chosen) arr[i] = c;
      out.add(arr.join(""));
    }
    if (chosen.length === n) return;
    for (let k = start; k < pos.length; k += 1) rec(k + 1, [...chosen, pos[k]]);
  };
  rec(0, []);
  return out;
}

export function makeDevFixer(dev) {
  const cache = new Map();
  function fixWord(w) {
    if (cache.has(w)) return cache.get(w);
    let best = w;
    if (!(dev[w] > 0) && [...w].length > 2) {
      const cands = new Set(swapVariants(w));
      cands.add(w + "ं");
      if (w.endsWith("न्")) cands.add(w.slice(0, -2) + "ञ्");
      let bestCount = 0;
      for (const v of cands) {
        const c = dev[v] ?? 0;
        if (c >= MIN_COUNT && c > bestCount) { best = v; bestCount = c; }
      }
      if (best === w && [...w].length >= 4) {
        if (RUKI_PU.test(w)) best = w.replace(/पु$/u, "षु");
        else if (RUKI_PAM.test(w)) best = w.replace(/पा(ं|म्)$/u, "षा$1");
      }
    }
    cache.set(w, best);
    return best;
  }
  // Letters and signs only: the dandas and digits share the block but are not words.
  const WORD = /[ऀ-ॣॱ-ॿ]+/gu;
  return function fixDevanagari(text) {
    let fixes = 0;
    const out = String(text).replace(WORD, (w) => {
      const f = fixWord(w);
      if (f !== w) fixes += 1;
      return f;
    });
    return { text: out, fixes };
  };
}
