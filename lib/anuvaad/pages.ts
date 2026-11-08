// "Any PDF pages": the reader picks pages of a PDF by number ("12-15 20"),
// each page is read and given its Gujarati anuvaad. These helpers have no
// server code: the browser page and the API both use them.

/** At most this many pages in one go: each page is read and translated on its own. */
export const MAX_PAGES_PER_RUN = 30;

export type PageChoice = { pages: number[]; problem: string };

/**
 * "12-15 20" → [12, 13, 14, 15, 20]. Commas, spaces and "to" all separate;
 * "15-12" counts as 12-15. Pages are counted as the PDF viewer counts them.
 */
export function parsePageList(spec: string, total: number): PageChoice {
  const text = spec
    .replace(/[૦-૯]/g, (d) => String(d.charCodeAt(0) - 0x0ae6))
    .replace(/[०-९]/g, (d) => String(d.charCodeAt(0) - 0x0966))
    .replace(/[–—]/g, "-")
    .replace(/\s*(?:-|to)\s*/gi, "-")
    .trim();
  if (!text) return { pages: [], problem: "" };
  const chosen = new Set<number>();
  for (const part of text.split(/[\s,;]+/).filter(Boolean)) {
    const m = part.match(/^(\d+)(?:-(\d+))?$/);
    if (!m) return { pages: [], problem: `“${part}” is not a page number. Write numbers like 12-15 20.` };
    let from = Number(m[1]);
    let to = m[2] ? Number(m[2]) : from;
    if (from > to) [from, to] = [to, from];
    if (from < 1 || to > total) return { pages: [], problem: total === 1 ? "This PDF has only 1 page." : `This PDF has pages 1 to ${total}.` };
    if (to - from >= MAX_PAGES_PER_RUN) return { pages: [], problem: `At most ${MAX_PAGES_PER_RUN} pages at a time.` };
    for (let n = from; n <= to; n += 1) chosen.add(n);
  }
  const pages = [...chosen].sort((a, b) => a - b);
  if (pages.length > MAX_PAGES_PER_RUN) return { pages: [], problem: `That is ${pages.length} pages; choose at most ${MAX_PAGES_PER_RUN} at a time.` };
  return { pages, problem: "" };
}

/** [12, 13, 14, 15, 20] → "12-15, 20". */
export function describePages(pages: number[]) {
  const out: string[] = [];
  for (let i = 0; i < pages.length; ) {
    let j = i;
    while (j + 1 < pages.length && pages[j + 1] === pages[j] + 1) j += 1;
    out.push(j > i ? `${pages[i]}-${pages[j]}` : String(pages[i]));
    i = j + 1;
  }
  return out.join(", ");
}

/**
 * A page as the OCR gives it (Markdown, tables as HTML) → plain text, one
 * paragraph per block: what the reader checks and what is translated.
 */
export function pageText(markdown: string) {
  return markdown
    .replace(/!\[[^\]]*\]\([^)]*\)/g, "")
    .replace(/<\/(?:td|th)>/gi, " ")
    .replace(/<(?:br|\/tr|\/p|\/div|\/li)\s*\/?>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/\*\*(.+?)\*\*/g, "$1")
    .replace(/^\s*\|?[-:| ]{3,}\|?\s*$/gm, "")
    .replace(/\|/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/ *\n */g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
    .split("\n")
    .filter((line, i, lines) => !isPageMark(line, i, lines.length))
    .join("\n")
    .trim();
}

/**
 * A line that is only the page number, or the running head at the top or
 * foot of the page ("६२ [ कूपदृष्टान्त ]", "[ २० ]"): not part of the text.
 */
function isPageMark(line: string, index: number, count: number) {
  const t = line.trim();
  if (/^[\[(]?\s*[0-9०-९૦-૯]+\s*[\])]?$/.test(t)) return true;
  const edge = index === 0 || index === count - 1;
  return edge && t.length < 60 && /[0-9०-९]/.test(t) && /[[\]]/.test(t);
}

const GUJARATI_DIGITS = "૦૧૨૩૪૫૬૭૮૯";
export const gujaratiNumber = (n: number) => String(n).replace(/[0-9]/g, (d) => GUJARATI_DIGITS[Number(d)]);
