// Box files printed from the box software (Chrome "Save as PDF": "10.8.1 -
// 10.8.6.pdf", 81 pages, 25 Nov 2026) carry their own text. Reading it takes a
// second where the OCR took minutes, and it is exact: no misread letters.
//
// pdf.js loses the conjuncts of these files (its text has "\u0000" where the
// font has no unicode for a glyph); poppler's pdftotext reads the ActualText
// the PDF carries and gives clean Devanagari, so it is used here. Without
// pdftotext (or for a scan with no text) this returns null and the pages go to
// the OCR as before.
//
// The table is rebuilt from where each word sits: the header row (Sr. | V.T |
// Granth | ShastraPath | Pub.Rem | In.Rem) gives the columns, a number in the
// Sr. column starts a row, and the lines under it are its cells. A cell's
// text is broken at any letter where the line is full ("अभिधानव्युत्पत्तिप्र" /
// "क्रियाकोश"), so a full line is joined to the next without a space. The
// result is the same Markdown table per page the OCR gives, for parseBoxPages.

import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

export type TextWord = { x0: number; y0: number; x1: number; y1: number; text: string };

const ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" };
const decode = (s: string) =>
  s.replace(/&(#\d+|#x[0-9a-f]+|[a-z]+);/gi, (all, name: string) =>
    name[0] === "#" ? String.fromCodePoint(name[1] === "x" || name[1] === "X" ? parseInt(name.slice(2), 16) : parseInt(name.slice(1), 10)) : ENTITIES[name] ?? all
  );

/** pdftotext -bbox-layout's XHTML, as words per page. */
export function wordsFromBbox(xhtml: string): TextWord[][] {
  const pages: TextWord[][] = [];
  for (const page of xhtml.split(/<page\b/).slice(1)) {
    const words: TextWord[] = [];
    const re = /<word xMin="([\d.]+)" yMin="([\d.]+)" xMax="([\d.]+)" yMax="([\d.]+)">([^<]*)<\/word>/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(page))) {
      const text = decode(m[5]).replace(/[‌‍\u0000]/g, "").trim();
      if (text) words.push({ x0: +m[1], y0: +m[2], x1: +m[3], y1: +m[4], text });
    }
    pages.push(words);
  }
  return pages;
}

/** The words of each page of the PDF, or null when pdftotext is not there or fails. */
export async function pdfWords(bytes: Uint8Array): Promise<TextWord[][] | null> {
  const dir = await mkdtemp(path.join(tmpdir(), "box-"));
  try {
    const file = path.join(dir, "box.pdf");
    await writeFile(file, bytes);
    const xhtml = await new Promise<string | null>((resolve) => {
      execFile("pdftotext", ["-bbox-layout", "-enc", "UTF-8", file, "-"], { maxBuffer: 256 * 1024 * 1024, timeout: 60_000 }, (error, stdout) =>
        resolve(error ? null : stdout)
      );
    });
    return xhtml ? wordsFromBbox(xhtml) : null;
  } catch {
    return null;
  } finally {
    await rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

type Line = { y: number; words: TextWord[] };

function linesOf(words: TextWord[]): Line[] {
  const sorted = [...words].sort((a, b) => a.y0 - b.y0 || a.x0 - b.x0);
  const lines: Line[] = [];
  for (const w of sorted) {
    const line = lines.find((l) => Math.abs(l.y - w.y0) < 4);
    if (line) line.words.push(w);
    else lines.push({ y: w.y0, words: [w] });
  }
  for (const l of lines) l.words.sort((a, b) => a.x0 - b.x0);
  return lines.sort((a, b) => a.y - b.y);
}

const lineText = (l: Line) => l.words.map((w) => w.text).join(" ");

const COLUMNS = ["sr", "vt", "granth", "shastraPath", "pubRem", "inRem"] as const;
type Col = (typeof COLUMNS)[number];
const LABELS: Record<Col, RegExp> = {
  sr: /^sr\.?$/i,
  vt: /^v\.?t\.?$/i,
  granth: /^granth/i,
  shastraPath: /^shastra\s*path/i,
  pubRem: /^pub\./i,
  inRem: /^in\.?\s*rem/i,
};
const HEAD_NAMES: Record<Col, string> = { sr: "Sr.", vt: "V.T", granth: "Granth", shastraPath: "ShastraPath", pubRem: "Pub.Rem", inRem: "In.Rem" };

/** The x where each column starts, from a header line, or null when the line is not the header. */
function headerColumns(line: Line): Partial<Record<Col, number>> | null {
  const found: Partial<Record<Col, number>> = {};
  for (const w of line.words) for (const c of COLUMNS) if (found[c] === undefined && LABELS[c].test(w.text)) found[c] = w.x0;
  return found.sr !== undefined && found.granth !== undefined && found.shastraPath !== undefined ? found : null;
}

const VISHAY_BAND = /^\d{1,3}(?:\.\d{1,3})+\s*[-–—:]?\s*[ऀ-ॿ][ऀ-ॿ\s()-]{0,60}$/;
const FOOTER = /\b\d+\s+of\s+\d+\b/;
const SR = /^[0-9]{1,4}$/;
// a line that starts with a vowel sign, a virama or a nasal mark carries on the word before it
const JOINER = /^[ऀ-ःऺ-ॏ॑-ॗॢॣ]/;

/** A cell's lines; `glyph` is about how wide one letter of the line's first word is. */
type Cell = Array<{ text: string; x1: number; page: number; glyph: number }>;

// Names the Granth column prints again and again, so a cut inside them is known
const SEEN_WHOLE = [
  "अभयदेवसूरि", "हेमचंद्रसूरि", "हेमचन्द्रसूरि", "मुनिचन्द्रसूरि", "मलयगिरिसूरि", "हरिभद्रसूरि", "यशोविजयजी", "यशोभद्रसूरि",
  "शीलांकाचार्य", "सम्यक्त्व", "स्वोपज्ञ", "टीका", "गाथा", "श्लोक", "सूत्र", "अध्ययन", "अधिकार", "प्रकरण", "मूल", "भाग",
];

const graphemes = (word: string) => Math.max(1, word.replace(/[\u0900-\u0903\u093a-\u094f\u0951-\u0957\u0962\u0963]/g, "").length);
type RawRow = { band?: string; cells: Partial<Record<Col, Cell>>; page: number };

export type TextLayerBox = {
  /** One Markdown page per PDF page, as the OCR route gives them (an HTML table each). */
  pages: string[];
  /** How many box rows (Sr. numbers) were read. */
  rows: number;
};

const escape = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/**
 * The box table rebuilt from the words, or null when the pages hold no box
 * header (a scan, another kind of PDF).
 */
export function boxFromWords(pages: TextWord[][]): TextLayerBox | null {
  const rows: RawRow[] = [];
  let starts: Partial<Record<Col, number>> | null = null;
  let current: RawRow | null = null;
  const notes: string[][] = pages.map(() => []);
  let anyHeader = false;

  pages.forEach((words, index) => {
    const lines = linesOf(words);
    const headAt = lines.findIndex((l) => headerColumns(l));
    if (headAt < 0) {
      // no table on this page: its text is a note (a parichay, a cover)
      notes[index].push(...lines.map(lineText));
      current = null;
      return;
    }
    anyHeader = true;
    starts = headerColumns(lines[headAt]);
    const headY = lines[headAt].y;
    let inNote = false;
    for (const line of lines.slice(headAt + 1)) {
      const text = lineText(line).trim();
      if (FOOTER.test(text)) break;
      // the second line of the header ("m" of Pub.Rem broken over two lines)
      if (line.y - headY < 30 && line.words.every((w) => /^[a-z.]{1,3}$/i.test(w.text))) continue;
      if (VISHAY_BAND.test(text) && line.words[0].x0 > (starts!.granth ?? 100)) {
        rows.push({ band: text, cells: {}, page: index });
        current = null;
        inNote = false;
        continue;
      }
      if (text === "परिचय") {
        inNote = true;
        current = null;
        continue;
      }
      if (inNote) {
        notes[index].push(text);
        continue;
      }
      const at = (x: number): Col => {
        let col: Col = "sr";
        for (const c of COLUMNS) if (starts![c] !== undefined && x >= starts![c]! - 3) col = c;
        return col;
      };
      const byCol: Partial<Record<Col, TextWord[]>> = {};
      for (const w of line.words) (byCol[at(w.x0)] ??= []).push(w);
      const srWord = byCol.sr?.map((w) => w.text).join("") ?? "";
      if (SR.test(srWord) || !current) {
        current = { cells: {}, page: index };
        rows.push(current);
      }
      for (const c of COLUMNS) {
        const ws = byCol[c];
        if (!ws?.length) continue;
        (current.cells[c] ??= []).push({
          text: ws.map((w) => w.text).join(" "),
          x1: Math.max(...ws.map((w) => w.x1)),
          page: index,
          glyph: (ws[0].x1 - ws[0].x0) / graphemes(ws[0].text),
        });
      }
    }
  });
  if (!anyHeader) return null;

  // how far right each column's text reaches on each page (the browser sized each page's table on
  // its own): a line reaching it was broken at a letter, not at a space
  const reach = new Map<string, number>();
  for (const r of rows)
    for (const c of COLUMNS)
      for (const l of r.cells[c] ?? []) reach.set(`${l.page}:${c}`, Math.max(reach.get(`${l.page}:${c}`) ?? 0, l.x1));
  // Words seen whole in the file (not first or last on a line, where they may be cut): a cut is
  // judged by them first ("स्तव" + "न" is "स्तवन"; "गाथा" + "1-2" stays two words), then by the line's reach.
  const norm = (w: string) => w.replace(/[()[\]{}"'`‘’“”,;:।॥.?!*=]+/g, "");
  const whole = new Set<string>(SEEN_WHOLE);
  for (const r of rows)
    for (const c of COLUMNS)
      (r.cells[c] ?? []).forEach((l, i, lines) => {
        const ws = l.text.split(" ");
        const ends = /[,;।॥)]$/;
        // a word ending in a comma or a danda is whole; so is the first word after such a line
        // a line that stops well short of the column's edge was broken at a space: its last word is
        // whole, and so is the next line's first
        const short = (l2: Cell[number]) => (reach.get(`${l2.page}:${c}`) ?? 0) - l2.x1 > 3 * (l2.glyph || 8);
        const first = i === 0 || ends.test(lines[i - 1].text) || short(lines[i - 1]) ? 0 : 1;
        const last = ends.test(ws[ws.length - 1]) || (i < lines.length - 1 && short(l)) ? ws.length : ws.length - 1;
        for (const w of ws.slice(first, last)) if (norm(w)) whole.add(norm(w));
      });
  const cutHere = (c: Col, prev: Cell[number], next: Cell[number]) => {
    // a vowel sign or a reph ("र्भित") never starts a word
    if (JOINER.test(next.text) || /^र्/.test(next.text) || /्$/.test(prev.text)) return true;
    const a = norm(prev.text.split(" ").pop() ?? "");
    const b = norm(next.text.split(" ")[0] ?? "");
    if (!a || !b) return false;
    if (/\d$/.test(a) && /^\d/.test(b)) return true;
    if (/^[\d-]+$/.test(a) || /^[\d-]+$/.test(b)) return false;
    if (whole.has(a + b)) return true;
    if (whole.has(a) && whole.has(b)) return false;
    // one of them is a word seen whole: two words, unless the other is a lone letter ("स्तव" + "न")
    if (whole.has(a) && graphemes(b) > 1) return false;
    if (whole.has(b) && graphemes(b) > 1) return false;
    if (graphemes(b) === 1 && !whole.has(a)) return true;
    if (graphemes(a) === 1 && !whole.has(a)) return true;
    // not even the next line's first letter would have fitted after it: the word was cut there
    return (reach.get(`${prev.page}:${c}`) ?? 0) - prev.x1 < next.glyph * 0.9;
  };
  const join = (c: Col, cell: Cell | undefined) => {
    if (!cell?.length) return "";
    let out = cell[0].text;
    for (let i = 1; i < cell.length; i += 1) out += cutHere(c, cell[i - 1], cell[i]) ? cell[i].text : ` ${cell[i].text}`;
    return out.replace(/\s+/g, " ").trim();
  };

  const html = pages.map(() => [] as string[]);
  let count = 0;
  for (const r of rows) {
    if (r.band) {
      html[r.page].push(`<tr><td></td><td></td><td></td><td>${escape(r.band)}</td><td></td><td></td></tr>`);
      continue;
    }
    const cells = COLUMNS.map((c) => join(c, r.cells[c]));
    if (!cells[2] && !cells[3]) continue;
    if (SR.test(cells[0])) count += 1;
    html[r.page].push(`<tr>${cells.map((c) => `<td>${escape(c)}</td>`).join("")}</tr>`);
  }
  const head = `<tr>${COLUMNS.map((c) => `<th>${HEAD_NAMES[c]}</th>`).join("")}</tr>`;
  return {
    rows: count,
    pages: html.map((body, i) => {
      const parts: string[] = [];
      if (body.length) parts.push(`BOX No - 1\n\n<table>${head}${body.join("")}</table>`);
      if (notes[i].length) parts.push(notes[i].join("\n"));
      return parts.join("\n\n");
    }),
  };
}

/** The box read from the PDF's own text, or null (no pdftotext, no text, not a box table). */
export async function boxFromTextLayer(bytes: Uint8Array): Promise<TextLayerBox | null> {
  const words = await pdfWords(bytes);
  if (!words || !words.some((p) => p.length)) return null;
  const box = boxFromWords(words);
  return box && box.rows > 0 ? box : null;
}
