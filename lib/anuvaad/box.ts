// Reads a box file (the printed table Sr | V.T | Granth | ShastraPath |
// Pub.Rem | In.Rem, one vishay per box, WhatsApp scans of 29 Oct 2026) from the
// OCR of its pages into rows.
//
// The OCR gives each page as Markdown holding an HTML <table>. A passage that
// runs on to the next page comes back there as a row with no Sr. and no Granth;
// it is joined to the row before it, so each row is one whole passage.

export type BoxRow = {
  /** Serial number as printed ("60"); empty when the scan cut it off. */
  sr: string;
  /** V.T as printed ("स्व."). */
  vt: string;
  granth: string;
  shastraPath: string;
  pubRem: string;
  inRem: string;
  /** Pages of the upload the row was read from (1-based, across all files). */
  pages: number[];
};

export type BoxPage = { page: number; markdown: string };

export type ParsedBox = {
  /** The vishay printed over the ShastraPath column ("प्राणातिपातविरमणमहाव्रत"). */
  vishay: string;
  box: string;
  rows: BoxRow[];
  /** Pages that had no table (a cover, a note); their text is kept for the reader. */
  loose: Array<{ page: number; text: string }>;
};

type Column = "sr" | "vt" | "granth" | "shastraPath" | "pubRem" | "inRem";

const HEADER_LABELS: Array<[RegExp, Column]> = [
  [/^sr\.?$|^sr\s*no/i, "sr"],
  [/^(v\s*)?\.?\s*t\.?$/i, "vt"],
  [/^granth/i, "granth"],
  [/^shastra\s*path/i, "shastraPath"],
  [/^pub\b/i, "pubRem"],
  [/^in\.?\s*r/i, "inRem"],
];

const ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " ", "#39": "'" };

function decode(text: string) {
  return text.replace(/&(#\d+|#x[0-9a-f]+|[a-z]+\d*);/gi, (all, name: string) => {
    if (name[0] === "#") {
      const code = name[1] === "x" || name[1] === "X" ? parseInt(name.slice(2), 16) : parseInt(name.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : all;
    }
    return ENTITIES[name.toLowerCase()] ?? all;
  });
}

/**
 * A cell's text. A <br/> is a printed line end inside a narrow column: a number
 * broken across it ("पद - 2<br/>2") is one number again; otherwise a space.
 */
function cellText(html: string) {
  const joined = html.replace(/(\d)\s*<br\s*\/?>\s*(?=\d)/gi, "$1").replace(/<br\s*\/?>/gi, " ");
  return decode(joined.replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function cellsOf(rowHtml: string, tag: "td" | "th" | "t[dh]") {
  const out: string[] = [];
  const re = new RegExp(`<(${tag})\\b[^>]*>([\\s\\S]*?)</\\1>`, "gi");
  let m: RegExpExecArray | null;
  while ((m = re.exec(rowHtml))) out.push(cellText(m[2]));
  return out;
}

function rowsOf(tableHtml: string) {
  const rows: Array<{ header: boolean; cells: string[] }> = [];
  const re = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(tableHtml))) {
    const inner = m[1];
    const header = /<th\b/i.test(inner) && !/<td\b/i.test(inner);
    rows.push({ header, cells: cellsOf(inner, "t[dh]") });
  }
  return rows;
}

const DEVANAGARI = /[ऀ-ॿ]/;

/** The column each header cell names; a Devanagari cell is the vishay band, not a column. */
function readHeader(cells: string[]) {
  const columns: Array<Column | null> = [];
  let vishay = "";
  for (const raw of cells) {
    // "ShastraPath प्राणातिपातविरमणमहाव्रत": the band printed over the column
    const label = raw.replace(/[ऀ-ॿ][ऀ-ॿ\s]*/g, (band) => {
      if (!vishay) vishay = band.trim();
      return "";
    }).trim();
    if (!label && DEVANAGARI.test(raw)) {
      columns.push(null); // the vishay alone in a header cell: it holds no data
      continue;
    }
    const hit = HEADER_LABELS.find(([re]) => re.test(label));
    // A blank or unreadable first header ("BOX N…" on a photo) is the Sr. column cut off.
    columns.push(hit ? hit[1] : !columns.length ? "sr" : null);
  }
  return { columns, vishay };
}

const SR = /^[0-9०-९]{1,4}\.?$/;
const VT = /^[ऀ-ॿ]{1,6}\.?$/;

/**
 * Cells to columns without a header (or when the header does not fit the
 * cells): ShastraPath is the longest cell, Granth the one before it, Sr. a
 * bare number at the start, V.T a short abbreviation, the rest remarks.
 */
function guessColumns(cells: string[]): Partial<Record<Column, string>> {
  const out: Partial<Record<Column, string>> = {};
  if (!cells.length) return out;
  let longest = 0;
  cells.forEach((c, i) => {
    if (c.length > cells[longest].length) longest = i;
  });
  out.shastraPath = cells[longest];
  const before = cells.slice(0, longest);
  const after = cells.slice(longest + 1);
  if (before.length && SR.test(before[0])) out.sr = before.shift();
  // "स्व." alone, or with a hand-written note or a stray line of the Granth after it
  const vtLead = before[0]?.match(/^([\u0900-\u097f]{1,6}\.)(?:\s+.*)?$/);
  if (before.length > 1 && vtLead) {
    out.vt = vtLead[1];
    before.shift();
  } else if (before.length && VT.test(before[0])) out.vt = before.shift();
  const granth = before.join(" ").trim();
  if (granth) out.granth = granth;
  if (after.length === 1) out.inRem = after[0];
  else if (after.length >= 2) {
    out.pubRem = after[0];
    out.inRem = after.slice(1).join(" ");
  }
  return out;
}

/**
 * A header read on one page can be off on the next (a photo cuts a column
 * off, a hand-written note becomes a cell). The mapping is kept only if it
 * looks like a box row: the passage is the longest cell, the Granth is not a
 * V.T mark, and the V.T is not a number.
 */
function plausible(row: Partial<Record<Column, string>>, cells: string[]) {
  const longest = Math.max(...cells.map((c) => c.length));
  if ((row.shastraPath ?? "").length < longest * 0.8) return false;
  return plausibleCells(row);
}

/** No cell holds what belongs to its neighbour: Sr. is a number, V.T a short mark, Granth not a mark. */
function plausibleCells(row: Partial<Record<Column, string>>) {
  if (row.sr && !SR.test(row.sr)) return false;
  if (row.granth && VT.test(row.granth)) return false;
  if (row.vt && (SR.test(row.vt) || row.vt.length > 40)) return false;
  return true;
}

function assign(columns: Array<Column | null>, cells: string[]): Partial<Record<Column, string>> {
  let dataColumns = columns.filter((c): c is Column => c !== null);
  // A photo cut the Sr. header off, but the rows still have the column.
  if (dataColumns.length + 1 === cells.length && !dataColumns.includes("sr")) dataColumns = ["sr", ...dataColumns];
  // The header names exactly as many columns as the row has cells: trusted,
  // unless the cells have plainly moved over (a V.T mark under Granth).
  if (dataColumns.length === cells.length && dataColumns.includes("shastraPath")) {
    const out = byColumns(dataColumns, cells);
    if (plausibleCells(out)) return out;
  }
  if (columns.length === cells.length && columns.includes("shastraPath")) {
    const out = byColumns(columns, cells);
    if (plausible(out, cells)) return out;
  }
  return guessColumns(cells);
}

function byColumns(columns: Array<Column | null>, cells: string[]) {
  const out: Partial<Record<Column, string>> = {};
  columns.forEach((c, i) => {
    if (c && cells[i]) out[c] = out[c] ? `${out[c]} ${cells[i]}` : cells[i];
  });
  return out;
}

/** The text of a page with its tables taken out (headers, page numbers and dates dropped). */
function looseText(markdown: string) {
  return markdown
    .replace(/<table[\s\S]*?<\/table>/gi, "\n")
    .replace(/!\[[^\]]*]\([^)]*\)/g, "")
    .split("\n")
    .map((l) => l.replace(/^#+\s*/, "").trim())
    .filter((l) => l && l !== "---")
    .filter((l) => !/^\d+\s+of\s+\d+/i.test(l) && !/^\d{2}-\d{2}-\d{4}/.test(l) && !/^BOX\s*No/i.test(l) && !/^[0-9]+$/.test(l))
    .join("\n");
}

export function parseBoxPages(pages: BoxPage[]): ParsedBox {
  const rows: BoxRow[] = [];
  const loose: ParsedBox["loose"] = [];
  let vishay = "";
  let box = "";
  let columns: Array<Column | null> = [];

  for (const { page, markdown } of pages) {
    const boxNo = markdown.match(/BOX\s*No\.?\s*[-–:]?\s*([0-9]+)/i);
    if (boxNo && !box) box = boxNo[1];
    const tables = markdown.match(/<table[\s\S]*?<\/table>/gi) ?? [];
    if (!tables.length) {
      const text = looseText(markdown);
      if (text) loose.push({ page, text });
      continue;
    }
    let firstOnPage = true;
    for (const table of tables) {
      for (const tr of rowsOf(table)) {
        // A header row, or a body row that only repeats the header labels.
        const labelled = tr.cells.filter((c) => HEADER_LABELS.some(([re]) => re.test(c.replace(DEVANAGARI_RUN, "").trim()))).length;
        if (tr.header || labelled >= 3) {
          const read = readHeader(tr.cells);
          columns = read.columns;
          if (read.vishay && !vishay) vishay = read.vishay;
          continue;
        }
        if (tr.cells.every((c) => !c)) continue;
        const cells = assign(columns, tr.cells);
        const row: BoxRow = {
          sr: (cells.sr ?? "").replace(/\.$/, ""),
          vt: cells.vt ?? "",
          granth: cells.granth ?? "",
          shastraPath: cells.shastraPath ?? "",
          pubRem: cells.pubRem ?? "",
          inRem: cells.inRem ?? "",
          pages: [page],
        };
        // The V.T cell sometimes swallows the Granth's first line ("स्व. गिरिसूरि टीका…").
        const vtHead = row.vt.match(/^([ऀ-ॿ]{1,6}\.)\s+(.+)$/);
        if (vtHead && !SR.test(vtHead[1])) row.vt = vtHead[1];

        // A row with no Sr. and no V.T carries on the row before it: always at the top of a
        // page (the passage ran over the page end), and anywhere when it lacks a Granth or a passage.
        const previous = rows[rows.length - 1];
        const continues = previous && !row.sr && !row.vt && (firstOnPage || !row.granth || !row.shastraPath);
        firstOnPage = false;
        if (continues) {
          if (row.granth) previous.granth = `${previous.granth} ${row.granth}`.trim();
          if (row.shastraPath) previous.shastraPath = `${previous.shastraPath} ${row.shastraPath}`.trim();
          if (row.pubRem && !previous.pubRem.includes(row.pubRem)) previous.pubRem = `${previous.pubRem} ${row.pubRem}`.trim();
          if (row.inRem && !previous.inRem.includes(row.inRem)) previous.inRem = `${previous.inRem} ${row.inRem}`.trim();
          if (!previous.pages.includes(page)) previous.pages.push(page);
          continue;
        }
        if (!row.shastraPath && !row.granth) continue;
        rows.push(row);
      }
    }
  }
  return { vishay, box, rows, loose };
}

const DEVANAGARI_RUN = /[ऀ-ॿ][ऀ-ॿ\s]*/g;
