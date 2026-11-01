// The anuvaad's markup, read into blocks and written back. The markup is what
// the reader edits; the blocks are what the page and the PDF draw:
//
//   # heading                 → title
//   વિષય: …                   → vishay
//   ## ૧. part                → section
//   - **label:** text         → point (level 1)
//     - **label:** text       → point (level 2)
//   anything else             → paragraph
//
// No server code here: the browser page uses it too.

export type AnuvaadBlock =
  | { kind: "title"; text: string }
  | { kind: "vishay"; text: string }
  | { kind: "section"; text: string }
  | { kind: "para"; text: string }
  | { kind: "point"; label: string; text: string; level: 1 | 2 };

const LABELLED = /^\*\*(.+?)\*\*\s*[:：]?\s*(.*)$/;

/** The markup as blocks; lines it does not recognise are kept as paragraphs. */
export function parseAnuvaad(markup: string): AnuvaadBlock[] {
  const blocks: AnuvaadBlock[] = [];
  const lines = markup
    .replace(/\r/g, "")
    .replace(/^```[a-z]*\s*$/gim, "")
    .split("\n");
  for (const raw of lines) {
    if (!raw.trim()) continue;
    const indent = (raw.match(/^\s*/)?.[0] ?? "").replace(/\t/g, "  ").length;
    const line = raw.trim();
    if (/^#\s+/.test(line)) {
      blocks.push({ kind: "title", text: clean(line.replace(/^#\s+/, "")) });
    } else if (/^#{2,}\s+/.test(line)) {
      blocks.push({ kind: "section", text: clean(line.replace(/^#{2,}\s+/, "")) });
    } else if (/^\**\s*વિષય\s*\**\s*[:：]/.test(line)) {
      blocks.push({ kind: "vishay", text: clean(line.replace(/^\**\s*વિષય\s*\**\s*[:：]\s*\**\s*/, "")) });
    } else if (/^[-*•◦○o]\s+/.test(line) && !/^\*\*/.test(line)) {
      const body = line.replace(/^[-*•◦○o]\s+/, "");
      const m = body.match(LABELLED);
      const level: 1 | 2 = indent >= 2 || /^[◦○o]\s/.test(line) ? 2 : 1;
      blocks.push(m ? { kind: "point", label: clean(m[1]).replace(/[:：]$/, ""), text: clean(m[2]), level } : { kind: "point", label: "", text: clean(body), level });
    } else if (LABELLED.test(line)) {
      const m = line.match(LABELLED)!;
      blocks.push({ kind: "point", label: clean(m[1]).replace(/[:：]$/, ""), text: clean(m[2]), level: 1 });
    } else {
      blocks.push({ kind: "para", text: clean(line) });
    }
  }
  return blocks;
}

/** Markdown emphasis the reader should not see as stars. */
function clean(text: string) {
  return text.replace(/\*\*(.+?)\*\*/g, "$1").replace(/(^|\s)\*(\S[^*]*?)\*(?=\s|$)/g, "$1$2").replace(/\s+/g, " ").trim();
}

/** Blocks back to markup, for editing by hand. */
export function toMarkup(blocks: AnuvaadBlock[]) {
  return blocks
    .map((b) => {
      switch (b.kind) {
        case "title":
          return `# ${b.text}`;
        case "vishay":
          return `વિષય: ${b.text}`;
        case "section":
          return `## ${b.text}`;
        case "point":
          return `${b.level === 2 ? "  " : ""}- ${b.label ? `**${b.label}:** ` : ""}${b.text}`;
        default:
          return b.text;
      }
    })
    .join("\n");
}

