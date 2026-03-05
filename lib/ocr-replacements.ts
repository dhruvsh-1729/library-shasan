import type { Client } from "@libsql/client";

let schemaReady = false;

export function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function findExactWordMatches(content: string, word: string) {
  const normalizedWord = String(word ?? "");
  if (!normalizedWord) return [];

  const pattern = new RegExp(`(^|\\s)(${escapeRegExp(normalizedWord)})(?=\\s|$)`, "g");
  const out: Array<{ start: number; end: number; word: string; context: string }> = [];

  let match: RegExpExecArray | null;
  while ((match = pattern.exec(content)) !== null) {
    const leftBoundary = match[1] ?? "";
    const capturedWord = match[2] ?? "";
    const start = (match.index ?? 0) + leftBoundary.length;
    const end = start + capturedWord.length;

    const contextStart = Math.max(0, start - 40);
    const contextEnd = Math.min(content.length, end + 40);
    const context = content.slice(contextStart, contextEnd);

    out.push({
      start,
      end,
      word: capturedWord,
      context,
    });
  }

  return out;
}

export function hasWhitespaceWordBoundary(content: string, start: number, end: number) {
  const left = start <= 0 ? " " : content[start - 1] ?? " ";
  const right = end >= content.length ? " " : content[end] ?? " ";
  return /\s/.test(left) && /\s/.test(right);
}

export async function ensureReplacementSchema(client: Client) {
  if (schemaReady) return;

  const sql = [
    `CREATE TABLE IF NOT EXISTS ocr_word_changes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      change_group_id TEXT,
      granth_key TEXT NOT NULL,
      page_number INTEGER NOT NULL,
      match_start INTEGER NOT NULL,
      match_end INTEGER NOT NULL,
      old_word TEXT NOT NULL,
      new_word TEXT NOT NULL,
      old_content TEXT NOT NULL,
      new_content TEXT NOT NULL,
      changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      reverted_from_change_id INTEGER,
      FOREIGN KEY (granth_key) REFERENCES ocr_granths(granth_key) ON DELETE CASCADE
    );`,
    "CREATE INDEX IF NOT EXISTS idx_ocr_word_changes_granth_page ON ocr_word_changes(granth_key, page_number);",
    "CREATE INDEX IF NOT EXISTS idx_ocr_word_changes_word ON ocr_word_changes(old_word, new_word);",
    "CREATE INDEX IF NOT EXISTS idx_ocr_word_changes_changed_at ON ocr_word_changes(changed_at DESC);",
  ];

  for (const stmt of sql) {
    await client.execute(stmt);
  }

  schemaReady = true;
}
