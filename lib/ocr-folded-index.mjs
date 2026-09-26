// The folded search index: word, trigram and suffix FTS over every
// ocr_pages row's text folded by lib/sanskrit-fold.mjs.
//
// The FTS tables are contentless (content='', contentless_delete=1): search
// only needs page ids from them, and every candidate page is verified against
// ocr_pages.content, so a second copy of the corpus is not stored.
//
// SQLite cannot run the fold, so rows are written by the app and the
// pipeline scripts (syncFoldedIndex / upsertFoldedPages). A trigger marks a
// page stale when its text changes; search still uses the stale entry as a
// prefilter and the next sync re-folds it. Plain JS so both Next.js and the
// Node scripts use it.

import { SANSKRIT_FOLD_VERSION, buildFoldedIndexRow } from "./sanskrit-fold.mjs";

const TOKENIZE_WORDS = `tokenize="unicode61 remove_diacritics 0 categories 'L* N* Co M*'"`;

export const FOLDED_TABLES = {
  state: "ocr_pages_folded_state",
  words: "ocr_pages_folded_fts",
  trigram: "ocr_pages_folded_trigram_fts",
  suffix: "ocr_pages_folded_suffix_fts",
};

export const FOLDED_SCHEMA_STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS ocr_pages_folded_state (
    page_id INTEGER PRIMARY KEY,
    granth_key TEXT NOT NULL,
    page_number INTEGER NOT NULL,
    fold_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  );`,
  "CREATE INDEX IF NOT EXISTS idx_ocr_pages_folded_state_granth ON ocr_pages_folded_state(granth_key, page_number);",
  `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_folded_fts USING fts5(
    folded_content, content='', contentless_delete=1, ${TOKENIZE_WORDS}, prefix='2 3 4'
  );`,
  `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_folded_trigram_fts USING fts5(
    folded_content, content='', contentless_delete=1, tokenize='trigram'
  );`,
  `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_folded_suffix_fts USING fts5(
    reversed_content, content='', contentless_delete=1, ${TOKENIZE_WORDS}, prefix='2 3 4'
  );`,
  `CREATE TRIGGER IF NOT EXISTS ocr_pages_folded_stale AFTER UPDATE OF content ON ocr_pages BEGIN
    UPDATE ocr_pages_folded_state SET fold_version = 0 WHERE page_id = old.id;
  END;`,
  `CREATE TRIGGER IF NOT EXISTS ocr_pages_folded_cleanup AFTER DELETE ON ocr_pages BEGIN
    DELETE FROM ocr_pages_folded_fts WHERE rowid = old.id;
    DELETE FROM ocr_pages_folded_trigram_fts WHERE rowid = old.id;
    DELETE FROM ocr_pages_folded_suffix_fts WHERE rowid = old.id;
    DELETE FROM ocr_pages_folded_state WHERE page_id = old.id;
  END;`,
];

export async function ensureFoldedSchema(client) {
  for (const sql of FOLDED_SCHEMA_STATEMENTS) await client.execute(sql);
}

/** Statements that (re)index one ocr_pages row ({id, granth_key, page_number, content}). */
export function foldedUpsertStatements(page) {
  const id = Number(page.id);
  const { folded, reversed } = buildFoldedIndexRow(page.content);
  return [
    { sql: "INSERT OR REPLACE INTO ocr_pages_folded_fts(rowid, folded_content) VALUES (?, ?)", args: [id, folded] },
    { sql: "INSERT OR REPLACE INTO ocr_pages_folded_trigram_fts(rowid, folded_content) VALUES (?, ?)", args: [id, folded] },
    { sql: "INSERT OR REPLACE INTO ocr_pages_folded_suffix_fts(rowid, reversed_content) VALUES (?, ?)", args: [id, reversed] },
    {
      sql: `INSERT INTO ocr_pages_folded_state (page_id, granth_key, page_number, fold_version, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(page_id) DO UPDATE SET
              granth_key = excluded.granth_key,
              page_number = excluded.page_number,
              fold_version = excluded.fold_version,
              updated_at = CURRENT_TIMESTAMP`,
      args: [id, String(page.granth_key), Number(page.page_number), SANSKRIT_FOLD_VERSION],
    },
  ];
}

/** Re-folds the given ocr_pages rows ({id, granth_key, page_number, content}). */
export async function upsertFoldedPages(client, pages) {
  if (!pages.length) return 0;
  await client.batch(pages.flatMap(foldedUpsertStatements), "write");
  return pages.length;
}

/**
 * Brings the folded index up to date: pages with no row, a stale row, or a
 * row from an older fold version are re-folded; rows of deleted pages go.
 * Scope with { granthKey } after writing one granth.
 */
export async function syncFoldedIndex(
  client,
  { granthKey = null, batchSize = 100, onProgress = null, minId = 0, maxId = null, skipOrphans = false } = {}
) {
  await ensureFoldedSchema(client);
  const scope = `${granthKey ? " AND p.granth_key = ?" : ""}${maxId != null ? " AND p.id <= ?" : ""}`;
  const scopeArgs = [...(granthKey ? [String(granthKey)] : []), ...(maxId != null ? [Number(maxId)] : [])];
  let done = 0;
  let lastId = Math.max(0, Number(minId) - 1);
  for (;;) {
    const r = await client.execute({
      sql: `SELECT p.id, p.granth_key, p.page_number, p.content
            FROM ocr_pages p
            LEFT JOIN ocr_pages_folded_state f ON f.page_id = p.id
            WHERE p.id > ? AND (f.page_id IS NULL OR f.fold_version != ?)${scope}
            ORDER BY p.id
            LIMIT ?`,
      args: [lastId, SANSKRIT_FOLD_VERSION, ...scopeArgs, batchSize],
    });
    if (!r.rows.length) break;
    await upsertFoldedPages(client, r.rows);
    done += r.rows.length;
    lastId = Number(r.rows[r.rows.length - 1].id);
    if (onProgress) onProgress(done, lastId);
  }
  if (skipOrphans) return done;
  // Pages deleted while the cleanup trigger did not exist yet.
  const orphans = await client.execute({
    sql: `SELECT page_id FROM ocr_pages_folded_state s
          WHERE NOT EXISTS (SELECT 1 FROM ocr_pages p WHERE p.id = s.page_id)${granthKey ? " AND s.granth_key = ?" : ""}`,
    args: granthKey ? [String(granthKey)] : [],
  });
  for (const row of orphans.rows) {
    const id = Number(row.page_id);
    await client.batch(
      [
        { sql: "DELETE FROM ocr_pages_folded_fts WHERE rowid = ?", args: [id] },
        { sql: "DELETE FROM ocr_pages_folded_trigram_fts WHERE rowid = ?", args: [id] },
        { sql: "DELETE FROM ocr_pages_folded_suffix_fts WHERE rowid = ?", args: [id] },
        { sql: "DELETE FROM ocr_pages_folded_state WHERE page_id = ?", args: [id] },
      ],
      "write"
    );
  }
  return done;
}
