import type { Client } from "@libsql/client";

const WORD_TOKEN_PATTERN = /[\p{L}\p{N}\p{M}_]+/gu;
type SearchIndexExecutor = Pick<Client, "execute">;
let ensureSchemaPromise: Promise<void> | null = null;

const graphemeSegmenter =
  typeof Intl !== "undefined" && typeof Intl.Segmenter === "function"
    ? new Intl.Segmenter(undefined, { granularity: "grapheme" })
    : null;

function reverseGraphemes(input: string) {
  if (!input) return "";
  if (!graphemeSegmenter) return Array.from(input).reverse().join("");
  return Array.from(graphemeSegmenter.segment(input), (segment) => segment.segment).reverse().join("");
}

export function buildOCRSuffixIndexContent(content: string) {
  const tokens = String(content ?? "").match(WORD_TOKEN_PATTERN) ?? [];
  return tokens.map(reverseGraphemes).join(" ");
}

export function buildOCRSuffixQuery(query: string) {
  return `${escapeFtsToken(reverseGraphemes(String(query ?? "").trim()))}*`;
}

export function escapeFtsPhrase(input: string) {
  return `"${String(input ?? "").replace(/"/g, '""')}"`;
}

export function escapeFtsToken(input: string) {
  return escapeFtsPhrase(input);
}

async function createOCRSearchSchema(client: SearchIndexExecutor) {
  const statements = [
    "PRAGMA foreign_keys = ON;",
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_search_fts USING fts5(
      content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages',
      content_rowid='id',
      tokenize="unicode61 remove_diacritics 0 categories 'L* N* Co M*'",
      prefix='2 3 4'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_search_ai AFTER INSERT ON ocr_pages BEGIN
      INSERT INTO ocr_pages_search_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_search_ad AFTER DELETE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_search_fts(ocr_pages_search_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_search_au AFTER UPDATE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_search_fts(ocr_pages_search_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_search_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_trigram_fts USING fts5(
      content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages',
      content_rowid='id',
      tokenize='trigram'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_trigram_ai AFTER INSERT ON ocr_pages BEGIN
      INSERT INTO ocr_pages_trigram_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_trigram_ad AFTER DELETE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_trigram_fts(ocr_pages_trigram_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_trigram_au AFTER UPDATE ON ocr_pages BEGIN
      INSERT INTO ocr_pages_trigram_fts(ocr_pages_trigram_fts, rowid, content, granth_key, page_number)
      VALUES ('delete', old.id, old.content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_trigram_fts(rowid, content, granth_key, page_number)
      VALUES (new.id, new.content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TABLE IF NOT EXISTS ocr_pages_suffix (
      page_id INTEGER PRIMARY KEY,
      granth_key TEXT NOT NULL,
      page_number INTEGER NOT NULL,
      reversed_content TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (page_id) REFERENCES ocr_pages(id) ON DELETE CASCADE,
      UNIQUE (granth_key, page_number)
    );`,
    "CREATE INDEX IF NOT EXISTS idx_ocr_pages_suffix_granth_page ON ocr_pages_suffix(granth_key, page_number);",
    `CREATE VIRTUAL TABLE IF NOT EXISTS ocr_pages_suffix_fts USING fts5(
      reversed_content,
      granth_key UNINDEXED,
      page_number UNINDEXED,
      content='ocr_pages_suffix',
      content_rowid='page_id',
      tokenize="unicode61 remove_diacritics 0 categories 'L* N* Co M*'",
      prefix='2 3 4'
    );`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_ai AFTER INSERT ON ocr_pages_suffix BEGIN
      INSERT INTO ocr_pages_suffix_fts(rowid, reversed_content, granth_key, page_number)
      VALUES (new.page_id, new.reversed_content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_ad AFTER DELETE ON ocr_pages_suffix BEGIN
      INSERT INTO ocr_pages_suffix_fts(ocr_pages_suffix_fts, rowid, reversed_content, granth_key, page_number)
      VALUES ('delete', old.page_id, old.reversed_content, old.granth_key, CAST(old.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_au AFTER UPDATE ON ocr_pages_suffix BEGIN
      INSERT INTO ocr_pages_suffix_fts(ocr_pages_suffix_fts, rowid, reversed_content, granth_key, page_number)
      VALUES ('delete', old.page_id, old.reversed_content, old.granth_key, CAST(old.page_number AS TEXT));
      INSERT INTO ocr_pages_suffix_fts(rowid, reversed_content, granth_key, page_number)
      VALUES (new.page_id, new.reversed_content, new.granth_key, CAST(new.page_number AS TEXT));
    END;`,
    `CREATE TRIGGER IF NOT EXISTS ocr_pages_suffix_cleanup AFTER DELETE ON ocr_pages BEGIN
      DELETE FROM ocr_pages_suffix WHERE page_id = old.id;
    END;`,
  ];

  for (const sql of statements) {
    await client.execute(sql);
  }
}

export async function ensureOCRSearchSchema(client: SearchIndexExecutor) {
  if (!ensureSchemaPromise) {
    ensureSchemaPromise = createOCRSearchSchema(client).catch((error) => {
      ensureSchemaPromise = null;
      throw error;
    });
  }
  return ensureSchemaPromise;
}

export async function upsertOCRPageSuffixIndex(
  client: SearchIndexExecutor,
  granthKey: string,
  pageNumber: number,
  content: string
) {
  const pageResult = await client.execute({
    sql: `SELECT id FROM ocr_pages WHERE granth_key = ? AND page_number = ?`,
    args: [granthKey, pageNumber],
  });
  const pageId = pageResult.rows[0]?.id;
  if (pageId == null) return;

  await client.execute({
    sql: `INSERT INTO ocr_pages_suffix (page_id, granth_key, page_number, reversed_content, updated_at)
          SELECT id, granth_key, page_number, ?, CURRENT_TIMESTAMP
          FROM ocr_pages
          WHERE granth_key = ? AND page_number = ?
          ON CONFLICT(page_id) DO UPDATE SET
            granth_key = excluded.granth_key,
            page_number = excluded.page_number,
            reversed_content = excluded.reversed_content,
            updated_at = CURRENT_TIMESTAMP`,
    args: [buildOCRSuffixIndexContent(content), granthKey, pageNumber],
  });
}
