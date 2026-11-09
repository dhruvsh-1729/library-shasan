import type { Client } from "@libsql/client";
import { type OCRSearchModes, foldedNeedlesForQuery, searchModeList } from "./ocr-search";
import { FOLDED_SCHEMA_STATEMENTS, FOLDED_TABLES, foldedUpsertStatements } from "./ocr-folded-index.mjs";
import { reverseCodePoints as reverseFoldedCodePoints } from "./sanskrit-fold.mjs";

const WORD_TOKEN_PATTERN = /[\p{L}\p{N}\p{M}_]+/gu;
type SearchIndexExecutor = Pick<Client, "execute">;
let ensureSchemaPromise: Promise<void> | null = null;

/**
 * Reverses by CODE POINT, not by grapheme cluster.
 *
 * Grapheme reversal looks right but silently breaks "ends with": a virama
 * welds a consonant to the next one into one cluster, so in अर्हिंसा the ह
 * that starts a search for हिंसा sits *inside* the cluster र्हिं. The reversed
 * token then never begins with the reversed query, the page is never
 * returned, and the match disappears with no error. Code-point reversal
 * matches how findOCRSearchMatches scans the text, so the two agree.
 */
function reverseCodePoints(input: string) {
  if (!input) return "";
  return Array.from(input).reverse().join("");
}

/** Canonical form for both stored text and queries, so the same glyph written
 *  two ways (e.g. nukta before vs after virama) compares equal. */
export function normalizeOCRText(input: string) {
  return String(input ?? "").normalize("NFC");
}

export function buildOCRSuffixIndexContent(content: string) {
  const tokens = normalizeOCRText(content).match(WORD_TOKEN_PATTERN) ?? [];
  return tokens.map(reverseCodePoints).join(" ");
}

export function buildOCRSuffixQuery(query: string) {
  return `${escapeFtsToken(reverseCodePoints(normalizeOCRText(query).trim()))}*`;
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

  for (const sql of [...statements, ...FOLDED_SCHEMA_STATEMENTS]) {
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

  // The folded index is what search reads; keep it in step with the page.
  for (const statement of foldedUpsertStatements({ id: pageId, granth_key: granthKey, page_number: pageNumber, content })) {
    await client.execute(statement as { sql: string; args: Array<string | number> });
  }
}

/**
 * The FTS table(s) and MATCH expression(s) that prefilter pages for a set of
 * queries. Everything runs on the folded index, so the MATCH terms are folded
 * with the same rules as the indexed text; findOCRSearchMatches then verifies
 * every candidate page against its real content.
 *
 * One Find option reads one table. Two may read two (anywhere-inside-a-word
 * reads the letter-triple index, the word options the word index): `parts`
 * holds one MATCH per table, and a page is a candidate when any part has it.
 * `table`/`match` are the first part, for callers that only ever pass one option.
 */
export function buildOCRPrefilter(queries: string[], modes: OCRSearchModes) {
  const byTable = new Map<string, Set<string>>();
  for (const mode of searchModeList(modes)) {
    const table =
      mode === "contains" ? FOLDED_TABLES.trigram : mode === "ends_with" ? FOLDED_TABLES.suffix : FOLDED_TABLES.words;
    const terms = byTable.get(table) ?? new Set<string>();
    byTable.set(table, terms);
    for (const query of queries) {
      for (const needle of foldedNeedlesForQuery(query, mode)) {
        if (mode === "begins_with") terms.add(`${escapeFtsToken(needle)}*`);
        else if (mode === "ends_with") terms.add(`${escapeFtsToken(reverseFoldedCodePoints(needle))}*`);
        else terms.add(escapeFtsPhrase(needle));
      }
    }
  }
  const parts = [...byTable]
    .filter(([, terms]) => terms.size > 0)
    .map(([table, terms]) => ({ table, match: [...terms].join(" OR "), termCount: terms.size }));
  if (!parts.length) parts.push({ table: FOLDED_TABLES.words, match: "", termCount: 0 });
  return { ...parts[0], termCount: parts.reduce((n, part) => n + part.termCount, 0), parts };
}

type Prefilter = ReturnType<typeof buildOCRPrefilter>;

/**
 * The prefilter as the start of a page query: `from` joins the hit pages as
 * `p` (ocr_pages), `where` is the index condition, `args` its values. One table
 * keeps the plain FTS join; two read the union of both indexes' page ids.
 */
export function prefilterFrom(prefilter: Prefilter) {
  if (prefilter.parts.length === 1) {
    const { table, match } = prefilter.parts[0];
    return { from: `FROM ${table} JOIN ocr_pages p ON p.id = ${table}.rowid`, where: `${table} MATCH ?`, args: [match] };
  }
  const ids = prefilterIds(prefilter);
  return { from: `FROM (${ids.sql}) hit_ids JOIN ocr_pages p ON p.id = hit_ids.id`, where: "1 = 1", args: ids.args };
}

/** Page ids the prefilter offers (column `id`), every index read once; duplicates removed by UNION. */
export function prefilterIds(prefilter: Prefilter) {
  return {
    sql: prefilter.parts.map(({ table }) => `SELECT rowid AS id FROM ${table} WHERE ${table} MATCH ?`).join(" UNION "),
    args: prefilter.parts.map((part) => part.match),
  };
}

/** Page ids after `after`, in order, `limit` at a time (a cursor over the hits). */
export function prefilterIdsAfter(prefilter: Prefilter, after: number, limit: number) {
  if (prefilter.parts.length === 1) {
    const { table, match } = prefilter.parts[0];
    return { sql: `SELECT rowid AS id FROM ${table} WHERE ${table} MATCH ? AND rowid > ? ORDER BY rowid LIMIT ?`, args: [match, after, limit] };
  }
  return {
    sql: `SELECT id FROM (${prefilter.parts
      .map(({ table }) => `SELECT rowid AS id FROM ${table} WHERE ${table} MATCH ? AND rowid > ?`)
      .join(" UNION ")}) ORDER BY id LIMIT ?`,
    args: [...prefilter.parts.flatMap((part) => [part.match, after]), limit],
  };
}

/** How many pages the prefilter offers. */
export function prefilterCount(prefilter: Prefilter) {
  if (prefilter.parts.length === 1) {
    const { table, match } = prefilter.parts[0];
    return { sql: `SELECT count(*) AS n FROM ${table} WHERE ${table} MATCH ?`, args: [match] };
  }
  const ids = prefilterIds(prefilter);
  return { sql: `SELECT count(*) AS n FROM (${ids.sql})`, args: ids.args };
}
