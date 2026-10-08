// One reading per kosh entry, kept in Turso (kosh_entry_readings) so every
// vishay that cites an entry prints the same derivation and the same full
// meaning. Read fresh for each vishay, Shabda Ratna Mahodadhi's देवी (भाग-2,
// पृ. 1114) came out at four lengths on four parṣadā sheets, and Sahebji asked
// why (voice note, 7 Oct 2026).
//
// Only what the kosh prints is kept: the headword, label, derivation and
// meanings. Nothing about a vishay is stored (Sahebji's rule): whether a sense
// fits the vishay is judged again on every run.
//
// A cache that cannot be reached costs a fresh reading, never the run.

import { getTursoClient } from "@/lib/turso";

/** Raised when the reading prompt changes, so older readings are read again. */
export const READING_VERSION = 2;

export type EntryReading = {
  is_entry?: boolean;
  why?: string;
  head?: string;
  gender?: string;
  derivation?: string;
  meaning?: string;
  meaning_gu?: string;
  base?: string | null;
};

let ready: Promise<unknown> | null = null;

function ensureTable() {
  ready ??= getTursoClient().execute(
    `CREATE TABLE IF NOT EXISTS kosh_entry_readings (
      entry_id TEXT NOT NULL,
      word_key TEXT NOT NULL,
      engine TEXT NOT NULL,
      version INTEGER NOT NULL,
      reading TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (entry_id, word_key, engine, version)
    )`
  );
  ready.catch(() => {
    ready = null;
  });
  return ready;
}

export const cacheKey = (entryId: string, wordKey: string) => `${entryId}|${wordKey}`;

export async function loadReadings(keys: Array<{ entryId: string; wordKey: string }>, engine: string) {
  const out = new Map<string, EntryReading>();
  if (!keys.length) return out;
  try {
    await ensureTable();
    const ids = [...new Set(keys.map((k) => k.entryId))];
    const result = await getTursoClient().execute({
      sql: `SELECT entry_id, word_key, reading FROM kosh_entry_readings WHERE engine = ? AND version = ? AND entry_id IN (${ids.map(() => "?").join(",")})`,
      args: [engine, READING_VERSION, ...ids],
    });
    const wanted = new Set(keys.map((k) => cacheKey(k.entryId, k.wordKey)));
    for (const row of result.rows) {
      const key = cacheKey(String(row.entry_id), String(row.word_key));
      if (!wanted.has(key)) continue;
      try {
        out.set(key, JSON.parse(String(row.reading)));
      } catch {
        // an unreadable row is read again
      }
    }
  } catch {
    // no cache: every entry is read fresh
  }
  return out;
}

export async function saveReadings(items: Array<{ entryId: string; wordKey: string; reading: EntryReading }>, engine: string) {
  if (!items.length) return;
  try {
    await ensureTable();
    await getTursoClient().batch(
      items.map((item) => ({
        sql: "INSERT OR REPLACE INTO kosh_entry_readings (entry_id, word_key, engine, version, reading) VALUES (?, ?, ?, ?, ?)",
        args: [item.entryId, item.wordKey, engine, READING_VERSION, JSON.stringify(item.reading)],
      })),
      "write"
    );
  } catch {
    // not kept: read again next time
  }
}
