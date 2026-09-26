type Executor = {
  execute: (stmt: string | { sql: string; args?: unknown[] }) => Promise<{ rows: Array<Record<string, unknown>> }>;
  batch: (stmts: Array<{ sql: string; args?: unknown[] }>, mode?: "write" | "read" | "deferred") => Promise<unknown>;
};
export const FOLDED_TABLES: { state: string; words: string; trigram: string; suffix: string };
export const FOLDED_SCHEMA_STATEMENTS: string[];
export function ensureFoldedSchema(client: Pick<Executor, "execute">): Promise<void>;
export function upsertFoldedPages(
  client: Executor,
  pages: Array<{ id: unknown; granth_key: unknown; page_number: unknown; content: unknown }>
): Promise<number>;
export function syncFoldedIndex(
  client: Executor,
  options?: {
    granthKey?: string | null;
    batchSize?: number;
    onProgress?: ((done: number, lastId: number) => void) | null;
    minId?: number;
    maxId?: number | null;
    skipOrphans?: boolean;
  }
): Promise<number>;
export function foldedUpsertStatements(page: {
  id: unknown;
  granth_key: unknown;
  page_number: unknown;
  content: unknown;
}): Array<{ sql: string; args: unknown[] }>;
