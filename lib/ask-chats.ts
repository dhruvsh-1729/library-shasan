// Saved "Ask the library" conversations, in Turso. Each chat belongs to one
// signed-in user and is only ever read or changed on that user's behalf.
// A chat keeps what it was reading (scope, answer language, think-harder) so
// reopening it continues where it left off.

import { randomUUID } from "node:crypto";
import { getTursoClient } from "@/lib/turso";

export type StoredMessage = {
  id: number;
  role: "user" | "assistant";
  content: string;
  /** Sources, scope line, notes: whatever the page shows under a message. */
  meta: Record<string, unknown>;
  created_at: string;
};

export type ChatSummary = { id: string; title: string; updated_at: string; message_count: number };

export type StoredChat = ChatSummary & {
  scope: Record<string, unknown> | null;
  language: string | null;
  deep: boolean;
  messages: StoredMessage[];
};

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS ask_chats (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    title TEXT NOT NULL,
    scope_json TEXT,
    language TEXT,
    deep INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`,
  "CREATE INDEX IF NOT EXISTS idx_ask_chats_user_updated ON ask_chats(user_id, updated_at DESC)",
  `CREATE TABLE IF NOT EXISTS ask_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL REFERENCES ask_chats(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    meta_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`,
  "CREATE INDEX IF NOT EXISTS idx_ask_messages_chat ON ask_messages(chat_id, id)",
];

let ready: Promise<void> | null = null;
function ensureSchema() {
  ready ??= (async () => {
    const client = getTursoClient();
    for (const sql of SCHEMA) await client.execute(sql);
  })().catch((error) => {
    ready = null;
    throw error;
  });
  return ready;
}

function parseJson(value: unknown) {
  if (typeof value !== "string" || !value) return null;
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** A title from the first question: its first line, cut at a word. */
export function chatTitle(question: string) {
  const line = question.trim().split(/\r?\n/)[0] ?? "";
  if (line.length <= 70) return line || "New chat";
  const cut = line.slice(0, 70);
  return `${cut.slice(0, Math.max(40, cut.lastIndexOf(" ")))}…`;
}

/** The user's chats, newest first; with `query`, only those whose title or messages contain it. */
export async function listChats(userId: string, query = ""): Promise<ChatSummary[]> {
  await ensureSchema();
  const q = query.trim().slice(0, 100);
  const like = `%${q.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;
  const filter = q
    ? ` AND (c.title LIKE ? ESCAPE '\\' OR EXISTS (SELECT 1 FROM ask_messages m2 WHERE m2.chat_id = c.id AND m2.content LIKE ? ESCAPE '\\'))`
    : "";
  const result = await getTursoClient().execute({
    sql: `SELECT c.id, c.title, c.updated_at, (SELECT COUNT(*) FROM ask_messages m WHERE m.chat_id = c.id) AS message_count
          FROM ask_chats c WHERE c.user_id = ?${filter} ORDER BY c.updated_at DESC LIMIT 200`,
    args: q ? [userId, like, like] : [userId],
  });
  return result.rows.map((row) => ({
    id: String(row.id),
    title: String(row.title),
    updated_at: String(row.updated_at),
    message_count: Number(row.message_count ?? 0),
  }));
}

export async function getChat(userId: string, chatId: string): Promise<StoredChat | null> {
  await ensureSchema();
  const client = getTursoClient();
  const chat = (
    await client.execute({ sql: "SELECT * FROM ask_chats WHERE id = ? AND user_id = ?", args: [chatId, userId] })
  ).rows[0];
  if (!chat) return null;
  const messages = (
    await client.execute({
      sql: "SELECT id, role, content, meta_json, created_at FROM ask_messages WHERE chat_id = ? ORDER BY id",
      args: [chatId],
    })
  ).rows.map((row) => ({
    id: Number(row.id),
    role: String(row.role) as "user" | "assistant",
    content: String(row.content),
    meta: parseJson(row.meta_json) ?? {},
    created_at: String(row.created_at),
  }));
  return {
    id: String(chat.id),
    title: String(chat.title),
    updated_at: String(chat.updated_at),
    message_count: messages.length,
    scope: parseJson(chat.scope_json),
    language: chat.language == null ? null : String(chat.language),
    deep: Number(chat.deep ?? 0) === 1,
    messages,
  };
}

/**
 * Saves one question and its answer, creating the chat on its first turn.
 * Returns the chat id. A chatId that is not the user's starts a new chat.
 */
export async function saveTurn(opts: {
  userId: string;
  chatId?: string | null;
  question: string;
  answer: string;
  answerMeta: Record<string, unknown>;
  questionMeta: Record<string, unknown>;
  scope: Record<string, unknown> | null;
  language: string;
  deep: boolean;
}) {
  await ensureSchema();
  const client = getTursoClient();
  let chatId = opts.chatId ? String(opts.chatId) : "";
  if (chatId) {
    const owned = await client.execute({ sql: "SELECT 1 FROM ask_chats WHERE id = ? AND user_id = ?", args: [chatId, opts.userId] });
    if (!owned.rows.length) chatId = "";
  }
  const scopeJson = opts.scope ? JSON.stringify(opts.scope) : null;
  const statements: Array<{ sql: string; args: Array<string | number | null> }> = [];
  if (!chatId) {
    chatId = randomUUID();
    statements.push({
      sql: "INSERT INTO ask_chats (id, user_id, title, scope_json, language, deep) VALUES (?, ?, ?, ?, ?, ?)",
      args: [chatId, opts.userId, chatTitle(opts.question), scopeJson, opts.language, opts.deep ? 1 : 0],
    });
  } else {
    statements.push({
      sql: "UPDATE ask_chats SET scope_json = ?, language = ?, deep = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
      args: [scopeJson, opts.language, opts.deep ? 1 : 0, chatId],
    });
  }
  statements.push(
    {
      sql: "INSERT INTO ask_messages (chat_id, role, content, meta_json) VALUES (?, 'user', ?, ?)",
      args: [chatId, opts.question, JSON.stringify(opts.questionMeta)],
    },
    {
      sql: "INSERT INTO ask_messages (chat_id, role, content, meta_json) VALUES (?, 'assistant', ?, ?)",
      args: [chatId, opts.answer, JSON.stringify(opts.answerMeta)],
    }
  );
  await client.batch(statements, "write");
  return chatId;
}

/** The last turns of a stored chat, for the model's context on a follow-up. */
export async function recentTurns(userId: string, chatId: string, limit: number) {
  const chat = await getChat(userId, chatId);
  if (!chat) return [];
  return chat.messages
    .filter((m) => !m.meta?.error)
    .slice(-limit)
    .map((m) => ({ role: m.role, content: m.content }));
}

export async function deleteChat(userId: string, chatId: string) {
  await ensureSchema();
  const client = getTursoClient();
  const owned = await client.execute({ sql: "SELECT 1 FROM ask_chats WHERE id = ? AND user_id = ?", args: [chatId, userId] });
  if (!owned.rows.length) return false;
  await client.batch(
    [
      { sql: "DELETE FROM ask_messages WHERE chat_id = ?", args: [chatId] },
      { sql: "DELETE FROM ask_chats WHERE id = ?", args: [chatId] },
    ],
    "write"
  );
  return true;
}

export async function renameChat(userId: string, chatId: string, title: string) {
  await ensureSchema();
  const clean = title.trim().slice(0, 120);
  if (!clean) return false;
  const result = await getTursoClient().execute({
    sql: "UPDATE ask_chats SET title = ? WHERE id = ? AND user_id = ?",
    args: [clean, chatId, userId],
  });
  return result.rowsAffected > 0;
}
