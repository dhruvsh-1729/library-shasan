import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { setNoStore } from "@/lib/api-cache";
import {
  resolveContext,
  parseScope,
  buildPrompt,
  buildMergePrompt,
  chunkPassages,
  limitToChunks,
  LANGUAGES,
  type ContextScope,
  type LanguageId,
  type ResolvedContext,
} from "@/lib/ai-context";
import { chatStream, CHAT_MODELS, SarvamChatError, type ChatTurn, type ChatModelId } from "@/lib/sarvam-chat";
import { parseOCRSearchMode } from "@/lib/ocr-search";
import { recentTurns, saveTurn } from "@/lib/ask-chats";
import { describeGranth, getGranthCatalog } from "@/lib/granth-catalog";
import type { SessionUser } from "@/lib/auth-users";

// Think-harder answers read in several parts can take minutes.
export const config = { maxDuration: 300 };

type Body = {
  scope?: Record<string, unknown>;
  question?: string;
  language?: string;
  model?: string;
  history?: Array<{ role?: string; content?: string }>;
  /** The saved chat this question continues; omitted for a new chat. */
  chatId?: string | null;
  /** How the page describes the scope ("Acharang · gatha 1–8"), kept with the question. */
  scopeLine?: string;
  /** Answer as a stream of NDJSON events (see AskEvent) instead of one JSON body. */
  stream?: boolean;
};

/**
 * What a streamed answer sends, one JSON object per line: the passages first,
 * so the page can show them while the model reads, then its thinking and the
 * answer as they arrive, and finally the saved result.
 */
type AskEvent =
  | { type: "context"; context: ReturnType<typeof describeContext> }
  | { type: "status"; text: string }
  | { type: "thinking"; delta: string }
  | { type: "answer"; delta: string }
  | { type: "restart" }
  | { type: "done"; [key: string]: unknown }
  | { type: "error"; error: string; [key: string]: unknown };

/** Kept with a saved answer, so a reopened chat can still show what the model thought. */
const MAX_SAVED_THINKING_CHARS = 20_000;

/**
 * Earlier turns compete with the scripture for the 32k window, so they are kept
 * short and few — enough for "explain that further", not enough to push the
 * passages out of the request.
 */
const MAX_HISTORY_TURNS = 4;
const MAX_HISTORY_CHARS = 900;

export function describeContext(context: ResolvedContext) {
  return {
    summaryLine: context.summaryLine,
    truncated: context.truncated,
    totalChars: context.totalChars,
    pages: context.passages.map((p) => p.pageNumber),
    verses: context.verses.map((v) => ({
      adhikar: v.adhikar,
      gatha: v.gatha,
      pageStart: v.pageStart,
      pageEnd: v.pageEnd,
    })),
    droppedGathas: context.droppedVerses.map((v) => v.gatha),
    passages: context.passages.map((p, i) => ({
      index: i + 1,
      granthKey: p.granthKey,
      granthName: p.granthName,
      pageNumber: p.pageNumber,
      label: p.label,
      preview: p.text.slice(0, 400),
    })),
  };
}

/**
 * A scope that does not fit one request is read in consecutive parts and the
 * part answers are merged, rather than dropping the tail of the scripture and
 * answering as if the whole of it had been read. The part answers are working
 * notes, so they stream as thinking; only the merged answer streams as answer.
 */
async function answer(opts: {
  context: ResolvedContext;
  chunks: ResolvedContext["passages"][];
  question: string;
  language: LanguageId;
  model: ChatModelId;
  history: ChatTurn[];
  emit: (event: AskEvent) => void;
}) {
  const { emit, chunks } = opts;
  let thinking = "";
  const think = (delta: string) => { thinking += delta; emit({ type: "thinking", delta }); };

  if (chunks.length <= 1) {
    const { system, user } = buildPrompt({ question: opts.question, language: opts.language, context: opts.context });
    const result = await chatStream({
      messages: [{ role: "system", content: system }, ...opts.history, { role: "user", content: user }],
      model: opts.model,
      onThinking: think,
      onAnswer: (delta) => emit({ type: "answer", delta }),
      onRestart: () => emit({ type: "restart" }),
    });
    return { content: result.content, thinking, model: result.model, usage: result.usage, parts: 1 };
  }

  const parts: string[] = [];
  for (const [i, passages] of chunks.entries()) {
    const label = `Reading part ${i + 1} of ${chunks.length}`;
    emit({ type: "status", text: label });
    think(`${thinking ? "\n\n" : ""}── ${label} ──\n`);
    const { system, user } = buildPrompt({
      question: opts.question,
      language: opts.language,
      context: opts.context,
      part: { index: i + 1, total: chunks.length },
      passages,
    });
    let sawThinking = false;
    const result = await chatStream({
      messages: [{ role: "system", content: system }, { role: "user", content: user }],
      model: opts.model,
      onThinking: (d) => { sawThinking = true; think(d); },
      onAnswer: (d) => { if (sawThinking) { think("\n\n"); sawThinking = false; } think(d); },
    });
    parts.push(result.content);
  }

  emit({ type: "status", text: "Bringing the parts together" });
  think("\n\n── Bringing the parts together ──\n");
  const merge = buildMergePrompt({
    question: opts.question,
    language: opts.language,
    context: opts.context,
    parts,
  });
  const merged = await chatStream({
    messages: [{ role: "system", content: merge.system }, { role: "user", content: merge.user }],
    model: opts.model,
    onThinking: think,
    onAnswer: (delta) => emit({ type: "answer", delta }),
    onRestart: () => emit({ type: "restart" }),
  });

  return { content: merged.content, thinking, model: merged.model, usage: merged.usage, parts: chunks.length };
}

async function handler(req: NextApiRequest, res: NextApiResponse, user: SessionUser) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  setNoStore(res);

  const body = (req.body ?? {}) as Body;
  const question = String(body.question ?? "").trim();
  if (!question) return res.status(400).json({ error: "Ask a question." });
  if (question.length > 2000) return res.status(400).json({ error: "That question is too long." });

  const language = (LANGUAGES.find((l) => l.id === body.language)?.id ?? "gujarati") as LanguageId;
  const model = body.model === "reasoning" ? CHAT_MODELS.reasoning : CHAT_MODELS.fast;

  const scope = parseScope(body.scope);
  if ("error" in scope) return res.status(400).json({ error: scope.error });

  // Every answered turn is saved, so a conversation can be reopened later.
  // Saving never costs the reader the answer: a storage failure is logged.
  const deep = body.model === "reasoning";

  // Streamed: every reply, including errors, is an event line on a 200.
  const streaming = body.stream === true;
  if (streaming) {
    res.writeHead(200, {
      "Content-Type": "application/x-ndjson; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      // Keeps proxies and compression from holding the stream back.
      "Content-Encoding": "none",
      "X-Accel-Buffering": "no",
    });
    res.flushHeaders?.();
  }
  const emit = (event: AskEvent) => {
    if (streaming && !res.writableEnded) res.write(`${JSON.stringify(event)}\n`);
  };
  /** Ends the request with `payload`: a final event when streaming, else JSON. */
  const finish = (status: number, payload: Record<string, unknown>) => {
    if (!streaming) return res.status(status).json(payload);
    emit(payload.error && !payload.answer ? { type: "error", error: String(payload.error), ...payload } : { type: "done", ...payload });
    res.end();
  };
  async function save(answerText: string, answerMeta: Record<string, unknown>) {
    try {
      return await saveTurn({
        userId: user.id,
        chatId: body.chatId,
        question,
        answer: answerText,
        answerMeta,
        questionMeta: { scopeLine: body.scopeLine ?? null },
        scope: (body.scope ?? null) as Record<string, unknown> | null,
        language,
        deep,
      });
    } catch (error) {
      console.error("ask chat save failed", error instanceof Error ? error.message : error);
      return body.chatId ?? null;
    }
  }

  try {
    const resolved = await resolveContext(scope);

    // A gatha number that exists in every chapter is not a scope. Answering it
    // would mean summarising whichever chapter sorted first.
    // What is read is decided once, here, so the sources shown, the "not read"
    // note and the model's input always agree.
    const { chunks, dropped } = chunkPassages(resolved.passages, resolved.verses);
    const context = limitToChunks(resolved, chunks.flat(), dropped);

    if (context.needsAdhikar) {
      const message = `${context.summaryLine} Choose the chapter you mean.`;
      const chatId = await save(message, { error: true, needsAdhikar: context.needsAdhikar });
      return finish(200, {
        chatId,
        answer: null,
        needsAdhikar: context.needsAdhikar,
        error: message,
        context: { summaryLine: context.summaryLine, passages: [], truncated: false },
      });
    }

    if (!context.passages.length) {
      const message = `No text found for that scope. ${context.summaryLine}`;
      const chatId = await save(message, { error: true });
      return finish(200, {
        chatId,
        answer: null,
        error: message,
        context: { summaryLine: context.summaryLine, passages: [], truncated: false },
      });
    }

    // A saved chat supplies its own earlier turns; otherwise the page's.
    const stored = body.chatId ? await recentTurns(user.id, String(body.chatId), MAX_HISTORY_TURNS).catch(() => []) : [];
    const history: ChatTurn[] = (stored.length ? stored : body.history ?? [])
      .filter((m) => m?.role === "user" || m?.role === "assistant")
      .slice(-MAX_HISTORY_TURNS)
      .map((m) => ({ role: m.role as "user" | "assistant", content: String(m.content ?? "").slice(0, MAX_HISTORY_CHARS) }));

    // Where each source opens (its PDF page, or the OCR text page) is known
    // before the model starts, so the page can show the passages meanwhile.
    const described = describeContext(context);
    const catalog = await getGranthCatalog().catch(() => null);
    described.passages = described.passages.map((p) => {
      const info = catalog ? describeGranth(catalog, p.granthKey) : null;
      return { ...p, pdfUrl: info?.pdfUrl || null, granthName: info?.displayName ?? p.granthName };
    });
    emit({ type: "context", context: described });

    const result = await answer({ context, chunks, question, language, model, history, emit });
    const chatId = await save(result.content, {
      scopeLine: described.summaryLine,
      sources: described.passages,
      truncated: described.truncated,
      droppedGathas: described.droppedGathas,
      thinking: result.thinking ? result.thinking.slice(0, MAX_SAVED_THINKING_CHARS) : undefined,
    });

    return finish(200, {
      chatId,
      answer: result.content,
      model: result.model,
      usage: result.usage,
      parts: result.parts,
      context: described,
    });
  } catch (e) {
    if (e instanceof SarvamChatError) {
      console.error("sarvam chat error", e.status, e.message);
      return finish(e.status === 429 ? 429 : 502, { error: e.message });
    }
    const message = e instanceof Error ? e.message : String(e);
    console.error("ai/ask failed", message);
    return finish(500, { error: message });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
