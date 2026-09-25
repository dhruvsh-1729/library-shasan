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
  LANGUAGES,
  type ContextScope,
  type LanguageId,
  type ResolvedContext,
} from "@/lib/ai-context";
import { chat, CHAT_MODELS, SarvamChatError, type ChatTurn, type ChatModelId } from "@/lib/sarvam-chat";
import { parseOCRSearchMode } from "@/lib/ocr-search";

type Body = {
  scope?: Record<string, unknown>;
  question?: string;
  language?: string;
  model?: string;
  history?: Array<{ role?: string; content?: string }>;
};

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
 * answering as if the whole of it had been read.
 */
async function answer(opts: {
  context: ResolvedContext;
  question: string;
  language: LanguageId;
  model: ChatModelId;
  history: ChatTurn[];
}) {
  const chunks = chunkPassages(opts.context.passages, opts.context.verses);

  if (chunks.length <= 1) {
    const { system, user } = buildPrompt({ question: opts.question, language: opts.language, context: opts.context });
    const result = await chat({
      messages: [{ role: "system", content: system }, ...opts.history, { role: "user", content: user }],
      model: opts.model,
    });
    return { content: result.content, model: result.model, usage: result.usage, parts: 1 };
  }

  const parts: string[] = [];
  for (const [i, passages] of chunks.entries()) {
    const { system, user } = buildPrompt({
      question: opts.question,
      language: opts.language,
      context: opts.context,
      part: { index: i + 1, total: chunks.length },
      passages,
    });
    const result = await chat({
      messages: [{ role: "system", content: system }, { role: "user", content: user }],
      model: opts.model,
    });
    parts.push(result.content);
  }

  const merge = buildMergePrompt({
    question: opts.question,
    language: opts.language,
    context: opts.context,
    parts,
  });
  const merged = await chat({
    messages: [{ role: "system", content: merge.system }, { role: "user", content: merge.user }],
    model: opts.model,
  });

  return { content: merged.content, model: merged.model, usage: merged.usage, parts: chunks.length };
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
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

  try {
    const context = await resolveContext(scope);

    // A gatha number that exists in every chapter is not a scope. Answering it
    // would mean summarising whichever chapter sorted first.
    if (context.needsAdhikar) {
      return res.status(200).json({
        answer: null,
        needsAdhikar: context.needsAdhikar,
        error: `${context.summaryLine} Choose the chapter you mean.`,
        context: { summaryLine: context.summaryLine, passages: [], truncated: false },
      });
    }

    if (!context.passages.length) {
      return res.status(200).json({
        answer: null,
        error: `No text found for that scope. ${context.summaryLine}`,
        context: { summaryLine: context.summaryLine, passages: [], truncated: false },
      });
    }

    const history: ChatTurn[] = (body.history ?? [])
      .filter((m) => m?.role === "user" || m?.role === "assistant")
      .slice(-MAX_HISTORY_TURNS)
      .map((m) => ({ role: m.role as "user" | "assistant", content: String(m.content ?? "").slice(0, MAX_HISTORY_CHARS) }));

    const result = await answer({ context, question, language, model, history });

    return res.status(200).json({
      answer: result.content,
      model: result.model,
      usage: result.usage,
      parts: result.parts,
      context: describeContext(context),
    });
  } catch (e) {
    if (e instanceof SarvamChatError) {
      console.error("sarvam chat error", e.status, e.message);
      return res.status(e.status === 429 ? 429 : 502).json({ error: e.message });
    }
    const message = e instanceof Error ? e.message : String(e);
    console.error("ai/ask failed", message);
    return res.status(500).json({ error: message });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
