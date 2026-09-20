import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { setNoStore } from "@/lib/api-cache";
import { resolveContext, buildPrompt, LANGUAGES, type ContextScope, type LanguageId } from "@/lib/ai-context";
import { chat, CHAT_MODELS, SarvamChatError, type ChatTurn } from "@/lib/sarvam-chat";
import { parseOCRSearchMode } from "@/lib/ocr-search";

type Body = {
  scope?: Record<string, unknown>;
  question?: string;
  language?: string;
  model?: string;
  history?: Array<{ role?: string; content?: string }>;
};

function toInt(value: unknown, fallback: number | null = null) {
  const n = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(n) ? n : fallback;
}

function parseScope(raw: Record<string, unknown> | undefined): ContextScope | { error: string } {
  const kind = String(raw?.kind ?? "");
  if (kind === "gatha") {
    const granthKey = String(raw?.granthKey ?? "").trim();
    const gathaFrom = toInt(raw?.gathaFrom);
    if (!granthKey) return { error: "Choose a granth." };
    if (gathaFrom == null || gathaFrom < 1) return { error: "Enter a starting gatha number." };
    return {
      kind: "gatha",
      granthKey,
      adhikar: raw?.adhikar == null || raw.adhikar === "" ? null : toInt(raw.adhikar),
      gathaFrom,
      gathaTo: raw?.gathaTo == null || raw.gathaTo === "" ? null : toInt(raw.gathaTo),
    };
  }
  if (kind === "pages") {
    const granthKey = String(raw?.granthKey ?? "").trim();
    const pageFrom = toInt(raw?.pageFrom);
    if (!granthKey) return { error: "Choose a granth." };
    if (pageFrom == null || pageFrom < 1) return { error: "Enter a starting page number." };
    return { kind: "pages", granthKey, pageFrom, pageTo: raw?.pageTo == null || raw.pageTo === "" ? null : toInt(raw.pageTo) };
  }
  if (kind === "search") {
    const query = String(raw?.query ?? "").trim();
    if (query.length < 2) return { error: "Enter at least two characters to search for." };
    return {
      kind: "search",
      query,
      matchMode: parseOCRSearchMode(raw?.matchMode),
      granthKeys: Array.isArray(raw?.granthKeys) ? raw.granthKeys.map(String) : [],
    };
  }
  return { error: "Pick what to look at: a gatha, a page range, or a word." };
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
    if (!context.passages.length) {
      return res.status(200).json({
        answer: null,
        error: `No text found for that scope. ${context.summaryLine}`,
        context: { summaryLine: context.summaryLine, passages: [], truncated: false },
      });
    }

    const { system, user } = buildPrompt({ question, language, context });
    const history: ChatTurn[] = (body.history ?? [])
      .filter((m) => m?.role === "user" || m?.role === "assistant")
      .slice(-6)
      .map((m) => ({ role: m.role as "user" | "assistant", content: String(m.content ?? "").slice(0, 4000) }));

    const result = await chat({
      messages: [{ role: "system", content: system }, ...history, { role: "user", content: user }],
      model,
    });

    return res.status(200).json({
      answer: result.content,
      model: result.model,
      usage: result.usage,
      context: {
        summaryLine: context.summaryLine,
        truncated: context.truncated,
        totalChars: context.totalChars,
        passages: context.passages.map((p, i) => ({
          index: i + 1,
          granthKey: p.granthKey,
          granthName: p.granthName,
          pageNumber: p.pageNumber,
          label: p.label,
          preview: p.text.slice(0, 400),
        })),
      },
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
