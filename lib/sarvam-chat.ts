/**
 * Thin client for Sarvam's chat completions.
 *
 * sarvam-105b is a reasoning model: it spends its budget in `reasoning_content`
 * and only then fills `content`, so a short max_tokens returns an empty answer.
 * sarvam-105b-conversations replies directly and is the default here.
 *
 * sarvam-105b-conversations holds 32,000 tokens including the answer, and the
 * API rejects the whole request rather than truncating, so callers must budget
 * the passages they send. See MAX_CONTEXT_CHARS in lib/ai-context.
 */
const ENDPOINT = "https://api.sarvam.ai/v1/chat/completions";

export const CHAT_MODELS = {
  fast: "sarvam-105b-conversations",
  reasoning: "sarvam-105b",
} as const;

export type ChatModelId = (typeof CHAT_MODELS)[keyof typeof CHAT_MODELS];

export type ChatTurn = { role: "system" | "user" | "assistant"; content: string };

export class SarvamChatError extends Error {
  constructor(message: string, readonly status: number, readonly body: unknown) {
    super(message);
  }
}

const WINDOW_TOKENS = 32_000;
/** Scripture on this corpus runs about 2.4 characters per token (see lib/ai-context). */
const CHARS_PER_TOKEN = 2.4;

/**
 * The reasoning model thinks before it answers, and a fixed 4,000 tokens was
 * often all spent thinking, leaving no answer at all. It gets whatever the
 * window has left after the prompt, up to 16,000.
 */
function reasoningBudget(messages: ChatTurn[]) {
  const promptTokens = Math.ceil(messages.reduce((n, m) => n + m.content.length, 0) / CHARS_PER_TOKEN) + 500;
  return Math.max(4000, Math.min(16_000, WINDOW_TOKENS - promptTokens - 1000));
}

type ChatOpts = {
  messages: ChatTurn[];
  model?: ChatModelId;
  maxTokens?: number;
  temperature?: number;
};

/**
 * If the reasoning model still runs out of room before writing anything, the
 * question is asked again of the direct model, so the reader gets an answer
 * instead of an error.
 */
export async function chat(opts: ChatOpts) {
  try {
    return await chatOnce(opts);
  } catch (e) {
    if (e instanceof SarvamChatError && e.status === 502 && (opts.model ?? CHAT_MODELS.fast) === CHAT_MODELS.reasoning) {
      console.warn("sarvam reasoning model gave no answer; retrying with", CHAT_MODELS.fast);
      return chatOnce({ ...opts, model: CHAT_MODELS.fast, maxTokens: undefined });
    }
    throw e;
  }
}

async function chatOnce(opts: ChatOpts) {
  const key = process.env.SARVAM_API_KEY;
  if (!key) throw new Error("Missing SARVAM_API_KEY");

  const model = opts.model ?? CHAT_MODELS.fast;
  const maxTokens = opts.maxTokens ?? (model === CHAT_MODELS.reasoning ? reasoningBudget(opts.messages) : 2000);

  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: { "api-subscription-key": key, "Content-Type": "application/json" },
    body: JSON.stringify({
      model,
      messages: opts.messages,
      max_tokens: maxTokens,
      temperature: opts.temperature ?? 0.2,
    }),
  });

  const text = await res.text();
  let body: unknown;
  try { body = JSON.parse(text); } catch { body = text; }
  if (!res.ok) {
    const msg = (body as { error?: { message?: string } })?.error?.message ?? `Sarvam chat failed (${res.status})`;
    throw new SarvamChatError(msg, res.status, body);
  }

  const choice = (body as { choices?: Array<{ finish_reason?: string; message?: { content?: string | null } }> })?.choices?.[0];
  const content = choice?.message?.content ?? "";
  const usage = (body as { usage?: { total_tokens?: number } })?.usage ?? {};

  if (!content) {
    throw new SarvamChatError(
      choice?.finish_reason === "length"
        ? "The model ran out of room before answering. Please ask again."
        : "The model returned an empty answer.",
      502,
      body
    );
  }

  return { content, model, usage, finishReason: choice?.finish_reason ?? "stop" };
}
