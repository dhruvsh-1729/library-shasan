/**
 * Thin client for Sarvam's chat completions.
 *
 * sarvam-105b is a reasoning model: it spends its budget in `reasoning_content`
 * and only then fills `content`, so a short max_tokens returns an empty answer.
 * sarvam-105b-conversations replies directly and is the default here.
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

export async function chat(opts: {
  messages: ChatTurn[];
  model?: ChatModelId;
  maxTokens?: number;
  temperature?: number;
}) {
  const key = process.env.SARVAM_API_KEY;
  if (!key) throw new Error("Missing SARVAM_API_KEY");

  const model = opts.model ?? CHAT_MODELS.fast;
  // The reasoning model needs headroom for its chain of thought before it
  // writes anything the user will see.
  const maxTokens = opts.maxTokens ?? (model === CHAT_MODELS.reasoning ? 4000 : 1200);

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
        ? "The model ran out of tokens before answering. Try a smaller scope."
        : "The model returned an empty answer.",
      502,
      body
    );
  }

  return { content, model, usage, finishReason: choice?.finish_reason ?? "stop" };
}
