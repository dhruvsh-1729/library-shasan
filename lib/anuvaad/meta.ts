// Meta's Llama model through OpenRouter, for the page anuvaad. Maharaj Saheb
// tried Meta AI on 11-14 Nov 2026 and asked for it ("Meta is the fastest; one
// page perfect in 2 seconds; for now keep only Meta"). It reads the page's
// Sanskrit/Prakrit itself, so one call gives the whole page.
//
// When OPENROUTER_API_KEY is missing or refused, metaReady() says so and the
// anuvaad falls back to the Sarvam pipeline in lib/anuvaad/translate.
//
// The reader never sees which service writes the anuvaad.

const ENDPOINT = "https://openrouter.ai/api/v1/chat/completions";
export const META_MODEL = process.env.ANUVAAD_META_MODEL || "meta-llama/llama-4-maverick";

export class MetaError extends Error {
  constructor(message: string, readonly status = 502) {
    super(message);
  }
}

// A refused key is remembered for 10 minutes, so a whole PDF does not wait on
// a dead key page after page.
let refusedUntil = 0;

export function metaReady() {
  return Boolean(process.env.OPENROUTER_API_KEY) && Date.now() > refusedUntil;
}

export async function metaChat(opts: { system: string; user: string; maxTokens?: number }) {
  const key = process.env.OPENROUTER_API_KEY;
  if (!key) throw new MetaError("OPENROUTER_API_KEY is not set.", 503);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 120_000);
  try {
    const res = await fetch(ENDPOINT, {
      method: "POST",
      signal: controller.signal,
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: META_MODEL,
        temperature: 0,
        max_tokens: opts.maxTokens ?? 6000,
        messages: [
          { role: "system", content: opts.system },
          { role: "user", content: opts.user },
        ],
      }),
    });
    const body = (await res.json().catch(() => null)) as {
      choices?: Array<{ message?: { content?: string | null } }>;
      error?: { message?: string };
    } | null;
    if (res.status === 401 || res.status === 402 || res.status === 403) refusedUntil = Date.now() + 10 * 60_000;
    const content = body?.choices?.[0]?.message?.content ?? "";
    if (!res.ok || !content.trim()) throw new MetaError(`Meta did not answer (${res.status}${body?.error?.message ? `: ${body.error.message}` : ""}).`, res.status || 502);
    return content;
  } finally {
    clearTimeout(timer);
  }
}
