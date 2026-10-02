// The two engines the vyutpatti page can use, behind one call that returns
// JSON: Claude (through OpenRouter, the default) and Sarvam. Only Claude can
// read a page scan, so Shabda Ratna Mahodadhi's Gujarati (which our OCR read
// as Devanagari letters) is exact only with Claude; with Sarvam it is
// restored from the OCR text.
//
// The reader never sees model names, only "Claude" or "Sarvam".

import { chat as sarvamChat } from "@/lib/sarvam-chat";

export type Engine = "claude" | "sarvam";

export const ENGINES: Engine[] = ["claude", "sarvam"];

export function parseEngine(value: unknown): Engine {
  return value === "sarvam" ? "sarvam" : "claude";
}

// Sonnet reads the Gujarati of a scanned kosh page correctly where Haiku only
// copied the broken OCR (measured 2026-10-02 on Shabda Ratna Mahodadhi 2,
// page 868); with reasoning effort "low" it spends no thinking tokens, about
// $0.01 per page read.
const CLAUDE_MODEL = "anthropic/claude-sonnet-5.5";
const OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions";

export type LlmFile = { name: string; pdf: Uint8Array };

export type LlmRequest = {
  system: string;
  prompt: string;
  /** One-page PDFs of kosh scans; Claude reads them, Sarvam does not get them. */
  files?: LlmFile[];
  maxTokens?: number;
};

export class LlmError extends Error {
  constructor(message: string, readonly status = 502) {
    super(message);
  }
}

/** Running total of what a run cost, in US dollars (Claude) — Sarvam bills separately. */
export type Usage = { calls: number; costUsd: number };

export function newUsage(): Usage {
  return { calls: 0, costUsd: 0 };
}

/** The first JSON object or array in a reply, which may come fenced in ```json. */
export function parseJsonReply(text: string): unknown {
  const body = String(text ?? "").trim().replace(/^```(?:json)?\s*/i, "").replace(/```\s*$/, "");
  const start = body.search(/[[{]/);
  if (start < 0) throw new LlmError("The model did not return JSON.");
  const open = body[start];
  const close = open === "{" ? "}" : "]";
  const end = body.lastIndexOf(close);
  if (end <= start) throw new LlmError("The model's JSON was cut off.");
  try {
    return JSON.parse(body.slice(start, end + 1));
  } catch {
    throw new LlmError("The model's JSON could not be read.");
  }
}

async function claude(request: LlmRequest, usage: Usage) {
  const key = process.env.OPENROUTER_API_KEY;
  if (!key) throw new LlmError("Claude is not set up on this server (OPENROUTER_API_KEY is missing).", 503);
  const content: unknown[] = (request.files ?? []).map((file) => ({
    type: "file",
    file: { filename: file.name, file_data: `data:application/pdf;base64,${Buffer.from(file.pdf).toString("base64")}` },
  }));
  content.push({ type: "text", text: request.prompt });
  const res = await fetch(OPENROUTER_ENDPOINT, {
    method: "POST",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: request.maxTokens ?? 4000,
      temperature: 0,
      reasoning: { effort: "low" },
      messages: [
        { role: "system", content: request.system },
        { role: "user", content },
      ],
    }),
  });
  const body = (await res.json().catch(() => null)) as {
    choices?: Array<{ message?: { content?: string | null }; finish_reason?: string }>;
    usage?: { cost?: number };
    error?: { message?: string };
  } | null;
  if (!res.ok || !body?.choices?.length) {
    throw new LlmError(`Claude did not answer (${res.status}${body?.error?.message ? `: ${body.error.message}` : ""}).`);
  }
  usage.calls += 1;
  usage.costUsd += Number(body.usage?.cost ?? 0);
  const choice = body.choices[0];
  if (choice.finish_reason === "length") throw new LlmError("Claude's answer was too long and was cut off.");
  return String(choice.message?.content ?? "");
}

async function sarvam(request: LlmRequest, usage: Usage) {
  const result = await sarvamChat({
    messages: [
      { role: "system", content: request.system },
      { role: "user", content: request.prompt },
    ],
    maxTokens: request.maxTokens ?? 4000,
    temperature: 0,
  });
  usage.calls += 1;
  return String(result.content ?? "");
}

/** Asks the engine and parses its JSON answer; one retry when the JSON is broken. */
export async function askJson(engine: Engine, request: LlmRequest, usage: Usage): Promise<unknown> {
  const ask = engine === "claude" ? claude : sarvam;
  try {
    return parseJsonReply(await ask(request, usage));
  } catch (error) {
    if (!(error instanceof LlmError) || error.status === 503) throw error;
    return parseJsonReply(await ask({ ...request, prompt: `${request.prompt}\n\nReturn only valid JSON.` }, usage));
  }
}
