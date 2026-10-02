import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { LlmError, parseEngine } from "@/lib/vyutpatti/llm";
import { VishayInputError, buildVyutpatti, cleanVishay } from "@/lib/vyutpatti/pipeline";

// One vishay's vyutpatti, streamed as NDJSON events: {type:"progress"} while
// it is looked up and read, then {type:"done", result} or {type:"error"}.
// Nothing about the vishay is stored (Sahebji's instruction): it lives only in
// this request and in the reader's page.

export const config = { maxDuration: 300 };

type Body = { vishay?: unknown; number?: unknown; box?: unknown; engine?: unknown };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const body = (req.body ?? {}) as Body;
  let vishay: string;
  try {
    vishay = cleanVishay(String(body.vishay ?? ""));
  } catch (error) {
    return res.status(400).json({ error: error instanceof Error ? error.message : "Type the vishay." });
  }
  const number = String(body.number ?? "").trim().slice(0, 24);
  const box = String(body.box ?? "").trim().slice(0, 12);
  const engine = parseEngine(body.engine);

  res.writeHead(200, {
    "Content-Type": "application/x-ndjson; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Content-Encoding": "none",
    "X-Accel-Buffering": "no",
  });
  res.flushHeaders?.();
  const emit = (event: Record<string, unknown>) => {
    if (!res.writableEnded) res.write(`${JSON.stringify(event)}\n`);
  };
  try {
    const result = await buildVyutpatti({ number, vishay, box }, engine, (message) => emit({ type: "progress", message }));
    emit({ type: "done", result });
  } catch (error) {
    const message =
      error instanceof VishayInputError || error instanceof LlmError
        ? error.message
        : "The vyutpatti could not be prepared. Please try again.";
    if (!(error instanceof VishayInputError)) console.error("vyutpatti run failed:", error instanceof Error ? error.message : error);
    emit({ type: "error", error: message });
  }
  res.end();
}

export default protectApi(handler, PERMISSIONS.libraryRead);
