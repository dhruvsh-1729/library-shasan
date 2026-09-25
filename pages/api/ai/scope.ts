import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { getCachedJson, setNoStore } from "@/lib/api-cache";
import { listAdhikars, parseScope, previewScope } from "@/lib/ai-context";

/**
 * Tells the page what the assistant would actually read, before it is asked.
 *
 * The reported failure was an answer written from the wrong chapter, which
 * nothing on screen would have revealed. Showing the resolved pages next to the
 * question makes a wrong scope visible while it can still be corrected.
 */
async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }
  setNoStore(res);

  const body = (req.body ?? {}) as { scope?: Record<string, unknown>; granthKey?: string };
  const granthKey = String(body.granthKey ?? body.scope?.granthKey ?? "").trim();

  let chapters: Awaited<ReturnType<typeof listAdhikars>> = [];
  if (granthKey) {
    try {
      const cached = await getCachedJson(`ai-adhikars:${granthKey}`, 300, () => listAdhikars(granthKey));
      chapters = cached.value;
    } catch (e) {
      console.error("adhikar list failed", e instanceof Error ? e.message : e);
    }
  }

  const scope = parseScope(body.scope);
  if ("error" in scope) {
    return res.status(200).json({ chapters, resolved: null, hint: scope.error });
  }

  try {
    const preview = await previewScope(scope);
    return res.status(200).json({ chapters, resolved: preview });
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    console.error("ai/scope failed", message);
    return res.status(200).json({ chapters, resolved: null, hint: message });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
