import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { getSessionUser, protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { deleteSavedWord, listSavedWords, saveWords, updateSavedWord } from "@/lib/anuvaad/saved-words";

// Maharaj Saheb's saved Gujarati for śāstra words (lib/anuvaad/saved-words):
//   GET    ?q=…                          the saved words (matching q)
//   POST   { words: [{word, gujarati}], source }   save checked words
//   PATCH  { id, word?, gujarati? }       correct one
//   DELETE ?id=…                          remove one

export const config = { api: { bodyParser: { sizeLimit: "200kb" } } };

async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);
  try {
    if (req.method === "GET") return res.status(200).json({ words: await listSavedWords(String(req.query.q ?? "")) });
    const body = (req.body ?? {}) as Record<string, unknown>;
    if (req.method === "POST") {
      const user = await getSessionUser(req, res);
      const words = (Array.isArray(body.words) ? body.words : []).slice(0, 300).map((w) => {
        const item = (w ?? {}) as Record<string, unknown>;
        return { word: String(item.word ?? "").slice(0, 120), gujarati: String(item.gujarati ?? "").slice(0, 1500) };
      });
      const saved = await saveWords(words, String(body.source ?? "").slice(0, 300), String(user?.email ?? user?.name ?? ""));
      return res.status(200).json({ saved });
    }
    if (req.method === "PATCH") {
      const id = Number(body.id);
      if (!Number.isInteger(id)) return res.status(400).json({ error: "Which word?" });
      await updateSavedWord(id, {
        word: body.word == null ? undefined : String(body.word).slice(0, 120),
        gujarati: body.gujarati == null ? undefined : String(body.gujarati).slice(0, 1500),
      });
      return res.status(200).json({ ok: true });
    }
    if (req.method === "DELETE") {
      const id = Number(req.query.id);
      if (!Number.isInteger(id)) return res.status(400).json({ error: "Which word?" });
      await deleteSavedWord(id);
      return res.status(200).json({ ok: true });
    }
    res.setHeader("Allow", "GET, POST, PATCH, DELETE");
    return res.status(405).json({ error: "Method not allowed" });
  } catch (error) {
    console.error("saved words failed:", error instanceof Error ? error.message : error);
    return res.status(500).json({ error: error instanceof Error ? error.message : "The saved words could not be read just now." });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
