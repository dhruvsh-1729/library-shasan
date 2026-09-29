import type { NextApiRequest, NextApiResponse } from "next";
import { setNoStore } from "@/lib/api-cache";
import { deleteChat, getChat, renameChat } from "@/lib/ask-chats";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import type { SessionUser } from "@/lib/auth-users";

// One saved Ask conversation: read it, rename it, or delete it. Only the
// user who owns it can do any of these; anyone else gets a 404.
async function handler(req: NextApiRequest, res: NextApiResponse, user: SessionUser) {
  setNoStore(res);
  const id = String(req.query.id ?? "");
  try {
    if (req.method === "GET") {
      const chat = await getChat(user.id, id);
      return chat ? res.status(200).json({ chat }) : res.status(404).json({ error: "Chat not found." });
    }
    if (req.method === "PATCH") {
      const ok = await renameChat(user.id, id, String((req.body ?? {}).title ?? ""));
      return ok ? res.status(200).json({ ok: true }) : res.status(404).json({ error: "Chat not found." });
    }
    if (req.method === "DELETE") {
      const ok = await deleteChat(user.id, id);
      return ok ? res.status(200).json({ ok: true }) : res.status(404).json({ error: "Chat not found." });
    }
    res.setHeader("Allow", "GET, PATCH, DELETE");
    return res.status(405).json({ error: "Method not allowed" });
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
