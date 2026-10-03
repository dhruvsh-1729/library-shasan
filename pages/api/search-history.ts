import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import type { SessionUser } from "@/lib/auth-users";
import { getSupabaseAdmin } from "@/lib/supabase-server";

const KEEP = 10;
const text = (v: unknown, max: number) => (typeof v === "string" ? v.trim().slice(0, max) : "");

/** The signed-in user's recent searches: list, remember one (by its link), or forget one. */
async function handler(req: NextApiRequest, res: NextApiResponse, user: SessionUser) {
  const sb = getSupabaseAdmin();
  try {
    if (req.method === "GET") {
      const { data, error } = await sb
        .from("search_history")
        .select("id,query,url,label,used_at")
        .eq("user_id", user.id)
        .order("used_at", { ascending: false })
        .limit(KEEP);
      if (error) throw new Error(error.message);
      res.setHeader("Cache-Control", "no-store");
      return res.status(200).json({ items: data ?? [] });
    }
    if (req.method === "POST") {
      const query = text(req.body?.query, 200);
      const url = text(req.body?.url, 2000);
      // Only links to this page are kept, so a recent search can never send anyone elsewhere.
      if (!query || !url.startsWith("/?")) return res.status(400).json({ error: "Missing query or link." });
      const { error } = await sb
        .from("search_history")
        .upsert({ user_id: user.id, query, url, label: text(req.body?.label, 200) || null, used_at: new Date().toISOString() }, { onConflict: "user_id,url" });
      if (error) throw new Error(error.message);
      const { data: old } = await sb.from("search_history").select("id").eq("user_id", user.id).order("used_at", { ascending: false }).range(KEEP, KEEP + 200);
      if (old?.length) await sb.from("search_history").delete().in("id", old.map((r) => r.id));
      return res.status(200).json({ ok: true });
    }
    if (req.method === "DELETE") {
      const id = Number(req.query.id);
      if (!Number.isFinite(id)) return res.status(400).json({ error: "Missing id." });
      const { error } = await sb.from("search_history").delete().eq("id", id).eq("user_id", user.id);
      if (error) throw new Error(error.message);
      return res.status(200).json({ ok: true });
    }
    res.setHeader("Allow", "GET, POST, DELETE");
    return res.status(405).json({ error: "Method not allowed" });
  } catch (error) {
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
