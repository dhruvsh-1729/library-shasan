import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import type { SessionUser } from "@/lib/auth-users";
import { getSupabaseAdmin } from "@/lib/supabase-server";

const KEEP = 12;
const text = (v: unknown, max = 300) => (typeof v === "string" ? v.trim().slice(0, max) : "");

/** The signed-in user's recent extractions: list, remember one, or forget one. */
async function handler(req: NextApiRequest, res: NextApiResponse, user: SessionUser) {
  const sb = getSupabaseAdmin();
  try {
    if (req.method === "GET") {
      const { data, error } = await sb
        .from("extract_history")
        .select("id,mode,work_id,work_title,volume_key,chapter_id,chapter_label,spec,used_at")
        .eq("user_id", user.id)
        .order("used_at", { ascending: false })
        .limit(KEEP);
      if (error) throw new Error(error.message);
      res.setHeader("Cache-Control", "no-store");
      return res.status(200).json({ items: data ?? [] });
    }
    if (req.method === "POST") {
      const b = req.body ?? {};
      const mode = b.mode === "pages" ? "pages" : "gathas";
      const row = {
        user_id: user.id,
        mode,
        work_id: text(b.workId),
        work_title: text(b.workTitle),
        volume_key: text(b.volumeKey) || null,
        chapter_id: text(b.chapterId) || null,
        chapter_label: text(b.chapterLabel) || null,
        spec: text(b.spec, 200),
      };
      if (!row.work_id || !row.work_title || !row.spec) return res.status(400).json({ error: "Missing granth or numbers." });
      const signature = [mode, row.work_id, row.volume_key ?? "", row.chapter_id ?? "", row.spec].join("|");
      const { error } = await sb
        .from("extract_history")
        .upsert({ ...row, signature, used_at: new Date().toISOString() }, { onConflict: "user_id,signature" });
      if (error) throw new Error(error.message);
      // Only the latest few are kept.
      const { data: old } = await sb.from("extract_history").select("id").eq("user_id", user.id).order("used_at", { ascending: false }).range(KEEP, KEEP + 200);
      if (old?.length) await sb.from("extract_history").delete().in("id", old.map((r) => r.id));
      return res.status(200).json({ ok: true });
    }
    if (req.method === "DELETE") {
      const id = Number(req.query.id);
      if (!Number.isFinite(id)) return res.status(400).json({ error: "Missing id." });
      const { error } = await sb.from("extract_history").delete().eq("id", id).eq("user_id", user.id);
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
