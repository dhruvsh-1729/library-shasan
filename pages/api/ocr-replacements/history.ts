import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";
import { ensureReplacementSchema } from "@/lib/ocr-replacements";

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toStr(value: unknown, fallback = "") {
  if (value == null) return fallback;
  return String(value);
}

function parseLimit(raw: unknown) {
  const value = Number(raw ?? 150);
  if (!Number.isFinite(value) || value <= 0) return 150;
  return Math.min(Math.floor(value), 500);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const granthKey = String(req.query.granthKey ?? "").trim();
  const word = String(req.query.word ?? "").trim();
  const limit = parseLimit(req.query.limit);

  try {
    const client = getTursoClient();
    await ensureReplacementSchema(client);

    const where: string[] = [];
    const args: Array<string | number> = [];

    if (granthKey) {
      where.push("c.granth_key = ?");
      args.push(granthKey);
    }
    if (word) {
      where.push("(c.old_word = ? OR c.new_word = ?)");
      args.push(word, word);
    }

    const whereSql = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    const query = `SELECT
        c.id,
        c.change_group_id,
        c.granth_key,
        c.page_number,
        c.match_start,
        c.match_end,
        c.old_word,
        c.new_word,
        c.changed_at,
        c.reverted_from_change_id,
        g.book_number,
        g.library_code,
        g.granth_name
      FROM ocr_word_changes c
      JOIN ocr_granths g ON g.granth_key = c.granth_key
      ${whereSql}
      ORDER BY c.id DESC
      LIMIT ?`;

    const result = await client.execute({
      sql: query,
      args: [...args, limit],
    });

    return res.status(200).json({
      items: result.rows.map((row) => ({
        id: toInt(row.id),
        change_group_id: row.change_group_id == null ? null : toStr(row.change_group_id),
        granth_key: toStr(row.granth_key),
        granth_title: `${toStr(row.book_number)}${row.library_code == null ? "" : ` ${toStr(row.library_code)}`} ${toStr(row.granth_name)}`
          .replace(/\s+/g, " ")
          .trim(),
        page_number: toInt(row.page_number),
        match_start: toInt(row.match_start),
        match_end: toInt(row.match_end),
        old_word: toStr(row.old_word),
        new_word: toStr(row.new_word),
        changed_at: toStr(row.changed_at),
        reverted_from_change_id:
          row.reverted_from_change_id == null ? null : toInt(row.reverted_from_change_id),
      })),
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
