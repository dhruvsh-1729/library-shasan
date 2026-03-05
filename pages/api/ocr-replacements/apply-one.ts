import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";
import { ensureReplacementSchema, hasWhitespaceWordBoundary } from "@/lib/ocr-replacements";

type ApplyBody = {
  granth_key?: string;
  page_number?: number;
  start?: number;
  end?: number;
  old_word?: string;
  new_word?: string;
  change_group_id?: string | null;
};

function toInt(value: unknown, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function toStr(value: unknown, fallback = "") {
  if (value == null) return fallback;
  return String(value);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const body = (req.body ?? {}) as ApplyBody;
  const granthKey = String(body.granth_key ?? "").trim();
  const pageNumber = toInt(body.page_number, 0);
  const start = toInt(body.start, -1);
  const end = toInt(body.end, -1);
  const oldWord = String(body.old_word ?? "").trim();
  const newWord = String(body.new_word ?? "").trim();
  const changeGroupId = body.change_group_id == null ? null : String(body.change_group_id);

  if (!granthKey || pageNumber <= 0 || start < 0 || end <= start || !oldWord || !newWord) {
    return res.status(400).json({
      error: "Missing required fields: granth_key, page_number, start, end, old_word, new_word",
    });
  }

  try {
    const client = getTursoClient();
    await ensureReplacementSchema(client);

    const tx = await client.transaction("write");
    try {
      const rowResult = await tx.execute({
        sql: `SELECT content
              FROM ocr_pages
              WHERE granth_key = ? AND page_number = ?`,
        args: [granthKey, pageNumber],
      });

      const row = rowResult.rows[0];
      if (!row) {
        await tx.rollback();
        return res.status(404).json({ error: "Target granth/page not found" });
      }

      const currentContent = toStr(row.content);
      const currentSlice = currentContent.slice(start, end);
      if (currentSlice !== oldWord || !hasWhitespaceWordBoundary(currentContent, start, end)) {
        await tx.rollback();
        return res.status(409).json({
          error: "Exact word mismatch at provided location. Refresh preview and try again.",
          expected: oldWord,
          found: currentSlice,
        });
      }

      const updatedContent = currentContent.slice(0, start) + newWord + currentContent.slice(end);
      const newEnd = start + newWord.length;

      await tx.execute({
        sql: `UPDATE ocr_pages
              SET content = ?, updated_at = CURRENT_TIMESTAMP
              WHERE granth_key = ? AND page_number = ?`,
        args: [updatedContent, granthKey, pageNumber],
      });

      const insertLog = await tx.execute({
        sql: `INSERT INTO ocr_word_changes (
                change_group_id,
                granth_key,
                page_number,
                match_start,
                match_end,
                old_word,
                new_word,
                old_content,
                new_content
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          changeGroupId,
          granthKey,
          pageNumber,
          start,
          newEnd,
          oldWord,
          newWord,
          currentContent,
          updatedContent,
        ],
      });

      await tx.commit();
      return res.status(200).json({
        ok: true,
        change_id: toInt(insertLog.lastInsertRowid, 0),
        granth_key: granthKey,
        page_number: pageNumber,
        old_word: oldWord,
        new_word: newWord,
      });
    } catch (error) {
      if (!tx.closed) {
        try {
          await tx.rollback();
        } catch {
          // ignore rollback close races
        }
      }
      throw error;
    } finally {
      tx.close();
    }
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
