import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";
import { ensureReplacementSchema, hasWhitespaceWordBoundary } from "@/lib/ocr-replacements";

type RevertBody = {
  change_id?: number;
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

  const body = (req.body ?? {}) as RevertBody;
  const changeId = toInt(body.change_id, 0);
  if (changeId <= 0) {
    return res.status(400).json({ error: "Missing valid change_id" });
  }

  try {
    const client = getTursoClient();
    await ensureReplacementSchema(client);

    const tx = await client.transaction("write");
    try {
      const changeResult = await tx.execute({
        sql: `SELECT
                id,
                granth_key,
                page_number,
                match_start,
                old_word,
                new_word,
                old_content,
                new_content
              FROM ocr_word_changes
              WHERE id = ?`,
        args: [changeId],
      });

      const change = changeResult.rows[0];
      if (!change) {
        await tx.rollback();
        return res.status(404).json({ error: "Change record not found" });
      }

      const granthKey = toStr(change.granth_key);
      const pageNumber = toInt(change.page_number);
      const matchStart = toInt(change.match_start);
      const oldWord = toStr(change.old_word);
      const newWord = toStr(change.new_word);
      const oldContentSnapshot = toStr(change.old_content);
      const newContentSnapshot = toStr(change.new_content);

      const pageResult = await tx.execute({
        sql: `SELECT content FROM ocr_pages WHERE granth_key = ? AND page_number = ?`,
        args: [granthKey, pageNumber],
      });
      const pageRow = pageResult.rows[0];
      if (!pageRow) {
        await tx.rollback();
        return res.status(404).json({ error: "Target granth/page not found" });
      }

      const currentContent = toStr(pageRow.content);
      let revertedContent: string | null = null;

      if (currentContent === newContentSnapshot) {
        revertedContent = oldContentSnapshot;
      } else {
        const dynamicEnd = matchStart + newWord.length;
        const segment = currentContent.slice(matchStart, dynamicEnd);
        if (segment === newWord && hasWhitespaceWordBoundary(currentContent, matchStart, dynamicEnd)) {
          revertedContent = currentContent.slice(0, matchStart) + oldWord + currentContent.slice(dynamicEnd);
        }
      }

      if (revertedContent == null) {
        await tx.rollback();
        return res.status(409).json({
          error: "Cannot auto-revert this change because the page content has diverged. Re-run preview and apply manually.",
        });
      }

      await tx.execute({
        sql: `UPDATE ocr_pages
              SET content = ?, updated_at = CURRENT_TIMESTAMP
              WHERE granth_key = ? AND page_number = ?`,
        args: [revertedContent, granthKey, pageNumber],
      });

      const inserted = await tx.execute({
        sql: `INSERT INTO ocr_word_changes (
                change_group_id,
                granth_key,
                page_number,
                match_start,
                match_end,
                old_word,
                new_word,
                old_content,
                new_content,
                reverted_from_change_id
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          null,
          granthKey,
          pageNumber,
          matchStart,
          matchStart + oldWord.length,
          newWord,
          oldWord,
          currentContent,
          revertedContent,
          changeId,
        ],
      });

      await tx.commit();
      return res.status(200).json({
        ok: true,
        reverted_change_id: changeId,
        new_change_id: toInt(inserted.lastInsertRowid, 0),
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
