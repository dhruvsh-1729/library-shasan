import type { NextApiRequest, NextApiResponse } from "next";
import { getTursoClient } from "@/lib/turso";
import { findExactWordMatches } from "@/lib/ocr-replacements";

type Scope = "page" | "current_granth" | "selected_granths" | "all_granths" | "single" | "one_granth" | "multi_granth";

type PreviewBody = {
  word?: string;
  scope?: Scope;
  granthKey?: string;
  granthKeys?: string[];
  singleTarget?: {
    granth_key?: string;
    page_number?: number;
  };
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

function parseScope(value: unknown): Scope {
  const raw = String(value ?? "").trim();
  if (
    raw === "page" ||
    raw === "current_granth" ||
    raw === "selected_granths" ||
    raw === "all_granths" ||
    raw === "single" ||
    raw === "one_granth" ||
    raw === "multi_granth"
  ) {
    return raw;
  }
  return "all_granths";
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const body = (req.body ?? {}) as PreviewBody;
  const word = String(body.word ?? "").trim();
  if (!word) return res.status(400).json({ error: "Missing word" });

  const scope = parseScope(body.scope);
  const requestedGranthKey = String(body.granthKey ?? "").trim();
  const requestedGranthKeys = Array.isArray(body.granthKeys)
    ? body.granthKeys.map((v) => String(v ?? "").trim()).filter(Boolean)
    : [];

  const singleTargetKey = String(body.singleTarget?.granth_key ?? "").trim();
  const singleTargetPage = toInt(body.singleTarget?.page_number, 0);

  try {
    const client = getTursoClient();

    const where: string[] = [`p.content LIKE ?`];
    const args: Array<string | number> = [`%${word}%`];

    if (scope === "page" || scope === "single") {
      if (!singleTargetKey || singleTargetPage <= 0) {
        return res
          .status(400)
          .json({ error: "singleTarget.granth_key and singleTarget.page_number are required for page scope" });
      }
      where.push("p.granth_key = ?");
      where.push("p.page_number = ?");
      args.push(singleTargetKey, singleTargetPage);
    } else if (scope === "current_granth" || scope === "one_granth") {
      const granthKey = requestedGranthKey || requestedGranthKeys[0];
      if (!granthKey) {
        return res.status(400).json({ error: "granthKey is required for current_granth scope" });
      }
      where.push("p.granth_key = ?");
      args.push(granthKey);
    } else if ((scope === "selected_granths" || scope === "multi_granth") && requestedGranthKeys.length > 0) {
      where.push(`p.granth_key IN (${requestedGranthKeys.map(() => "?").join(",")})`);
      args.push(...requestedGranthKeys);
    }

    const sql = `SELECT
        p.granth_key,
        p.page_number,
        p.content,
        g.book_number,
        g.library_code,
        g.granth_name
      FROM ocr_pages p
      JOIN ocr_granths g ON g.granth_key = p.granth_key
      WHERE ${where.join(" AND ")}
      ORDER BY CAST(g.book_number AS INTEGER) ASC, p.page_number ASC`;

    const rowsResult = await client.execute({ sql, args });

    const maxMatches = 5000;
    const matches: Array<{
      match_id: string;
      granth_key: string;
      page_number: number;
      start: number;
      end: number;
      old_word: string;
      context: string;
      granth_title: string;
    }> = [];

    for (const row of rowsResult.rows) {
      const content = toStr(row.content);
      const located = findExactWordMatches(content, word);
      if (located.length === 0) continue;

      const granthKey = toStr(row.granth_key);
      const pageNumber = toInt(row.page_number);
      const granthTitle = `${toStr(row.book_number)}${row.library_code == null ? "" : ` ${toStr(row.library_code)}`} ${toStr(row.granth_name)}`
        .replace(/\s+/g, " ")
        .trim();

      for (const one of located) {
        matches.push({
          match_id: `${granthKey}:${pageNumber}:${one.start}:${one.end}`,
          granth_key: granthKey,
          page_number: pageNumber,
          start: one.start,
          end: one.end,
          old_word: one.word,
          context: one.context,
          granth_title: granthTitle,
        });
        if (matches.length >= maxMatches) {
          return res.status(200).json({
            matches,
            total_matches: matches.length,
            truncated: true,
          });
        }
      }
    }

    return res.status(200).json({
      matches,
      total_matches: matches.length,
      truncated: false,
    });
  } catch (error) {
    return res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
