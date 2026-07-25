import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";
import { groupSegments, parseNumberListSpec } from "@/lib/granth-mapping";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

async function fetchAll(build: (from: number, to: number) => unknown) {
  const rows: Record<string, unknown>[] = [];
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const { data, error } = (await build(from, from + pageSize - 1)) as {
      data?: Record<string, unknown>[] | null;
      error?: { message?: string } | null;
    };
    if (error) throw error;
    if (!data || data.length === 0) break;
    rows.push(...(data as Record<string, unknown>[]));
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return rows;
}

function firstString(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function parseBool(value: string | string[] | undefined) {
  const raw = String(firstString(value) || "").toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
}

function rangeSummary(rows: Record<string, unknown>[]) {
  const byId = new Map<string, { min: number; max: number }>();
  for (const row of rows) {
    const id = row.adhikar == null ? "none" : String(row.adhikar);
    const gatha = Number(row.gatha);
    if (!Number.isFinite(gatha)) continue;
    const existing = byId.get(id) || { min: gatha, max: gatha };
    existing.min = Math.min(existing.min, gatha);
    existing.max = Math.max(existing.max, gatha);
    byId.set(id, existing);
  }
  return [...byId.entries()].map(([id, value]) => ({
    adhikar: id === "none" ? null : Number(id),
    minGatha: value.min,
    maxGatha: value.max,
  }));
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const bookId = Number.parseInt(String(firstString(req.query.bookId) || ""), 10);
  const bookCode = String(firstString(req.query.bookCode) || "").trim();
  const kind = String(firstString(req.query.kind) || "gathas");
  const spec = String(firstString(req.query.spec) || "").trim();
  const adhikarRaw = String(firstString(req.query.adhikar) || "").trim();
  const adhikar = adhikarRaw ? Number.parseInt(adhikarRaw, 10) : null;
  const includeCover = parseBool(req.query.includeCover);

  if (!Number.isFinite(bookId) && !bookCode) {
    return res.status(400).json({ error: "bookId or bookCode is required" });
  }
  if (!spec) return res.status(400).json({ error: "spec is required" });

  let requested: number[];
  try {
    requested = parseNumberListSpec(spec);
  } catch (error) {
    return res.status(400).json({ error: error instanceof Error ? error.message : String(error) });
  }
  if (requested.length === 0) return res.status(400).json({ error: "No valid numbers requested" });

  try {
    if (kind === "pages") {
      const files = await fetchAll((from, to) => {
        let query = supabase
          .from("granth_library_files")
          .select("book_id,book_code,pdf_file_name,pdf_url,custom_id,page_count")
          .not("pdf_url", "is", null)
          .range(from, to);
        if (Number.isFinite(bookId)) query = query.eq("book_id", bookId);
        if (bookCode) query = query.eq("book_code", bookCode);
        return query.order("pdf_file_name", { ascending: true });
      });

      if (files.length === 0) return res.status(404).json({ error: "No uploaded PDF found for this selection" });
      if (files.length > 1 && !bookCode) {
        return res.status(409).json({
          error: "This granth has multiple PDFs. Select a book code first.",
          bookCodes: [...new Set(files.map((row) => row.book_code).filter(Boolean))],
        });
      }

      const rows = files.flatMap((file) =>
        requested.map((page) => ({
          pdf_url: file.pdf_url as string | null,
          pdf_file_name: String(file.pdf_file_name || ""),
          custom_id: (file.custom_id as string | null) || null,
          book_code: (file.book_code as string | null) || null,
          page_start: page,
          page_end: page,
          adhikar: null,
          gatha: null,
          anchor_text: null,
        }))
      );

      return res.status(200).json({
        kind,
        requested,
        includeCover,
        segments: groupSegments(rows, includeCover),
      });
    }

    if (kind !== "gathas") return res.status(400).json({ error: "kind must be pages or gathas" });

    const allRows = await fetchAll((from, to) => {
      let query = supabase
        .from("granth_gatha_map")
        .select(
          "book_id,book_code,pdf_file_name,pdf_url,custom_id,adhikar,gatha,page_start,page_end,anchor_text"
        )
        .not("pdf_url", "is", null)
        .range(from, to);
      if (Number.isFinite(bookId)) query = query.eq("book_id", bookId);
      if (bookCode) query = query.eq("book_code", bookCode);
      return query.order("adhikar", { ascending: true }).order("gatha", { ascending: true });
    });

    if (allRows.length === 0) {
      return res.status(404).json({ error: "No gatha mapping found for this selection" });
    }

    const requestedSet = new Set(requested);
    const scopedRows = adhikar == null ? allRows : allRows.filter((row) => Number(row.adhikar) === adhikar);
    const conflicts = [];

    if (adhikar == null) {
      for (const gatha of requested) {
        const ids = new Set(
          allRows.filter((row) => Number(row.gatha) === gatha).map((row) => String(row.adhikar ?? "none"))
        );
        if (ids.size > 1) conflicts.push({ gatha, adhikars: [...ids] });
      }
    }

    if (conflicts.length > 0) {
      return res.status(409).json({
        error: "Some gathas exist under multiple identifiers. Select an identifier.",
        conflicts,
        ranges: rangeSummary(allRows),
      });
    }

    const matched = scopedRows.filter((row) => requestedSet.has(Number(row.gatha)));
    const found = new Set(matched.map((row) => Number(row.gatha)));
    const missing = requested.filter((gatha) => !found.has(gatha));
    if (missing.length > 0) {
      return res.status(404).json({
        error: `Missing gatha(s): ${missing.join(", ")}`,
        missing,
        ranges: rangeSummary(scopedRows.length ? scopedRows : allRows),
      });
    }

    return res.status(200).json({
      kind,
      requested,
      adhikar,
      includeCover,
      ranges: rangeSummary(scopedRows),
      segments: groupSegments(
        matched.map((row) => ({
          pdf_url: row.pdf_url as string | null,
          pdf_file_name: String(row.pdf_file_name || ""),
          custom_id: (row.custom_id as string | null) || null,
          book_code: (row.book_code as string | null) || null,
          adhikar: row.adhikar == null ? null : Number(row.adhikar),
          gatha: Number(row.gatha),
          page_start: Number(row.page_start),
          page_end: row.page_end == null ? Number(row.page_start) : Number(row.page_end),
          anchor_text: (row.anchor_text as string | null) || null,
        })),
        includeCover
      ),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (/granth_library_files|granth_gatha_map|schema cache/i.test(message)) {
      return res.status(503).json({
        error:
          "Mapping tables are not available yet. Run supabase/migrations/20260725_granth_library_mapping.sql and import the mapping data.",
      });
    }
    return res.status(500).json({ error: message });
  }
}
