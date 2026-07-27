import { groupSegments, parseNumberListSpec, type MappingSegment } from "@/lib/granth-mapping";
import { getSupabaseAdmin } from "@/lib/supabase-server";

export type GranthResolveKind = "gathas" | "pages";

export type GranthResolveInput = {
  bookId?: number | null;
  bookCode?: string | null;
  kind: GranthResolveKind | string;
  spec: string;
  adhikar?: number | null;
  includeCover?: boolean;
  includeAllIdentifiers?: boolean;
};

export type GranthRangeSummary = {
  adhikar: number | null;
  minGatha: number;
  maxGatha: number;
};

export type GranthResolvePayload = {
  kind: string;
  requested: number[];
  includeCover: boolean;
  adhikar?: number | null;
  ranges?: GranthRangeSummary[];
  segments: MappingSegment[];
};

export class GranthResolveError extends Error {
  status: number;
  payload: Record<string, unknown>;

  constructor(status: number, payload: Record<string, unknown>) {
    super(String(payload.error || "Could not resolve granth selection"));
    this.status = status;
    this.payload = payload;
  }
}

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
    rows.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return rows;
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

export async function resolveGranthSelection(input: GranthResolveInput): Promise<GranthResolvePayload> {
  const supabase = getSupabaseAdmin();
  const bookId = Number(input.bookId);
  const hasBookId = Number.isFinite(bookId);
  const bookCode = String(input.bookCode || "").trim();
  const kind = String(input.kind || "gathas");
  const spec = String(input.spec || "").trim();
  const includeCover = Boolean(input.includeCover);
  const includeAllIdentifiers = Boolean(input.includeAllIdentifiers);
  const adhikar = input.adhikar == null || !Number.isFinite(Number(input.adhikar)) ? null : Number(input.adhikar);

  if (!hasBookId && !bookCode) {
    throw new GranthResolveError(400, { error: "bookId or bookCode is required" });
  }
  if (!spec) throw new GranthResolveError(400, { error: "spec is required" });

  let requested: number[];
  try {
    requested = parseNumberListSpec(spec);
  } catch (error) {
    throw new GranthResolveError(400, { error: error instanceof Error ? error.message : String(error) });
  }
  if (requested.length === 0) throw new GranthResolveError(400, { error: "No valid numbers requested" });

  if (kind === "pages") {
    const files = await fetchAll((from, to) => {
      let query = supabase
        .from("granth_library_files")
        .select("book_id,book_code,pdf_file_name,pdf_url,custom_id,page_count")
        .not("pdf_url", "is", null)
        .range(from, to);
      if (hasBookId) query = query.eq("book_id", bookId);
      if (bookCode) query = query.eq("book_code", bookCode);
      return query.order("pdf_file_name", { ascending: true });
    });

    if (files.length === 0) throw new GranthResolveError(404, { error: "No uploaded PDF found for this selection" });
    if (files.length > 1 && !bookCode) {
      throw new GranthResolveError(409, {
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

    return {
      kind,
      requested,
      includeCover,
      segments: groupSegments(rows, includeCover),
    };
  }

  if (kind !== "gathas") throw new GranthResolveError(400, { error: "kind must be pages or gathas" });

  const allRows = await fetchAll((from, to) => {
    let query = supabase
      .from("granth_gatha_map")
      .select("book_id,book_code,pdf_file_name,pdf_url,custom_id,adhikar,gatha,page_start,page_end,anchor_text")
      .not("pdf_url", "is", null)
      .range(from, to);
    if (hasBookId) query = query.eq("book_id", bookId);
    if (bookCode) query = query.eq("book_code", bookCode);
    return query.order("adhikar", { ascending: true }).order("gatha", { ascending: true });
  });

  if (allRows.length === 0) {
    throw new GranthResolveError(404, { error: "No gatha mapping found for this selection" });
  }

  const requestedSet = new Set(requested);
  const scopedRows = adhikar == null ? allRows : allRows.filter((row) => Number(row.adhikar) === adhikar);
  const conflicts = [];

  if (adhikar == null && !includeAllIdentifiers) {
    for (const gatha of requested) {
      const ids = new Set(
        allRows.filter((row) => Number(row.gatha) === gatha).map((row) => String(row.adhikar ?? "none"))
      );
      if (ids.size > 1) conflicts.push({ gatha, adhikars: [...ids] });
    }
  }

  if (conflicts.length > 0) {
    throw new GranthResolveError(409, {
      error: "Some gathas exist under multiple identifiers. Select an identifier.",
      conflicts,
      ranges: rangeSummary(allRows),
    });
  }

  const matched = scopedRows.filter((row) => requestedSet.has(Number(row.gatha)));
  const found = new Set(matched.map((row) => Number(row.gatha)));
  const missing = requested.filter((gatha) => !found.has(gatha));
  if (missing.length > 0) {
    throw new GranthResolveError(404, {
      error: `Missing gatha(s): ${missing.join(", ")}`,
      missing,
      ranges: rangeSummary(scopedRows.length ? scopedRows : allRows),
    });
  }

  return {
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
  };
}
