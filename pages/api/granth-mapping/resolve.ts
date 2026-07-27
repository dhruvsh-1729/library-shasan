import type { NextApiRequest, NextApiResponse } from "next";
import { GranthResolveError, resolveGranthSelection } from "@/lib/granth-resolver";

function firstString(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function parseBool(value: string | string[] | undefined) {
  const raw = String(firstString(value) || "").toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const bookId = Number.parseInt(String(firstString(req.query.bookId) || ""), 10);
    const adhikarRaw = String(firstString(req.query.adhikar) || "").trim();
    const payload = await resolveGranthSelection({
      bookId: Number.isFinite(bookId) ? bookId : null,
      bookCode: String(firstString(req.query.bookCode) || "").trim(),
      kind: String(firstString(req.query.kind) || "gathas"),
      spec: String(firstString(req.query.spec) || "").trim(),
      adhikar: adhikarRaw ? Number.parseInt(adhikarRaw, 10) : null,
      includeCover: parseBool(req.query.includeCover),
    });

    return res.status(200).json(payload);
  } catch (error) {
    if (error instanceof GranthResolveError) {
      return res.status(error.status).json(error.payload);
    }

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
