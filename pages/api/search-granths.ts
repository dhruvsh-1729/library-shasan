import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

type GranthOption = {
  custom_id: string;
  pdf_name: string | null;
  display_name: string;
};

function displayName(pdfName: string | null, customId: string) {
  const raw = pdfName && pdfName.trim() ? pdfName : customId;
  return raw.replace(/\s+OCR\.pdf$/i, "").replace(/\.pdf$/i, "");
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const limit = Math.min(Math.max(Number(req.query.limit ?? 1000) || 1000, 1), 5000);

  const { data, error } = await supabase
    .from("documents")
    .select("custom_id,pdf_name,status")
    .not("custom_id", "is", null)
    .eq("status", "processed")
    .order("pdf_name", { ascending: true })
    .limit(limit);

  if (error) return res.status(500).json({ error: error.message });

  const seen = new Set<string>();
  const items: GranthOption[] = [];

  for (const row of data ?? []) {
    const customId = String(row.custom_id ?? "").trim();
    if (!customId || seen.has(customId)) continue;
    seen.add(customId);
    items.push({
      custom_id: customId,
      pdf_name: row.pdf_name ?? null,
      display_name: displayName(row.pdf_name ?? null, customId),
    });
  }

  res.status(200).json({
    items,
    total: items.length,
  });
}
