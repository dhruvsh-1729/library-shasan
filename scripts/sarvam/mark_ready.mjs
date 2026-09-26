// Marks Sarvam-redone granths Ready. reocr_granth.mjs repoints the files and
// text but leaves documents.status as it was, so a redone book kept showing
// "Review". Only books whose live text is Sarvam's are touched.
//   node --env-file=.env scripts/sarvam/mark_ready.mjs <granth_key>...
import { createClient } from "@libsql/client";
import { createClient as createSupabase } from "@supabase/supabase-js";
const turso = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
for (const key of process.argv.slice(2)) {
  const g = (await turso.execute({ sql: "SELECT source_rel_path, xlsx_custom_id FROM ocr_granths WHERE granth_key = ?", args: [key] })).rows[0];
  if (!g || !String(g.xlsx_custom_id).includes("__sarvam__")) { console.log(`${key}: skipped (live text is not Sarvam)`); continue; }
  const { data, error } = await sb.from("documents").update({ status: "processed", updated_at: new Date().toISOString() })
    .eq("original_relative_path", String(g.source_rel_path)).select("custom_id,status");
  console.log(`${key}: ${error ? error.message : `${data.length} row(s) -> processed`}`);
}
