// READ-ONLY backup of a granth before re-OCR: ocr_granths row, page text, line boxes, documents rows -> JSON.
// usage: node --env-file=.env scripts/kraken/backup_granth.mjs <granth_key> <out.json>
import { createClient } from '@libsql/client';
import { createClient as createSupabase } from '@supabase/supabase-js';
import fs from 'fs';
const key = process.argv[2], out = process.argv[3];
const t = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const sb = createSupabase(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const g = (await t.execute({ sql: 'SELECT * FROM ocr_granths WHERE granth_key = ?', args: [key] })).rows[0];
const pages = (await t.execute({ sql: 'SELECT page_number, content, printed_page FROM ocr_pages WHERE granth_key = ? ORDER BY page_number', args: [key] })).rows;
const boxes = (await t.execute({ sql: 'SELECT page_number, boxes, source FROM ocr_line_boxes WHERE granth_key = ?', args: [key] })).rows;
const { data: docs } = await sb.from('documents').select('*').eq('original_relative_path', String(g.source_rel_path));
fs.writeFileSync(out, JSON.stringify({ granth: g, pages, boxes, documents: docs }, null, 1));
console.log(key, String(g.granth_name), 'pages', pages.length, 'boxes', boxes.length, 'status', docs?.[0]?.status, 'xlsx', String(g.xlsx_custom_id));
