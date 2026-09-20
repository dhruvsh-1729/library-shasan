// Re-scores every granth the batch has processed and prints before/after, so a
// run can be judged on whether the text actually improved rather than on
// whether the jobs merely completed.
import { createClient } from "@libsql/client";
import { readFileSync } from "node:fs";
import { scoreGranth } from "./assess_quality.mjs";
const t=createClient({url:process.env.TURSO_URL,authToken:process.env.TURSO_AUTH_TOKEN});
const rep=JSON.parse(readFileSync("./.sarvam/quality_report.json","utf8"));
const before=new Map(rep.report.map(r=>[r.granth_key,r]));
const done=Object.keys(JSON.parse(readFileSync("./.sarvam/logs/batch_results.json","utf8")));
console.log("granth".padEnd(22)+"before".padStart(8)+"after".padStart(8)+"  verdict");
let sumB=0,sumA=0,n=0;
for (const k of done) {
  const pages=await t.execute({sql:"SELECT content FROM ocr_pages WHERE granth_key=? ORDER BY page_number",args:[k]});
  if(!pages.rows.length){console.log(k.padEnd(22)+"  (no pages)");continue;}
  const s=scoreGranth(pages.rows.map(r=>({content:r.content})));
  const b=before.get(k)?.score ?? 0;
  sumB+=b; sumA+=s.score; n++;
  console.log(k.padEnd(22)+String(b).padStart(8)+String(s.score).padStart(8)+"  "+s.verdict);
}
console.log("\naverage: "+(sumB/n).toFixed(1)+" -> "+(sumA/n).toFixed(1)+"  across "+n+" granths");
