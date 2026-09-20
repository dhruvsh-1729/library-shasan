#!/usr/bin/env bash
# Master entry point for the Sarvam re-OCR pipeline.
#
#   ./scripts/sarvam/run.sh assess                 score every granth
#   ./scripts/sarvam/run.sh plan                   show what would be redone
#   ./scripts/sarvam/run.sh redo [--max-granths=N] redo worst-first
#   ./scripts/sarvam/run.sh granth <key>           redo one granth
#   ./scripts/sarvam/run.sh status                 what has been done so far
#   ./scripts/sarvam/run.sh all                    assess, then redo
#
# Budgets (Doc AI digitisation is charged per page):
#   --max-pages=N     default 40000   (~₹20,000)
#   --max-granths=N   default 1000
#   --minutes=N       wall-clock cap
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SARVAM_DIR="$ROOT/scripts/sarvam"
STATE="$ROOT/.sarvam"
REPORT="$STATE/quality_report.json"
ENVFILE="$ROOT/.env"

mkdir -p "$STATE/logs"
[ -f "$ENVFILE" ] || { echo "missing $ENVFILE"; exit 1; }

node_run() { node --env-file="$ENVFILE" "$@"; }

cmd="${1:-help}"; shift || true

case "$cmd" in
  assess)
    echo "scoring every granth from the text stored in Turso..."
    node_run "$SARVAM_DIR/assess_quality.mjs" --out="$REPORT" "$@"
    ;;

  plan)
    [ -f "$REPORT" ] || { echo "no report yet - run: $0 assess"; exit 1; }
    node -e '
      const r=require(process.argv[1]);
      const by={}; for(const x of r.report) by[x.verdict]=(by[x.verdict]||0)+1;
      const redo=r.report.filter(x=>x.verdict==="REDO");
      const pages=redo.reduce((n,x)=>n+(x.metrics.pages||0),0);
      console.log(`generated ${r.generatedAt}`);
      console.log(`  OK ${by.OK||0}   REVIEW ${by.REVIEW||0}   REDO ${by.REDO||0}`);
      console.log(`  REDO pages: ${pages.toLocaleString()}  (~Rs ${(pages*0.5).toLocaleString()})`);
      console.log("\nworst 25:");
      for(const x of r.report.slice(0,25))
        console.log(`  ${x.granth_key.padEnd(6)} ${String(x.score).padStart(6)}  ${x.verdict.padEnd(7)} ${(x.metrics.pages||0).toString().padStart(4)}p  ${x.reasons.slice(0,2).join("; ")}`);
    ' "$REPORT"
    ;;

  redo)
    [ -f "$REPORT" ] || { echo "no report yet - run: $0 assess"; exit 1; }
    node_run "$SARVAM_DIR/reocr_batch.mjs" --report="$REPORT" --log-dir="$STATE/logs" "$@"
    ;;

  granth)
    key="${1:?usage: $0 granth <granth_key>}"; shift || true
    node_run "$SARVAM_DIR/reocr_granth.mjs" "$key" "$@"
    ;;

  status)
    f="$STATE/logs/batch_results.json"
    [ -f "$f" ] || { echo "nothing run yet"; exit 0; }
    node -e '
      const r=require(process.argv[1]);
      const e=Object.entries(r);
      const ok=e.filter(([,v])=>v.status==="ok");
      const bad=e.filter(([,v])=>v.status!=="ok");
      const pages=ok.reduce((n,[,v])=>n+(v.pages||0),0);
      console.log(`${ok.length} granths redone, ${bad.length} failed, ${pages.toLocaleString()} pages (~Rs ${(pages*0.5).toLocaleString()})`);
      for(const [k,v] of bad) console.log(`  FAILED ${k}: ${(v.tail||"").split("\n").pop()}`);
    ' "$f"
    ;;

  all)
    "$0" assess
    "$0" plan
    "$0" redo "$@"
    ;;

  *)
    sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
esac
