#!/usr/bin/env bash
# One-look status of the Google OCR + publish run.
#   bash scripts/google/status.sh
W=/media/dell/KINGSTON/ocr_work
echo "== processes"
pgrep -af "gocr_run.mjs|publish_worker.mjs" | grep -v pgrep | grep node | sed 's/--env-file=[^ ]* //' | cut -c1-110 || echo "  none running"
echo "== OCR";      grep "progress:" "$W/modern_run.log" | tail -1
grep -E "FAILED|GAVE UP|finished:" "$W/modern_run.log" | tail -5
echo "== publish"; for f in "$W"/publish*.log; do grep "status:" "$f" | tail -1 | sed "s|^|  $(basename $f): |"; done
cat "$W"/publish*.log | grep -E "PUBLISHED" | sort | tail -3
cat "$W"/publish*.log | grep -E "QUARANTINED|RETRY LATER|Error" | tail -10
echo "== cost (Document AI bills \$1.50 per 1,000 pages)"
python3 - <<'PY'
import json
s=json.load(open('/media/dell/KINGSTON/ocr_work/modern/state.json'))
run=sum(sum(s['parts'][p]['pages'] for p in o['parts']) for o in s['ops'].values())
tests=2195   # all test and pilot pages sent on 2026-09-24 before the main run
print(f"  main run: {run:,} pages sent = ${run*1.5/1000:.2f}  (planned 69,852 = $104.78; {100*run/69852:.0f}%)")
print(f"  incl. tests: {run+tests:,} pages = ${(run+tests)*1.5/1000:.2f}")
PY
