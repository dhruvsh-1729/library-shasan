# Kraken re-OCR (our own model)

`reocr_granth.mjs` re-OCRs one granth with our fine-tuned Kraken model and publishes it the same way
`scripts/sarvam/reocr_granth.mjs` does. Only the pages the model flags as tables/charts (plus any page the
service failed on) are sent to Sarvam, so a typical book costs ~₹0.15 of Railway CPU plus ~₹0.5 for each
flagged page (about 9% of pages).

```
node --env-file=.env scripts/kraken/reocr_granth.mjs <granth_key> --dry-run          # build PDF + CSV only
node --env-file=.env scripts/kraken/reocr_granth.mjs <granth_key> --keep-old         # publish, keep old files
node --env-file=.env scripts/kraken/reocr_granth.mjs <granth_key> --pdf=/media/...    # use a local scan
node --env-file=.env scripts/kraken/reocr_granth.mjs <granth_key> --no-sarvam         # Kraken for every page
```

Steps (checkpointed in `$REOCR_WORK/kraken_<key>/state.json`; a rerun resumes):
resolve → source PDF → Kraken service → Sarvam for flagged pages → build CSV (method `kraken_r3` /
`sarvam_doc_ai` per page) → searchable PDF (old layer stripped, Kraken line boxes / Sarvam blocks) →
upload → `update_granth_sources.mjs` (Turso text, suffix + folded search index, Supabase pointers) →
`ocr_line_boxes` (Kraken pages; rows for Sarvam pages are deleted so stale boxes never mismatch) →
verify + `documents.status = processed` → delete old UploadThing files (skipped with `--keep-old`).

Needs `KRAKEN_OCR_URL` and `KRAKEN_OCR_TOKEN` in `.env` (service: `ndms/kraken-ocr`, Railway project
`ndms-kraken-ocr`), plus the usual Turso / Supabase / UploadThing / Sarvam variables.

Measured 2026-10-05 on granth 221 (548 scored pages, never trained on), dry run vs its full Sarvam text:
whole-book character difference 1.3%, median page 0.7%, 5 pages above 10%.
