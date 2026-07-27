# Budgeted Library OCR Pipeline

This pipeline is dry-run by default. It does not upload files, write Supabase or
Turso, or call Google Document AI unless `--execute` is passed.

## Safe Checks

```bash
npm run library:pipeline -- --help
npm run library:pipeline -- --limit 1 --maxPages 2 --noRemoteReads
```

## Recommended Full Run

Upload/catalog every PDF first, so the library page can show PDFs even before
OCR finishes:

```bash
npm run library:pipeline -- --execute --phase catalog
```

Then run OCR with the hard Google cap:

```bash
npm run library:pipeline -- --execute --phase text --googleBudgetUsd 20
```

The text phase is resume-aware. Before processing each granth, it reads existing
page text from Turso `ocr_pages` and Supabase `document_pages`. Pages that meet
the existing-text quality threshold are reused and are not sent through local OCR
or Google. Missing or low-quality pages are processed again. The script also
saves per-page checkpoints while it works, so rerunning after an interruption can
skip pages that were already checkpointed. Use `--reprocess` to ignore completed
granths, `--noResumePages` to ignore page-level resume text, or
`--noPageCheckpoints` to disable in-progress page saves.

The default Google price model is `$1.50 / 1000 pages`, so `$20` permits at most
13,333 paid page attempts. The script records paid attempts in
`.library_pipeline_state/state.json` before each Google call. Once the cap is
reached, it stops sending pages to Google and marks those pages/books as budget
exhausted while continuing to preserve local/embedded text where available.

## Useful Controls

```bash
# One complete book by leading filename number
npm run library:pipeline -- --execute --bookNumber 001 --googleBudgetUsd 20

# Faster local OCR if RAM is healthy
npm run library:pipeline -- --execute --phase text --pageConcurrency 4 --minFreeMemMB 4096

# Force local OCR language packs for a known Sanskrit-heavy run
npm run library:pipeline -- --execute --phase text --langs guj+san+eng

# PDF/covers only, no searchable text
npm run library:pipeline -- --execute --phase catalog

# No paid Google calls, only embedded text + local Tesseract
npm run library:pipeline -- --execute --phase text --googleMode off
```

Local OCR defaults to Gujarati, Sanskrit, and English (`guj+san+eng`). The
pipeline builds a combined tessdata directory under `stateDir/tessdata`, linking
system language packs plus extra packs from `/home/dell/Downloads` by default.
That lets `/home/dell/Downloads/san.traineddata` be used without a system-wide
install. Google Document AI language hints default to `gu,sa,en`; these are OCR
hints, not a guaranteed hard language constraint. Use `--googleLanguageHints auto`
to omit hints.

## What Gets Updated

- UploadThing: original PDFs, cover images, generated XLSX, generated CSV.
- Supabase `granth_ocr_files`: PDF URL, UploadThing key, file metadata, cover URL.
- Supabase `documents`: searchable status, PDF URL, CSV URL, processing summary.
- Supabase `document_pages`: extracted page text for the existing search path.
- Turso `ocr_granths`, `ocr_pages`, `ocr_pages_fts`: spreadsheet URL and searchable page text.

Existing Turso granths with enough pages and an XLSX URL are skipped unless
`--reprocess` is passed.

## Old Granth Library Mapping

The old `/media/dell/KINGSTON/Library Final/GRANTH-LIBRARY/index.html` and
linked HTML files contain book-code and gatha-to-PDF-page mappings. Apply the
mapping tables once in Supabase SQL:

```bash
supabase/migrations/20260725_granth_library_mapping.sql
```

Then dry-run the importer from this repo:

```bash
npm run library:mapping:import
```

When the dry-run counts look right, write the data:

```bash
npm run library:mapping:import -- --execute
```

The importer is idempotent. It matches uploaded PDFs from `granth_ocr_files` by
exact filename first, then by normalized filename aliases that strip common scan
suffixes such as `OCR`, `ocred`, `std`, and `hr6`. If catalog upload is still in
progress, rerun the import after catalog finishes; existing rows are updated and
newly uploaded PDF URLs are attached.

For each gatha, `page_end` is calculated from the next distinct mapped start
page minus one. For the last gatha in a PDF section, the importer uses `pdfinfo`
to extend the range to the end of the PDF when the local PDF is available.
