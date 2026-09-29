# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Mostly an internal team doing research and data work on a Jain granth (scripture) library: searching OCR'd pages for Sanskrit/Prakrit words, counting occurrences across granths (kosh word counts), checking each hit against the page image, and exporting matched pages as PDF or CSV. Sessions are focused and repeated; they are used on desktop and on phones.

## Product Purpose

Make the library's scanned granths searchable word by word, with counts a researcher can trust and cite: every number on screen must be the same one the exports produce, every hit must open the right page of the right book, and nothing may be silently guessed.

## Positioning

A Sanskrit-aware search over the library's own scans: queries and page text are folded by Sanskrit orthography (anusvara vs class nasal, Gujarati script vs Devanagari), Sanskrit declensions are matched, romanised input is offered as real Devanagari spellings with their page counts, and each hit is verified against the page text.

## Operating Context

- Granths are PDFs (uploaded, with OCR text indexed in Turso) or text-only spreadsheets; many are Gujarati anuwads or bhavanuvads of Sanskrit/Prakrit works, so one page often mixes Devanagari and Gujarati.
- Researchers pick granths by name or book number, choose a match mode (Sanskrit forms, exact word, begins/ends with, contains), and choose which scripts' hits count.
- Results open the PDF page (or the OCR text page when there is no PDF); matched pages export as a combined PDF or a CSV with page and line numbers.

## Capabilities and Constraints

- Next.js pages router; data in Supabase (metadata) and Turso (OCR pages, folded FTS index).
- Granth names come from file names and the library's book list; the granth catalog (lib/granth-catalog*.ts/.mjs + data/granth-catalog-overrides.json) is the single source of titles, PDF links and duplicates.
- Every Devanagari run is Sanskrit: transliteration and folding follow Sanskrit rules, never Hindi.

## Product Principles

1. Correct before convenient: a count, a page link or a granth name is either verified or labelled as not verified.
2. Show what is being searched: the exact spellings, scripts and granths are always visible.
3. The text is the interface: Indic text is large, high-contrast and never cramped.
4. One truth for every surface: results, previews, exports and the library list read the same catalog and the same matcher.

## Accessibility & Inclusion

- Must work well on phones.
- Devanagari and Gujarati text must be large and high-contrast (older readers).
- Interface labels in English; content stays in its own script.
