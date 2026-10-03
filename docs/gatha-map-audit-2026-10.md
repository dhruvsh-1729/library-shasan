# Gatha map audit, 2026-10-03

`granth_gatha_map` was checked row by row against three things: the old
Kingston HTML index it was imported from, the OCR text of the page it names,
and the printed page number of that page (`ocr_printed_pages`), which every
index anchor also carries (`ગાથા - 805 : [253]`).

## What was wrong, and what was done

| Problem | Rows | Fix |
|---|---|---|
| The 2026-09-20 offset "fix" (books 232, 235, 268, 269, 270) moved correct rows off their page; the OCR text was misaligned at the time | 5,269 | back to the index page (checked on the scan: 268 gatha 844 is PDF p23, the map said 20, the contents page) |
| Index built from a different scan of the book: Sthanang 332 (+24), Panchvastuk 189 (−13), Pragnapana 217, Anekant 022 | 480 | moved to the page whose printed number and text both match |
| Page beyond the end of the PDF (269 gathas 3100–3294 are in no scan we serve) | 238 | deleted |
| No `book_code`, so the app could not find them | 1,878 | linked to their Turso granth |
| `page_end` of a chapter's last verse ran to the end of the book | ~8,100 | recomputed across the whole PDF |
| Labels squeezed into (adhikar, gatha): Sthanang `4/1/266` stored as gatha 1, `770, 771` as adhikar 770, Jitkalpa page/line anchors as gathas, Pragnapana `17/6/57-531` as gatha 6 | 1,498 relabelled; 336 page/line anchors and 244 exact duplicates deleted | parsed level by level; adhikar is the level directly above the verse |
| Index anchors never imported: rows without a book code (Sirimaransamahi, Hinsashtakam, Nandisutram Gujarati), Gujarati digits (Avashyak Niryukti), HTML not linked from index.html (Agam-satik 08/09, 31 dwatrinshikas, Yogvinshika) | 2,912 | imported after the same checks; 3 catalog books added |
| Dwatrinshika rows from OCR detection that took a commentary's repeated marker | 46 | moved to the index page (its printed page matched every time) |

Books that print verse numbers differently are not errors: Abhaykumar Charitra
prints only the last two digits (`૨૩.` for 423), Jitkalpa `२५.`, Sthanang
`[सू० २००]`, Raypaseniya summarises sutras 417–653 on one page.

## State after

76,409 rows. 70,243 verified (printed page and verse number, or the verse's
`॥n॥` on the page or the next), 3,112 page confirmed (printed page matches,
number not readable in the OCR), 866 text only, 2,188 unverified — 2,112 of
those are the four PDFs with no OCR text (Yogshastra 005118, Samraicch Kaha
1–2, Samachari 1), which also have no `book_code`.

## Still to do

- Run `supabase/migrations/20261003_gatha_map_units.sql`, then apply the
  `unit / unit_label / gatha_to / parent_path / verification` values
  (`changes-2b-after-migration.json` in the audit folder), then ship the
  `preferVerseRows` app change that reads `unit`.
- OCR the four PDFs above; Yogshastra and Samachari 1 are different scans
  from the OCR'd granths 248 and 318 (618 vs 586 and 296 vs 276 pages), so
  their rows cannot borrow that text.
- Visheshavashyak part 3 needs a complete scan for gathas 3100–3294.

Change sets, before-images and the full pre-audit backup are in
`ndms/testing/gatha-map-audit/`; `scripts/apply_gatha_map_changes.mjs`
applies a change set and writes the before-image first.
