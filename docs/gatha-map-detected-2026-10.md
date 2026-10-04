# Gatha map for the granths the index never covered, 2026-10-04

**Coverage after both rounds:** 364 of the 414 PDF granths have a gatha map
(199 from the index, 7 of them supplemented; 165 from OCR). The other 50 are
21 koshes and 29 books with no numbering of their own or none readable
(reasons in the decisions file). 84 more granths are duplicates or
spreadsheet text without a PDF. Table: 191,298 rows, 114,889 from OCR.

Before this, `granth_gatha_map` covered 227 of the 498 Turso granths (76,409
rows, from the old Kingston HTML index plus 29 dvātriṃśikās read from OCR).
183 of the granths without rows are offered by the extractor (PDF-backed, not
hidden, not a duplicate). Every one of the 183 now has a reviewed decision in
`data/gatha-detect/decisions.json`:

| | granths | rows |
|---|---|---|
| mapped | 132 | 110,543 (108,090 read on the page, 2,453 between neighbours) |
| kosh (dictionary) — nothing of its own to map | 21 | – |
| prose, translation, discourse, collection — no numbering of its own | 25 | – |
| not mappable reliably (mool numbers lost or indistinguishable in the OCR) | 5 | – |

Table after: 186,952 rows. The rows carry `source_html_rel_path` /
`granth_library_files.source_kind` = `ocr-detected`.

## How a book was mapped

`scripts/gatha_markers.mjs` reads a granth's OCR pages as a sequence of
numbers in one marker style (the editions use `॥ २३ ॥`, `॥૩/૧૦॥`, line-leading
`[૭૭૨]` / `• સૂત્ર-૭૪ :-`, `मू. (४८)`, `[भा.७६९]`, `[सू० ३५५]` …, or a book's own
regex) and keeps the best chain: each number continues the one before, a
restart at 1 opens a chapter, a run the OCR misread can be stepped over. A
unit gets the first page its marker is on after the previous unit's page.
`--series` lists every numbered series in a book, so the book's own (mool)
numbering can be chosen over its commentary's, quoted verses or a translation's.

It was checked against the 114 index-mapped books first: where a book's own
series is chosen, its pages agree with the index 97–100% exact. Choosing the
series is the hard part (a commentary's verse list is often the cleanest
chain), so every book was decided by reading it: which series is the text's
own, what the unit and its label are, how chapters are named and numbered
(continuation volumes keep the work's numbering — Sthanang 2 starts at sutra
235 after part 1's 234), and 8+ units checked on the page per book. Mool and
bhashya / niryukti series are separate units and separate chapters.

`scripts/build_detected_gatha_map.mjs` turns the decisions into rows
(dry run by default, `--execute` replaces a granth's `ocr-detected` rows):

- `page_end`: verse-closing markers (`॥n॥`) end at the page before the next
  unit; leading markers (`[n] …`) end on the next unit's page unless it
  starts on the same page.
- A printed group (`[૭૮૧ થી ૭૮૪]`, `॥२०-२१॥`) gives every number a row; the
  numbers after the first start where the range is printed (leading) or run to
  it (closing).
- A number the OCR lost inside a run is kept only when its neighbours are
  within 6 pages (`verification = unverified`); units read off the page are
  `verified`. Numbers a reviewer found on a wrong page are listed in `exclude`
  and left out (315: 10, 18–20, 69; 207: 173–181).

Independent check after the build: every verified row's own number, or a
printed range containing it, is on the page it points to (108,090 of 108,090).

## Round 2: gaps (same day)

Every mapped book was checked for numbered series in pages its map does not
reach (`round2` / `no_change` notes in the decisions). Most were appendices
(gatha indexes, mool-only listings, parishishtas), the commentary's own story
verses or quotations. Real gaps that were filled:

- never mapped: Vyavahar Sutram 3–5 (spreadsheet OCR; `document_custom_id`
  names the PDF; pages checked against the PDF text layer, shifted pages
  excluded), Bhasha Rahasya 1–2, Sajjana Stuti Dwatrinshika, Lalitavistara 1
  and 3, Panchgranthi's Kupdrishtant gathas
- two-digit printing (`hundreds` option): Pravachan Saroddhar 1002–1599,
  Vicharsar 596–716; the full numbers printed at each hundred agree
- index maps that stop early (`supplement`: only numbers the index lacks, in
  its own chapter scheme): Panchvastuk 7 (1656–1715), Sambodh Prakaran 3
  (adhikars 6–11), Avashyak Niryukti 5 (1248–1249, Dhyanashatak)
- OCR maps that stopped early: Lokaprakash sarga 30, Vairagyarati sarga 4,
  Uttaradhyayan adhyayan 29, Acharang churni sutras, Shaddarshan 19/41/48/49/87

The last unit of a book now ends at its own closing marker within 10 pages.

## Known gaps (missing rows, not wrong pages)

- OCR reads ९ as ६ in some books (Samaraicch Kaha, Karmagranth, Tattvartha
  Rajavartik): numbers containing 9 are mostly missing there.
- Pages with six or more "a-b" ranges are still treated as contents pages and
  skipped (Lokaprakash p150, Pravachan Saroddhar p64, Nandi Churni p51,
  Bhagvati p478).
- Uttaradhyayan adhyayan 29 and the prose sutras of adhyayan 16 (double
  numbering ॥२॥४॥), Acharang churni sutras (067/068), Chandraprajnapti
  (unnumbered mool) are not mapped.
- The Deepratnasagar volumes are numbered straight through; their shatak /
  uddeshak levels exist only in running heads and are not chapters here.
- Samachari part 2 (317, index-mapped) starts at gatha 51 while part 1 (318)
  ends at 48: 49–50 are probably missing from 317's index rows.
- Index rows not touched that look wrong: Bhasha Rahasya 229/230 and
  Lalitavistara 252/254 keep their old `other` rows (ignored by the
  extractor); 307 labels its chapters `પઅધિકાર`; the last index row of 195,
  332, 306, 116 and 077 has a page_end running into the appendices.
- Not mappable reliably: Upamiti 1–2 (verse runs numbered per passage),
  Tattvarth Shlokavartik, Dharmaratna 1–2, Chandraprajnapti (unnumbered mool).