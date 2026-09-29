// The granth catalog: names read from file names, PDF links from exact evidence only.
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildGranthCatalog, granthDisplayName } from "@/lib/granth-catalog-build.mjs";
import { parseGranthFileName } from "@/lib/granth-title.mjs";

test("file names split into title, part, codes and tags", () => {
  const a = parseGranthFileName("010_adhyatmasar_shabdasha_vivechan_part_03_037090_hr6.pdf");
  assert.deepEqual([a.bookNumbers, a.romanTitle, a.part, a.archiveIds, a.tags], ["010", "Adhyatmasar Shabdasha Vivechan", "3", ["037090"], ["hr6"]]);
  const b = parseGranthFileName("083_B011146_उत्तराध्ययनसूत्रम् (अध्ययन 1 थी 17) भाग 1.pdf");
  assert.deepEqual([b.libraryCode, b.nativeTitle, b.part], ["B011146", "उत्तराध्ययनसूत्रम् (अध्ययन 1 थी 17)", "1"]);
  const c = parseGranthFileName("029_B035152_agam_satik_part_05_sthananga_sutra_gujarati_anuwad_1_008996_std.pdf");
  assert.deepEqual([c.series, c.volume, c.romanTitle, c.part], ["Agam Satik", "5", "Sthananga Sutra Gujarati Anuwad", "1"]);
  const d = parseGranthFileName("Trishashti Parv 2 3 4");
  assert.deepEqual([d.romanTitle, d.part], ["Trishashti Parv 2–4", null]);
  assert.deepEqual(parseGranthFileName("203_B001849_Paumchariya_004884 - Copy.pdf").tags, ["copy"]);
});

const turso = [
  { granth_key: "069", book_number: "069", granth_name: "आचारांग", source_rel_path: "069_B037418_आचारांग सूत्रम् भावानुवाद भाग 1.pdf", page_count: 488 },
  { granth_key: "284", book_number: "284", granth_name: "vyavahar", source_rel_path: "1/BO VishayKosh Books OCR_ed/284_vyavahar_sutram_part_03_020936_hr3/284_vyavahar_sutram_part_03_020936_hr3.xlsx", page_count: 540 },
  { granth_key: "398_B041343", book_number: "398", granth_name: "आचारांग सूत्रम् भावानुवाद भाग-2", source_rel_path: "2/GG 76 Prat OCR_ed/398_B041343_x/398_B041343_x.xlsx", page_count: 377 },
  { granth_key: "unnum_6f79", book_number: "000", granth_name: "acharang", source_rel_path: "_needs_OCR/acharang_sutra_bhavanuvad_part_02_040035_hr6.pdf", page_count: 380 },
  { granth_key: "unnum_7349", book_number: "000", granth_name: "gujarati anuwad", source_rel_path: "_needs_OCR/agam_satik_part_02_acharanga_sutra_gujarati_anuwad_2_008993_std.pdf", page_count: 142 },
];
const documents = [
  { custom_id: "doc-069", original_relative_path: "069_B037418_आचारांग सूत्रम् भावानुवाद भाग 1.pdf", pdf_name: "069_B037418_आचारांग सूत्रम् भावानुवाद भाग 1.pdf" },
  { custom_id: "doc-284", original_relative_path: "284_vyavahar_sutram_part_03_020936_hr3.pdf", pdf_name: "284_vyavahar_sutram_part_03_020936_hr3.pdf" },
  { custom_id: "doc-6f79", original_relative_path: "_needs_OCR/acharang_sutra_bhavanuvad_part_02_040035_hr6.pdf", pdf_name: "acharang_sutra_bhavanuvad_part_02_040035_hr6.pdf" },
  { custom_id: "doc-7349", original_relative_path: "_needs_OCR/agam_satik_part_02_acharanga_sutra_gujarati_anuwad_2_008993_std.pdf", pdf_name: "agam_satik_part_02_acharanga_sutra_gujarati_anuwad_2_008993_std.pdf" },
];
const books = [
  { id: 1, title_english: "Acharaangsutram bhavanuvaad", title_display: "आचारांग सूत्रम् भावानुवाद", author_text: null, book_codes: ["069", "070"] },
  { id: 58, title_english: "Tattvarthvartikam Rajvartikam", title_display: null, author_text: null, book_codes: ["146"] },
];
const libraryFiles = [
  { custom_id: "doc-069", book_id: 1 },
  { custom_id: "doc-284", book_id: 58 },
];

test("PDFs are linked by exact evidence, never by shared title words", () => {
  const { entries } = buildGranthCatalog({ turso, documents, libraryFiles, books, overrides: { granths: { "398_B041343": { duplicate_of: "unnum_6f79" } } } });
  const by = new Map(entries.map((e) => [e.granth_key, e]));
  assert.equal(by.get("069")!.document_custom_id, "doc-069");
  // Re-indexed from a spreadsheet under another folder: same number and file name.
  assert.equal(by.get("284")!.document_custom_id, "doc-284");
  assert.equal(by.get("284")!.pdf_link, "file-name");
  // A text-only granth is never handed the Gujarati anuwad's PDF.
  assert.equal(by.get("398_B041343")!.document_custom_id, null);
  assert.equal(by.get("398_B041343")!.duplicate_of, "unnum_6f79");
  assert.equal(granthDisplayName(by.get("069")!), "Acharaangsutram bhavanuvaad · Part 1");
});

test("a library-book link whose codes do not include the file's number is refused", () => {
  const { entries, issues } = buildGranthCatalog({ turso, documents, libraryFiles, books });
  assert.equal(entries.find((e) => e.granth_key === "284")!.series, null);
  assert.ok(issues.some((i) => i.problem.includes("codes do not include")));
});
