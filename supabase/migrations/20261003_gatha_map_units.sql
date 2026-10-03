-- granth_gatha_map rows were (adhikar, gatha) only, but the old index files
-- point at sutras, niryukti and bhashya gathas, mool/churni pairs and chapter
-- openings too, in texts nested three or four levels deep (श्रुतस्कन्ध › वर्ग ›
-- अध्ययन › गाथा). Squeezed into two numbers they collided and mislabelled
-- (Sthanang "4/1/266" was stored as gatha 1). These columns keep what each
-- row really is; adhikar stays the level directly above the verse.

ALTER TABLE public.granth_gatha_map
  ADD COLUMN IF NOT EXISTS unit TEXT,          -- gatha | shlok | karika | sutra | niryukti | bhashya | mool | churni | chapter | other
  ADD COLUMN IF NOT EXISTS unit_label TEXT,    -- the label as the index prints it: ગાથા, सूत्र, निरयुक्ति …
  ADD COLUMN IF NOT EXISTS gatha_to INTEGER,   -- last number when one anchor covers several ("770, 771", "1-4")
  ADD COLUMN IF NOT EXISTS parent_path TEXT,   -- the levels above, e.g. "कप्पसुत्तं › उदेश 3" or "श्रुत स्कन्ध 1 › वर्ग 3 › अध्ययन 1"
  ADD COLUMN IF NOT EXISTS verification TEXT;  -- verified | page_confirmed | text_only | unverified (2026-10 audit)

CREATE INDEX IF NOT EXISTS idx_granth_gatha_map_code_unit
  ON public.granth_gatha_map (book_code, unit, adhikar, gatha);
