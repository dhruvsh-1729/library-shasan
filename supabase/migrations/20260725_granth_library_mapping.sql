CREATE TABLE IF NOT EXISTS public.granth_library_books (
  id BIGSERIAL PRIMARY KEY,
  source_row_hash TEXT NOT NULL UNIQUE,
  title_english TEXT,
  title_display TEXT,
  author_text TEXT,
  details_text TEXT,
  book_codes TEXT[] NOT NULL DEFAULT '{}',
  index_href TEXT,
  index_href_type TEXT,
  cover_rel_path TEXT,
  raw_text TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.granth_library_files (
  id BIGSERIAL PRIMARY KEY,
  source_file_key TEXT NOT NULL UNIQUE,
  book_id BIGINT REFERENCES public.granth_library_books(id) ON DELETE CASCADE,
  book_code TEXT,
  granth_ocr_file_id BIGINT REFERENCES public.granth_ocr_files(id) ON DELETE SET NULL,
  custom_id TEXT,
  pdf_file_name TEXT NOT NULL,
  pdf_rel_path TEXT,
  pdf_url TEXT,
  page_count INTEGER,
  source_href TEXT,
  source_kind TEXT NOT NULL DEFAULT 'index',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.granth_gatha_map (
  id BIGSERIAL PRIMARY KEY,
  source_anchor_key TEXT NOT NULL UNIQUE,
  book_id BIGINT REFERENCES public.granth_library_books(id) ON DELETE CASCADE,
  library_file_id BIGINT REFERENCES public.granth_library_files(id) ON DELETE SET NULL,
  granth_ocr_file_id BIGINT REFERENCES public.granth_ocr_files(id) ON DELETE SET NULL,
  custom_id TEXT,
  book_code TEXT,
  source_html_rel_path TEXT NOT NULL,
  source_html_title TEXT,
  pdf_file_name TEXT NOT NULL,
  pdf_rel_path TEXT,
  pdf_url TEXT,
  adhikar INTEGER,
  gatha INTEGER NOT NULL,
  anchor_text TEXT,
  anchor_label TEXT,
  href TEXT,
  page_start INTEGER NOT NULL,
  next_page_start INTEGER,
  page_end INTEGER,
  page_count INTEGER,
  sequence_index INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_granth_library_books_codes
  ON public.granth_library_books USING GIN (book_codes);

CREATE INDEX IF NOT EXISTS idx_granth_library_books_title_english
  ON public.granth_library_books (title_english);

CREATE INDEX IF NOT EXISTS idx_granth_library_files_book_code
  ON public.granth_library_files (book_code);

CREATE INDEX IF NOT EXISTS idx_granth_library_files_pdf_name
  ON public.granth_library_files (pdf_file_name);

CREATE INDEX IF NOT EXISTS idx_granth_library_files_custom_id
  ON public.granth_library_files (custom_id);

CREATE INDEX IF NOT EXISTS idx_granth_gatha_map_book_gatha
  ON public.granth_gatha_map (book_id, book_code, adhikar, gatha);

CREATE INDEX IF NOT EXISTS idx_granth_gatha_map_pdf
  ON public.granth_gatha_map (pdf_file_name, page_start);

CREATE INDEX IF NOT EXISTS idx_granth_gatha_map_custom_id
  ON public.granth_gatha_map (custom_id);

ALTER TABLE public.granth_library_books ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.granth_library_files ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.granth_gatha_map ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'granth_library_books'
      AND policyname = 'Public read granth library books'
  ) THEN
    CREATE POLICY "Public read granth library books"
      ON public.granth_library_books FOR SELECT
      USING (true);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'granth_library_files'
      AND policyname = 'Public read granth library files'
  ) THEN
    CREATE POLICY "Public read granth library files"
      ON public.granth_library_files FOR SELECT
      USING (true);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'granth_gatha_map'
      AND policyname = 'Public read granth gatha map'
  ) THEN
    CREATE POLICY "Public read granth gatha map"
      ON public.granth_gatha_map FOR SELECT
      USING (true);
  END IF;
END $$;
