-- OCR page text is stored and searched in Turso. Keeping the same text in
-- Supabase document_pages blows past the free-tier database quota.
DROP INDEX IF EXISTS public.idx_document_pages_text_trgm;
DROP INDEX IF EXISTS public.idx_document_pages_text_fts;

DO $$
BEGIN
  IF to_regclass('public.document_pages') IS NOT NULL THEN
    TRUNCATE TABLE public.document_pages;
  END IF;
END $$;
