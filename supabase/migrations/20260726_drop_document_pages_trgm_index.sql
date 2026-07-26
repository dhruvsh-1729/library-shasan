-- The trigram GIN index makes OCR page writes very expensive because each page
-- can contain a large amount of text. Keep the FTS index for normal search and
-- drop this write-heavy substring index before running bulk OCR imports.
DROP INDEX IF EXISTS public.idx_document_pages_text_trgm;
