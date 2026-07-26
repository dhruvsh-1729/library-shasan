CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_documents_status_custom_id
  ON public.documents(status, custom_id);

CREATE INDEX IF NOT EXISTS idx_document_pages_custom_page
  ON public.document_pages(custom_id, page_number);

CREATE INDEX IF NOT EXISTS idx_document_pages_text_fts
  ON public.document_pages
  USING gin (to_tsvector('simple', coalesce(text, '')));

CREATE INDEX IF NOT EXISTS idx_document_pages_text_trgm
  ON public.document_pages
  USING gin (text gin_trgm_ops);

CREATE OR REPLACE FUNCTION public.search_pages_paginated(
  p_q TEXT,
  p_limit INTEGER DEFAULT 20,
  p_offset INTEGER DEFAULT 0,
  p_granths TEXT[] DEFAULT NULL
)
RETURNS TABLE (
  custom_id TEXT,
  pdf_name TEXT,
  pdf_url TEXT,
  page_number INTEGER,
  snippet TEXT,
  score REAL,
  csv_url TEXT,
  total_count BIGINT
)
LANGUAGE sql
STABLE
AS $$
  WITH params AS (
    SELECT
      trim(coalesce(p_q, '')) AS q,
      plainto_tsquery('simple', trim(coalesce(p_q, ''))) AS tsq,
      greatest(1, least(coalesce(p_limit, 20), 100)) AS page_limit,
      greatest(0, coalesce(p_offset, 0)) AS page_offset,
      CASE
        WHEN p_granths IS NULL OR cardinality(p_granths) = 0 THEN NULL
        ELSE p_granths
      END AS granths
  ),
  matches AS (
    SELECT
      dp.custom_id,
      coalesce(d.pdf_name, dp.custom_id) AS pdf_name,
      coalesce(d.pdf_url, '') AS pdf_url,
      dp.page_number,
      CASE
        WHEN to_tsvector('simple', coalesce(dp.text, '')) @@ params.tsq THEN
          ts_headline(
            'simple',
            coalesce(dp.text, ''),
            params.tsq,
            'MaxWords=42, MinWords=18, ShortWord=2, HighlightAll=false'
          )
        ELSE left(coalesce(dp.text, ''), 520)
      END AS snippet,
      CASE
        WHEN to_tsvector('simple', coalesce(dp.text, '')) @@ params.tsq THEN
          ts_rank_cd(to_tsvector('simple', coalesce(dp.text, '')), params.tsq)
        ELSE 0
      END::REAL AS score,
      d.csv_url
    FROM public.document_pages dp
    JOIN public.documents d ON d.custom_id = dp.custom_id
    CROSS JOIN params
    WHERE char_length(params.q) >= 2
      AND d.status = 'processed'
      AND (params.granths IS NULL OR dp.custom_id = ANY(params.granths))
      AND (
        to_tsvector('simple', coalesce(dp.text, '')) @@ params.tsq
        OR coalesce(dp.text, '') ILIKE (
          '%' ||
          replace(replace(replace(params.q, '\', '\\'), '%', '\%'), '_', '\_') ||
          '%'
        ) ESCAPE '\'
      )
  )
  SELECT
    matches.custom_id,
    matches.pdf_name,
    matches.pdf_url,
    matches.page_number,
    matches.snippet,
    matches.score,
    matches.csv_url,
    count(*) OVER () AS total_count
  FROM matches, params
  ORDER BY matches.score DESC, matches.pdf_name ASC, matches.page_number ASC
  LIMIT (SELECT page_limit FROM params)
  OFFSET (SELECT page_offset FROM params);
$$;
