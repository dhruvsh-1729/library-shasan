-- Recent extractions per signed-in user, shown on the extractor's first step
-- so a passage used before is one tap away on any device. Reached only through
-- the service-role client, so RLS is on with no policies.
CREATE TABLE IF NOT EXISTS public.extract_history (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES public.app_users(id) ON DELETE CASCADE,
  mode TEXT NOT NULL CHECK (mode IN ('gathas', 'pages')),
  work_id TEXT NOT NULL,
  work_title TEXT NOT NULL,
  volume_key TEXT,
  chapter_id TEXT,
  chapter_label TEXT,
  spec TEXT NOT NULL,
  -- one row per distinct passage; using it again only moves it to the top
  signature TEXT NOT NULL,
  used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, signature)
);

CREATE INDEX IF NOT EXISTS idx_extract_history_user_used
  ON public.extract_history (user_id, used_at DESC);

ALTER TABLE public.extract_history ENABLE ROW LEVEL SECURITY;
