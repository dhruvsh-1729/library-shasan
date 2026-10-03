-- Recent searches per signed-in user, shown on the search page's first step.
-- The url is the search page link (query, forms, match mode, scripts, books),
-- so opening one repeats the search exactly. Service-role access only.
CREATE TABLE IF NOT EXISTS public.search_history (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES public.app_users(id) ON DELETE CASCADE,
  query TEXT NOT NULL,
  url TEXT NOT NULL,
  label TEXT,
  used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, url)
);

CREATE INDEX IF NOT EXISTS idx_search_history_user_used
  ON public.search_history (user_id, used_at DESC);

ALTER TABLE public.search_history ENABLE ROW LEVEL SECURITY;
