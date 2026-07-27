CREATE TABLE IF NOT EXISTS public.download_email_recipients (
  id BIGSERIAL PRIMARY KEY,
  client_key TEXT NOT NULL DEFAULT '',
  email TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.download_email_recipients
  ADD COLUMN IF NOT EXISTS client_key TEXT NOT NULL DEFAULT '';

ALTER TABLE public.download_email_recipients
  DROP CONSTRAINT IF EXISTS download_email_recipients_email_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_download_email_recipients_client_email
  ON public.download_email_recipients (client_key, email);

CREATE INDEX IF NOT EXISTS idx_download_email_recipients_last_used
  ON public.download_email_recipients (last_used_at DESC);

ALTER TABLE public.download_email_recipients ENABLE ROW LEVEL SECURITY;
