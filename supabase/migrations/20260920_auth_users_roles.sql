-- Authentication, role and signup-approval tables for the library portal.
-- All access goes through the service-role client in lib/supabase-server.ts,
-- so RLS is enabled with no policies: anon and authenticated keys see nothing.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.app_roles (
  name TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  permissions TEXT[] NOT NULL DEFAULT '{}',
  is_system BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.app_users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL UNIQUE CHECK (email = lower(email)),
  name TEXT,
  image TEXT,
  password_hash TEXT,
  role TEXT NOT NULL DEFAULT 'viewer'
    REFERENCES public.app_roles(name) ON UPDATE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'approved', 'rejected')),
  providers TEXT[] NOT NULL DEFAULT '{}',
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decided_at TIMESTAMPTZ,
  decided_by TEXT,
  decision_note TEXT,
  last_login_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_users_status ON public.app_users (status, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_app_users_role ON public.app_users (role);

ALTER TABLE public.app_roles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.app_users ENABLE ROW LEVEL SECURITY;

-- Seed roles. super_admin is the only role that may manage roles or assign
-- them, and is_system stops it being renamed or deleted from the admin UI.
INSERT INTO public.app_roles (name, label, description, permissions, is_system) VALUES
  ('super_admin', 'Super Admin',
   'Full control, including approving signups, creating roles and assigning them.',
   ARRAY['library.read', 'ocr.write', 'pdf.build', 'users.manage', 'roles.manage'], TRUE),
  ('admin', 'Admin',
   'Can read the library, correct OCR text and build PDFs.',
   ARRAY['library.read', 'ocr.write', 'pdf.build'], FALSE),
  ('editor', 'Editor',
   'Can read the library and correct OCR text.',
   ARRAY['library.read', 'ocr.write'], FALSE),
  -- is_system because createPendingUser() assigns 'viewer' to every new signup;
  -- deleting it would break the signup flow on the foreign key.
  ('viewer', 'Viewer',
   'Read-only access to the library and search.',
   ARRAY['library.read'], TRUE)
ON CONFLICT (name) DO NOTHING;

-- Seed accounts. crypt()/gen_salt('bf') produces $2a$ bcrypt hashes, which the
-- bcryptjs verifier in lib/auth-users.ts reads natively.
INSERT INTO public.app_users (email, name, password_hash, role, status, providers, decided_at, decided_by)
VALUES
  ('dhruvsh2003@gmail.com', 'Dhruv Shah', crypt('1234', gen_salt('bf', 10)),
   'super_admin', 'approved', ARRAY['credentials', 'google'], NOW(), 'seed'),
  ('test@gg.com', 'Test User', crypt('1234', gen_salt('bf', 10)),
   'viewer', 'approved', ARRAY['credentials'], NOW(), 'seed')
ON CONFLICT (email) DO UPDATE
  SET role = EXCLUDED.role,
      status = EXCLUDED.status,
      updated_at = NOW();
