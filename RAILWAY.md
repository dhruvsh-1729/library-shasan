# Deploying on Railway

This app is a plain Next.js (Pages Router) server: static pages + Node API routes.
There is no Vercel-specific code, so Railway runs it as-is with `next start`.

## Files that drive the deploy

| File | Purpose |
| --- | --- |
| `railway.json` | Builder, start command, healthcheck, restart policy |
| `.nvmrc` / `package.json` `engines` | Pins Node 22 |
| `.railwayignore` | Keeps ~5.6 GB of local pipeline scratch data out of CLI uploads |

## One-time setup

1. `railway login`
2. `railway init` (or link an existing project with `railway link`)
3. Add the environment variables below in the Railway dashboard
   (**Variables → Raw Editor** accepts `KEY=VALUE` lines pasted in bulk).
4. **Settings → Networking → Generate Domain** to get a public URL.
5. Deploy: push to the connected GitHub branch, or run `railway up`.

## Required environment variables

Runtime (the web app fails without these):

- `TURSO_URL`, `TURSO_AUTH_TOKEN`
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`
- `NEXTAUTH_SECRET` — 32 random bytes, base64 (`openssl rand -base64 32`)
- `NEXTAUTH_URL` — the full public origin, e.g. `https://library.aryanculture.org`.
  Must match the browser's address exactly or OAuth callbacks fail.
- `MAILEROO_API_KEY` — or the SMTP fallback:
  `MAILEROO_SMTP_HOST`, `MAILEROO_SMTP_PORT`, `MAILEROO_SMTP_USERNAME`, `MAILEROO_SMTP_PASSWORD`

Build:

- `NPM_CONFIG_INCLUDE=dev` — `npm ci` omits devDependencies when `NODE_ENV=production`,
  which would strip TypeScript and Tailwind and break `next build`.

Optional — Google sign-in. When unset the portal offers email/password only:

- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`
  Authorised redirect URI in Google Cloud Console:
  `<NEXTAUTH_URL>/api/auth/callback/google`

Used only by `scripts/` (the offline indexing pipeline), not by the served app:

- `UPLOADTHING_TOKEN`, `UPLOADTHING_APP_ID`, `SARVAM_API_KEY`
- `OCR_LANGS`, `OCR_DPI`, `USE_OCR_FALLBACK`

`PORT` is injected by Railway — do not set it manually.

## Custom domain

1. Railway → service → **Settings → Networking → Custom Domain** → add
   `library.aryanculture.org`. Railway returns a CNAME target like
   `xyz.up.railway.app`.
2. At Namecheap → **Advanced DNS**, add a CNAME record:
   Host `library`, Value the Railway target, TTL Automatic.
3. Wait for the Railway dashboard to show the certificate as issued, then set
   `NEXTAUTH_URL=https://library.aryanculture.org` and redeploy.

## Differences from Vercel

- **No CDN.** The `s-maxage` headers set in `lib/api-cache.ts` were served by Vercel's
  edge cache; Railway has no shared cache in front of the app. The in-process memory
  cache in the same file still applies and is *more* effective here, because the server
  is long-lived instead of a per-request lambda.
- **No function timeout.** PDF builds in `pages/api/granth-mapping/build-pdf.ts` are no
  longer capped by a serverless execution limit.
- **Shared process.** One container serves every request, so an OOM takes down all of
  them rather than a single invocation. `lib/available-memory.ts` reads the cgroup limit
  so the guard in `build-pdf.ts` measures the container, not the host.
- **`os.tmpdir()` persists** between requests on an instance. The PDF source cache is
  capped at 4 GB by `MAX_SOURCE_CACHE_BYTES` in `lib/pdf-highlight-builder.ts`; lower it
  if the service disk is smaller.
