import { NextResponse } from "next/server";
import { withAuth } from "next-auth/middleware";
import { PERMISSIONS } from "@/lib/auth-permissions";

/**
 * Edge auth gate. The deployment URL is public, so every page route is closed
 * by default and only the paths listed below stay reachable signed out.
 *
 * `withAuth` reads the NextAuth JWT on the edge (no database round trip), so
 * the claims it checks — `status` and `permissions` — are the ones the `jwt`
 * callback in lib/auth-options refreshes on its own schedule.
 */

const SIGN_IN_PAGE = "/auth/signin";
const PENDING_PAGE = "/auth/pending";

/** Pages that must render for a visitor who has no session at all. */
const PUBLIC_PAGES = new Set<string>([SIGN_IN_PAGE, "/auth/signup", PENDING_PAGE]);

/** Either of these lets an account into the admin area. */
const ADMIN_PERMISSIONS: readonly string[] = [
  PERMISSIONS.usersManage,
  PERMISSIONS.rolesManage,
];

/** `/logo.png`, `/sw.js`, `/robots.txt`… anything whose last segment has an extension. */
const STATIC_FILE = /\.[^/]+$/;

function isNextInternal(pathname: string) {
  return pathname.startsWith("/_next/") || pathname === "/favicon.ico";
}

function isStaticAsset(pathname: string) {
  return pathname.startsWith("/public/") || STATIC_FILE.test(pathname);
}

/**
 * Every API route is left to its own server-side guard (`protectApi` in
 * lib/auth-guard) so that callers get a JSON 401/403 instead of a 302 to an
 * HTML sign-in page — and so the unauthenticated Railway health check at
 * /api/hello and the NextAuth endpoints under /api/auth keep working.
 */
function isApiRoute(pathname: string) {
  return pathname === "/api" || pathname.startsWith("/api/");
}

function isPublicPath(pathname: string) {
  return (
    PUBLIC_PAGES.has(pathname) ||
    isApiRoute(pathname) ||
    isNextInternal(pathname) ||
    isStaticAsset(pathname)
  );
}

export default withAuth(
  function middleware(req) {
    const { pathname } = req.nextUrl;
    const token = req.nextauth.token;

    // API routes, framework internals and static files pass straight through:
    // they must never be redirected to a page.
    if (isApiRoute(pathname) || isNextInternal(pathname) || isStaticAsset(pathname)) {
      return NextResponse.next();
    }

    // An account that exists but is still awaiting approval belongs on the
    // waiting page, not on the sign-in form it would otherwise bounce to.
    // This must stay ahead of every other page branch: `authorized` below
    // lets a pending token through only because this sends it away again.
    if (token?.status === "pending" && pathname !== PENDING_PAGE) {
      return NextResponse.redirect(new URL(PENDING_PAGE, req.url));
    }

    if (pathname === "/admin" || pathname.startsWith("/admin/")) {
      const permissions = token?.permissions ?? [];
      const canManage = ADMIN_PERMISSIONS.some((permission) =>
        permissions.includes(permission)
      );
      if (!canManage) return NextResponse.redirect(new URL("/", req.url));
    }

    return NextResponse.next();
  },
  {
    pages: { signIn: SIGN_IN_PAGE },
    callbacks: {
      authorized: ({ req, token }) => {
        if (isPublicPath(req.nextUrl.pathname)) return true;
        if (!token) return false;
        // `withAuth` skips the middleware above entirely when this returns
        // false, so a pending token has to be let past here for the redirect
        // to /auth/pending to run at all. It never reaches a page: the first
        // thing the middleware does is redirect it.
        if (token.status === "pending") return true;
        return token.status === "approved";
      },
    },
  }
);

export const config = {
  // Matches everything, including "/", except framework assets and any file
  // with an extension. Public pages are allowed in `authorized` instead of
  // here, so the middleware body still sees them.
  matcher: ["/((?!_next/static|_next/image|favicon.ico|.*\\.[^/]+$).*)"],
};
