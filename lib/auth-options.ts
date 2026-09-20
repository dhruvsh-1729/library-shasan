import type { NextAuthOptions } from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import GoogleProvider from "next-auth/providers/google";
import { normalizeEmail } from "@/lib/auth-permissions";
import {
  findUserByEmail,
  markLogin,
  toSessionUser,
  upsertGoogleUser,
  verifyPassword,
} from "@/lib/auth-users";

/**
 * Role and status live in the JWT so middleware can read them without a
 * database round trip, but they are re-read this often so that an approval,
 * a rejection or a role change takes effect without the user signing out.
 */
const TOKEN_REFRESH_MS = 60_000;

export const SIGNIN_ERRORS = {
  missingCredentials: "MISSING_CREDENTIALS",
  invalidCredentials: "INVALID_CREDENTIALS",
  pendingApproval: "PENDING_APPROVAL",
  accessRejected: "ACCESS_REJECTED",
  noEmail: "NO_EMAIL",
} as const;

const googleClientId = process.env.GOOGLE_CLIENT_ID;
const googleClientSecret = process.env.GOOGLE_CLIENT_SECRET;

export const authOptions: NextAuthOptions = {
  // Read lazily rather than asserted at module load: Next collects page data at
  // build time, and a throw here would fail the build instead of the request.
  secret: process.env.NEXTAUTH_SECRET,
  session: { strategy: "jwt", maxAge: 7 * 24 * 60 * 60 },
  pages: { signIn: "/auth/signin", error: "/auth/signin" },
  providers: [
    ...(googleClientId && googleClientSecret
      ? [
          GoogleProvider({
            clientId: googleClientId,
            clientSecret: googleClientSecret,
            // The app_users row keyed by email is the single identity record,
            // so a Google login attaches to the account that already exists.
            allowDangerousEmailAccountLinking: true,
          }),
        ]
      : []),
    CredentialsProvider({
      name: "Email and password",
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        const email = normalizeEmail(credentials?.email);
        const password = credentials?.password ?? "";
        if (!email || !password) throw new Error(SIGNIN_ERRORS.missingCredentials);

        const user = await findUserByEmail(email);
        if (!user?.password_hash) throw new Error(SIGNIN_ERRORS.invalidCredentials);

        const ok = await verifyPassword(password, user.password_hash);
        if (!ok) throw new Error(SIGNIN_ERRORS.invalidCredentials);

        if (user.status === "pending") throw new Error(SIGNIN_ERRORS.pendingApproval);
        if (user.status === "rejected") throw new Error(SIGNIN_ERRORS.accessRejected);

        await markLogin(user.id);
        return { id: user.id, email: user.email, name: user.name, image: user.image };
      },
    }),
  ],
  callbacks: {
    async signIn({ account, user }) {
      if (account?.provider !== "google") return true;

      const email = normalizeEmail(user.email);
      if (!email) return `/auth/signin?error=${SIGNIN_ERRORS.noEmail}`;

      const record = await upsertGoogleUser({
        email,
        name: user.name ?? null,
        image: user.image ?? null,
      });

      if (record.status === "pending") return "/auth/pending";
      if (record.status === "rejected") {
        return `/auth/signin?error=${SIGNIN_ERRORS.accessRejected}`;
      }

      await markLogin(record.id);
      return true;
    },

    async jwt({ token, user, trigger }) {
      const email = normalizeEmail(user?.email ?? token.email);
      if (!email) return token;

      const isStale = !token.syncedAt || Date.now() - token.syncedAt > TOKEN_REFRESH_MS;
      if (!user && trigger !== "update" && !isStale) return token;

      const record = await findUserByEmail(email);
      if (!record) {
        // The account was deleted; drop the claims so middleware rejects it.
        delete token.userId;
        delete token.permissions;
        token.status = "rejected";
        return token;
      }

      const sessionUser = await toSessionUser(record);
      token.userId = sessionUser.id;
      token.email = sessionUser.email;
      token.name = sessionUser.name;
      token.picture = sessionUser.image;
      token.role = sessionUser.role;
      token.roleLabel = sessionUser.roleLabel;
      token.status = sessionUser.status;
      token.permissions = sessionUser.permissions;
      token.syncedAt = Date.now();
      return token;
    },

    async session({ session, token }) {
      session.user = {
        id: token.userId ?? "",
        email: normalizeEmail(token.email),
        name: (token.name as string | null) ?? null,
        image: (token.picture as string | null) ?? null,
        role: token.role ?? "viewer",
        roleLabel: token.roleLabel ?? "Viewer",
        status: token.status ?? "pending",
        permissions: token.permissions ?? [],
      };
      return session;
    },
  },
};

export const googleSignInEnabled = Boolean(googleClientId && googleClientSecret);
