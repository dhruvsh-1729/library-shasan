import type { NextApiHandler, NextApiRequest, NextApiResponse } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import type { Permission } from "@/lib/auth-permissions";
import type { SessionUser } from "@/lib/auth-users";

export async function getSessionUser(
  req: NextApiRequest,
  res: NextApiResponse
): Promise<SessionUser | null> {
  const session = await getServerSession(req, res, authOptions);
  if (!session?.user?.id) return null;
  // A pending or rejected account holds a valid token but has no access.
  if (session.user.status !== "approved") return null;
  return session.user as SessionUser;
}

/**
 * Resolves the caller, or writes the 401/403 and resolves null. Callers must
 * return immediately when this yields null.
 */
export async function requireUser(
  req: NextApiRequest,
  res: NextApiResponse,
  permission?: Permission
): Promise<SessionUser | null> {
  const user = await getSessionUser(req, res);

  if (!user) {
    res.status(401).json({ error: "Authentication required" });
    return null;
  }

  if (permission && !user.permissions.includes(permission)) {
    res.status(403).json({ error: `Your role does not allow this action (${permission})` });
    return null;
  }

  return user;
}

export type AuthedApiHandler = (
  req: NextApiRequest,
  res: NextApiResponse,
  user: SessionUser
) => unknown | Promise<unknown>;

/** Wraps an API route so it only runs for an approved caller holding `permission`. */
export function protectApi(handler: AuthedApiHandler, permission?: Permission): NextApiHandler {
  return async (req, res) => {
    const user = await requireUser(req, res, permission);
    if (!user) return;
    return handler(req, res, user);
  };
}
