import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS, USER_STATUSES, type UserStatus } from "@/lib/auth-permissions";
import { listRoles, listUsers, type AppRole, type AppUser } from "@/lib/auth-users";

/** The admin user list. Password hashes never leave the server. */

export type PublicAppUser = Omit<AppUser, "password_hash">;

type ListResponse = { users: PublicAppUser[]; roles: AppRole[] } | { error: string };

function publicUser(user: AppUser): PublicAppUser {
  const { password_hash, ...rest } = user;
  void password_hash;
  return rest;
}

function firstQueryValue(value: string | string[] | undefined) {
  if (Array.isArray(value)) return value[0] ?? "";
  return value ?? "";
}

function isUserStatus(value: string): value is UserStatus {
  return (USER_STATUSES as readonly string[]).includes(value);
}

async function handler(req: NextApiRequest, res: NextApiResponse<ListResponse>) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const rawStatus = firstQueryValue(req.query.status).trim();
  if (rawStatus && !isUserStatus(rawStatus)) {
    return res.status(400).json({
      error: `Unknown status filter. Expected one of: ${USER_STATUSES.join(", ")}`,
    });
  }
  const status = rawStatus ? (rawStatus as UserStatus) : undefined;

  try {
    const [users, roles] = await Promise.all([listUsers(status), listRoles()]);
    return res.status(200).json({ users: users.map(publicUser), roles });
  } catch (error) {
    console.error("[api/admin/users] failed to list users", error);
    return res.status(500).json({ error: "Could not load users." });
  }
}

export default protectApi(handler, PERMISSIONS.usersManage);
