import type { NextApiRequest, NextApiResponse } from "next";
import { emailAccount } from "@/lib/account-email";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS, USER_STATUSES, normalizeEmail, type UserStatus } from "@/lib/auth-permissions";
import {
  MIN_PASSWORD_LENGTH,
  createApprovedUser,
  findUserByEmail,
  generatePassword,
  getRole,
  listRoles,
  listUsers,
  type AppRole,
  type AppUser,
  type SessionUser,
} from "@/lib/auth-users";

/**
 * The admin user list (GET), and a super admin making an account (POST):
 * approved at once with the role chosen and a password, which comes back once
 * so it can be shared, and is emailed to the user when asked. Password hashes
 * never leave the server.
 */

export type PublicAppUser = Omit<AppUser, "password_hash">;

type ListResponse =
  | { users: PublicAppUser[]; roles: AppRole[] }
  | { user: PublicAppUser; password: string; emailed: boolean; emailError?: string }
  | { error: string };

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

async function createUser(req: NextApiRequest, res: NextApiResponse<ListResponse>, user: SessionUser) {
  if (!user.permissions.includes(PERMISSIONS.rolesManage)) {
    return res.status(403).json({ error: "Only a super admin can make accounts" });
  }
  const body = (req.body ?? {}) as Record<string, unknown>;
  const email = normalizeEmail(body.email);
  const name = String(body.name ?? "").trim().slice(0, 120);
  const roleName = String(body.role ?? "viewer").trim();
  const password = String(body.password ?? "").trim() || generatePassword();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "Enter a valid email address." });
  if (password.length < MIN_PASSWORD_LENGTH) {
    return res.status(400).json({ error: `The password needs at least ${MIN_PASSWORD_LENGTH} characters.` });
  }
  try {
    const role = await getRole(roleName);
    if (!role) return res.status(400).json({ error: "Unknown role" });
    const existing = await findUserByEmail(email);
    if (existing) {
      return res.status(409).json({
        error:
          existing.status === "approved"
            ? `${email} already has an account. Use “New password” on it instead.`
            : `${email} has already asked for access (${existing.status}). Approve it in the list, then use “New password”.`,
      });
    }
    const created = await createApprovedUser({ email, name, role: role.name, password, decidedBy: user.email });
    const mail = body.sendEmail ? await emailAccount(created, password, req, role.label) : { emailed: false };
    return res.status(201).json({ user: publicUser(created), password, ...mail });
  } catch (error) {
    console.error("[api/admin/users] failed to create user", error);
    return res.status(500).json({ error: "Could not make this account." });
  }
}

async function handler(req: NextApiRequest, res: NextApiResponse<ListResponse>, user: SessionUser) {
  if (req.method === "POST") return createUser(req, res, user);
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET, POST");
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
