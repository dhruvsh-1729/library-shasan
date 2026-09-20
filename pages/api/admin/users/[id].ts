import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import {
  PERMISSIONS,
  SUPER_ADMIN_ROLE,
  USER_STATUSES,
  type UserStatus,
} from "@/lib/auth-permissions";
import {
  countActiveSuperAdmins,
  findUserById,
  getRole,
  setUserRole,
  setUserStatus,
  type AppUser,
  type SessionUser,
} from "@/lib/auth-users";

/**
 * Approve, reject or re-role a single account. The guard only proves
 * `users.manage`; assigning a role additionally needs `roles.manage`, and two
 * rails stop an admin locking either themselves or the portal out.
 */

type PatchBody = {
  status?: unknown;
  role?: unknown;
  note?: unknown;
};

type PublicAppUser = Omit<AppUser, "password_hash">;

type PatchResponse = { user: PublicAppUser } | { error: string };

const NOTE_MAX = 500;

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

async function handler(
  req: NextApiRequest,
  res: NextApiResponse<PatchResponse>,
  user: SessionUser
) {
  if (req.method !== "PATCH") {
    res.setHeader("Allow", "PATCH");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const id = firstQueryValue(req.query.id).trim();
  if (!id) {
    return res.status(400).json({ error: "A user id is required" });
  }

  const body = (req.body ?? {}) as PatchBody;

  if (body.status !== undefined && typeof body.status !== "string") {
    return res.status(400).json({ error: "status must be a string" });
  }
  if (body.role !== undefined && typeof body.role !== "string") {
    return res.status(400).json({ error: "role must be a string" });
  }
  if (body.note !== undefined && body.note !== null && typeof body.note !== "string") {
    return res.status(400).json({ error: "note must be a string" });
  }

  const requestedStatus = body.status === undefined ? undefined : body.status.trim();
  const requestedRole = body.role === undefined ? undefined : body.role.trim();
  const noteText = typeof body.note === "string" ? body.note.trim().slice(0, NOTE_MAX) : "";
  const note = noteText || null;

  if (requestedStatus === undefined && requestedRole === undefined) {
    return res.status(400).json({ error: "Provide a status or a role to change" });
  }

  try {
    const target = await findUserById(id);
    if (!target) {
      return res.status(404).json({ error: "User not found" });
    }

    // Assigning roles is a super-admin power; the wrapper only checked
    // users.manage, so the stricter permission is verified here.
    if (requestedRole !== undefined && !user.permissions.includes(PERMISSIONS.rolesManage)) {
      return res.status(403).json({ error: "Only a super admin can assign roles" });
    }

    if (requestedRole !== undefined) {
      const role = await getRole(requestedRole);
      if (!role) {
        return res.status(400).json({ error: "Unknown role" });
      }
    }

    if (requestedStatus !== undefined && !isUserStatus(requestedStatus)) {
      return res.status(400).json({
        error: `Unknown status. Expected one of: ${USER_STATUSES.join(", ")}`,
      });
    }
    const status = requestedStatus === undefined ? undefined : (requestedStatus as UserStatus);

    if (user.id === target.id) {
      return res.status(409).json({ error: "You cannot change your own access" });
    }

    // Refuse any edit that would leave the portal without an approved super admin.
    const nextRole = requestedRole ?? target.role;
    const nextStatus = status ?? target.status;
    const wasActiveSuperAdmin = target.role === SUPER_ADMIN_ROLE && target.status === "approved";
    const staysActiveSuperAdmin = nextRole === SUPER_ADMIN_ROLE && nextStatus === "approved";

    if (wasActiveSuperAdmin && !staysActiveSuperAdmin) {
      const remaining = await countActiveSuperAdmins(target.id);
      if (remaining === 0) {
        return res.status(409).json({ error: "The last super admin cannot be removed" });
      }
    }

    let updated: AppUser = target;
    if (requestedRole !== undefined) {
      updated = await setUserRole(target.id, requestedRole);
    }
    if (status !== undefined) {
      updated = await setUserStatus({
        userId: target.id,
        status,
        decidedBy: user.email,
        note,
      });
    }

    return res.status(200).json({ user: publicUser(updated) });
  } catch (error) {
    console.error("[api/admin/users/[id]] failed to update user", error);
    return res.status(500).json({ error: "Could not update this user." });
  }
}

export default protectApi(handler, PERMISSIONS.usersManage);
