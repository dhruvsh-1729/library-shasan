import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import {
  PERMISSIONS,
  SUPER_ADMIN_ROLE,
  isPermission,
  type Permission,
} from "@/lib/auth-permissions";
import { getRole, type AppRole } from "@/lib/auth-users";
import { getSupabaseAdmin } from "@/lib/supabase-server";

/**
 * Edits or deletes one role. A role name is the foreign key on app_users, so it
 * is never rewritten here, and a role still in use cannot be dropped.
 */

type PatchBody = {
  label?: unknown;
  description?: unknown;
  permissions?: unknown;
};

type RoleResponse = { role: AppRole } | { ok: true } | { error: string };

type RolePatch = {
  label?: string;
  description?: string;
  permissions?: Permission[];
  updated_at: string;
};

const LABEL_MAX = 80;
const DESCRIPTION_MAX = 400;

type Parsed<T> = { ok: true; value: T } | { ok: false; error: string };

function parseLabel(value: unknown): Parsed<string> {
  if (typeof value !== "string") return { ok: false, error: "label must be a string" };
  const label = value.trim();
  if (!label) return { ok: false, error: "A role label is required" };
  if (label.length > LABEL_MAX) {
    return { ok: false, error: `Label must be ${LABEL_MAX} characters or fewer` };
  }
  return { ok: true, value: label };
}

function parseDescription(value: unknown): Parsed<string> {
  if (value === null) return { ok: true, value: "" };
  if (typeof value !== "string") return { ok: false, error: "description must be a string" };
  const description = value.trim();
  if (description.length > DESCRIPTION_MAX) {
    return { ok: false, error: `Description must be ${DESCRIPTION_MAX} characters or fewer` };
  }
  return { ok: true, value: description };
}

function parsePermissions(value: unknown): Parsed<Permission[]> {
  if (!Array.isArray(value)) return { ok: false, error: "permissions must be an array" };

  const permissions: Permission[] = [];
  for (const entry of value) {
    if (!isPermission(entry)) {
      const shown = typeof entry === "string" ? entry : typeof entry;
      return { ok: false, error: `Unknown permission: ${shown}` };
    }
    if (!permissions.includes(entry)) permissions.push(entry);
  }
  return { ok: true, value: permissions };
}

function firstQueryValue(value: string | string[] | undefined) {
  if (Array.isArray(value)) return value[0] ?? "";
  return value ?? "";
}

async function handler(req: NextApiRequest, res: NextApiResponse<RoleResponse>) {
  if (req.method !== "PATCH" && req.method !== "DELETE") {
    res.setHeader("Allow", "PATCH, DELETE");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const name = firstQueryValue(req.query.name).trim();
  if (!name) {
    return res.status(400).json({ error: "A role name is required" });
  }

  try {
    const role = await getRole(name);
    if (!role) {
      return res.status(404).json({ error: "Role not found" });
    }

    if (req.method === "PATCH") {
      const body = (req.body ?? {}) as PatchBody;

      // `name` is the app_users foreign key; any name in the body is ignored.
      const patch: RolePatch = { updated_at: new Date().toISOString() };

      if (body.label !== undefined) {
        const label = parseLabel(body.label);
        if (!label.ok) return res.status(400).json({ error: label.error });
        patch.label = label.value;
      }

      if (body.description !== undefined) {
        const description = parseDescription(body.description);
        if (!description.ok) return res.status(400).json({ error: description.error });
        patch.description = description.value;
      }

      if (body.permissions !== undefined) {
        const permissions = parsePermissions(body.permissions);
        if (!permissions.ok) return res.status(400).json({ error: permissions.error });

        // Super admin is the only way back into user and role management.
        if (
          role.name === SUPER_ADMIN_ROLE &&
          (!permissions.value.includes(PERMISSIONS.rolesManage) ||
            !permissions.value.includes(PERMISSIONS.usersManage))
        ) {
          return res
            .status(409)
            .json({ error: "The super admin role must keep full permissions" });
        }

        patch.permissions = permissions.value;
      }

      if (patch.label === undefined && patch.description === undefined && patch.permissions === undefined) {
        return res
          .status(400)
          .json({ error: "Provide a label, description or permissions to update" });
      }

      const { data, error } = await getSupabaseAdmin()
        .from("app_roles")
        .update(patch)
        .eq("name", role.name)
        .select("*")
        .single();

      if (error) throw new Error(`Failed to update role: ${error.message}`);

      return res.status(200).json({ role: data as AppRole });
    }

    if (role.is_system) {
      return res.status(409).json({ error: "System roles cannot be deleted" });
    }

    const { count, error: countError } = await getSupabaseAdmin()
      .from("app_users")
      .select("id", { count: "exact", head: true })
      .eq("role", role.name);

    if (countError) throw new Error(`Failed to count role members: ${countError.message}`);

    const holders = count ?? 0;
    if (holders > 0) {
      return res
        .status(409)
        .json({ error: `${holders} user(s) still have this role. Reassign them first.` });
    }

    const { error: deleteError } = await getSupabaseAdmin()
      .from("app_roles")
      .delete()
      .eq("name", role.name);

    if (deleteError) throw new Error(`Failed to delete role: ${deleteError.message}`);

    return res.status(200).json({ ok: true });
  } catch (error) {
    console.error("[api/admin/roles/[name]] failed to update role", error);
    return res.status(500).json({ error: "Could not update this role." });
  }
}

export default protectApi(handler, PERMISSIONS.rolesManage);
