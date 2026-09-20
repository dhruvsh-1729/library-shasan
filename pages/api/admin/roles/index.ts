import type { NextApiRequest, NextApiResponse } from "next";
import { protectApi } from "@/lib/auth-guard";
import {
  PERMISSIONS,
  SUPER_ADMIN_ROLE,
  isPermission,
  normalizeRoleName,
  type Permission,
} from "@/lib/auth-permissions";
import { getRole, listRoles, type AppRole } from "@/lib/auth-users";
import { getSupabaseAdmin } from "@/lib/supabase-server";

/** Lists roles and creates new custom ones. System roles are seeded, not created here. */

type CreateBody = {
  name?: unknown;
  label?: unknown;
  description?: unknown;
  permissions?: unknown;
};

type RolesResponse = { roles: AppRole[] } | { role: AppRole } | { error: string };

const LABEL_MAX = 80;
const DESCRIPTION_MAX = 400;

type Parsed<T> = { ok: true; value: T } | { ok: false; error: string };

function parseLabel(value: unknown): Parsed<string> {
  if (typeof value !== "string") return { ok: false, error: "A role label is required" };
  const label = value.trim();
  if (!label) return { ok: false, error: "A role label is required" };
  if (label.length > LABEL_MAX) {
    return { ok: false, error: `Label must be ${LABEL_MAX} characters or fewer` };
  }
  return { ok: true, value: label };
}

function parseDescription(value: unknown): Parsed<string> {
  if (value === undefined || value === null) return { ok: true, value: "" };
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

async function handler(req: NextApiRequest, res: NextApiResponse<RolesResponse>) {
  if (req.method === "GET") {
    try {
      return res.status(200).json({ roles: await listRoles() });
    } catch (error) {
      console.error("[api/admin/roles] failed to list roles", error);
      return res.status(500).json({ error: "Could not load roles." });
    }
  }

  if (req.method === "POST") {
    const body = (req.body ?? {}) as CreateBody;

    const name = normalizeRoleName(body.name);
    if (!name) {
      return res.status(400).json({
        error: "A role name is required (letters, numbers and underscores)",
      });
    }

    const label = parseLabel(body.label);
    if (!label.ok) return res.status(400).json({ error: label.error });

    const description = parseDescription(body.description);
    if (!description.ok) return res.status(400).json({ error: description.error });

    const permissions = parsePermissions(body.permissions);
    if (!permissions.ok) return res.status(400).json({ error: permissions.error });

    if (name === SUPER_ADMIN_ROLE) {
      return res.status(409).json({ error: "The super admin role is reserved" });
    }

    try {
      const existing = await getRole(name);
      if (existing) {
        return res.status(409).json({ error: "A role with that name already exists" });
      }

      const { data, error } = await getSupabaseAdmin()
        .from("app_roles")
        .insert({
          name,
          label: label.value,
          description: description.value,
          permissions: permissions.value,
          is_system: false,
        })
        .select("*")
        .single();

      if (error) throw new Error(`Failed to create role: ${error.message}`);

      return res.status(201).json({ role: data as AppRole });
    } catch (error) {
      console.error("[api/admin/roles] failed to create role", error);
      return res.status(500).json({ error: "Could not create this role." });
    }
  }

  res.setHeader("Allow", "GET, POST");
  return res.status(405).json({ error: "Method not allowed" });
}

export default protectApi(handler, PERMISSIONS.rolesManage);
