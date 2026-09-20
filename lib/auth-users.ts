import bcrypt from "bcryptjs";
import { getSupabaseAdmin } from "@/lib/supabase-server";
import {
  SUPER_ADMIN_ROLE,
  normalizeEmail,
  type Permission,
  type UserStatus,
} from "@/lib/auth-permissions";

export type AppRole = {
  name: string;
  label: string;
  description: string;
  permissions: string[];
  is_system: boolean;
  created_at: string;
  updated_at: string;
};

export type AppUser = {
  id: string;
  email: string;
  name: string | null;
  image: string | null;
  password_hash: string | null;
  role: string;
  status: UserStatus;
  providers: string[];
  requested_at: string;
  decided_at: string | null;
  decided_by: string | null;
  decision_note: string | null;
  last_login_at: string | null;
  created_at: string;
  updated_at: string;
};

export type SessionUser = {
  id: string;
  email: string;
  name: string | null;
  image: string | null;
  role: string;
  roleLabel: string;
  status: UserStatus;
  permissions: string[];
};

const BCRYPT_ROUNDS = 10;

export function hashPassword(plain: string) {
  return bcrypt.hash(plain, BCRYPT_ROUNDS);
}

export function verifyPassword(plain: string, hash: string) {
  return bcrypt.compare(plain, hash);
}

export async function findUserByEmail(email: string): Promise<AppUser | null> {
  const { data, error } = await getSupabaseAdmin()
    .from("app_users")
    .select("*")
    .eq("email", normalizeEmail(email))
    .maybeSingle();

  if (error) throw new Error(`Failed to load user: ${error.message}`);
  return (data as AppUser | null) ?? null;
}

export async function findUserById(id: string): Promise<AppUser | null> {
  const { data, error } = await getSupabaseAdmin()
    .from("app_users")
    .select("*")
    .eq("id", id)
    .maybeSingle();

  if (error) throw new Error(`Failed to load user: ${error.message}`);
  return (data as AppUser | null) ?? null;
}

export async function getRole(name: string): Promise<AppRole | null> {
  const { data, error } = await getSupabaseAdmin()
    .from("app_roles")
    .select("*")
    .eq("name", name)
    .maybeSingle();

  if (error) throw new Error(`Failed to load role: ${error.message}`);
  return (data as AppRole | null) ?? null;
}

export async function listRoles(): Promise<AppRole[]> {
  const { data, error } = await getSupabaseAdmin()
    .from("app_roles")
    .select("*")
    .order("is_system", { ascending: false })
    .order("name", { ascending: true });

  if (error) throw new Error(`Failed to list roles: ${error.message}`);
  return (data as AppRole[]) ?? [];
}

export async function listUsers(status?: UserStatus): Promise<AppUser[]> {
  let query = getSupabaseAdmin().from("app_users").select("*");
  if (status) query = query.eq("status", status);

  const { data, error } = await query
    .order("status", { ascending: true })
    .order("requested_at", { ascending: false });

  if (error) throw new Error(`Failed to list users: ${error.message}`);
  return (data as AppUser[]) ?? [];
}

/**
 * Signups always land as `pending`; only an approval flips them to `approved`.
 * A Google sign-in for an unknown address creates the same pending record, so
 * both routes into the portal go through the same review queue.
 */
export async function createPendingUser(input: {
  email: string;
  name?: string | null;
  image?: string | null;
  password?: string | null;
  provider: "credentials" | "google";
}): Promise<AppUser> {
  const email = normalizeEmail(input.email);
  const passwordHash = input.password ? await hashPassword(input.password) : null;

  const { data, error } = await getSupabaseAdmin()
    .from("app_users")
    .insert({
      email,
      name: input.name ?? null,
      image: input.image ?? null,
      password_hash: passwordHash,
      role: "viewer",
      status: "pending",
      providers: [input.provider],
    })
    .select("*")
    .single();

  if (error) throw new Error(`Failed to create user: ${error.message}`);
  return data as AppUser;
}

/** Links a Google login to an existing record, or opens a pending request for a new one. */
export async function upsertGoogleUser(input: {
  email: string;
  name?: string | null;
  image?: string | null;
}): Promise<AppUser> {
  const email = normalizeEmail(input.email);
  const existing = await findUserByEmail(email);

  if (!existing) {
    return createPendingUser({ ...input, email, provider: "google" });
  }

  const providers = existing.providers.includes("google")
    ? existing.providers
    : [...existing.providers, "google"];

  const patch: Record<string, unknown> = { providers, updated_at: new Date().toISOString() };
  if (!existing.name && input.name) patch.name = input.name;
  if (input.image) patch.image = input.image;

  const { data, error } = await getSupabaseAdmin()
    .from("app_users")
    .update(patch)
    .eq("id", existing.id)
    .select("*")
    .single();

  if (error) throw new Error(`Failed to link Google account: ${error.message}`);
  return data as AppUser;
}

export async function markLogin(userId: string) {
  await getSupabaseAdmin()
    .from("app_users")
    .update({ last_login_at: new Date().toISOString() })
    .eq("id", userId);
}

export async function setUserStatus(input: {
  userId: string;
  status: UserStatus;
  decidedBy: string;
  note?: string | null;
}): Promise<AppUser> {
  const { data, error } = await getSupabaseAdmin()
    .from("app_users")
    .update({
      status: input.status,
      decided_at: new Date().toISOString(),
      decided_by: input.decidedBy,
      decision_note: input.note ?? null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.userId)
    .select("*")
    .single();

  if (error) throw new Error(`Failed to update user: ${error.message}`);
  return data as AppUser;
}

export async function setUserRole(userId: string, role: string): Promise<AppUser> {
  const { data, error } = await getSupabaseAdmin()
    .from("app_users")
    .update({ role, updated_at: new Date().toISOString() })
    .eq("id", userId)
    .select("*")
    .single();

  if (error) throw new Error(`Failed to assign role: ${error.message}`);
  return data as AppUser;
}

/** Guards against locking the portal out of its only super admin. */
export async function countActiveSuperAdmins(excludeUserId?: string) {
  let query = getSupabaseAdmin()
    .from("app_users")
    .select("id", { count: "exact", head: true })
    .eq("role", SUPER_ADMIN_ROLE)
    .eq("status", "approved");

  if (excludeUserId) query = query.neq("id", excludeUserId);

  const { count, error } = await query;
  if (error) throw new Error(`Failed to count super admins: ${error.message}`);
  return count ?? 0;
}

export async function toSessionUser(user: AppUser): Promise<SessionUser> {
  const role = await getRole(user.role);
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    image: user.image,
    role: user.role,
    roleLabel: role?.label ?? user.role,
    status: user.status,
    permissions: role?.permissions ?? [],
  };
}

export function sessionHasPermission(
  session: { user?: { permissions?: string[] } } | null | undefined,
  permission: Permission
) {
  return Boolean(session?.user?.permissions?.includes(permission));
}
