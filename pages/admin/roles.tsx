import Link from "next/link";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { signOut } from "next-auth/react";
import { useCallback, useEffect, useState } from "react";
import { authOptions } from "@/lib/auth-options";
import {
  ALL_PERMISSIONS,
  PERMISSIONS,
  PERMISSION_LABELS,
  isPermission,
  normalizeRoleName,
  type UserStatus,
} from "@/lib/auth-permissions";

type AdminRole = {
  name: string;
  label: string;
  description: string | null;
  permissions: string[];
  is_system: boolean;
  created_at: string | null;
  updated_at: string | null;
};

type SessionUser = {
  id: string;
  email: string;
  name: string | null;
  image: string | null;
  role: string;
  roleLabel: string;
  status: UserStatus;
  permissions: string[];
};

type RolesPageProps = {
  currentUser: SessionUser;
};

type RoleDraft = {
  label: string;
  description: string;
  permissions: string[];
};

const PAGE_PATH = "/admin/roles";

const EMPTY_DRAFT: RoleDraft = { label: "", description: "", permissions: [] };

/** Reads a JSON body without throwing on an empty or non-JSON response. */
async function readJson<T>(response: Response): Promise<Partial<T> & { error?: string }> {
  try {
    return (await response.json()) as Partial<T> & { error?: string };
  } catch {
    return {};
  }
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error && error.message ? error.message : fallback;
}

function permissionLabel(value: string) {
  return isPermission(value) ? PERMISSION_LABELS[value] : value;
}

function togglePermission(list: string[], permission: string) {
  return list.includes(permission)
    ? list.filter((entry) => entry !== permission)
    : [...list, permission];
}

export default function AdminRolesPage({ currentUser }: RolesPageProps) {
  const [roles, setRoles] = useState<AdminRole[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const [createName, setCreateName] = useState("");
  const [createDraft, setCreateDraft] = useState<RoleDraft>(EMPTY_DRAFT);
  const [nameTouched, setNameTouched] = useState(false);

  const [editingName, setEditingName] = useState<string | null>(null);
  const [editDraft, setEditDraft] = useState<RoleDraft>(EMPTY_DRAFT);
  const [confirmDeleteName, setConfirmDeleteName] = useState<string | null>(null);

  const canManageUsers = currentUser.permissions.includes(PERMISSIONS.usersManage);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      try {
        const response = await fetch("/api/admin/roles");
        const payload = await readJson<{ roles: AdminRole[] }>(response);
        if (!response.ok) {
          throw new Error(payload.error ?? `Could not load roles (${response.status}).`);
        }
        if (cancelled) return;
        setRoles(payload.roles ?? []);
        setError(null);
      } catch (loadError) {
        if (cancelled) return;
        setError(errorMessage(loadError, "Could not load roles."));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const resetCreateForm = useCallback(() => {
    setCreateName("");
    setCreateDraft(EMPTY_DRAFT);
    setNameTouched(false);
  }, []);

  const createRole = useCallback(async () => {
    const name = normalizeRoleName(createName);
    const label = createDraft.label.trim();

    if (!name) {
      setError("A role needs a name made of lowercase letters, digits or underscores.");
      return;
    }
    if (!label) {
      setError("A role needs a label.");
      return;
    }

    setBusyKey("create");
    setError(null);
    try {
      const response = await fetch("/api/admin/roles", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          label,
          description: createDraft.description.trim(),
          permissions: createDraft.permissions,
        }),
      });
      const payload = await readJson<{ role: AdminRole }>(response);
      if (!response.ok || !payload.role) {
        throw new Error(payload.error ?? `Could not create the role (${response.status}).`);
      }
      const created = payload.role;
      setRoles((previous) => [created, ...previous.filter((role) => role.name !== created.name)]);
      resetCreateForm();
    } catch (createError) {
      setError(errorMessage(createError, "Could not create the role."));
    } finally {
      setBusyKey(null);
    }
  }, [createDraft, createName, resetCreateForm]);

  const saveRole = useCallback(async (name: string, draft: RoleDraft) => {
    const label = draft.label.trim();
    if (!label) {
      setError("A role needs a label.");
      return;
    }

    setBusyKey(name);
    setError(null);
    try {
      const response = await fetch(`/api/admin/roles/${encodeURIComponent(name)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          label,
          description: draft.description.trim(),
          permissions: draft.permissions,
        }),
      });
      const payload = await readJson<{ role: AdminRole }>(response);
      if (!response.ok || !payload.role) {
        throw new Error(payload.error ?? `Could not save the role (${response.status}).`);
      }
      const saved = payload.role;
      setRoles((previous) => previous.map((role) => (role.name === name ? saved : role)));
      setEditingName(null);
      setEditDraft(EMPTY_DRAFT);
    } catch (saveError) {
      setError(errorMessage(saveError, "Could not save the role."));
    } finally {
      setBusyKey(null);
    }
  }, []);

  const deleteRole = useCallback(async (name: string) => {
    setBusyKey(name);
    setError(null);
    try {
      const response = await fetch(`/api/admin/roles/${encodeURIComponent(name)}`, {
        method: "DELETE",
      });
      const payload = await readJson<{ ok: boolean }>(response);
      if (!response.ok) {
        throw new Error(payload.error ?? `Could not delete the role (${response.status}).`);
      }
      setRoles((previous) => previous.filter((role) => role.name !== name));
      setConfirmDeleteName(null);
      setEditingName((current) => (current === name ? null : current));
    } catch (deleteError) {
      setError(errorMessage(deleteError, "Could not delete the role."));
    } finally {
      setBusyKey(null);
    }
  }, []);

  return (
    <div className="adminShell">
      <div className="adminFrame">
        <header className="adminHeader">
          <div className="adminHeaderText">
            <h1 className="adminTitle">Roles</h1>
            <p className="adminSubtitle">
              Create roles and choose what each one is allowed to do.
            </p>
            <div className="adminIdentity">
              <span className="adminIdentityEmail">{currentUser.email}</span>
              <span className="adminIdentityRole">{currentUser.roleLabel}</span>
            </div>
          </div>
          <nav className="adminNav">
            {canManageUsers ? <Link href="/admin/users">Users</Link> : null}
            <Link href="/">Back to library</Link>
            <button
              type="button"
              className="adminNavButton"
              onClick={() => {
                void signOut({ callbackUrl: "/auth/signin" });
              }}
            >
              Sign out
            </button>
          </nav>
        </header>

        {error ? <div className="adminError">{error}</div> : null}

        <section className="adminPanel">
          <div className="adminPanelHeader">
            <span>Create role</span>
          </div>

          <form
            className="adminEditGrid"
            onSubmit={(event) => {
              event.preventDefault();
              void createRole();
            }}
          >
            <div className="adminFieldGrid">
              <label className="adminField">
                Label
                <input
                  className="adminInput"
                  value={createDraft.label}
                  placeholder="Content editor"
                  disabled={busyKey === "create"}
                  onChange={(event) => {
                    const label = event.target.value;
                    setCreateDraft((previous) => ({ ...previous, label }));
                    if (!nameTouched) setCreateName(normalizeRoleName(label));
                  }}
                />
              </label>
              <label className="adminField">
                Name
                <input
                  className="adminInput"
                  value={createName}
                  placeholder="content_editor"
                  disabled={busyKey === "create"}
                  onChange={(event) => {
                    setNameTouched(true);
                    setCreateName(event.target.value);
                  }}
                  onBlur={(event) => setCreateName(normalizeRoleName(event.target.value))}
                />
                <span className="adminFieldHint">
                  Lowercase letters, digits and underscores. Used as the stored identifier.
                </span>
              </label>
            </div>

            <label className="adminField">
              Description
              <textarea
                className="adminTextarea"
                value={createDraft.description}
                placeholder="What this role is for."
                disabled={busyKey === "create"}
                onChange={(event) => {
                  const description = event.target.value;
                  setCreateDraft((previous) => ({ ...previous, description }));
                }}
              />
            </label>

            <div className="adminField">
              Permissions
              <div className="adminCheckGrid">
                {ALL_PERMISSIONS.map((permission) => (
                  <label key={permission} className="adminCheck">
                    <input
                      type="checkbox"
                      checked={createDraft.permissions.includes(permission)}
                      disabled={busyKey === "create"}
                      onChange={() =>
                        setCreateDraft((previous) => ({
                          ...previous,
                          permissions: togglePermission(previous.permissions, permission),
                        }))
                      }
                    />
                    <span>{PERMISSION_LABELS[permission]}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="adminFormActions">
              <button type="submit" className="adminPrimaryButton" disabled={busyKey === "create"}>
                {busyKey === "create" ? "Creating…" : "Create role"}
              </button>
              <button
                type="button"
                className="adminGhostButton"
                disabled={busyKey === "create"}
                onClick={resetCreateForm}
              >
                Reset
              </button>
            </div>
          </form>
        </section>

        {loading ? (
          <div className="adminPanel">
            <div className="adminLoading">Loading roles…</div>
          </div>
        ) : roles.length === 0 ? (
          <div className="adminPanel">
            <div className="adminEmpty">No roles defined yet.</div>
          </div>
        ) : (
          <div className="adminRoleList">
            {roles.map((role) => {
              const editing = editingName === role.name;
              const busy = busyKey === role.name;
              const confirming = confirmDeleteName === role.name;

              return (
                <section key={role.name} className="adminPanel">
                  <div className="adminRoleHead">
                    <div className="adminRoleHeadText">
                      <div className="adminRoleName">
                        <h3>{role.label}</h3>
                        <code className="adminMono">{role.name}</code>
                        {role.is_system ? <span className="adminBadge isSystem">System</span> : null}
                      </div>
                      {role.description ? (
                        <p className="adminRoleDescription">{role.description}</p>
                      ) : (
                        <p className="adminRoleDescription adminMuted">No description.</p>
                      )}
                    </div>
                    <div className="adminRoleActions">
                      <button
                        type="button"
                        className="adminGhostButton"
                        disabled={busy}
                        onClick={() => {
                          setConfirmDeleteName(null);
                          if (editing) {
                            setEditingName(null);
                            setEditDraft(EMPTY_DRAFT);
                            return;
                          }
                          setEditingName(role.name);
                          setEditDraft({
                            label: role.label,
                            description: role.description ?? "",
                            permissions: [...role.permissions],
                          });
                        }}
                      >
                        {editing ? "Cancel" : "Edit"}
                      </button>
                      {role.is_system ? null : confirming ? (
                        <>
                          <button
                            type="button"
                            className="adminDangerButton"
                            disabled={busy}
                            onClick={() => void deleteRole(role.name)}
                          >
                            {busy ? "Deleting…" : "Confirm delete?"}
                          </button>
                          <button
                            type="button"
                            className="adminGhostButton"
                            disabled={busy}
                            onClick={() => setConfirmDeleteName(null)}
                          >
                            Keep
                          </button>
                        </>
                      ) : (
                        <button
                          type="button"
                          className="adminDangerButton"
                          disabled={busy}
                          onClick={() => setConfirmDeleteName(role.name)}
                        >
                          Delete
                        </button>
                      )}
                    </div>
                  </div>

                  {role.permissions.length === 0 ? (
                    <span className="adminMuted">No permissions granted.</span>
                  ) : (
                    <div className="adminChips">
                      {role.permissions.map((permission) => (
                        <span key={permission} className="adminChip">
                          {permissionLabel(permission)}
                        </span>
                      ))}
                    </div>
                  )}

                  {editing ? (
                    <form
                      className="adminEditGrid"
                      onSubmit={(event) => {
                        event.preventDefault();
                        void saveRole(role.name, editDraft);
                      }}
                    >
                      <label className="adminField">
                        Label
                        <input
                          className="adminInput"
                          value={editDraft.label}
                          disabled={busy}
                          onChange={(event) => {
                            const label = event.target.value;
                            setEditDraft((previous) => ({ ...previous, label }));
                          }}
                        />
                      </label>

                      <label className="adminField">
                        Description
                        <textarea
                          className="adminTextarea"
                          value={editDraft.description}
                          disabled={busy}
                          onChange={(event) => {
                            const description = event.target.value;
                            setEditDraft((previous) => ({ ...previous, description }));
                          }}
                        />
                      </label>

                      <div className="adminField">
                        Permissions
                        <div className="adminCheckGrid">
                          {ALL_PERMISSIONS.map((permission) => (
                            <label key={permission} className="adminCheck">
                              <input
                                type="checkbox"
                                checked={editDraft.permissions.includes(permission)}
                                disabled={busy}
                                onChange={() =>
                                  setEditDraft((previous) => ({
                                    ...previous,
                                    permissions: togglePermission(previous.permissions, permission),
                                  }))
                                }
                              />
                              <span>{PERMISSION_LABELS[permission]}</span>
                            </label>
                          ))}
                        </div>
                      </div>

                      <div className="adminFormActions">
                        <button type="submit" className="adminPrimaryButton" disabled={busy}>
                          {busy ? "Saving…" : "Save changes"}
                        </button>
                        <button
                          type="button"
                          className="adminGhostButton"
                          disabled={busy}
                          onClick={() => {
                            setEditingName(null);
                            setEditDraft(EMPTY_DRAFT);
                          }}
                        >
                          Discard
                        </button>
                      </div>
                    </form>
                  ) : null}
                </section>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

export const getServerSideProps: GetServerSideProps<RolesPageProps> = async (ctx) => {
  const session = await getServerSession(ctx.req, ctx.res, authOptions);

  if (!session?.user || session.user.status !== "approved") {
    return {
      redirect: {
        destination: `/auth/signin?callbackUrl=${encodeURIComponent(PAGE_PATH)}`,
        permanent: false,
      },
    };
  }

  if (!session.user.permissions.includes(PERMISSIONS.rolesManage)) {
    return { redirect: { destination: "/", permanent: false } };
  }

  return { props: { currentUser: session.user } };
};
