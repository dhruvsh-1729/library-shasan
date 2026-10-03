import Head from "next/head";
import Link from "next/link";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { signOut } from "next-auth/react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { authOptions } from "@/lib/auth-options";
import { accountMessage } from "@/lib/account-message";
import { PERMISSIONS, type UserStatus } from "@/lib/auth-permissions";

type AdminUser = {
  id: string;
  email: string;
  name: string | null;
  image: string | null;
  role: string;
  status: UserStatus;
  providers: string[];
  requested_at: string | null;
  decided_at: string | null;
  decided_by: string | null;
  decision_note: string | null;
  last_login_at: string | null;
  created_at: string | null;
};

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

type UsersPageProps = {
  currentUser: SessionUser;
};

type StatusFilter = "all" | UserStatus;

type UserPatch = {
  status?: "approved" | "rejected";
  role?: string;
};

const PAGE_PATH = "/admin/users";

const FILTERS: { key: StatusFilter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "pending", label: "Pending" },
  { key: "approved", label: "Approved" },
  { key: "rejected", label: "Rejected" },
];

const STATUS_BADGE: Record<UserStatus, string> = {
  pending: "adminBadge isPending",
  approved: "adminBadge isApproved",
  rejected: "adminBadge isRejected",
};

const STATUS_LABEL: Record<UserStatus, string> = {
  pending: "Pending",
  approved: "Approved",
  rejected: "Rejected",
};

const EMPTY_MESSAGE: Record<StatusFilter, string> = {
  all: "No accounts yet.",
  pending: "No pending requests.",
  approved: "No approved users.",
  rejected: "No rejected users.",
};

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

function formatWhen(value: string | null) {
  if (!value) return "—";
  const at = new Date(value);
  if (Number.isNaN(at.getTime())) return "—";
  return at.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

// No 0/O, 1/l/I: the password is read out or copied from a phone message.
const PASSWORD_ALPHABET = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";

function newPassword(length = 10) {
  const bytes = new Uint32Array(length);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => PASSWORD_ALPHABET[b % PASSWORD_ALPHABET.length]).join("");
}

type Credentials = { user: AdminUser; password: string; emailed: boolean; emailError?: string; isNew: boolean };

/** The sign-in details, shown once, with ways to send them on. */
function CredentialsCard({ credentials, roleLabel, onDone }: { credentials: Credentials; roleLabel: string; onDone: () => void }) {
  const [copied, setCopied] = useState(false);
  const signInUrl = typeof window === "undefined" ? "/auth/signin" : `${window.location.origin}/auth/signin`;
  const message = accountMessage({ name: credentials.user.name, email: credentials.user.email, password: credentials.password, signInUrl, roleLabel, reset: !credentials.isNew });
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(message);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      setCopied(false);
    }
  };
  return (
    <section className="adminPanel adminCredentials" aria-live="polite">
      <div className="adminPanelHeader">
        <span>{credentials.isNew ? "Account made" : "New password set"}</span>
        <strong>{credentials.user.email}</strong>
      </div>
      <p className="adminFieldHint">
        This password is shown only now. Send it to the user, then close this.
        {credentials.emailed ? " It has also been emailed to them." : ""}
      </p>
      {credentials.emailError ? <div className="adminError">The email could not be sent ({credentials.emailError}). Share it another way.</div> : null}
      <pre className="adminMessage">{message}</pre>
      <div className="adminFormActions">
        <button type="button" className="adminPrimaryButton" onClick={() => void copy()}>
          {copied ? "Copied" : "Copy message"}
        </button>
        <a className="adminGhostButton" href={`https://wa.me/?text=${encodeURIComponent(message)}`} target="_blank" rel="noopener noreferrer">
          WhatsApp
        </a>
        <a
          className="adminGhostButton"
          href={`mailto:${encodeURIComponent(credentials.user.email)}?subject=${encodeURIComponent("Your Granth Library account")}&body=${encodeURIComponent(message)}`}
        >
          Email
        </a>
        <button type="button" className="adminGhostButton" onClick={onDone}>
          Done
        </button>
      </div>
    </section>
  );
}

function initialFor(user: AdminUser) {
  const source = user.name?.trim() || user.email;
  return source.slice(0, 1).toUpperCase();
}

export default function AdminUsersPage({ currentUser }: UsersPageProps) {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [roles, setRoles] = useState<AdminRole[]>([]);
  const [filter, setFilter] = useState<StatusFilter>("pending");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  const canManageRoles = currentUser.permissions.includes(PERMISSIONS.rolesManage);

  // Making an account, and the details to share once it is made or its password reset.
  const [addOpen, setAddOpen] = useState(false);
  const [draft, setDraft] = useState({ name: "", email: "", role: "viewer", password: "", sendEmail: true });
  const [creating, setCreating] = useState(false);
  const [credentials, setCredentials] = useState<Credentials | null>(null);
  // The row whose "New password" was pressed once and waits for a confirm.
  const [confirmResetId, setConfirmResetId] = useState<string | null>(null);

  const openAdd = () => {
    setDraft({ name: "", email: "", role: "viewer", password: newPassword(), sendEmail: true });
    setAddOpen(true);
  };

  async function createUser() {
    setCreating(true);
    setError(null);
    try {
      const response = await fetch("/api/admin/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(draft),
      });
      const payload = await readJson<{ user: AdminUser; password: string; emailed: boolean; emailError?: string }>(response);
      if (!response.ok || !payload.user || !payload.password) {
        throw new Error(payload.error ?? `Could not make the account (${response.status}).`);
      }
      const created = payload.user;
      setUsers((previous) => [created, ...previous]);
      setFilter("approved");
      setCredentials({ user: created, password: payload.password, emailed: Boolean(payload.emailed), emailError: payload.emailError, isNew: true });
      setAddOpen(false);
    } catch (createError) {
      setError(errorMessage(createError, "Could not make the account."));
    } finally {
      setCreating(false);
    }
  }

  async function resetPassword(target: AdminUser) {
    setConfirmResetId(null);
    setBusyId(target.id);
    setError(null);
    try {
      const response = await fetch(`/api/admin/users/${encodeURIComponent(target.id)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ resetPassword: true, password: newPassword() }),
      });
      const payload = await readJson<{ user: AdminUser; password: string }>(response);
      if (!response.ok || !payload.user || !payload.password) {
        throw new Error(payload.error ?? `Could not set a new password (${response.status}).`);
      }
      const updated = payload.user;
      setUsers((previous) => previous.map((row) => (row.id === updated.id ? updated : row)));
      setCredentials({ user: updated, password: payload.password, emailed: false, isNew: false });
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (resetError) {
      setError(errorMessage(resetError, "Could not set a new password."));
    } finally {
      setBusyId(null);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      try {
        const response = await fetch("/api/admin/users");
        const payload = await readJson<{ users: AdminUser[]; roles: AdminRole[] }>(response);
        if (!response.ok) {
          throw new Error(payload.error ?? `Could not load users (${response.status}).`);
        }
        if (cancelled) return;
        setUsers(payload.users ?? []);
        setRoles(payload.roles ?? []);
        setError(null);
      } catch (loadError) {
        if (cancelled) return;
        setError(errorMessage(loadError, "Could not load users."));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const roleLabels = useMemo(() => {
    const map = new Map<string, string>();
    for (const role of roles) map.set(role.name, role.label);
    return map;
  }, [roles]);

  const counts = useMemo(() => {
    const totals: Record<StatusFilter, number> = {
      all: users.length,
      pending: 0,
      approved: 0,
      rejected: 0,
    };
    for (const user of users) totals[user.status] += 1;
    return totals;
  }, [users]);

  const visibleUsers = useMemo(
    () => (filter === "all" ? users : users.filter((user) => user.status === filter)),
    [filter, users]
  );

  const patchUser = useCallback(async (id: string, patch: UserPatch) => {
    setBusyId(id);
    setError(null);
    try {
      const response = await fetch(`/api/admin/users/${encodeURIComponent(id)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });
      const payload = await readJson<{ user: AdminUser }>(response);
      if (!response.ok || !payload.user) {
        throw new Error(payload.error ?? `Could not update the account (${response.status}).`);
      }
      const updated = payload.user;
      setUsers((previous) => previous.map((row) => (row.id === updated.id ? updated : row)));
    } catch (patchError) {
      setError(errorMessage(patchError, "Could not update the account."));
    } finally {
      setBusyId(null);
    }
  }, []);

  return (
    <div className="adminShell">
      <Head>
        <title>Users</title>
      </Head>
      <div className="adminFrame">
        <header className="adminHeader">
          <div className="adminHeaderText">
            <h1 className="adminTitle">User access</h1>
            <p className="adminSubtitle">Make accounts, share sign-in details, approve requests and set what each user can do.</p>
            <div className="adminIdentity">
              <span className="adminIdentityEmail">{currentUser.email}</span>
              <span className="adminIdentityRole">{currentUser.roleLabel}</span>
            </div>
          </div>
          <nav className="adminNav">
            {canManageRoles ? <Link href="/admin/roles">Roles</Link> : null}
            <Link href="/library">Back to library</Link>
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

        {credentials ? (
          <CredentialsCard
            credentials={credentials}
            roleLabel={roleLabels.get(credentials.user.role) ?? credentials.user.role}
            onDone={() => setCredentials(null)}
          />
        ) : null}

        {canManageRoles && addOpen ? (
          <section className="adminPanel">
            <div className="adminPanelHeader">
              <span>Add a user</span>
              <strong>Approved at once</strong>
            </div>
            <form
              className="adminEditGrid"
              onSubmit={(event) => {
                event.preventDefault();
                void createUser();
              }}
            >
              <div className="adminFieldGrid">
                <label className="adminField">
                  Name
                  <input
                    className="adminInput"
                    value={draft.name}
                    placeholder="Full name"
                    autoComplete="off"
                    disabled={creating}
                    onChange={(event) => setDraft((d) => ({ ...d, name: event.target.value }))}
                  />
                </label>
                <label className="adminField">
                  Email
                  <input
                    className="adminInput"
                    type="email"
                    inputMode="email"
                    autoCapitalize="off"
                    autoComplete="off"
                    required
                    value={draft.email}
                    placeholder="name@example.com"
                    disabled={creating}
                    onChange={(event) => setDraft((d) => ({ ...d, email: event.target.value }))}
                  />
                </label>
                <label className="adminField">
                  Role
                  <select
                    className="adminInput"
                    value={draft.role}
                    disabled={creating}
                    onChange={(event) => setDraft((d) => ({ ...d, role: event.target.value }))}
                  >
                    {roles.map((role) => (
                      <option key={role.name} value={role.name}>
                        {role.label}
                      </option>
                    ))}
                  </select>
                  <span className="adminFieldHint">{roles.find((r) => r.name === draft.role)?.description ?? ""}</span>
                </label>
                <label className="adminField">
                  Password
                  <span className="adminPasswordRow">
                    <input
                      className="adminInput adminMono"
                      value={draft.password}
                      minLength={8}
                      required
                      autoComplete="new-password"
                      autoCapitalize="off"
                      spellCheck={false}
                      disabled={creating}
                      onChange={(event) => setDraft((d) => ({ ...d, password: event.target.value }))}
                    />
                    <button type="button" className="adminGhostButton" disabled={creating} onClick={() => setDraft((d) => ({ ...d, password: newPassword() }))}>
                      New
                    </button>
                  </span>
                  <span className="adminFieldHint">At least 8 characters. Shown once after the account is made.</span>
                </label>
              </div>
              <label className="adminCheck">
                <input type="checkbox" checked={draft.sendEmail} disabled={creating} onChange={(event) => setDraft((d) => ({ ...d, sendEmail: event.target.checked }))} />
                Also email the sign-in details to them
              </label>
              <div className="adminFormActions">
                <button type="submit" className="adminPrimaryButton" disabled={creating}>
                  {creating ? "Making account…" : "Make account"}
                </button>
                <button type="button" className="adminGhostButton" disabled={creating} onClick={() => setAddOpen(false)}>
                  Cancel
                </button>
              </div>
            </form>
          </section>
        ) : null}

        <section className="adminPanel">
          <div className="adminPanelHeader">
            <span>Accounts</span>
            <strong>{users.length} total</strong>
            {canManageRoles && !addOpen ? (
              <button type="button" className="adminPrimaryButton" onClick={openAdd}>
                Add user
              </button>
            ) : null}
          </div>

          <div className="adminTabs">
            {FILTERS.map((option) => (
              <button
                key={option.key}
                type="button"
                className={filter === option.key ? "adminTab isActive" : "adminTab"}
                aria-pressed={filter === option.key}
                onClick={() => setFilter(option.key)}
              >
                {option.label}
                <span className="adminTabCount">{counts[option.key]}</span>
              </button>
            ))}
          </div>

          {loading ? (
            <div className="adminLoading">Loading accounts…</div>
          ) : visibleUsers.length === 0 ? (
            <div className="adminEmpty">{EMPTY_MESSAGE[filter]}</div>
          ) : (
            <div className="adminTableWrap">
              <table className="adminTable">
                <thead>
                  <tr>
                    <th scope="col">User</th>
                    <th scope="col">Role</th>
                    <th scope="col">Status</th>
                    <th scope="col">Requested</th>
                    <th scope="col">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleUsers.map((user) => {
                    const busy = busyId === user.id;
                    const roleLabel = roleLabels.get(user.role) ?? user.role;

                    return (
                      <tr key={user.id} className={busy ? "isBusy" : undefined}>
                        <td data-label="User">
                          <div className="adminUserCell">
                            {user.image ? (
                              /* eslint-disable-next-line @next/next/no-img-element */
                              <img
                                className="adminAvatar"
                                src={user.image}
                                alt=""
                                width={34}
                                height={34}
                              />
                            ) : (
                              <span className="adminAvatarFallback" aria-hidden="true">
                                {initialFor(user)}
                              </span>
                            )}
                            <span className="adminUserText">
                              <span className="adminUserName">{user.name?.trim() || user.email}</span>
                              <span className="adminUserEmail">{user.email}</span>
                            </span>
                          </div>
                        </td>
                        <td data-label="Role">
                          {canManageRoles ? (
                            <select
                              className="adminSelect"
                              aria-label={`Role for ${user.email}`}
                              value={user.role}
                              disabled={busy}
                              onChange={(event) => {
                                const next = event.target.value;
                                if (next === user.role) return;
                                void patchUser(user.id, { role: next });
                              }}
                            >
                              {roles.some((role) => role.name === user.role) ? null : (
                                <option value={user.role}>{roleLabel}</option>
                              )}
                              {roles.map((role) => (
                                <option key={role.name} value={role.name}>
                                  {role.label}
                                </option>
                              ))}
                            </select>
                          ) : (
                            <span className="adminRoleText">{roleLabel}</span>
                          )}
                        </td>
                        <td data-label="Status">
                          <span className={STATUS_BADGE[user.status]}>{STATUS_LABEL[user.status]}</span>
                        </td>
                        <td data-label="Requested">
                          <span className="adminWhen">{formatWhen(user.requested_at)}</span>
                        </td>
                        <td data-label="Actions">
                          <div className="adminRowActions">
                            {user.status !== "approved" ? (
                              <button
                                type="button"
                                className="adminPrimaryButton"
                                disabled={busy}
                                onClick={() => void patchUser(user.id, { status: "approved" })}
                              >
                                Approve
                              </button>
                            ) : null}
                            {user.status === "pending" ? (
                              <button
                                type="button"
                                className="adminDangerButton"
                                disabled={busy}
                                onClick={() => void patchUser(user.id, { status: "rejected" })}
                              >
                                Reject
                              </button>
                            ) : null}
                            {canManageRoles && user.status === "approved" ? (
                              confirmResetId === user.id ? (
                                <>
                                  <button type="button" className="adminPrimaryButton" disabled={busy} onClick={() => void resetPassword(user)}>
                                    Set new password
                                  </button>
                                  <button type="button" className="adminGhostButton" disabled={busy} onClick={() => setConfirmResetId(null)}>
                                    Keep
                                  </button>
                                </>
                              ) : (
                                <button type="button" className="adminGhostButton" disabled={busy} onClick={() => setConfirmResetId(user.id)}>
                                  New password
                                </button>
                              )
                            ) : null}
                            {/* Revoking your own access would lock you out of this page. */}
                            {user.status === "approved" && user.id !== currentUser.id ? (
                              <button
                                type="button"
                                className="adminDangerButton"
                                disabled={busy}
                                onClick={() => void patchUser(user.id, { status: "rejected" })}
                              >
                                Revoke
                              </button>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

export const getServerSideProps: GetServerSideProps<UsersPageProps> = async (ctx) => {
  const session = await getServerSession(ctx.req, ctx.res, authOptions);

  if (!session?.user || session.user.status !== "approved") {
    return {
      redirect: {
        destination: `/auth/signin?callbackUrl=${encodeURIComponent(PAGE_PATH)}`,
        permanent: false,
      },
    };
  }

  if (!session.user.permissions.includes(PERMISSIONS.usersManage)) {
    return { redirect: { destination: "/", permanent: false } };
  }

  return { props: { currentUser: session.user } };
};
