import type { NextApiRequest, NextApiResponse } from "next";
import { normalizeEmail } from "@/lib/auth-permissions";
import { createPendingUser, findUserByEmail } from "@/lib/auth-users";

/**
 * Public signup. Every account lands as `pending` and stays locked out until a
 * user manager approves it, so this route never returns the stored record.
 */

type SignupBody = {
  name?: unknown;
  email?: unknown;
  password?: unknown;
};

type SignupResponse = { ok: true; status: "pending" } | { error: string };

const NAME_MAX = 120;
const EMAIL_MAX = 254;
const PASSWORD_MIN = 4;
const PASSWORD_MAX = 200;

// Deliberately permissive: the approval queue is the real gate, this only
// catches obvious typos before a row is written.
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const DUPLICATE_EMAIL_PATTERN = /duplicate key|already exists|23505/i;

function readString(value: unknown) {
  return typeof value === "string" ? value : "";
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<SignupResponse>
) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const body = (req.body ?? {}) as SignupBody;

  const name = readString(body.name).trim();
  if (!name) {
    return res.status(400).json({ error: "Please enter your name." });
  }
  if (name.length > NAME_MAX) {
    return res.status(400).json({ error: `Name must be ${NAME_MAX} characters or fewer.` });
  }

  const email = normalizeEmail(body.email);
  if (!email) {
    return res.status(400).json({ error: "Please enter your email address." });
  }
  if (email.length > EMAIL_MAX || !EMAIL_PATTERN.test(email)) {
    return res.status(400).json({ error: "Please enter a valid email address." });
  }

  const password = readString(body.password);
  if (password.length < PASSWORD_MIN) {
    return res
      .status(400)
      .json({ error: `Password must be at least ${PASSWORD_MIN} characters.` });
  }
  if (password.length > PASSWORD_MAX) {
    return res
      .status(400)
      .json({ error: `Password must be ${PASSWORD_MAX} characters or fewer.` });
  }

  try {
    const existing = await findUserByEmail(email);
    if (existing) {
      return res.status(409).json({
        error: "An account with that email already exists. Try signing in instead.",
      });
    }

    await createPendingUser({ email, name, password, provider: "credentials" });

    return res.status(201).json({ ok: true, status: "pending" });
  } catch (error) {
    console.error("[api/auth/signup] failed to create account", error);

    // Two signups racing on the same address trip the unique index; that is a
    // conflict, not a server fault.
    const message = error instanceof Error ? error.message : String(error);
    if (DUPLICATE_EMAIL_PATTERN.test(message)) {
      return res.status(409).json({
        error: "An account with that email already exists. Try signing in instead.",
      });
    }

    return res
      .status(500)
      .json({ error: "Could not create your account. Please try again." });
  }
}
