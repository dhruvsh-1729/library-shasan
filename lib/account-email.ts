import type { NextApiRequest } from "next";
import { accountMessage } from "@/lib/account-message";
import type { AppUser } from "@/lib/auth-users";
import { escapeEmailHtml, sendPlainEmail } from "@/lib/download-email";

// Emailing a user their sign-in details (server only).

/** The address users sign in at, from the request (or NEXTAUTH_URL). */
function signInUrl(req: NextApiRequest) {
  const base = process.env.NEXTAUTH_URL?.trim() || `https://${req.headers["x-forwarded-host"] ?? req.headers.host ?? ""}`;
  return `${base.replace(/\/+$/, "")}/auth/signin`;
}

/** Emails the sign-in details; a failure is reported, never fatal (the details are still shown to share). */
export async function emailAccount(user: AppUser, password: string, req: NextApiRequest, roleLabel: string, reset = false) {
  const plain = accountMessage({ name: user.name, email: user.email, password, signInUrl: signInUrl(req), roleLabel, reset });
  try {
    await sendPlainEmail({
      to: user.email,
      subject: "Your Granth Library account",
      plain,
      html: plain.split("\n").map((line) => (line ? `<p style="margin:0 0 6px">${escapeEmailHtml(line)}</p>` : "<br>")).join(""),
    });
    return { emailed: true };
  } catch (error) {
    console.error("[api/admin/users] account email failed", error);
    return { emailed: false, emailError: error instanceof Error ? error.message : "The email could not be sent." };
  }
}

