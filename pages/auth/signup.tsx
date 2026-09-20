import Head from "next/head";
import Link from "next/link";
import { useState, type FormEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";

const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const MIN_PASSWORD_LENGTH = 4;

type SignUpResponse = {
  ok?: boolean;
  status?: string;
  error?: string;
};

type SignUpPageProps = Record<string, never>;

/** Returns the first problem with the form, or "" when everything looks valid. */
function validate(
  name: string,
  email: string,
  password: string,
  confirmPassword: string
): string {
  if (!name) return "Enter your full name.";
  if (!email) return "Enter your email address.";
  if (!EMAIL_PATTERN.test(email)) return "Enter a valid email address.";
  if (!password) return "Enter a password.";
  if (password.length < MIN_PASSWORD_LENGTH) {
    return `Your password must be at least ${MIN_PASSWORD_LENGTH} characters.`;
  }
  if (!confirmPassword) return "Confirm your password.";
  if (password !== confirmPassword) return "Both passwords must match.";
  return "";
}

export default function SignUpPage() {
  const [name, setName] = useState<string>("");
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [submitted, setSubmitted] = useState<boolean>(false);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    if (submitting) return;

    const trimmedName = name.trim();
    const trimmedEmail = email.trim().toLowerCase();

    const problem = validate(trimmedName, trimmedEmail, password, confirmPassword);
    if (problem) {
      setError(problem);
      return;
    }

    setError("");
    setSubmitting(true);

    try {
      const response = await fetch("/api/auth/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: trimmedName,
          email: trimmedEmail,
          password,
        }),
      });

      const payload: SignUpResponse | null = await response
        .json()
        .then((value: unknown) => value as SignUpResponse)
        .catch(() => null);

      if (!response.ok || !payload?.ok) {
        setError(
          payload?.error ?? "We could not submit your request. Please try again."
        );
        setSubmitting(false);
        return;
      }

      setSubmitted(true);
      setSubmitting(false);
    } catch {
      setError("We could not reach the server. Check your connection and try again.");
      setSubmitting(false);
    }
  };

  return (
    <>
      <Head>
        <title>Request an account · Shasan Library</title>
      </Head>

      <main className="authShell">
        <div className="authCard">
          {submitted ? (
            <>
              <div className="authHeader">
                <h1 className="authTitle">Request submitted</h1>
                <p className="authSubtitle">Your account is awaiting approval</p>
              </div>

              <div className="authNotice" role="status">
                We have created your account and sent it to the administrators.
              </div>

              <p className="authBody">
                An administrator has to approve your account before you can sign in.
                Once that happens, you can sign in with the email and password you
                just chose.
              </p>

              <div className="authFoot">
                <Link className="authFootLink" href="/auth/signin">
                  Back to sign in
                </Link>
              </div>
            </>
          ) : (
            <>
              <div className="authHeader">
                <h1 className="authTitle">Request access</h1>
                <p className="authSubtitle">
                  Create an account for the Shasan Library
                </p>
              </div>

              {error ? (
                <div className="authError" role="alert">
                  {error}
                </div>
              ) : null}

              <form className="authForm" onSubmit={handleSubmit} noValidate>
                <label className="authFieldLabel" htmlFor="signup-name">
                  Full name
                  <input
                    id="signup-name"
                    className="authInput"
                    type="text"
                    name="name"
                    value={name}
                    onChange={(event) => setName(event.target.value)}
                    autoComplete="name"
                    placeholder="Your name"
                    disabled={submitting}
                    required
                  />
                </label>

                <label className="authFieldLabel" htmlFor="signup-email">
                  Email
                  <input
                    id="signup-email"
                    className="authInput"
                    type="email"
                    name="email"
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    autoComplete="email"
                    placeholder="you@example.com"
                    disabled={submitting}
                    required
                  />
                </label>

                <label className="authFieldLabel" htmlFor="signup-password">
                  Password
                  <input
                    id="signup-password"
                    className="authInput"
                    type="password"
                    name="password"
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    autoComplete="new-password"
                    placeholder={`At least ${MIN_PASSWORD_LENGTH} characters`}
                    disabled={submitting}
                    required
                  />
                </label>

                <label className="authFieldLabel" htmlFor="signup-confirm">
                  Confirm password
                  <input
                    id="signup-confirm"
                    className="authInput"
                    type="password"
                    name="confirmPassword"
                    value={confirmPassword}
                    onChange={(event) => setConfirmPassword(event.target.value)}
                    autoComplete="new-password"
                    placeholder="Repeat your password"
                    disabled={submitting}
                    required
                  />
                </label>

                <button
                  type="submit"
                  className="authPrimaryButton"
                  disabled={submitting}
                  aria-busy={submitting}
                >
                  {submitting ? "Submitting…" : "Request an account"}
                </button>

                <p className="authHint">
                  An administrator reviews every request before access is granted.
                </p>
              </form>

              <div className="authFoot">
                <span>Already have an account?</span>
                <Link className="authFootLink" href="/auth/signin">
                  Sign in
                </Link>
              </div>
            </>
          )}
        </div>
      </main>
    </>
  );
}

export const getServerSideProps: GetServerSideProps<SignUpPageProps> = async (context) => {
  const session = await getServerSession(context.req, context.res, authOptions);

  if (session?.user?.status === "approved") {
    return { redirect: { destination: "/", permanent: false } };
  }

  return { props: {} };
};
