import Head from "next/head";
import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useState, type FormEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { signIn } from "next-auth/react";
import { authOptions, googleSignInEnabled } from "@/lib/auth-options";

/**
 * The codes mirror SIGNIN_ERRORS in @/lib/auth-options. They are repeated here
 * rather than imported because that module pulls in server-only code, and this
 * mapping runs in the browser.
 */
const ERROR_MESSAGES: Record<string, string> = {
  PENDING_APPROVAL:
    "Your account is waiting for approval. You'll get access once an administrator approves it.",
  ACCESS_REJECTED:
    "Your access request was declined. Contact an administrator if you think this is a mistake.",
  MISSING_CREDENTIALS: "Enter both your email and password.",
  NO_EMAIL: "That Google account has no email address attached.",
};

const FALLBACK_ERROR =
  "Incorrect email or password, or your access has not been approved yet.";

function messageForError(code: string): string {
  return ERROR_MESSAGES[code] ?? FALLBACK_ERROR;
}

function firstQueryValue(value: string | string[] | undefined): string {
  if (Array.isArray(value)) return value[0] ?? "";
  return value ?? "";
}

/** Only same-origin paths are followed, so a crafted ?callbackUrl cannot redirect off-site. */
function safeCallbackUrl(value: string | string[] | undefined): string {
  const raw = firstQueryValue(value);
  if (raw.startsWith("/") && !raw.startsWith("//")) return raw;
  return "/";
}

type SignInPageProps = {
  googleEnabled: boolean;
};

function GoogleLogo() {
  return (
    <svg className="authGoogleIcon" viewBox="0 0 18 18" aria-hidden="true" focusable="false">
      <path
        fill="#4285F4"
        d="M17.64 9.2045c0-.6381-.0573-1.2518-.1636-1.8409H9v3.4814h4.8436c-.2086 1.125-.8427 2.0782-1.7959 2.7164v2.2581h2.9087c1.7018-1.5668 2.6836-3.874 2.6836-6.615z"
      />
      <path
        fill="#34A853"
        d="M9 18c2.43 0 4.4673-.806 5.9564-2.1805l-2.9087-2.2581c-.8059.54-1.8368.859-3.0477.859-2.344 0-4.3282-1.5831-5.036-3.7104H.9574v2.3318C2.4382 15.9832 5.4818 18 9 18z"
      />
      <path
        fill="#FBBC05"
        d="M3.964 10.71c-.18-.54-.2822-1.1168-.2822-1.71s.1023-1.17.2823-1.71V4.9582H.9573A8.9965 8.9965 0 0 0 0 9c0 1.4523.3477 2.8268.9573 4.0418L3.964 10.71z"
      />
      <path
        fill="#EA4335"
        d="M9 3.5795c1.3214 0 2.5077.4541 3.4405 1.346l2.5813-2.5814C13.4632.8918 11.426 0 9 0 5.4818 0 2.4382 2.0168.9573 4.9582L3.964 7.29C4.6718 5.1627 6.656 3.5795 9 3.5795z"
      />
    </svg>
  );
}

export default function SignInPage({ googleEnabled }: SignInPageProps) {
  const router = useRouter();
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [submitting, setSubmitting] = useState<boolean>(false);

  // A failed Google sign-in comes back to this page as ?error=<code>.
  useEffect(() => {
    if (!router.isReady) return;
    const code = firstQueryValue(router.query.error);
    if (code) setError(messageForError(code));
  }, [router.isReady, router.query.error]);

  const handleGoogleSignIn = (): void => {
    setError("");
    setSubmitting(true);
    void signIn("google", { callbackUrl: "/" });
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    if (submitting) return;

    setError("");
    setSubmitting(true);

    try {
      const result = await signIn("credentials", {
        email: email.trim(),
        password,
        redirect: false,
      });

      if (result?.error) {
        setError(messageForError(result.error));
        setSubmitting(false);
        return;
      }

      if (result?.ok) {
        // Stay disabled through the navigation so the form cannot be resubmitted.
        await router.push(safeCallbackUrl(router.query.callbackUrl));
        return;
      }

      setError(FALLBACK_ERROR);
      setSubmitting(false);
    } catch {
      setError("Something went wrong while signing in. Please try again.");
      setSubmitting(false);
    }
  };

  return (
    <>
      <Head>
        <title>Sign in · Shasan Library</title>
      </Head>

      <main className="authShell">
        <div className="authCard">
          <div className="authHeader">
            <h1 className="authTitle">Shasan Library</h1>
            <p className="authSubtitle">Sign in to continue</p>
          </div>

          {error ? (
            <div className="authError" role="alert">
              {error}
            </div>
          ) : null}

          {googleEnabled ? (
            <>
              <button
                type="button"
                className="authGoogleButton"
                onClick={handleGoogleSignIn}
                disabled={submitting}
              >
                <GoogleLogo />
                <span>Continue with Google</span>
              </button>

              <div className="authDivider">
                <span>or</span>
              </div>
            </>
          ) : null}

          <form className="authForm" onSubmit={handleSubmit} noValidate>
            <label className="authFieldLabel" htmlFor="auth-email">
              Email
              <input
                id="auth-email"
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

            <label className="authFieldLabel" htmlFor="auth-password">
              Password
              <input
                id="auth-password"
                className="authInput"
                type="password"
                name="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                autoComplete="current-password"
                placeholder="Your password"
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
              {submitting ? "Signing in…" : "Sign in"}
            </button>
          </form>

          <div className="authFoot">
            <span>Need access?</span>
            <Link className="authFootLink" href="/auth/signup">
              Request an account
            </Link>
          </div>
        </div>
      </main>
    </>
  );
}

export const getServerSideProps: GetServerSideProps<SignInPageProps> = async (context) => {
  const session = await getServerSession(context.req, context.res, authOptions);

  if (session?.user?.status === "approved") {
    return { redirect: { destination: "/", permanent: false } };
  }

  return { props: { googleEnabled: googleSignInEnabled } };
};
