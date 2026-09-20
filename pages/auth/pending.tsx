import Head from "next/head";
import Link from "next/link";
import { signOut } from "next-auth/react";

export default function PendingApprovalPage() {
  const handleSignOut = (): void => {
    void signOut({ callbackUrl: "/auth/signin" });
  };

  return (
    <>
      <Head>
        <title>Awaiting approval · Shasan Library</title>
      </Head>

      <main className="authShell">
        <div className="authCard">
          <div className="authHeader">
            <h1 className="authTitle">Awaiting approval</h1>
            <p className="authSubtitle">Your account is not active yet</p>
          </div>

          <div className="authNotice" role="status">
            Your account has been created and is waiting for an administrator.
          </div>

          <p className="authBody">
            An administrator has to approve your account before you can open the
            Shasan Library. You do not need to sign up again — as soon as your
            request is approved, you can sign in with the same account.
          </p>

          <div className="authActions">
            <Link className="authPrimaryButton" href="/auth/signin">
              Back to sign in
            </Link>
          </div>

          <div className="authFoot">
            <span>Signed in with the wrong account?</span>
            <button type="button" className="authTextButton" onClick={handleSignOut}>
              Sign out
            </button>
          </div>
        </div>
      </main>
    </>
  );
}
