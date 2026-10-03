// The sign-in details an admin gives a new user (or one whose password was
// reset): the same words on WhatsApp, copied, or emailed.

export function accountMessage(input: { name?: string | null; email: string; password: string; signInUrl: string; roleLabel?: string; reset?: boolean }) {
  const hello = input.name?.trim() ? `Jai Jinendra ${input.name.trim()},` : "Jai Jinendra,";
  return [
    hello,
    "",
    input.reset ? "Your Granth Library password has been changed." : "Your Granth Library account is ready.",
    "",
    `Open: ${input.signInUrl}`,
    `Email: ${input.email}`,
    `Password: ${input.password}`,
    ...(input.roleLabel ? [`Access: ${input.roleLabel}`] : []),
    "",
    "Sign in with this email and password. Please keep the password to yourself.",
  ].join("\n");
}
