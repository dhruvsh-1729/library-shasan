// Sets a new password for a library login, from the terminal:
//
//   npm run reset-password                      → ms@gg.com, a new random password
//   npm run reset-password -- ms@gg.com          → the same
//   npm run reset-password -- ms@gg.com MyPass99 → that password (8+ characters)
//
// The new password is printed once, with the sign-in link, ready to send on.
// (The same is one click on the site: Library › Users › "Shared login".)
import { randomInt } from "node:crypto";
import bcrypt from "bcryptjs";
import { createClient } from "@supabase/supabase-js";

const SHARED_LOGIN = "ms@gg.com";
// No 0/O, 1/l/I: the password is read out or copied from a phone message.
const ALPHABET = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const email = (process.argv[2] || SHARED_LOGIN).trim().toLowerCase();
const password = process.argv[3] || Array.from({ length: 10 }, () => ALPHABET[randomInt(ALPHABET.length)]).join("");
if (password.length < 8) {
  console.error("The password needs at least 8 characters.");
  process.exit(1);
}

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const { data: user, error } = await sb.from("app_users").select("id,email,providers,status").eq("email", email).maybeSingle();
if (error) throw new Error(error.message);
if (!user) {
  console.error(`No account with the email ${email}.`);
  process.exit(1);
}
const providers = (user.providers ?? []).includes("credentials") ? user.providers : [...(user.providers ?? []), "credentials"];
const { error: updateError } = await sb
  .from("app_users")
  .update({ password_hash: await bcrypt.hash(password, 10), providers, updated_at: new Date().toISOString() })
  .eq("id", user.id);
if (updateError) throw new Error(updateError.message);

console.log(`New password set for ${email}${user.status !== "approved" ? ` (the account is ${user.status})` : ""}.`);
console.log("");
console.log("Sign in: https://library.aryanculture.org/auth/signin");
console.log(`Email: ${email}`);
console.log(`Password: ${password}`);
