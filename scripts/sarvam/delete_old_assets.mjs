// Deletes superseded UploadThing objects. Run only after every database
// reference has been repointed and verified.
import { UTApi } from "uploadthing/server";

const keys = process.argv.slice(2);
if (!keys.length) {
  console.error("usage: delete_old_assets.mjs <key> [key...]");
  process.exit(1);
}

const api = new UTApi({ token: process.env.UPLOADTHING_TOKEN });

console.log("verifying each key is reachable before deleting...");
for (const key of keys) {
  const res = await fetch(`https://pk3cp5aaix.ufs.sh/f/${key}`, { method: "HEAD" });
  console.log(`  ${key.slice(0, 16)}...  http=${res.status} size=${res.headers.get("content-length") ?? "?"}`);
}

const result = await api.deleteFiles(keys);
console.log("\ndelete result:", JSON.stringify(result));

console.log("\nconfirming they are gone:");
for (const key of keys) {
  const res = await fetch(`https://pk3cp5aaix.ufs.sh/f/${key}`, { method: "HEAD" });
  console.log(`  ${key.slice(0, 16)}...  http=${res.status} ${res.status === 404 || res.status === 403 ? "(deleted)" : "(STILL PRESENT)"}`);
}
