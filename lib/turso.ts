import { createClient, type Client } from "@libsql/client";

let tursoClient: Client | null = null;

export function getTursoClient() {
  if (tursoClient) return tursoClient;

  const url = process.env.TURSO_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;

  if (!url) {
    throw new Error("Missing TURSO_URL");
  }
  if (!authToken) {
    throw new Error("Missing TURSO_AUTH_TOKEN");
  }

  tursoClient = createClient({ url, authToken });
  return tursoClient;
}
