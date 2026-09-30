import { createClient, type Client, type InStatement } from "@libsql/client";

let tursoClient: Client | null = null;

// A dropped connection ("socket hang up", a reset, a gateway error) is usually
// gone a moment later. Reads are retried a couple of times so one blip does
// not fail a search; writes are not, since a write whose reply was lost may
// already have been applied.
const TRANSIENT = /socket hang up|ECONNRESET|ETIMEDOUT|EAI_AGAIN|ECONNREFUSED|fetch failed|network|502|503|504/i;
const RETRY_DELAYS_MS = [250, 900];

function sqlOf(statement: InStatement) {
  return typeof statement === "string" ? statement : statement.sql;
}

function isRead(statement: InStatement) {
  return /^\s*(select|with|pragma|explain)\b/i.test(sqlOf(statement));
}

function withReadRetries(client: Client): Client {
  const execute = client.execute.bind(client) as (...args: unknown[]) => ReturnType<Client["execute"]>;
  const retrying = async (...args: unknown[]) => {
    const statement = args[0] as InStatement;
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await execute(...args);
      } catch (error) {
        const message = error instanceof Error ? `${error.message} ${String((error as { cause?: unknown }).cause ?? "")}` : String(error);
        if (attempt >= RETRY_DELAYS_MS.length || !isRead(statement) || !TRANSIENT.test(message)) throw error;
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAYS_MS[attempt]));
      }
    }
  };
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "execute") return retrying;
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

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

  tursoClient = withReadRetries(createClient({ url, authToken }));
  return tursoClient;
}
