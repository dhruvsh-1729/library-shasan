// Checks against the real library data (Turso + Supabase). Run with LIVE=1.
// Each one pins a bug that was fixed, so it cannot come back unnoticed.
import "dotenv/config";
import assert from "node:assert/strict";
import { test } from "node:test";

const live = process.env.LIVE === "1";
type Handler = (req: unknown, res: unknown) => unknown;

async function call(modulePath: string, query: Record<string, string>) {
  const mod = (await import(modulePath)) as { default: Handler | { default: Handler } };
  const handler = (typeof mod.default === "function" ? mod.default : mod.default.default) as Handler;
  return new Promise<Record<string, unknown>>((resolve) => {
    const res = {
      statusCode: 200,
      setHeader() {},
      status(code: number) {
        this.statusCode = code;
        return this;
      },
      json(body: Record<string, unknown>) {
        resolve({ status: this.statusCode, ...body });
      },
    };
    handler({ method: "GET", query, headers: {}, url: `/x?${new URLSearchParams(query)}` }, res);
  });
}

test("live: the catalog has no problems", { skip: !live }, async () => {
  const { getGranthCatalog } = await import("@/lib/granth-catalog");
  const catalog = await getGranthCatalog();
  assert.deepEqual(catalog.issues, []);
  assert.ok(catalog.entries.length > 450);
});

test("live: a Devanagari-only Acharang search leaves the Gujarati anuwads out", { skip: !live }, async () => {
  const granths = (await call("@/pages/api/search-granths", {})) as { items: Array<{ custom_id: string; source_name: string; title: string; native_title: string }> };
  const ach = granths.items.filter((i) => /acharang|आचारां/i.test(i.source_name + i.title + i.native_title)).map((i) => i.custom_id);
  const result = (await call("@/pages/api/search", { q: "हिंसा", matchMode: "sanskrit_forms", granths: ach.join(","), scripts: "devanagari", limit: "20" })) as {
    total: number;
    results: Array<{ pdf_name: string }>;
  };
  assert.ok(result.total > 0);
  assert.ok(result.results.every((r) => !/gujarati anuwad/i.test(r.pdf_name)));
});

test("live: a book indexed from a spreadsheet is searchable when chosen", { skip: !live }, async () => {
  const granths = (await call("@/pages/api/search-granths", {})) as { items: Array<{ granth_key: string; custom_id: string }> };
  const id = granths.items.find((i) => i.granth_key === "284")!.custom_id;
  const result = (await call("@/pages/api/search", { q: "सूत्र", granths: id, limit: "2" })) as { total: number; results: Array<{ pdf_url: string }> };
  assert.ok(result.total > 0);
  assert.ok(result.results[0].pdf_url.startsWith("https://"));
});
