// Tests call API handlers directly; this stands in for the session check.
import type { NextApiHandler } from "next";
export type AuthedApiHandler = (req: any, res: any, user: any) => unknown;
export function protectApi(handler: AuthedApiHandler): NextApiHandler {
  return (req, res) => handler(req, res, { id: "test", permissions: [] }) as any;
}
export async function requireUser() {
  return { id: "test", permissions: [] };
}
