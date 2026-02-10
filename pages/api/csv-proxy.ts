import type { NextApiRequest, NextApiResponse } from "next";

function readSingleQueryValue(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function isAllowedHost(hostname: string) {
  return (
    hostname.endsWith(".ufs.sh") ||
    hostname === "utfs.io" ||
    hostname.endsWith(".utfs.io")
  );
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  const rawUrl = readSingleQueryValue(req.query.url);
  if (!rawUrl) return res.status(400).json({ error: "Missing query param: url" });

  let parsedUrl: URL;
  try {
    parsedUrl = new URL(rawUrl);
  } catch {
    return res.status(400).json({ error: "Invalid url" });
  }

  if (!["http:", "https:"].includes(parsedUrl.protocol)) {
    return res.status(400).json({ error: "Unsupported url protocol" });
  }
  if (!isAllowedHost(parsedUrl.hostname)) {
    return res.status(400).json({ error: "URL host is not allowed" });
  }

  try {
    const upstream = await fetch(parsedUrl.toString(), {
      headers: { accept: "text/csv,text/plain;q=0.9,*/*;q=0.8" },
    });
    const body = await upstream.text();

    if (!upstream.ok) {
      return res
        .status(upstream.status)
        .json({ error: body || `Failed to fetch upstream (${upstream.status})` });
    }

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Cache-Control", "private, max-age=60");
    return res.status(200).send(body);
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return res.status(500).json({ error: message });
  }
}
