import type { NextApiRequest, NextApiResponse } from "next";
import {
  DownloadEmailError,
  getDownloadRecipientClientKey,
  listDownloadRecipients,
  rememberDownloadRecipient,
} from "@/lib/download-email";
import { setNoStore } from "@/lib/api-cache";

type RecipientBody = {
  email?: unknown;
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  setNoStore(res);

  try {
    const clientKey = getDownloadRecipientClientKey(req, res);

    if (req.method === "GET") {
      const emails = await listDownloadRecipients(clientKey);
      return res.status(200).json({ emails });
    }

    if (req.method === "POST") {
      const body = (req.body || {}) as RecipientBody;
      const email = await rememberDownloadRecipient(String(body.email || ""), clientKey);
      return res.status(200).json({ email });
    }

    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ error: "Method not allowed" });
  } catch (error) {
    if (error instanceof DownloadEmailError) {
      return res.status(error.status).json({ error: error.message });
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
