// Minimal client for Sarvam's Doc AI digitise flow:
//   POST /doc-ai/v1/job/digitise -> poll /status -> GET /download-url -> fetch ZIP
import { readFile } from "node:fs/promises";
import path from "node:path";

const BASE = "https://api.sarvam.ai/doc-ai/v1";

export class SarvamError extends Error {
  constructor(message, status, body) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

function key() {
  const k = process.env.SARVAM_API_KEY;
  if (!k) throw new Error("Missing SARVAM_API_KEY");
  return k;
}

async function readBody(res) {
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export async function submitDigitise(pdfPath, { language, outputFormat = "html", contentType, model } = {}) {
  const form = new FormData();
  const bytes = await readFile(pdfPath);
  form.append("file", new Blob([bytes], { type: "application/pdf" }), path.basename(pdfPath));
  form.append("output_format", outputFormat);
  if (language) form.append("language", language);
  if (contentType) form.append("content_type", contentType);
  if (model) form.append("model", model);

  const res = await fetch(`${BASE}/job/digitise`, {
    method: "POST",
    headers: { "api-subscription-key": key() },
    body: form,
  });

  const body = await readBody(res);
  if (!res.ok) throw new SarvamError(`digitise failed (${res.status})`, res.status, body);
  return body;
}

export async function getStatus(jobId) {
  const res = await fetch(`${BASE}/job/${jobId}/status`, {
    headers: { "api-subscription-key": key() },
  });
  const body = await readBody(res);
  if (!res.ok) throw new SarvamError(`status failed (${res.status})`, res.status, body);
  return body;
}

export async function getDownloadUrl(jobId) {
  const res = await fetch(`${BASE}/job/${jobId}/download-url`, {
    headers: { "api-subscription-key": key() },
  });
  const body = await readBody(res);
  if (!res.ok) throw new SarvamError(`download-url failed (${res.status})`, res.status, body);
  return body;
}

const TERMINAL = new Set(["completed", "partially_completed", "failed", "rejected"]);

export async function waitForJob(jobId, { intervalMs = 5000, timeoutMs = 20 * 60_000, onTick } = {}) {
  const started = Date.now();
  for (;;) {
    const status = await getStatus(jobId);
    const state = String(status.status ?? status.job_status ?? "").toLowerCase();
    if (onTick) onTick(state, status);
    if (TERMINAL.has(state)) return { state, status };
    if (Date.now() - started > timeoutMs) throw new Error(`job ${jobId} timed out in state "${state}"`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
