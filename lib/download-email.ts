import type { NextApiRequest, NextApiResponse } from "next";
import { createHash, randomBytes } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import nodemailer from "nodemailer";
import { getSupabaseAdmin } from "@/lib/supabase-server";

export const MAX_EMAIL_ATTACHMENT_BYTES = 15 * 1024 * 1024;
const RECIPIENT_COOKIE = "download_email_client";
const RECIPIENT_COOKIE_MAX_AGE = 60 * 60 * 24 * 365;
const MAILEROO_FROM_EMAIL = "14ef21.4106.fcc9a23f31b2c197b2f1377c82d0a1fe@a.maileroo.net";
const MAILEROO_FROM_NAME = "Granth Library";

export class DownloadEmailError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export function normalizeDownloadEmail(value: unknown) {
  const email = String(value || "").trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw new DownloadEmailError(400, "Enter a valid email address.");
  }
  return email;
}

function isMissingRecipientTable(message: string) {
  return /download_email_recipients|client_key|schema cache|relation .* does not exist/i.test(message);
}

function readCookie(req: NextApiRequest, name: string) {
  const header = req.headers.cookie || "";
  const cookies = header.split(";").map((part) => part.trim()).filter(Boolean);
  for (const cookie of cookies) {
    const index = cookie.indexOf("=");
    if (index === -1) continue;
    const key = cookie.slice(0, index);
    if (key === name) return decodeURIComponent(cookie.slice(index + 1));
  }
  return "";
}

function clientKeyForToken(token: string) {
  return createHash("sha256").update(token).digest("hex");
}

export function getDownloadRecipientClientKey(req: NextApiRequest, res: NextApiResponse) {
  let token = readCookie(req, RECIPIENT_COOKIE);
  if (!/^[a-f0-9]{32,128}$/i.test(token)) {
    token = randomBytes(24).toString("hex");
    const secure = process.env.NODE_ENV === "production" ? "; Secure" : "";
    res.setHeader(
      "Set-Cookie",
      `${RECIPIENT_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${RECIPIENT_COOKIE_MAX_AGE}${secure}`
    );
  }
  return clientKeyForToken(token);
}

export async function listDownloadRecipients(clientKey: string, limit = 50) {
  const { data, error } = await getSupabaseAdmin()
    .from("download_email_recipients")
    .select("email")
    .eq("client_key", clientKey)
    .order("last_used_at", { ascending: false, nullsFirst: false })
    .limit(Math.max(1, Math.min(Math.floor(limit), 100)));

  if (error) {
    if (isMissingRecipientTable(error.message)) return [];
    throw new DownloadEmailError(500, error.message);
  }

  return (data ?? []).map((row) => String(row.email || "")).filter(Boolean);
}

export async function rememberDownloadRecipient(email: string, clientKey: string) {
  const normalized = normalizeDownloadEmail(email);
  const { error } = await getSupabaseAdmin()
    .from("download_email_recipients")
    .upsert(
      {
        client_key: clientKey,
        email: normalized,
        last_used_at: new Date().toISOString(),
      },
      { onConflict: "client_key,email" }
    );

  if (error && !isMissingRecipientTable(error.message)) {
    throw new DownloadEmailError(500, error.message);
  }

  return normalized;
}

export async function assertEmailAttachmentSize(filePath: string) {
  const info = await stat(filePath);
  if (info.size > MAX_EMAIL_ATTACHMENT_BYTES) {
    throw new DownloadEmailError(
      413,
      `This file is ${(info.size / 1024 / 1024).toFixed(1)} MB. Email is recommended only for files up to 15 MB; download it on this device instead.`
    );
  }
  return info.size;
}

function smtpPort() {
  const parsed = Number.parseInt(process.env.MAILEROO_SMTP_PORT || "587", 10);
  return Number.isFinite(parsed) ? parsed : 587;
}

function mailFrom() {
  const from = fromAddress();
  const name = fromName();
  return `"${name.replace(/"/g, "'")}" <${from}>`;
}

function fromAddress() {
  return MAILEROO_FROM_EMAIL;
}

function fromName() {
  return MAILEROO_FROM_NAME;
}

function createMailerooTransport() {
  const user = process.env.MAILEROO_SMTP_USERNAME || process.env.MAILEROO_SMTP_USER;
  const pass = process.env.MAILEROO_SMTP_PASSWORD;
  const port = smtpPort();

  if (!user || !pass) {
    throw new DownloadEmailError(
      500,
      "Email delivery is not configured. Set MAILEROO_SMTP_USERNAME and MAILEROO_SMTP_PASSWORD."
    );
  }

  return nodemailer.createTransport({
    host: process.env.MAILEROO_SMTP_HOST || "smtp.maileroo.com",
    port,
    secure: port === 465,
    auth: { user, pass },
  });
}

async function sendViaMailerooApi(options: {
  to: string;
  filePath: string;
  filename: string;
  contentType: string;
  title: string;
}) {
  const apiKey = process.env.MAILEROO_API_KEY || process.env.MAILEROO_SENDING_KEY;
  if (!apiKey) return null;

  const file = await readFile(options.filePath);
  const response = await fetch("https://smtp.maileroo.com/api/v2/emails", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      from: {
        address: fromAddress(),
        display_name: fromName(),
      },
      to: {
        address: options.to,
      },
      subject: `Granth Library download: ${options.title}`,
      plain:
        `Attached is your Granth Library download: ${options.title}.\n\n` +
        "For larger files, downloading directly on the device is recommended.",
      html:
        `<p>Attached is your Granth Library download: <strong>${options.title}</strong>.</p>` +
        "<p>For larger files, downloading directly on the device is recommended.</p>",
      tracking: false,
      tags: {
        app: "granth-library",
        type: "download",
      },
      attachments: [
        {
          file_name: options.filename,
          content_type: options.contentType,
          content: file.toString("base64"),
          inline: false,
        },
      ],
    }),
  });

  const payload = (await response.json().catch(() => null)) as { success?: boolean; message?: string } | null;
  if (!response.ok || payload?.success === false) {
    throw new DownloadEmailError(
      response.status || 500,
      payload?.message || `Maileroo email request failed (${response.status})`
    );
  }

  return true;
}

async function sendViaSmtp(options: {
  to: string;
  filePath: string;
  filename: string;
  contentType: string;
  title: string;
}) {
  const transport = createMailerooTransport();

  await transport.sendMail({
    from: mailFrom(),
    to: options.to,
    subject: `Granth Library download: ${options.title}`,
    text:
      `Attached is your Granth Library download: ${options.title}.\n\n` +
      "For larger files, downloading directly on the device is recommended.",
    html:
      `<p>Attached is your Granth Library download: <strong>${options.title}</strong>.</p>` +
      "<p>For larger files, downloading directly on the device is recommended.</p>",
    attachments: [
      {
        filename: options.filename,
        path: options.filePath,
        contentType: options.contentType,
      },
    ],
  });
}

export async function sendDownloadEmail(options: {
  to: string;
  filePath: string;
  filename: string;
  contentType: string;
  title: string;
  recipientClientKey?: string;
}) {
  const to = normalizeDownloadEmail(options.to);
  const size = await assertEmailAttachmentSize(options.filePath);
  const sentViaApi = await sendViaMailerooApi({ ...options, to });
  if (!sentViaApi) await sendViaSmtp({ ...options, to });

  if (options.recipientClientKey) await rememberDownloadRecipient(to, options.recipientClientKey);
  return { email: to, sizeBytes: size };
}
