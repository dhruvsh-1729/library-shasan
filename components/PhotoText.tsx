import { useRef, useState } from "react";
import { readStream } from "@/lib/anuvaad/browser";
import { pageText } from "@/lib/anuvaad/pages";

// "Words from a photo" on Search and Vyutpatti (Maharaj Saheb, 15 Nov 2026:
// "the vishay words are hard and not everyone can type them; take a photo and
// have it typed in automatically, as Anuvaad already allows"). The photo is
// read by the same OCR as Anuvaad (/api/anuvaad/ocr) and kept nowhere.

const MAX_SIDE = 2400;

/**
 * Any photo the phone gives (HEIC included, where the browser can draw it) →
 * a JPEG of at most 2400 px, turned a quarter `turns` times clockwise.
 */
export async function toJpeg(file: File | Blob, turns = 0): Promise<Blob> {
  try {
    const bitmap = await createImageBitmap(file);
    const scale = Math.min(1, MAX_SIDE / Math.max(bitmap.width, bitmap.height));
    const w = Math.round(bitmap.width * scale);
    const h = Math.round(bitmap.height * scale);
    const canvas = document.createElement("canvas");
    const side = turns % 2 === 1;
    canvas.width = side ? h : w;
    canvas.height = side ? w : h;
    const ctx = canvas.getContext("2d")!;
    ctx.translate(canvas.width / 2, canvas.height / 2);
    ctx.rotate((turns * Math.PI) / 2);
    ctx.drawImage(bitmap, -w / 2, -h / 2, w, h);
    const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, "image/jpeg", 0.9));
    if (blob) return blob;
  } catch {
    // fall through: send the file as it is
  }
  if (turns === 0 && /image\/(jpeg|png)/.test(file.type)) return file;
  throw new Error("This photo could not be opened. Take it again as a JPG or PNG.");
}

async function readOnce(body: Blob, language: "hi-IN" | "gu-IN") {
  const res = await fetch(`/api/anuvaad/ocr${language === "gu-IN" ? "?language=gu-IN" : ""}`, {
    method: "POST",
    headers: { "Content-Type": body.type || "image/jpeg" },
    body,
  });
  let pages: string[] | null = null;
  let problem = "";
  await readStream(res, (event) => {
    if (event.type === "done") pages = (event.pages as string[]) ?? [];
    else if (event.type === "error") problem = String(event.error ?? "");
  });
  if (pages === null) throw new Error(problem || "The photo could not be read. Please try again.");
  return pageText((pages as string[]).join("\n\n"));
}

/**
 * The text of a photo, read as Sanskrit/Hindi (or Gujarati). A Devanagari
 * photo is read as taken and turned a quarter either way, at the same time,
 * and the reading made of kosh words is kept (Diksha, 18 Nov 2026: a list
 * photographed sideways read "लोकोत्तर" as "अकोत्तर").
 */
export async function readPhoto(file: File, language: "hi-IN" | "gu-IN" = "hi-IN") {
  if (language === "gu-IN") return readOnce(await toJpeg(file), language);
  const readings = await Promise.all([0, 1, 3].map(async (turns) => readOnce(await toJpeg(file, turns), language).catch(() => "")));
  if (!readings.some(Boolean)) return readOnce(await toJpeg(file), language);
  try {
    const res = await fetch("/api/photo-best", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ texts: readings }) });
    const json = await res.json();
    if (res.ok && Number.isInteger(json.best) && readings[json.best]) return readings[json.best];
  } catch {
    // the photo as taken, if the choice cannot be made
  }
  return readings.find(Boolean)!;
}

/** The distinct Indic words of a text, in order: what the reader picks from. */
export function photoWords(text: string) {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of text.split(/[\s,;:।॥.()[\]{}"'“”‘’!?|/\\—–-]+/)) {
    const word = raw.replace(/[0-9०-९૦-૯]+/g, "").trim();
    if ([...word].length < 2 || !/[ऀ-ॿ઀-૿]/.test(word) || seen.has(word)) continue;
    seen.add(word);
    out.push(word);
  }
  return out;
}

type Props = {
  /** The text read from the photo. */
  onText: (text: string) => void;
  label?: string;
  className?: string;
  disabled?: boolean;
};

/** A button that takes or chooses a photo and hands back the text read from it. */
export function PhotoTextButton({ onText, label = "Words from a photo", className = "exGhost", disabled }: Props) {
  const picker = useRef<HTMLInputElement>(null);
  // Diksha, 24 Nov 2026: "can it have the camera directly?" — the camera opens at once, no chooser
  const camera = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function onFile(file: File) {
    setBusy(true);
    setError("");
    try {
      const text = await readPhoto(file);
      if (!photoWords(text).length) throw new Error("No words were found in this photo. Take it closer, in good light.");
      onText(text);
    } catch (e) {
      setError(e instanceof TypeError ? "Could not reach the server. Check the connection and try again." : e instanceof Error ? e.message : "The photo could not be read.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <span className="photoText">
      <input
        ref={picker}
        type="file"
        accept="image/*"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (file) void onFile(file);
        }}
      />
      <input
        ref={camera}
        type="file"
        accept="image/*"
        capture="environment"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (file) void onFile(file);
        }}
      />
      <button type="button" className={`${className} photoTextCamera`} onClick={() => camera.current?.click()} disabled={disabled || busy} title="Opens the camera">
        <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M4 8h3l2-3h6l2 3h3v11H4z" />
          <circle cx="12" cy="13" r="3.5" />
        </svg>
        Camera
      </button>
      <button type="button" className={className} onClick={() => picker.current?.click()} disabled={disabled || busy} aria-busy={busy}>
        <svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M4 8h3l2-3h6l2 3h3v11H4z" />
          <circle cx="12" cy="13" r="3.5" />
        </svg>
        {busy ? "Reading the photo…" : label}
      </button>
      {error ? (
        <span className="photoTextError" role="alert">
          {error}
        </span>
      ) : null}
    </span>
  );
}
