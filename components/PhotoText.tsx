import { useRef, useState } from "react";
import { readStream } from "@/lib/anuvaad/browser";
import { pageText } from "@/lib/anuvaad/pages";

// "Words from a photo" on Search and Vyutpatti (Maharaj Saheb, 15 Nov 2026:
// "the vishay words are hard and not everyone can type them; take a photo and
// have it typed in automatically, as Anuvaad already allows"). The photo is
// read by the same OCR as Anuvaad (/api/anuvaad/ocr) and kept nowhere.

const MAX_SIDE = 2400;

/** Any photo the phone gives (HEIC included, where the browser can draw it) → a JPEG of at most 2400 px. */
async function toJpeg(file: File): Promise<Blob> {
  try {
    const bitmap = await createImageBitmap(file);
    const scale = Math.min(1, MAX_SIDE / Math.max(bitmap.width, bitmap.height));
    const canvas = document.createElement("canvas");
    canvas.width = Math.round(bitmap.width * scale);
    canvas.height = Math.round(bitmap.height * scale);
    canvas.getContext("2d")!.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
    const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, "image/jpeg", 0.9));
    if (blob) return blob;
  } catch {
    // fall through: send the file as it is
  }
  if (/image\/(jpeg|png)/.test(file.type)) return file;
  throw new Error("This photo could not be opened. Take it again as a JPG or PNG.");
}

/** The text of a photo, read as Sanskrit/Hindi (or Gujarati). */
export async function readPhoto(file: File, language: "hi-IN" | "gu-IN" = "hi-IN") {
  const body = await toJpeg(file);
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
