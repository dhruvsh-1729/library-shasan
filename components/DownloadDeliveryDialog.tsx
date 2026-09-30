import { useEffect, useId, useState } from "react";
import { LightTableIcon } from "@/components/LightTableIcon";
import { Sheet } from "@/components/Sheet";

export type DeliveryMode = "download" | "email";

type DownloadDeliveryDialogProps = {
  open: boolean;
  title: string;
  fileLabel: string;
  busy?: boolean;
  error?: string | null;
  onClose: () => void;
  onDownload: () => void;
  onEmail: (email: string) => void;
};

export function DownloadDeliveryDialog({
  open,
  title,
  fileLabel,
  busy = false,
  error = null,
  onClose,
  onDownload,
  onEmail,
}: DownloadDeliveryDialogProps) {
  const inputId = useId();
  const listId = useId();
  const [email, setEmail] = useState("");
  const [savedEmails, setSavedEmails] = useState<string[]>([]);
  const [loadingEmails, setLoadingEmails] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    let active = true;
    setLocalError(null);
    setLoadingEmails(true);

    void (async () => {
      try {
        const res = await fetch("/api/download-email-recipients");
        const json = (await res.json()) as { emails?: string[] };
        if (active) setSavedEmails(Array.isArray(json.emails) ? json.emails : []);
      } catch {
        if (active) setSavedEmails([]);
      } finally {
        if (active) setLoadingEmails(false);
      }
    })();

    return () => {
      active = false;
    };
  }, [open]);

  if (!open) return null;

  function submitEmail() {
    const value = email.trim();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
      setLocalError("Enter a valid email address.");
      return;
    }
    setLocalError(null);
    onEmail(value);
  }

  return (
    <Sheet open title={title} subtitle={fileLabel} onClose={onClose} busy={busy} size="narrow" layer={1}>
      {(error || localError) ? <p className="sheetNote is-error" role="alert">{error || localError}</p> : null}

      <button type="button" className="sheetAction is-primary" onClick={onDownload} disabled={busy}>
        {busy ? (
          <>
            <span className="sheetSpinner" aria-hidden="true" />
            Preparing the file
          </>
        ) : (
          <>
            <LightTableIcon name="export" size={18} /> Download to this device
          </>
        )}
      </button>

      <div className="sheetOr">or email it</div>

      <form
        className="sheetField"
        onSubmit={(event) => {
          event.preventDefault();
          submitEmail();
        }}
      >
        <label htmlFor={inputId}>Email address</label>
        <input
          id={inputId}
          type="email"
          inputMode="email"
          autoComplete="email"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          list={listId}
          placeholder={loadingEmails ? "Loading saved addresses" : "name@example.com"}
          disabled={busy}
        />
        <datalist id={listId}>
          {savedEmails.map((savedEmail) => (
            <option key={savedEmail} value={savedEmail} />
          ))}
        </datalist>
        <button type="submit" className="sheetAction is-secondary" disabled={busy}>
          <LightTableIcon name="mail" size={18} /> {busy ? "Sending" : "Send by email"}
        </button>
      </form>

      <p className="sheetSmall">Files over 15 MB are too big to email; download those instead.</p>
    </Sheet>
  );
}
