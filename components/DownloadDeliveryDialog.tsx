import { useEffect, useId, useState } from "react";

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
    <div className="downloadDeliveryOverlay" role="dialog" aria-modal="true" aria-labelledby="download-delivery-title">
      <div className="downloadDeliveryPanel">
        <header className="downloadDeliveryHeader">
          <div>
            <h2 id="download-delivery-title">{title}</h2>
            <p>{fileLabel}</p>
          </div>
          <button type="button" onClick={onClose} disabled={busy}>
            Close
          </button>
        </header>

        <div className="downloadDeliveryNotice">
          If the generated file is more than 15 MB, download it on this device. Email sharing is recommended for smaller files.
        </div>

        {(error || localError) ? <div className="downloadDeliveryError" role="alert">{error || localError}</div> : null}

        <div className="downloadDeliveryActions">
          <button type="button" onClick={onDownload} disabled={busy}>
            {busy ? (
              <span className="buttonSpinnerLabel">
                <span className="loadingSpinner" aria-hidden="true" />
                Preparing
              </span>
            ) : (
              "Download on device"
            )}
          </button>
        </div>

        <div className="downloadDeliveryEmailBox">
          <label htmlFor={inputId}>Email recipient</label>
          <div className="downloadDeliveryEmailRow">
            <input
              id={inputId}
              type="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              list={listId}
              placeholder={loadingEmails ? "Loading saved emails" : "name@example.com"}
              disabled={busy}
            />
            <datalist id={listId}>
              {savedEmails.map((savedEmail) => (
                <option key={savedEmail} value={savedEmail} />
              ))}
            </datalist>
            <button type="button" onClick={submitEmail} disabled={busy}>
              {busy ? "Sending" : "Email file"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
