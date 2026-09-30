import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { LightTableIcon } from "@/components/LightTableIcon";

/** Open sheets, innermost last: Escape closes only the one on top. */
const openSheets: symbol[] = [];

/**
 * The one dialog shell: a drawer that rises from the bottom on a phone and a
 * centred panel on a wider screen. The header and footer stay put and only
 * the middle scrolls, so the actions are always in reach of a thumb.
 */
export function Sheet({
  open,
  title,
  subtitle,
  onClose,
  busy = false,
  footer,
  size = "wide",
  layer = 0,
  children,
}: {
  open: boolean;
  title: ReactNode;
  subtitle?: ReactNode;
  onClose: () => void;
  /** While true the sheet cannot be dismissed (a file is being built). */
  busy?: boolean;
  footer?: ReactNode;
  size?: "wide" | "narrow";
  /** A sheet opened from another sheet sits one layer above it. */
  layer?: number;
  children?: ReactNode;
}) {
  const titleId = useId();
  const panelRef = useRef<HTMLDivElement | null>(null);
  const busyRef = useRef(busy);
  busyRef.current = busy;
  const closeRef = useRef(onClose);
  closeRef.current = onClose;

  useEffect(() => {
    if (!open) return;
    const previous = document.activeElement as HTMLElement | null;
    panelRef.current?.focus({ preventScroll: true });
    const me = Symbol("sheet");
    openSheets.push(me);
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || openSheets[openSheets.length - 1] !== me || event.defaultPrevented) return;
      event.preventDefault();
      if (!busyRef.current) closeRef.current();
    };
    window.addEventListener("keydown", onKey);
    // The page behind stays still while the sheet is open.
    const html = document.documentElement;
    const locked = html.dataset.sheetLocks ? Number(html.dataset.sheetLocks) : 0;
    if (locked === 0) html.style.overflow = "hidden";
    html.dataset.sheetLocks = String(locked + 1);
    return () => {
      openSheets.splice(openSheets.indexOf(me), 1);
      window.removeEventListener("keydown", onKey);
      const left = Math.max(0, Number(html.dataset.sheetLocks ?? 1) - 1);
      html.dataset.sheetLocks = String(left);
      if (left === 0) html.style.overflow = "";
      previous?.focus?.({ preventScroll: true });
    };
  }, [open]);

  if (!open) return null;

  return (
    <div
      className={`sheetScrim${layer ? " isRaised" : ""}`}
      style={layer ? { zIndex: 1000 + layer * 10 } : undefined}
      onMouseDown={(event) => {
        if (event.target === event.currentTarget && !busy) onClose();
      }}
    >
      <div
        ref={panelRef}
        className={`sheet is-${size}`}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
      >
        <header className="sheetHead">
          <span className="sheetGrip" aria-hidden="true" />
          <div className="sheetTitles">
            <h2 id={titleId}>{title}</h2>
            {subtitle ? <p>{subtitle}</p> : null}
          </div>
          <button type="button" className="sheetClose" onClick={onClose} disabled={busy} aria-label="Close">
            <LightTableIcon name="close" size={18} />
          </button>
        </header>
        <div className="sheetBody">{children}</div>
        {footer ? <footer className="sheetFoot">{footer}</footer> : null}
      </div>
    </div>
  );
}

/** A number chosen with − and + buttons, big enough to tap; the field can still be typed in. */
export function Stepper({
  label,
  hint,
  value,
  min = 0,
  max,
  onChange,
}: {
  label: string;
  hint?: string;
  value: number;
  min?: number;
  max: number;
  onChange: (value: number) => void;
}) {
  const id = useId();
  const clamp = (n: number) => Math.min(max, Math.max(min, Math.floor(Number.isFinite(n) ? n : min)));
  return (
    <div className="sheetStepper">
      <label htmlFor={id}>
        <span>{label}</span>
        {hint ? <em>{hint}</em> : null}
      </label>
      <div className="sheetStepperControl">
        <button type="button" onClick={() => onChange(clamp(value - 1))} disabled={value <= min} aria-label={`Fewer: ${label}`}>
          <LightTableIcon name="minus" size={16} />
        </button>
        <input
          id={id}
          type="number"
          inputMode="numeric"
          min={min}
          max={max}
          value={value}
          onChange={(event) => onChange(event.target.value === "" ? min : clamp(Number(event.target.value)))}
        />
        <button type="button" onClick={() => onChange(clamp(value + 1))} disabled={value >= max} aria-label={`More: ${label}`}>
          <LightTableIcon name="plus" size={16} />
        </button>
      </div>
    </div>
  );
}

export type ExportChoice = {
  id: string;
  label: string;
  /** One line saying what the file holds. */
  detail: string;
  onSelect: () => void;
  disabled?: boolean;
  busy?: boolean;
  busyLabel?: string;
};

/**
 * The export buttons: the two everyone uses in view, the rest behind "More",
 * each with a line saying what it makes.
 */
export function ExportActions({
  primary,
  secondary,
  more,
  hint,
}: {
  /** The main file, drawn as the filled button. */
  primary: ExportChoice;
  secondary?: ExportChoice;
  more?: ExportChoice[];
  /** Why an export is not possible yet, or what to watch for. */
  hint?: ReactNode;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const menuId = useId();

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (event: MouseEvent | TouchEvent) => {
      if (!wrapRef.current?.contains(event.target as Node)) setMenuOpen(false);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setMenuOpen(false);
      }
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("touchstart", onDown);
    window.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("touchstart", onDown);
      window.removeEventListener("keydown", onKey, true);
    };
  }, [menuOpen]);

  const button = (choice: ExportChoice, kind: "primary" | "secondary") => (
    <button
      type="button"
      className={`sheetAction is-${kind}`}
      onClick={choice.onSelect}
      disabled={choice.disabled || choice.busy}
      title={choice.detail}
    >
      {choice.busy ? (
        <>
          <span className="sheetSpinner" aria-hidden="true" />
          {choice.busyLabel ?? "Preparing"}
        </>
      ) : (
        choice.label
      )}
    </button>
  );

  return (
    <div className="sheetActions" ref={wrapRef}>
      {hint ? <p className="sheetActionsHint">{hint}</p> : null}
      <div className="sheetActionsRow">
        {more?.length ? (
          <div className="sheetMore">
            <button
              type="button"
              className="sheetAction is-quiet"
              aria-expanded={menuOpen}
              aria-controls={menuId}
              onClick={() => setMenuOpen((o) => !o)}
            >
              More
              <span className={`sheetMoreCaret${menuOpen ? " isOpen" : ""}`} aria-hidden="true">
                <LightTableIcon name="chevron" size={14} />
              </span>
            </button>
            {menuOpen ? (
              <div className="sheetMenu" id={menuId} role="menu">
                <p className="sheetMenuTitle">Other formats</p>
                {more.map((choice) => (
                  <button
                    key={choice.id}
                    type="button"
                    role="menuitem"
                    className="sheetMenuItem"
                    disabled={choice.disabled || choice.busy}
                    onClick={() => {
                      setMenuOpen(false);
                      choice.onSelect();
                    }}
                  >
                    <strong>{choice.busy ? choice.busyLabel ?? "Preparing" : choice.label}</strong>
                    <span>{choice.detail}</span>
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
        {secondary ? button(secondary, "secondary") : null}
        {button(primary, "primary")}
      </div>
    </div>
  );
}
