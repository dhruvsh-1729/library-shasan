import Router from "next/router";
import { useEffect, useRef } from "react";

/**
 * The phone's back button closes the sheet or page viewer on top instead of
 * leaving the library (Sahebji, 5 Nov 2026: "when we go back the whole
 * website goes away and we have to open it again").
 *
 * Each open overlay adds one history entry; back pops it and closes the
 * overlay. Closing it any other way (×, Escape, a button) takes its entry off
 * again so the history stays as it was.
 */
type Entry = { close: () => void; canClose: () => boolean; closedByBack: boolean };

const MARK = "__libOverlay";
const stack: Entry[] = [];
let ignorePops = 0;
let pendingBacks = 0;
let installed = false;

/** Closes the overlay on top for a back press; true lets Next.js go back as usual. */
function onBack() {
  if (ignorePops > 0) {
    ignorePops -= 1;
    return false;
  }
  const top = stack[stack.length - 1];
  if (!top) return true;
  if (!top.canClose()) {
    // busy (a file is being built): stay open and put the entry back
    window.history.pushState({ ...window.history.state, [MARK]: true }, "");
    return false;
  }
  stack.pop();
  top.closedByBack = true;
  top.close();
  return false;
}

function install() {
  if (installed) return;
  installed = true;
  // Next.js asks first on most back presses. It skips asking on the first one
  // after a reload when the address is unchanged, so a plain listener (which
  // runs after the router's) catches that one.
  let asked = false;
  Router.beforePopState(() => {
    asked = true;
    return onBack();
  });
  window.addEventListener("popstate", () => {
    if (!asked) onBack();
    asked = false;
  });
}

export function useBackToClose(open: boolean, onClose: () => void, busy = false) {
  const closeRef = useRef(onClose);
  const busyRef = useRef(busy);
  useEffect(() => {
    closeRef.current = onClose;
    busyRef.current = busy;
  });

  useEffect(() => {
    if (!open || typeof window === "undefined") return;
    install();
    const entry: Entry = { close: () => closeRef.current(), canClose: () => !busyRef.current, closedByBack: false };
    stack.push(entry);
    window.history.pushState({ ...window.history.state, [MARK]: true }, "");
    return () => {
      const at = stack.indexOf(entry);
      if (at >= 0) stack.splice(at, 1);
      // Closed by ×, Escape or a button: take our entry off, unless the page
      // has already moved on (a link was followed), when it is not on top.
      // Two sheets closing together (a download done) are taken off in one
      // step, as one history.go(-2) gives one popstate where two back() calls
      // may give one or two.
      if (!entry.closedByBack && window.history.state?.[MARK]) {
        pendingBacks += 1;
        if (pendingBacks === 1) {
          queueMicrotask(() => {
            const steps = pendingBacks;
            pendingBacks = 0;
            ignorePops += 1;
            window.history.go(-steps);
          });
        }
      }
    };
  }, [open]);
}
