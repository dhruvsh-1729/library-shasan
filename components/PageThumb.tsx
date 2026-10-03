import { useEffect, useRef, useState } from "react";
import { openPdf } from "@/lib/pdf-range-source";

type PdfJs = typeof import("pdfjs-dist/legacy/build/pdf.mjs");
type Doc = import("pdfjs-dist").PDFDocumentProxy;

let enginePromise: Promise<PdfJs> | null = null;
const docs = new Map<string, Promise<Doc>>();

function engine() {
  enginePromise ??= import("pdfjs-dist/legacy/build/pdf.mjs").then((mod) => {
    mod.GlobalWorkerOptions.workerSrc = `https://unpkg.com/pdfjs-dist@${mod.version}/build/pdf.worker.min.mjs`;
    return mod;
  });
  return enginePromise;
}

/** Each PDF is opened once per visit and shared by every thumbnail of it. */
function openDoc(url: string) {
  let doc = docs.get(url);
  if (!doc) {
    doc = engine().then(async (pdfjs) => {
      const opened = await openPdf(
        pdfjs,
        url,
        {
          cMapUrl: `https://unpkg.com/pdfjs-dist@${pdfjs.version}/cmaps/`,
          cMapPacked: true,
          standardFontDataUrl: `https://unpkg.com/pdfjs-dist@${pdfjs.version}/standard_fonts/`,
        },
        () => {}
      );
      if (!opened) throw new Error("cancelled");
      return opened.task.promise;
    });
    doc.catch(() => docs.delete(url));
    docs.set(url, doc);
  }
  return doc;
}

/**
 * One page of a library PDF, drawn when it scrolls into view. Clicking it is
 * left to the parent (it opens the full viewer).
 */
export function PageThumb({ pdfUrl, page, width = 150, eager = false }: { pdfUrl: string; page: number; width?: number; eager?: boolean }) {
  const holder = useRef<HTMLDivElement | null>(null);
  const canvas = useRef<HTMLCanvasElement | null>(null);
  const [visible, setVisible] = useState(eager);
  const [state, setState] = useState<"idle" | "done" | "error">("idle");
  // Drawing has started once the page is in view and has not finished yet.
  const shown = visible && state === "idle" ? "loading" : state;

  useEffect(() => {
    const el = holder.current;
    if (!el || visible) return;
    const io = new IntersectionObserver((entries) => entries.some((e) => e.isIntersecting) && setVisible(true), { rootMargin: "300px" });
    io.observe(el);
    return () => io.disconnect();
  }, [visible]);

  useEffect(() => {
    if (!visible) return;
    let active = true;
    let task: { cancel: () => void } | null = null;
    void (async () => {
      try {
        const doc = await openDoc(pdfUrl);
        if (!active) return;
        const pdfPage = await doc.getPage(Math.min(page, doc.numPages));
        const base = pdfPage.getViewport({ scale: 1 });
        const ratio = typeof window === "undefined" ? 1 : Math.min(2, window.devicePixelRatio || 1);
        const viewport = pdfPage.getViewport({ scale: (width / base.width) * ratio });
        const c = canvas.current;
        if (!c || !active) return;
        c.width = Math.floor(viewport.width);
        c.height = Math.floor(viewport.height);
        const pdfjs = await engine();
        const render = pdfPage.render({
          canvasContext: c.getContext("2d")!,
          viewport,
          annotationMode: pdfjs.AnnotationMode.DISABLE,
          // "print" draws without waiting for animation frames, which a hidden
          // tab never gets; a thumbnail then still appears behind another window.
          intent: "print",
        });
        task = render;
        // A page that never finishes drawing must not spin forever.
        await Promise.race([render.promise, new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 20_000))]);
        if (active) setState("done");
      } catch {
        if (active) setState("error");
      }
    })();
    return () => {
      active = false;
      task?.cancel();
    };
  }, [visible, pdfUrl, page, width]);

  return (
    <div ref={holder} className={`exThumb is-${shown}`}>
      <canvas ref={canvas} aria-hidden="true" />
      {state === "error" ? <span className="exThumbNote">Page {page} could not be drawn</span> : null}
    </div>
  );
}
