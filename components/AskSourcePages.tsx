import { useEffect, useRef, useState } from "react";
import { openPdf } from "@/lib/pdf-range-source";

type PdfJs = typeof import("pdfjs-dist/legacy/build/pdf.mjs");
type PDFDocumentProxy = import("pdfjs-dist").PDFDocumentProxy;

export type SourcePage = {
  key: string;
  granthName: string;
  pageNumber: number;
  label: string;
  pdfUrl?: string | null;
};

/**
 * The PDF pages an answer was read from, shown beside it so the reader can
 * see the gathas themselves. Each page renders once it scrolls into view; a
 * tap opens it in the full viewer.
 */
export function AskSourcePages({ pages, onOpen }: { pages: SourcePage[]; onOpen: (page: SourcePage) => void }) {
  const withPdf = pages.filter((p) => p.pdfUrl);
  if (!withPdf.length) return null;
  return (
    <div className="chPages" aria-label="Pages this answer was read from">
      {withPdf.map((p) => (
        <button key={p.key} type="button" className="chPage" onClick={() => onOpen(p)} title={`Open ${p.granthName} · ${p.label}`}>
          <PageCanvas pdfUrl={p.pdfUrl as string} pageNumber={p.pageNumber} />
          <span className="chPageLabel">{p.label}</span>
        </button>
      ))}
    </div>
  );
}

let pdfjs: Promise<PdfJs> | null = null;
function loadPdfJs() {
  pdfjs ??= import("pdfjs-dist/legacy/build/pdf.mjs").then((mod) => {
    mod.GlobalWorkerOptions.workerSrc = `https://unpkg.com/pdfjs-dist@${mod.version}/build/pdf.worker.min.mjs`;
    return mod;
  }).catch((e) => {
    pdfjs = null;
    throw e;
  });
  return pdfjs;
}

// One open document per PDF, shared by every page shown from it.
const docs = new Map<string, Promise<PDFDocumentProxy>>();
function loadDoc(url: string) {
  let doc = docs.get(url);
  if (!doc) {
    doc = (async () => {
      const mod = await loadPdfJs();
      const opened = await openPdf(
        mod,
        url,
        {
          cMapUrl: `https://unpkg.com/pdfjs-dist@${mod.version}/cmaps/`,
          cMapPacked: true,
          standardFontDataUrl: `https://unpkg.com/pdfjs-dist@${mod.version}/standard_fonts/`,
        },
        () => undefined,
        () => false
      );
      if (!opened) throw new Error("PDF could not be opened");
      return opened.task.promise;
    })();
    doc.catch(() => docs.delete(url));
    docs.set(url, doc);
  }
  return doc;
}

function PageCanvas({ pdfUrl, pageNumber }: { pdfUrl: string; pageNumber: number }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [visible, setVisible] = useState(false);
  const [state, setState] = useState<"loading" | "ready" | "failed">("loading");

  useEffect(() => {
    const el = canvasRef.current;
    if (!el) return;
    if (typeof IntersectionObserver === "undefined") { setVisible(true); return; }
    const io = new IntersectionObserver((entries) => {
      if (entries.some((e) => e.isIntersecting)) { setVisible(true); io.disconnect(); }
    }, { rootMargin: "200px" });
    io.observe(el);
    return () => io.disconnect();
  }, []);

  useEffect(() => {
    if (!visible) return;
    let cancelled = false;
    let task: { cancel: () => void; promise: Promise<unknown> } | null = null;
    // On a slow connection a page gives up after a while and offers the viewer instead.
    const giveUp = window.setTimeout(() => { if (!cancelled) setState((s) => (s === "ready" ? s : "failed")); }, 45_000);
    void (async () => {
      try {
        const doc = await loadDoc(pdfUrl);
        const page = await doc.getPage(Math.min(Math.max(1, pageNumber), doc.numPages));
        const canvas = canvasRef.current;
        if (cancelled || !canvas) return;
        // Sharp enough to read the gathas on a phone without opening the page.
        const base = page.getViewport({ scale: 1 });
        const cssWidth = canvas.parentElement?.clientWidth || 320;
        const ratio = Math.min(window.devicePixelRatio || 1, 2);
        const viewport = page.getViewport({ scale: (cssWidth * ratio) / base.width });
        canvas.width = Math.floor(viewport.width);
        canvas.height = Math.floor(viewport.height);
        const ctx = canvas.getContext("2d");
        if (!ctx) throw new Error("no canvas");
        const mod = await loadPdfJs();
        task = page.render({ canvasContext: ctx, viewport, annotationMode: mod.AnnotationMode.DISABLE });
        await task.promise;
        if (!cancelled) { setState("ready"); window.clearTimeout(giveUp); }
      } catch {
        if (!cancelled) setState("failed");
      }
    })();
    return () => { cancelled = true; window.clearTimeout(giveUp); task?.cancel(); };
  }, [visible, pdfUrl, pageNumber]);

  return (
    <span className={`chPageImage is-${state}`}>
      <canvas ref={canvasRef} aria-hidden="true" />
      {state === "loading" ? <span className="chPageNote">Loading page {pageNumber}…</span> : null}
      {state === "failed" ? <span className="chPageNote">Tap to open page {pageNumber}</span> : null}
    </span>
  );
}
