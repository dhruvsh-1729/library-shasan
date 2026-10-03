import Head from "next/head";
import { openPdf } from "@/lib/pdf-range-source";
import Link from "next/link";
import { useRouter } from "next/router";
import type { FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { AppNav } from "@/components/AppNav";

type PdfJsModule = typeof import("pdfjs-dist/legacy/build/pdf.mjs");
type PDFDocumentLoadingTask = import("pdfjs-dist").PDFDocumentLoadingTask;
type PDFDocumentProxy = import("pdfjs-dist").PDFDocumentProxy;
type RenderTask = import("pdfjs-dist").RenderTask;
type TextLayer = import("pdfjs-dist").TextLayer;

function firstParam(value: string | string[] | undefined) {
  if (Array.isArray(value)) return value[0] ?? "";
  return value ?? "";
}

function parsePageParam(value: string | string[] | undefined, fallback = 1) {
  const raw = firstParam(value).trim();
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.floor(parsed));
}

function normalizePdfUrl(value: string | string[] | undefined) {
  const raw = firstParam(value).trim();
  if (!raw) return null;

  try {
    const url = new URL(raw);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    return url.toString();
  } catch {
    return null;
  }
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function renderErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message;
  return String(error);
}

export default function PdfViewerPage() {
  const router = useRouter();

  const pdfUrl = useMemo(() => normalizePdfUrl(router.query.pdf), [router.query.pdf]);
  const requestedPage = useMemo(() => parsePageParam(router.query.page, 1), [router.query.page]);
  const originalHref = pdfUrl;

  const [pdfModule, setPdfModule] = useState<PdfJsModule | null>(null);
  const [pdfDoc, setPdfDoc] = useState<PDFDocumentProxy | null>(null);
  const [docLoading, setDocLoading] = useState(false);
  const [pageLoading, setPageLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pageCount, setPageCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageEntry, setPageEntry] = useState("1");
  const [zoom, setZoom] = useState(1.45);
  const [showTextLayer, setShowTextLayer] = useState(true);
  const [textDivCount, setTextDivCount] = useState(0);

  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const textLayerContainerRef = useRef<HTMLDivElement | null>(null);
  const renderTaskRef = useRef<RenderTask | null>(null);
  const textLayerRef = useRef<TextLayer | null>(null);
  const renderTokenRef = useRef(0);

  const pageTitle = useMemo(() => {
    if (!pdfUrl) return "PDF · Shasan Library";
    try {
      // Stored files are named by an opaque key; only a real file name makes a title.
      const name = decodeURIComponent(new URL(pdfUrl).pathname.split("/").pop() || "");
      return /\.pdf$/i.test(name) ? `${name.replace(/\.pdf$/i, "").replace(/_/g, " ")} · Shasan Library` : "PDF · Shasan Library";
    } catch {
      return "PDF · Shasan Library";
    }
  }, [pdfUrl]);

  useEffect(() => {
    let active = true;

    void (async () => {
      try {
        const mod = await import("pdfjs-dist/legacy/build/pdf.mjs");
        if (!active) return;

        mod.GlobalWorkerOptions.workerSrc = `https://unpkg.com/pdfjs-dist@${mod.version}/build/pdf.worker.min.mjs`;
        setPdfModule(mod);
      } catch (err) {
        if (!active) return;
        setError(`Failed to load PDF engine: ${renderErrorMessage(err)}`);
      }
    })();

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!pdfModule || !pdfUrl) return;

    let active = true;
    let loadingTask: PDFDocumentLoadingTask | null = null;

    setError(null);
    setDocLoading(true);
    setPageLoading(false);
    setPageCount(0);
    setCurrentPage(1);

    setPdfDoc((prev) => {
      if (prev) {
        void prev.destroy();
      }
      return null;
    });

    void (async () => {
      try {
        const opened = await openPdf(
          pdfModule,
          pdfUrl,
          {
            useSystemFonts: true,
            disableFontFace: false,
            cMapUrl: `https://unpkg.com/pdfjs-dist@${pdfModule.version}/cmaps/`,
            cMapPacked: true,
            standardFontDataUrl: `https://unpkg.com/pdfjs-dist@${pdfModule.version}/standard_fonts/`,
          },
          (task) => {
            loadingTask = task;
          },
          () => !active
        );
        if (!opened) return;

        const doc = await opened.task.promise;
        if (!active) {
          void doc.destroy();
          return;
        }

        setPdfDoc(doc);
        setPageCount(doc.numPages);
        setCurrentPage((prev) => clamp(prev, 1, doc.numPages));
      } catch (err) {
        if (!active) return;
        setError(`Failed to open PDF: ${renderErrorMessage(err)}`);
      } finally {
        if (active) setDocLoading(false);
      }
    })();

    return () => {
      active = false;
      if (loadingTask) loadingTask.destroy();
    };
  }, [pdfModule, pdfUrl]);

  useEffect(() => {
    if (!pdfDoc) return;
    setCurrentPage(clamp(requestedPage, 1, pdfDoc.numPages));
  }, [pdfDoc, requestedPage]);

  useEffect(() => {
    setPageEntry(String(currentPage));
  }, [currentPage]);

  useEffect(() => {
    if (!pdfDoc || !pdfModule) return;
    if (!canvasRef.current || !textLayerContainerRef.current) return;

    let active = true;
    let detachSelectionHandlers: (() => void) | null = null;
    const token = ++renderTokenRef.current;
    const pageNumber = clamp(currentPage, 1, Math.max(1, pageCount || 1));

    setPageLoading(true);
    setError(null);
    setTextDivCount(0);

    void (async () => {
      try {
        const page = await pdfDoc.getPage(pageNumber);
        if (!active || token !== renderTokenRef.current) return;

        const viewport = page.getViewport({ scale: zoom });
        const canvas = canvasRef.current;
        const textLayerContainer = textLayerContainerRef.current;
        if (!canvas || !textLayerContainer) return;

        const context = canvas.getContext("2d", { alpha: false });
        if (!context) throw new Error("Canvas 2D context is not available.");

        const ratio = Math.max(1, window.devicePixelRatio || 1);
        canvas.width = Math.max(1, Math.floor(viewport.width * ratio));
        canvas.height = Math.max(1, Math.floor(viewport.height * ratio));
        canvas.style.width = `${viewport.width}px`;
        canvas.style.height = `${viewport.height}px`;

        textLayerContainer.innerHTML = "";
        textLayerContainer.style.width = `${viewport.width}px`;
        textLayerContainer.style.height = `${viewport.height}px`;
        // pdf.js lays the text layer out at this scale; "1" put every word at
        // 1/zoom of its place (selection landed lines above the text at 125%).
        textLayerContainer.style.setProperty("--scale-factor", String(viewport.scale));
        textLayerContainer.style.setProperty("--total-scale-factor", String(viewport.scale));
        textLayerContainer.setAttribute("data-main-rotation", String(viewport.rotation));
        textLayerContainer.classList.remove("selecting");

        const beginSelecting = () => textLayerContainer.classList.add("selecting");
        const endSelecting = () => textLayerContainer.classList.remove("selecting");
        textLayerContainer.addEventListener("mousedown", beginSelecting);
        window.addEventListener("mouseup", endSelecting);
        textLayerContainer.addEventListener("touchstart", beginSelecting, { passive: true });
        window.addEventListener("touchend", endSelecting);
        detachSelectionHandlers = () => {
          textLayerContainer.removeEventListener("mousedown", beginSelecting);
          window.removeEventListener("mouseup", endSelecting);
          textLayerContainer.removeEventListener("touchstart", beginSelecting);
          window.removeEventListener("touchend", endSelecting);
        };

        const renderTask = page.render({
          canvasContext: context,
          viewport,
          transform: ratio === 1 ? undefined : [ratio, 0, 0, ratio, 0, 0],
          annotationMode: pdfModule.AnnotationMode.DISABLE,
        });
        renderTaskRef.current = renderTask;
        await renderTask.promise;
        if (!active || token !== renderTokenRef.current) return;

        const textContent = await page.getTextContent();
        if (!active || token !== renderTokenRef.current) return;

        const textLayer = new pdfModule.TextLayer({
          textContentSource: textContent,
          container: textLayerContainer,
          viewport,
        });
        textLayerRef.current = textLayer;

        await textLayer.render();
        if (!active || token !== renderTokenRef.current) return;

        // No font swap after rendering: pdf.js sizes each span to its text in the

        // font it measured with, and a different font afterwards stretches every

        // span off the words (selection landed beside the text).

        for (const textDiv of textLayer.textDivs) textDiv.style.unicodeBidi = "plaintext";
        setTextDivCount(textLayer.textDivs.length);

        const endOfContent = document.createElement("div");
        endOfContent.className = "endOfContent";
        textLayerContainer.append(endOfContent);
      } catch (err) {
        if (!active || token !== renderTokenRef.current) return;
        setError(`Failed to render page ${pageNumber}: ${renderErrorMessage(err)}`);
      } finally {
        if (active && token === renderTokenRef.current) {
          setPageLoading(false);
        }
      }
    })();

    return () => {
      active = false;

      if (renderTaskRef.current) {
        renderTaskRef.current.cancel();
        renderTaskRef.current = null;
      }

      if (textLayerRef.current) {
        textLayerRef.current.cancel();
        textLayerRef.current = null;
      }

      if (detachSelectionHandlers) {
        detachSelectionHandlers();
        detachSelectionHandlers = null;
      }
    };
  }, [currentPage, pageCount, pdfDoc, pdfModule, zoom]);

  useEffect(() => {
    return () => {
      if (pdfDoc) {
        void pdfDoc.destroy();
      }
    };
  }, [pdfDoc]);

  const canGoPrev = currentPage > 1;
  const canGoNext = pageCount > 0 && currentPage < pageCount;
  const canZoomOut = zoom > 0.7;
  const canZoomIn = zoom < 2.8;

  function submitPage(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const parsed = Number(pageEntry);
    if (!Number.isFinite(parsed)) {
      setPageEntry(String(currentPage));
      return;
    }
    setCurrentPage(clamp(Math.floor(parsed), 1, Math.max(1, pageCount || currentPage)));
  }

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
      </Head>

      <main className="lt pv">
        <header className="pvBar">
          <AppNav
            extra={
              originalHref ? (
                <a href={originalHref} target="_blank" rel="noreferrer">
                  Original
                </a>
              ) : null
            }
          />

          <div className="pvTools">
            <div className="pvGroup">
              <button type="button" className="pvBtn" onClick={() => setCurrentPage((p) => Math.max(1, p - 1))} disabled={!canGoPrev} aria-label="Previous page">
                ‹
              </button>
              <form onSubmit={submitPage} className="pvPageForm">
                <input
                  value={pageEntry}
                  onChange={(event) => setPageEntry(event.target.value)}
                  onBlur={() => setPageEntry(String(currentPage))}
                  type="number"
                  min={1}
                  max={pageCount || undefined}
                  inputMode="numeric"
                  aria-label="Page number"
                />
                <span>/ {pageCount || "–"}</span>
              </form>
              <button type="button" className="pvBtn" onClick={() => setCurrentPage((p) => Math.min(pageCount, p + 1))} disabled={!canGoNext} aria-label="Next page">
                ›
              </button>
            </div>

            <div className="pvGroup">
              <button type="button" className="pvBtn" onClick={() => setZoom((z) => Math.max(0.7, Number((z - 0.15).toFixed(2))))} disabled={!canZoomOut} aria-label="Zoom out">
                −
              </button>
              <span className="pvZoom">{Math.round(zoom * 100)}%</span>
              <button type="button" className="pvBtn" onClick={() => setZoom((z) => Math.min(2.8, Number((z + 0.15).toFixed(2))))} disabled={!canZoomIn} aria-label="Zoom in">
                +
              </button>
            </div>

            <button
              type="button"
              className={`pvBtn pvToggle${showTextLayer ? " isOn" : ""}`}
              aria-pressed={showTextLayer}
              onClick={() => setShowTextLayer((prev) => !prev)}
              title="Lets you select and copy the text on the page"
            >
              Select text
            </button>
          </div>
        </header>

        {router.isReady && !pdfUrl ? <p className="ltError pvNote" role="alert">No PDF to show.</p> : null}
        {error ? <p className="ltError pvNote" role="alert">{error}</p> : null}
        {(docLoading || pageLoading) && !error ? (
          <p className="ltLoading pvNote" role="status">
            <span className="ltSpinner" aria-hidden="true" /> {docLoading ? "Opening PDF…" : `Page ${currentPage}…`}
          </p>
        ) : null}
        {pdfDoc && !docLoading && !pageLoading && !error && showTextLayer && textDivCount === 0 ? (
          <p className="ltMuted pvNote">This page has no text to select.</p>
        ) : null}

        <section className="pvStage">
          <div className="pdfOverlayRoot pvSheet" data-show-text-layer={showTextLayer ? "true" : "false"}>
            <canvas ref={canvasRef} />
            <div ref={textLayerContainerRef} className="textLayer" aria-label="Page text" />
          </div>
        </section>
      </main>
    </>
  );
}
