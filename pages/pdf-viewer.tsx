import Head from "next/head";
import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useMemo, useRef, useState } from "react";

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
  const [zoom, setZoom] = useState(1.45);
  const [showTextLayer, setShowTextLayer] = useState(true);
  const [textDivCount, setTextDivCount] = useState(0);

  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const textLayerContainerRef = useRef<HTMLDivElement | null>(null);
  const renderTaskRef = useRef<RenderTask | null>(null);
  const textLayerRef = useRef<TextLayer | null>(null);
  const renderTokenRef = useRef(0);

  const pageTitle = useMemo(() => {
    if (!pdfUrl) return "PDF Viewer";
    try {
      const name = decodeURIComponent(new URL(pdfUrl).pathname.split("/").pop() || "PDF");
      return `${name} | PDF Viewer`;
    } catch {
      return "PDF Viewer";
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
        loadingTask = pdfModule.getDocument({
          url: pdfUrl,
          useSystemFonts: true,
          disableFontFace: false,
          cMapUrl: `https://unpkg.com/pdfjs-dist@${pdfModule.version}/cmaps/`,
          cMapPacked: true,
          standardFontDataUrl: `https://unpkg.com/pdfjs-dist@${pdfModule.version}/standard_fonts/`,
        });

        const doc = await loadingTask.promise;
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
        textLayerContainer.style.setProperty("--scale-factor", "1");
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

        for (const textDiv of textLayer.textDivs) {
          const existing = textDiv.style.fontFamily || "";
          textDiv.style.fontFamily =
            `${existing}, "Noto Sans Gujarati", "Noto Serif Devanagari", ` +
            `"Nirmala UI", "Mangal", "Kohinoor Devanagari", sans-serif`;
          textDiv.style.unicodeBidi = "plaintext";
        }
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

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
      </Head>

      <main
        style={{
          minHeight: "100vh",
          background: "radial-gradient(circle at 16% 0%, #f7ebdd 0%, #eef2e7 34%, #e8edf3 100%)",
          color: "#1f2120",
          padding: "16px 12px 28px",
          fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
        }}
      >
        <div style={{ maxWidth: 1360, margin: "0 auto" }}>
          <header
            style={{
              border: "1px solid #d0d6df",
              borderRadius: 12,
              background: "#fffefb",
              padding: 12,
              boxShadow: "0 8px 20px rgba(35, 42, 51, 0.08)",
              display: "flex",
              gap: 10,
              flexWrap: "wrap",
              alignItems: "center",
              justifyContent: "space-between",
            }}
          >
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
              <Link href="/">Library</Link>
              <Link href="/search">Search</Link>
              {originalHref ? (
                <a href={originalHref} target="_blank" rel="noreferrer">
                  Original PDF URL
                </a>
              ) : null}
            </div>

            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <button
                type="button"
                onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                disabled={!canGoPrev}
                style={{
                  padding: "7px 11px",
                  borderRadius: 8,
                  border: "1px solid #c7cfd9",
                  background: "#fff",
                  cursor: canGoPrev ? "pointer" : "default",
                }}
              >
                Prev
              </button>
              <span style={{ fontWeight: 700, minWidth: 84, textAlign: "center" }}>
                {pageCount > 0 ? `Page ${currentPage}/${pageCount}` : "Page -"}
              </span>
              <button
                type="button"
                onClick={() => setCurrentPage((p) => Math.min(pageCount, p + 1))}
                disabled={!canGoNext}
                style={{
                  padding: "7px 11px",
                  borderRadius: 8,
                  border: "1px solid #c7cfd9",
                  background: "#fff",
                  cursor: canGoNext ? "pointer" : "default",
                }}
              >
                Next
              </button>

              <button
                type="button"
                onClick={() => setZoom((z) => Math.max(0.7, Number((z - 0.15).toFixed(2))))}
                disabled={!canZoomOut}
                style={{
                  padding: "7px 11px",
                  borderRadius: 8,
                  border: "1px solid #c7cfd9",
                  background: "#fff",
                  cursor: canZoomOut ? "pointer" : "default",
                }}
              >
                -
              </button>
              <span style={{ minWidth: 62, textAlign: "center", fontSize: 13 }}>{Math.round(zoom * 100)}%</span>
              <button
                type="button"
                onClick={() => setZoom((z) => Math.min(2.8, Number((z + 0.15).toFixed(2))))}
                disabled={!canZoomIn}
                style={{
                  padding: "7px 11px",
                  borderRadius: 8,
                  border: "1px solid #c7cfd9",
                  background: "#fff",
                  cursor: canZoomIn ? "pointer" : "default",
                }}
              >
                +
              </button>

              <button
                type="button"
                onClick={() => setShowTextLayer((prev) => !prev)}
                style={{
                  padding: "7px 11px",
                  borderRadius: 8,
                  border: "1px solid #c7cfd9",
                  background: showTextLayer ? "#1f2120" : "#fff",
                  color: showTextLayer ? "#fff" : "#222",
                  cursor: "pointer",
                }}
              >
                Text Layer {showTextLayer ? "On" : "Off"}
              </button>
            </div>
          </header>

          {!pdfUrl ? (
            <div style={{ marginTop: 14, color: "#9f1f1f", fontWeight: 700 }}>
              Invalid or missing <code>pdf</code> query parameter.
            </div>
          ) : null}

          {error ? (
            <div style={{ marginTop: 14, color: "#9f1f1f", fontWeight: 700 }}>
              {error}
            </div>
          ) : null}

          {(docLoading || pageLoading) && !error ? (
            <div style={{ marginTop: 14, opacity: 0.82 }}>
              {docLoading ? "Loading PDF..." : `Rendering page ${currentPage}...`}
            </div>
          ) : null}

          {!docLoading && !pageLoading && !error && showTextLayer && textDivCount === 0 ? (
            <div style={{ marginTop: 14, color: "#8a4b00", fontWeight: 700 }}>
              This page has no embedded text layer, so direct select/copy is not available here.
            </div>
          ) : null}

          <section style={{ marginTop: 14, overflowX: "auto" }}>
            <div
              className="pdfOverlayRoot"
              data-show-text-layer={showTextLayer ? "true" : "false"}
              style={{
                position: "relative",
                width: "fit-content",
                minHeight: 320,
                margin: "0 auto",
                border: "1px solid #d4dae4",
                borderRadius: 8,
                background: "#fff",
                boxShadow: "0 8px 22px rgba(25, 33, 44, 0.1)",
              }}
            >
              <canvas
                ref={canvasRef}
                style={{ display: "block", maxWidth: "100%" }}
              />
              <div
                ref={textLayerContainerRef}
                className="textLayer"
                aria-label="Extracted text layer"
              />
            </div>
          </section>
        </div>
      </main>
    </>
  );
}
