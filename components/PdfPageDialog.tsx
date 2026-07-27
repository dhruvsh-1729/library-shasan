import type { FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  findOCRSearchMatchesForQueries,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
  type OCRSearchMode,
} from "@/lib/ocr-search";

type PdfJsModule = typeof import("pdfjs-dist/legacy/build/pdf.mjs");
type PDFDocumentLoadingTask = import("pdfjs-dist").PDFDocumentLoadingTask;
type PDFDocumentProxy = import("pdfjs-dist").PDFDocumentProxy;
type RenderTask = import("pdfjs-dist").RenderTask;
type TextLayer = import("pdfjs-dist").TextLayer;

export type PdfDialogTarget = {
  pdfUrl: string;
  page?: number | null;
  title?: string | null;
  pageCount?: number | null;
  searchTerm?: string | null;
  searchTerms?: string[] | null;
  searchMode?: OCRSearchMode | null;
};

type PdfPageDialogProps = {
  target: PdfDialogTarget | null;
  onClose: () => void;
};

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function renderErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message;
  return String(error);
}

function normalizePdfUrl(value: string | null | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  try {
    const url = new URL(raw);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    return url.toString();
  } catch {
    return null;
  }
}

function parsePage(value: number | null | undefined) {
  const parsed = Number(value || 1);
  if (!Number.isFinite(parsed)) return 1;
  return Math.max(1, Math.floor(parsed));
}

function titleFromUrl(pdfUrl: string | null) {
  if (!pdfUrl) return "PDF";
  try {
    return decodeURIComponent(new URL(pdfUrl).pathname.split("/").pop() || "PDF");
  } catch {
    return "PDF";
  }
}

function applySearchHighlights(textDivs: HTMLElement[], queries: string[], mode: OCRSearchMode) {
  if (queries.length === 0) return 0;

  let count = 0;

  for (const textDiv of textDivs) {
    const text = textDiv.textContent ?? "";
    const matches = findOCRSearchMatchesForQueries(text, queries, mode);
    if (matches.length === 0) continue;

    textDiv.textContent = "";
    textDiv.setAttribute("data-search-hit", "true");

    let cursor = 0;
    matches.forEach((match, index) => {
      if (match.start > cursor) {
        textDiv.append(document.createTextNode(text.slice(cursor, match.start)));
      }

      const mark = document.createElement("mark");
      mark.className = "pdfSearchHit";
      mark.textContent = text.slice(match.start, match.end);
      mark.setAttribute("data-hit-index", String(index + 1));
      textDiv.append(mark);
      cursor = match.end;
      count += 1;
    });

    if (cursor < text.length) {
      textDiv.append(document.createTextNode(text.slice(cursor)));
    }
  }

  return count;
}

export function PdfPageDialog({ target, onClose }: PdfPageDialogProps) {
  const dialogRef = useRef<HTMLDialogElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const textLayerContainerRef = useRef<HTMLDivElement | null>(null);
  const renderTaskRef = useRef<RenderTask | null>(null);
  const textLayerRef = useRef<TextLayer | null>(null);
  const renderTokenRef = useRef(0);

  const pdfUrl = useMemo(() => normalizePdfUrl(target?.pdfUrl), [target?.pdfUrl]);
  const requestedPage = useMemo(() => parsePage(target?.page), [target?.page]);
  const pageCountHint = Number.isFinite(Number(target?.pageCount)) ? Math.max(0, Number(target?.pageCount)) : 0;
  const dialogTitle = target?.title?.trim() || titleFromUrl(pdfUrl);
  const highlightTerms = useMemo(
    () => normalizeOCRSearchQueries(target?.searchTerm || "", target?.searchTerms || []),
    [target?.searchTerm, target?.searchTerms]
  );
  const highlightMode = useMemo(() => parseOCRSearchMode(target?.searchMode), [target?.searchMode]);

  const [pdfModule, setPdfModule] = useState<PdfJsModule | null>(null);
  const [pdfDoc, setPdfDoc] = useState<PDFDocumentProxy | null>(null);
  const [docLoading, setDocLoading] = useState(false);
  const [pageLoading, setPageLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pageCount, setPageCount] = useState(pageCountHint);
  const [currentPage, setCurrentPage] = useState(requestedPage);
  const [pageEntry, setPageEntry] = useState(String(requestedPage));
  const [zoom, setZoom] = useState(1.25);
  const [showTextLayer, setShowTextLayer] = useState(true);
  const [textDivCount, setTextDivCount] = useState(0);
  const [highlightCount, setHighlightCount] = useState(0);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;

    if (target) {
      if (!dialog.open && typeof dialog.showModal === "function") dialog.showModal();
      return;
    }

    if (dialog.open) dialog.close();
  }, [target]);

  useEffect(() => {
    if (!target || pdfModule) return;
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
  }, [pdfModule, target]);

  useEffect(() => {
    setCurrentPage(requestedPage);
    setPageEntry(String(requestedPage));
    setPageCount(pageCountHint);
    setTextDivCount(0);
    setHighlightCount(0);
    setError(pdfUrl || !target ? null : "Invalid PDF URL.");
  }, [pageCountHint, pdfUrl, requestedPage, target]);

  useEffect(() => {
    if (!target) {
      setPdfDoc((prev) => {
        if (prev) void prev.destroy();
        return null;
      });
      setDocLoading(false);
      setPageLoading(false);
    }
  }, [target]);

  useEffect(() => {
    return () => {
      if (pdfDoc) void pdfDoc.destroy();
    };
  }, [pdfDoc]);

  useEffect(() => {
    if (!pdfModule || !pdfUrl || !target) return;

    let active = true;
    let loadingTask: PDFDocumentLoadingTask | null = null;

    setError(null);
    setDocLoading(true);
    setPageLoading(false);
    setPageCount(pageCountHint);

    setPdfDoc((prev) => {
      if (prev) void prev.destroy();
      return null;
    });

    void (async () => {
      try {
        loadingTask = pdfModule.getDocument({
          url: pdfUrl,
          useSystemFonts: true,
          disableFontFace: false,
          disableStream: true,
          disableAutoFetch: true,
          rangeChunkSize: 65536,
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
        setCurrentPage(clamp(requestedPage, 1, doc.numPages));
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
  }, [pageCountHint, pdfModule, pdfUrl, requestedPage, target]);

  useEffect(() => {
    setPageEntry(String(currentPage));
  }, [currentPage]);

  useEffect(() => {
    if (!pdfDoc || !pdfModule || !target) return;
    if (!canvasRef.current || !textLayerContainerRef.current) return;

    let active = true;
    let detachSelectionHandlers: (() => void) | null = null;
    const token = ++renderTokenRef.current;
    const pageNumber = clamp(currentPage, 1, Math.max(1, pageCount || 1));

    setPageLoading(true);
    setError(null);
    setTextDivCount(0);
    setHighlightCount(0);

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
        setHighlightCount(applySearchHighlights(textLayer.textDivs, highlightTerms, highlightMode));
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
  }, [currentPage, highlightMode, highlightTerms, pageCount, pdfDoc, pdfModule, target, zoom]);

  function goToPage(page: number) {
    const maxPage = pageCount || Math.max(1, page);
    setCurrentPage(clamp(Math.floor(page), 1, maxPage));
  }

  function submitPage(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const parsed = Number(pageEntry);
    if (!Number.isFinite(parsed)) {
      setPageEntry(String(currentPage));
      return;
    }
    goToPage(parsed);
  }

  const canGoPrev = currentPage > 1;
  const canGoNext = pageCount > 0 && currentPage < pageCount;
  const canZoomOut = zoom > 0.7;
  const canZoomIn = zoom < 2.8;

  return (
    <dialog
      ref={dialogRef}
      className="pdfDialog"
      aria-label="PDF page viewer"
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
    >
      <div className="pdfDialogPanel">
        <header className="pdfDialogHeader">
          <div className="pdfDialogTitleBlock">
            <div className="pdfDialogTitle" title={dialogTitle}>
              {dialogTitle}
            </div>
            <div className="pdfDialogSubline">
              {pageCount > 0 ? `Page ${currentPage} of ${pageCount}` : `Page ${currentPage}`}
              {docLoading || pageLoading ? " | Loading current page" : ""}
              {highlightTerms.length > 0 && !docLoading && !pageLoading ? ` | ${highlightCount} highlight(s)` : ""}
            </div>
          </div>

          <div className="pdfDialogControls">
            <button type="button" onClick={() => goToPage(currentPage - 1)} disabled={!canGoPrev}>
              Previous
            </button>

            <form className="pdfDialogPageForm" onSubmit={submitPage}>
              <label htmlFor="pdf-dialog-page">Page</label>
              <input
                id="pdf-dialog-page"
                value={pageEntry}
                onChange={(event) => setPageEntry(event.target.value)}
                inputMode="numeric"
                min={1}
                max={pageCount || undefined}
                type="number"
              />
              <button type="submit">Go</button>
            </form>

            <button type="button" onClick={() => goToPage(currentPage + 1)} disabled={!canGoNext}>
              Next
            </button>

            <button
              type="button"
              onClick={() => setZoom((value) => Math.max(0.7, Number((value - 0.15).toFixed(2))))}
              disabled={!canZoomOut}
              aria-label="Zoom out"
            >
              -
            </button>
            <span className="pdfDialogZoom">{Math.round(zoom * 100)}%</span>
            <button
              type="button"
              onClick={() => setZoom((value) => Math.min(2.8, Number((value + 0.15).toFixed(2))))}
              disabled={!canZoomIn}
              aria-label="Zoom in"
            >
              +
            </button>

            <button type="button" onClick={() => setShowTextLayer((prev) => !prev)}>
              Text {showTextLayer ? "On" : "Off"}
            </button>

            {pdfUrl ? (
              <a href={pdfUrl} target="_blank" rel="noreferrer">
                Raw
              </a>
            ) : null}

            <button type="button" className="pdfDialogCloseButton" onClick={onClose} aria-label="Close PDF viewer">
              Close
            </button>
          </div>
        </header>

        {error ? <div className="pdfDialogError">{error}</div> : null}
        {!error && showTextLayer && !docLoading && !pageLoading && textDivCount === 0 ? (
          <div className="pdfDialogNotice">This page has no embedded text layer.</div>
        ) : null}

        <section className="pdfDialogViewport" aria-busy={docLoading || pageLoading}>
          {!error && (docLoading || pageLoading) ? (
            <div className="pdfDialogLoading">{docLoading ? "Opening PDF..." : `Rendering page ${currentPage}...`}</div>
          ) : null}
          <div
            className="pdfOverlayRoot pdfDialogPage"
            data-show-text-layer={showTextLayer ? "true" : "false"}
          >
            <canvas ref={canvasRef} />
            <div ref={textLayerContainerRef} className="textLayer" aria-label="Extracted text layer" />
          </div>
        </section>
      </div>
    </dialog>
  );
}
