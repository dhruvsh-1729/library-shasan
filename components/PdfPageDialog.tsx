import type { FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  findOCRSearchMatchesForQueries,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
  type OCRSearchMode,
  type OCRSearchScripts,
} from "@/lib/ocr-search";
import { openPdf } from "@/lib/pdf-range-source";
import { findGlyphWordBoxes, type GlyphPage } from "@/lib/pdf-glyph-boxes";
import { type PdfTextItem, type WordBox, findWordBoxes } from "@/lib/pdf-word-boxes";

type PdfJsModule = typeof import("pdfjs-dist/legacy/build/pdf.mjs");
type PDFDocumentLoadingTask = import("pdfjs-dist").PDFDocumentLoadingTask;
type PDFDocumentProxy = import("pdfjs-dist").PDFDocumentProxy;
type RenderTask = import("pdfjs-dist").RenderTask;
type TextLayer = import("pdfjs-dist").TextLayer;

/** Ring boxes (page fractions, lower-left origin) from our OCR's line boxes, or null. */
async function fetchLineRings(pdfUrl: string, page: number, terms: string[], mode: OCRSearchMode, scripts: OCRSearchScripts) {
  try {
    const params = new URLSearchParams({ pdfUrl, page: String(page), q: terms[0] ?? "", matchMode: mode });
    for (const term of terms.slice(1)) params.append("queryVariant", term);
    if (scripts) params.set("scripts", scripts.join(","));
    const res = await fetch(`/api/page-rings?${params.toString()}`);
    if (!res.ok) return null;
    const json = (await res.json()) as { rings?: Array<{ x: number; y: number; width: number; height: number }> | null };
    return json.rings ?? null;
  } catch {
    return null;
  }
}

export type PdfDialogTarget = {
  pdfUrl: string;
  page?: number | null;
  title?: string | null;
  pageCount?: number | null;
  searchTerm?: string | null;
  searchTerms?: string[] | null;
  searchMode?: OCRSearchMode | null;
  searchScripts?: OCRSearchScripts;
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
    if (url.protocol !== "http:" && url.protocol !== "https:" && url.protocol !== "blob:") return null;
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

function applySearchHighlights(
  textDivs: HTMLElement[],
  queries: string[],
  mode: OCRSearchMode,
  scripts: OCRSearchScripts = null
) {
  if (queries.length === 0) return 0;

  let count = 0;

  for (const textDiv of textDivs) {
    const text = textDiv.textContent ?? "";
    const matches = findOCRSearchMatchesForQueries(text, queries, mode, scripts);
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
  // Red rings around the searched word, in page pixels, drawn over the canvas.
  const [rings, setRings] = useState<Array<{ cx: number; cy: number; rx: number; ry: number }>>([]);
  const [ringFrame, setRingFrame] = useState({ width: 0, height: 0 });
  const ringsRef = useRef<SVGSVGElement | null>(null);
  const textLayerRef = useRef<TextLayer | null>(null);
  const renderTokenRef = useRef(0);
  // The open document and the URL it came from: moving to another page of the
  // same PDF (e.g. a second search result) reuses it instead of re-downloading.
  const loadedDocRef = useRef<{ url: string; doc: PDFDocumentProxy } | null>(null);

  const pdfUrl = useMemo(() => normalizePdfUrl(target?.pdfUrl), [target?.pdfUrl]);
  const requestedPage = useMemo(() => parsePage(target?.page), [target?.page]);
  const pageCountHint = Number.isFinite(Number(target?.pageCount)) ? Math.max(0, Number(target?.pageCount)) : 0;
  const dialogTitle = target?.title?.trim() || titleFromUrl(pdfUrl);
  const highlightTerms = useMemo(
    () => normalizeOCRSearchQueries(target?.searchTerm || "", target?.searchTerms || []),
    [target?.searchTerm, target?.searchTerms]
  );
  const highlightMode = useMemo(() => parseOCRSearchMode(target?.searchMode), [target?.searchMode]);
  const highlightScripts = target?.searchScripts ?? null;

  const [pdfModule, setPdfModule] = useState<PdfJsModule | null>(null);
  const [pdfDoc, setPdfDoc] = useState<PDFDocumentProxy | null>(null);
  const [engineLoading, setEngineLoading] = useState(false);
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
  const requestedPageRef = useRef(requestedPage);
  requestedPageRef.current = requestedPage;
  const isOpen = Boolean(target);

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

    setEngineLoading(true);

    void (async () => {
      try {
        const mod = await import("pdfjs-dist/legacy/build/pdf.mjs");
        if (!active) return;
        mod.GlobalWorkerOptions.workerSrc = `https://unpkg.com/pdfjs-dist@${mod.version}/build/pdf.worker.min.mjs`;
        setPdfModule(mod);
      } catch (err) {
        if (!active) return;
        setError(`Failed to load PDF engine: ${renderErrorMessage(err)}`);
      } finally {
        if (active) setEngineLoading(false);
      }
    })();

    return () => {
      active = false;
    };
  }, [pdfModule, target]);

  useEffect(() => {
    const loaded = loadedDocRef.current && loadedDocRef.current.url === pdfUrl ? loadedDocRef.current.doc : null;
    const page = loaded ? clamp(requestedPage, 1, loaded.numPages) : requestedPage;
    setCurrentPage(page);
    setPageEntry(String(page));
    setPageCount(loaded ? loaded.numPages : pageCountHint);
    setTextDivCount(0);
    setHighlightCount(0);
    setError(pdfUrl || !target ? null : "Invalid PDF URL.");
  }, [pageCountHint, pdfUrl, requestedPage, target]);

  useEffect(() => {
    if (!target) {
      // The last PDF stays open after the dialog closes, so opening another
      // result from the same granth needs no new download; it is released when
      // a different PDF is opened or the page is left.
      setEngineLoading(false);
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
    if (!pdfModule || !pdfUrl || !isOpen) return;

    const kept = loadedDocRef.current;
    if (kept && kept.url === pdfUrl) {
      setError(null);
      setDocLoading(false);
      setPageCount(kept.doc.numPages);
      setCurrentPage(clamp(requestedPageRef.current, 1, kept.doc.numPages));
      return;
    }

    let active = true;
    let loadingTask: PDFDocumentLoadingTask | null = null;

    setError(null);
    setDocLoading(true);
    setPageLoading(false);
    setPageCount(pageCountHint);

    loadedDocRef.current = null;
    setPdfDoc((prev) => {
      if (prev) void prev.destroy();
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

        loadedDocRef.current = { url: pdfUrl, doc };
        setPdfDoc(doc);
        setPageCount(doc.numPages);
        setCurrentPage(clamp(requestedPageRef.current, 1, doc.numPages));
      } catch (err) {
        if (!active) return;
        setError(`Failed to open PDF: ${renderErrorMessage(err)}`);
      } finally {
        if (active) setDocLoading(false);
      }
    })();

    return () => {
      active = false;
      // An opened document is kept (see above); only an unfinished load is cancelled.
      if (loadingTask && loadedDocRef.current?.url !== pdfUrl) loadingTask.destroy();
    };
    // Re-open only for a different PDF (or after the dialog was closed); a new
    // page of the same PDF is handled by the effect above.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOpen, pdfModule, pdfUrl]);

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
    setRings([]);

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
        // Ring the word where it is printed: our OCR's line boxes when the book
        // has them, else the PDF's own text layer (exact glyph positions, then
        // the text-content estimate), else the old text highlight.
        let boxes: WordBox[] = [];
        if (highlightTerms.length) {
          const [vx0, vy0, vx1, vy1] = page.view;
          const fromLines = pdfUrl ? await fetchLineRings(pdfUrl, pageNumber, highlightTerms, highlightMode, highlightScripts) : null;
          if (!active || token !== renderTokenRef.current) return;
          if (fromLines) {
            const w = vx1 - vx0;
            const h = vy1 - vy0;
            boxes = fromLines.map((b) => ({ x: vx0 + b.x * w, y: vy0 + b.y * h, width: b.width * w, height: b.height * h, fontSize: b.height * h * 0.8, text: "" }));
          } else {
            const exact = await findGlyphWordBoxes(page as unknown as GlyphPage, pdfModule.OPS, highlightTerms, highlightMode, highlightScripts);
            if (!active || token !== renderTokenRef.current) return;
            boxes = exact ?? findWordBoxes(textContent.items as PdfTextItem[], highlightTerms, highlightMode, highlightScripts);
            // A text layer that runs past the page edge cannot be shown.
            boxes = boxes.filter((b) => b.x < vx1 && b.x + b.width > vx0 && b.y < vy1 && b.y + b.height > vy0);
          }
        }
        if (boxes.length) {
          setRingFrame({ width: viewport.width, height: viewport.height });
          setRings(
            boxes.map((box) => {
              const [x1, y1, x2, y2] = viewport.convertToViewportRectangle([box.x, box.y, box.x + box.width, box.y + box.height]);
              const pad = box.fontSize * zoom * 0.3;
              return {
                cx: (x1 + x2) / 2,
                cy: (y1 + y2) / 2,
                rx: Math.abs(x2 - x1) / 2 + pad,
                ry: Math.abs(y2 - y1) / 2 + pad * 0.35,
              };
            })
          );
          setHighlightCount(boxes.length);
        } else {
          setHighlightCount(applySearchHighlights(textLayer.textDivs, highlightTerms, highlightMode, highlightScripts));
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
  }, [currentPage, highlightMode, highlightScripts, highlightTerms, pageCount, pdfDoc, pdfModule, target, zoom]);

  // Bring the first ring into view when a page's rings appear.
  useEffect(() => {
    const first = ringsRef.current?.querySelector("ellipse");
    if (first) first.scrollIntoView({ block: "center", inline: "center", behavior: "smooth" });
  }, [rings]);

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
  const isPdfLoading = Boolean(target && !error && (engineLoading || docLoading || pageLoading));
  const loadingLabel = engineLoading
    ? "Loading PDF viewer..."
    : docLoading
      ? "Opening PDF..."
      : `Rendering page ${currentPage}...`;

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
            {highlightTerms.length > 0 && !isPdfLoading ? (
              <div className={`pdfDialogSubline${highlightCount ? "" : " isMissing"}`}>
                {highlightCount ? `${highlightCount} ${highlightCount === 1 ? "match" : "matches"} ringed` : "The word could not be placed on this scan"}
              </div>
            ) : null}
          </div>

          <div className="pdfDialogTools pvTools">
            <div className="pvGroup">
              <button type="button" className="pvBtn" onClick={() => goToPage(currentPage - 1)} disabled={!canGoPrev} aria-label="Previous page">
                ‹
              </button>
              <form className="pvPageForm" onSubmit={submitPage}>
                <input
                  id="pdf-dialog-page"
                  value={pageEntry}
                  onChange={(event) => setPageEntry(event.target.value)}
                  onBlur={() => setPageEntry(String(currentPage))}
                  inputMode="numeric"
                  min={1}
                  max={pageCount || undefined}
                  type="number"
                  aria-label="Page number"
                />
                <span>/ {pageCount || "–"}</span>
              </form>
              <button type="button" className="pvBtn" onClick={() => goToPage(currentPage + 1)} disabled={!canGoNext} aria-label="Next page">
                ›
              </button>
            </div>

            <div className="pvGroup">
              <button
                type="button"
                className="pvBtn"
                onClick={() => setZoom((value) => Math.max(0.7, Number((value - 0.15).toFixed(2))))}
                disabled={!canZoomOut}
                aria-label="Zoom out"
              >
                −
              </button>
              <span className="pvZoom">{Math.round(zoom * 100)}%</span>
              <button
                type="button"
                className="pvBtn"
                onClick={() => setZoom((value) => Math.min(2.8, Number((value + 0.15).toFixed(2))))}
                disabled={!canZoomIn}
                aria-label="Zoom in"
              >
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

            {pdfUrl ? (
              <a className="pvBtn pvToggle" href={pdfUrl} target="_blank" rel="noreferrer" title="The whole PDF in a new tab">
                Whole PDF
              </a>
            ) : null}

            <button type="button" className="pvBtn pdfDialogClose" onClick={onClose} aria-label="Close">
              ×
            </button>
          </div>
        </header>

        {error ? <div className="pdfDialogError">{error}</div> : null}
        {!error && showTextLayer && !isPdfLoading && textDivCount === 0 ? (
          <div className="pdfDialogNotice">This page has no text to select.</div>
        ) : null}

        <section className="pdfDialogViewport" aria-busy={isPdfLoading}>
          {!error && isPdfLoading ? (
            <div className="pdfDialogLoading" role="status" aria-live="polite">
              <span className="loadingSpinner" aria-hidden="true" />
              <span>{loadingLabel}</span>
            </div>
          ) : null}
          <div
            className="pdfOverlayRoot pdfDialogPage"
            data-show-text-layer={showTextLayer ? "true" : "false"}
          >
            <canvas ref={canvasRef} />
            <div ref={textLayerContainerRef} className="textLayer" aria-label="Extracted text layer" />
            {rings.length ? (
              <svg
                ref={ringsRef}
                className="pdfRings"
                width={ringFrame.width}
                height={ringFrame.height}
                viewBox={`0 0 ${ringFrame.width} ${ringFrame.height}`}
                aria-hidden="true"
              >
                {rings.map((ring, i) => (
                  <ellipse
                    key={i}
                    cx={ring.cx}
                    cy={ring.cy}
                    rx={ring.rx}
                    ry={ring.ry}
                    transform={`rotate(-2 ${ring.cx} ${ring.cy})`}
                  />
                ))}
              </svg>
            ) : null}
            {!error && isPdfLoading ? (
              <div className="pdfDialogPageLoading" aria-hidden="true">
                <span className="loadingSpinner" />
                <span>{loadingLabel}</span>
              </div>
            ) : null}
          </div>
        </section>
      </div>
    </dialog>
  );
}
