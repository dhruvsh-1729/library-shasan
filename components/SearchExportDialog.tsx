import { useEffect, useMemo, useState } from "react";
import { DownloadDeliveryDialog, type DeliveryMode } from "@/components/DownloadDeliveryDialog";
import { downloadBlob, filenameFromResponse } from "@/lib/download-file";
import type { OCRSearchMode, OCRSearchScripts } from "@/lib/ocr-search";
import { MAX_CONTEXT_PAGE_RADIUS, normalizeContextPageRadius } from "@/lib/page-context";

/**
 * pdf: matched pages; csv: page and line list; wordlist: PDF table of each word
 * and its granth page; linelist: PDF table of each matched line (granth page,
 * line number, the line with the words marked, the words).
 */
export type ExportFormat = "pdf" | "csv" | "wordlist" | "linelist";

export const EXPORT_ENDPOINTS: Record<ExportFormat, string> = {
  pdf: "/api/search-match-pdf",
  csv: "/api/search-match-csv",
  wordlist: "/api/search-match-wordlist",
  linelist: "/api/search-match-wordlist?layout=lines",
};

type ExportGranth = {
  granth_key: string;
  source_rel_path: string;
  granth_name: string;
  custom_id: string;
  pdf_name: string;
  matched_pages: number;
  exportable: boolean;
  unavailable_reason: string | null;
};

type ExportPreview = {
  granths: ExportGranth[];
  total_granths: number;
  exportable_granths: number;
  total_matched_pages: number;
  truncated: boolean;
  match_mode: OCRSearchMode;
  queries: string[];
  max_export_granth_preview: number;
  max_combined_pdf_granths: number;
  max_download_pages: number;
  max_csv_granths: number;
  max_csv_rows: number;
};

type SearchExportDialogProps = {
  open: boolean;
  queries: string[];
  matchMode: OCRSearchMode;
  scripts: OCRSearchScripts;
  granthIds: string[];
  scopeLabel: string;
  onClose: () => void;
};

function pagesTakenFromGranth(granth: ExportGranth, maxPagesPerGranth: number) {
  if (maxPagesPerGranth <= 0) return granth.matched_pages;
  return Math.min(granth.matched_pages, maxPagesPerGranth);
}

/**
 * Estimates the combined PDF size. Nearby-page ranges overlap on granths with
 * clustered matches, so the true count sits between the two bounds.
 */
function estimatePdfPages(granths: ExportGranth[], contextPages: number, maxPagesPerGranth: number) {
  const radius = normalizeContextPageRadius(contextPages);
  const perMatch = radius * 2 + 1;
  let min = 0;
  let max = 0;

  for (const granth of granths) {
    const pages = pagesTakenFromGranth(granth, maxPagesPerGranth);
    min += pages + 1;
    max += pages * perMatch + 1;
  }

  return { min, max: Math.max(min, max) };
}

export function SearchExportDialog({
  open,
  queries,
  matchMode,
  scripts,
  granthIds,
  scopeLabel,
  onClose,
}: SearchExportDialogProps) {
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<ExportPreview | null>(null);
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [contextPages, setContextPages] = useState(0);
  const [maxPagesPerGranth, setMaxPagesPerGranth] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busyFormat, setBusyFormat] = useState<ExportFormat | null>(null);
  const [deliveryFormat, setDeliveryFormat] = useState<ExportFormat | null>(null);

  const granthKey = granthIds.join(",");
  const queryKey = queries.join("\n");
  const scriptKey = scripts?.join(",") ?? "";

  useEffect(() => {
    if (!open) {
      setPreview(null);
      setSelectedKeys([]);
      setError(null);
      setNotice(null);
      setBusyFormat(null);
      setDeliveryFormat(null);
      setContextPages(0);
      setMaxPagesPerGranth(0);
      return;
    }
    if (queries.length === 0) {
      setError("Run a search before exporting.");
      return;
    }

    let active = true;
    setLoading(true);
    setError(null);
    setNotice(null);

    void (async () => {
      try {
        const activeQueries = queryKey.split("\n").filter(Boolean);
        const params = new URLSearchParams();
        params.set("q", activeQueries[0]);
        for (const variant of activeQueries.slice(1)) params.append("queryVariant", variant);
        params.set("matchMode", matchMode);
        if (scriptKey) params.set("scripts", scriptKey);
        if (granthKey) params.set("granths", granthKey);

        const res = await fetch(`/api/search-match-granths?${params.toString()}`);
        const json = (await res.json()) as ExportPreview & { error?: string };
        if (!res.ok) throw new Error(json.error || `Could not load matched granths (${res.status})`);
        if (!active) return;

        setPreview(json);
        setSelectedKeys((json.granths ?? []).map((granth) => granth.granth_key));
      } catch (loadError) {
        if (active) setError(loadError instanceof Error ? loadError.message : String(loadError));
      } finally {
        if (active) setLoading(false);
      }
    })();

    return () => {
      active = false;
    };
    // Query and granth lists are compared by their joined keys so a re-render
    // with equal-but-new arrays does not refetch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [granthKey, matchMode, open, queryKey, scriptKey]);

  const selectedSet = useMemo(() => new Set(selectedKeys), [selectedKeys]);
  const selectedGranths = useMemo(
    () => (preview?.granths ?? []).filter((granth) => selectedSet.has(granth.granth_key)),
    [preview, selectedSet]
  );
  const pdfGranths = useMemo(() => selectedGranths.filter((granth) => granth.exportable), [selectedGranths]);
  const selectedMatchedPages = useMemo(
    () => selectedGranths.reduce((sum, granth) => sum + granth.matched_pages, 0),
    [selectedGranths]
  );
  const pdfEstimate = useMemo(
    () => estimatePdfPages(pdfGranths, contextPages, maxPagesPerGranth),
    [contextPages, maxPagesPerGranth, pdfGranths]
  );

  if (!open) return null;

  const maxDownloadPages = preview?.max_download_pages ?? 900;
  const maxCombinedGranths = preview?.max_combined_pdf_granths ?? 60;
  const maxCsvGranths = preview?.max_csv_granths ?? 500;

  const tooManyPdfGranths = pdfGranths.length > maxCombinedGranths;
  const tooManyPdfPages = pdfEstimate.min > maxDownloadPages;
  const pdfBlockedReason = !preview
    ? "Loading matched granths."
    : pdfGranths.length === 0
      ? "Select at least one granth that has an uploaded PDF."
      : tooManyPdfGranths
        ? `Select up to ${maxCombinedGranths} PDF-ready granths for one combined PDF.`
        : tooManyPdfPages
          ? `This selection needs at least ${pdfEstimate.min} PDF pages. Keep it at ${maxDownloadPages} or fewer.`
          : null;
  const csvBlockedReason = !preview
    ? "Loading matched granths."
    : selectedGranths.length === 0
      ? "Select at least one granth."
      : selectedGranths.length > maxCsvGranths
        ? `Select up to ${maxCsvGranths} granths for one CSV or word list.`
        : null;
  const pdfPagesOverLimit = !tooManyPdfPages && pdfEstimate.max > maxDownloadPages;

  function setGranthSelected(key: string, selected: boolean) {
    setSelectedKeys((prev) => {
      if (selected) return prev.includes(key) ? prev : [...prev, key];
      return prev.filter((value) => value !== key);
    });
  }

  function selectAll(filter: "all" | "pdf" | "none") {
    if (!preview) return;
    if (filter === "none") {
      setSelectedKeys([]);
      return;
    }
    setSelectedKeys(
      preview.granths
        .filter((granth) => filter === "all" || granth.exportable)
        .map((granth) => granth.granth_key)
    );
  }

  /**
   * Keeps as many PDF-ready granths as the combined PDF limit allows, so a
   * search that matched hundreds of granths still yields one PDF in one click.
   */
  function fitSelectionToPdfLimit() {
    if (!preview) return;
    const radius = normalizeContextPageRadius(contextPages);
    const keys: string[] = [];
    let pages = 0;

    for (const granth of preview.granths) {
      if (!granth.exportable) continue;
      if (keys.length >= maxCombinedGranths) break;
      const cost = pagesTakenFromGranth(granth, maxPagesPerGranth) * (radius * 2 + 1) + 1;
      if (pages + cost > maxDownloadPages) continue;
      pages += cost;
      keys.push(granth.granth_key);
    }

    setSelectedKeys(keys);
    setNotice(
      keys.length > 0
        ? `Kept ${keys.length} PDF-ready granth(s) that fit inside ${maxDownloadPages} PDF pages.`
        : `No granth fits inside ${maxDownloadPages} PDF pages on its own. Lower the nearby-page or per-granth page limit.`
    );
  }

  async function runExport(format: ExportFormat, delivery: DeliveryMode, email?: string) {
    if (!preview) return;
    setBusyFormat(format);
    setError(null);
    setNotice(null);

    try {
      const endpoint = EXPORT_ENDPOINTS[format];
      const granths =
        format === "pdf"
          ? pdfGranths.map((granth) => ({
              customId: granth.custom_id,
              sourceRelPath: granth.source_rel_path,
            }))
          : selectedGranths.map((granth) => ({
              customId: granth.custom_id,
              sourceRelPath: granth.source_rel_path,
              granthName: granth.granth_name,
            }));

      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          granths,
          q: queries[0],
          queryVariants: queries.slice(1),
          matchMode,
          scripts,
          ...(format === "pdf" ? { contextPages, maxPagesPerGranth } : {}),
          delivery,
          email,
        }),
      });

      if (!res.ok) {
        const json = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(json.error || `Export failed (${res.status})`);
      }

      if (delivery === "email") {
        const json = (await res.json()) as { email?: string; row_count?: number };
        setDeliveryFormat(null);
        setNotice(
          `Email sent to ${json.email || email}${
            typeof json.row_count === "number"
              ? ` with ${json.row_count} ${format === "wordlist" ? "word" : format === "linelist" ? "line" : "CSV row"}(s)`
              : ""
          }.`
        );
        return;
      }

      const blob = await res.blob();
      const listPdf = format === "wordlist" || format === "linelist";
      const rowCount = res.headers.get(listPdf ? "X-Library-Wordlist-Rows" : "X-Library-Csv-Rows");
      const rowsTruncated =
        res.headers.get(listPdf ? "X-Library-Wordlist-Truncated" : "X-Library-Csv-Truncated") === "1";
      const fallbackName = {
        pdf: "granth_search_matched_pages.pdf",
        csv: "granth_search_page_lines.csv",
        wordlist: "granth_search_word_list.pdf",
        linelist: "granth_search_line_list.pdf",
      }[format];
      downloadBlob(blob, filenameFromResponse(res, fallbackName));
      setDeliveryFormat(null);
      const cutOff = rowsTruncated ? `, cut off at the ${preview.max_csv_rows} row limit` : "";
      setNotice(
        format === "pdf"
          ? "Combined PDF downloaded."
          : format === "wordlist"
            ? `Word list downloaded${rowCount ? ` with ${rowCount} word(s)` : ""}${cutOff}.`
            : format === "linelist"
              ? `Line list downloaded${rowCount ? ` with ${rowCount} line(s)` : ""}${cutOff}.`
              : `CSV downloaded${rowCount ? ` with ${rowCount} row(s)` : ""}${cutOff}.`
      );
    } catch (exportError) {
      setError(exportError instanceof Error ? exportError.message : String(exportError));
    } finally {
      setBusyFormat(null);
    }
  }

  return (
    <div className="searchDownloadOverlay" role="dialog" aria-modal="true" aria-label="Export search results">
      <div className="searchDownloadPanel searchExportPanel">
        <header className="searchDownloadHeader">
          <div>
            <h2>Export whole search</h2>
            <p>
              {queries.join(", ")} | {scopeLabel}
            </p>
          </div>
          <button type="button" onClick={onClose} disabled={busyFormat !== null}>
            Close
          </button>
        </header>

        {loading ? (
          <div className="searchDownloadNotice" role="status">
            <span className="buttonSpinnerLabel">
              <span className="loadingSpinner" aria-hidden="true" />
              Finding every granth that matches
            </span>
          </div>
        ) : null}
        {error ? (
          <div className="searchDownloadError" role="alert">
            {error}
          </div>
        ) : null}
        {notice ? (
          <div className="searchDownloadNotice" role="status">
            {notice}
          </div>
        ) : null}

        {preview ? (
          <>
            <div className="searchDownloadSummary">
              <strong>{selectedGranths.length}</strong> of {preview.total_granths} books · <strong>{selectedMatchedPages}</strong> pages
              selected. Combined PDF: about {pdfEstimate.max > pdfEstimate.min ? `${pdfEstimate.min}–${pdfEstimate.max}` : pdfEstimate.min} pages
              (up to {maxDownloadPages} pages from {maxCombinedGranths} books).
              {preview.total_granths > preview.exportable_granths ? (
                <span> {preview.total_granths - preview.exportable_granths} books have no PDF, so they go only into the CSV, word list and line list.</span>
              ) : null}
              {preview.truncated ? (
                <span> Only the first {preview.max_export_granth_preview} books are listed; narrow the search.</span>
              ) : null}
            </div>

            <div className="searchDownloadToolbar">
              <button type="button" onClick={() => selectAll("all")}>
                Select all
              </button>
              <button type="button" onClick={() => selectAll("pdf")}>
                Only PDF-ready
              </button>
              <button type="button" onClick={() => selectAll("none")}>
                Clear
              </button>
              <button
                type="button"
                onClick={fitSelectionToPdfLimit}
                title={`Keep the granths that fit inside ${maxDownloadPages} PDF pages`}
              >
                Fit to PDF limit
              </button>
              <label className="searchDownloadContextInput">
                <span>Nearby pages</span>
                <input
                  type="number"
                  min={0}
                  max={MAX_CONTEXT_PAGE_RADIUS}
                  inputMode="numeric"
                  value={contextPages}
                  onChange={(event) =>
                    setContextPages(event.target.value === "" ? 0 : normalizeContextPageRadius(event.target.value))
                  }
                />
              </label>
              <label className="searchDownloadContextInput" title="0 keeps every matched page of every granth">
                <span>Max pages per granth</span>
                <input
                  type="number"
                  min={0}
                  max={maxDownloadPages}
                  inputMode="numeric"
                  value={maxPagesPerGranth}
                  onChange={(event) => {
                    const parsed = Math.floor(Number(event.target.value));
                    setMaxPagesPerGranth(
                      Number.isFinite(parsed) && parsed > 0 ? Math.min(parsed, maxDownloadPages) : 0
                    );
                  }}
                />
              </label>
            </div>

            <div className="searchDownloadPageList">
              {preview.granths.length === 0 ? (
                <div style={{ padding: 10, fontWeight: 700, opacity: 0.8 }}>No granth matched this search.</div>
              ) : (
                preview.granths.map((granth) => (
                  <label key={granth.granth_key} className="searchExportGranthRow">
                    <input
                      type="checkbox"
                      checked={selectedSet.has(granth.granth_key)}
                      onChange={(event) => setGranthSelected(granth.granth_key, event.target.checked)}
                    />
                    <span className="searchExportGranthName">
                      {granth.granth_name || granth.pdf_name || granth.custom_id}
                      <em>{granth.pdf_name || "No uploaded PDF"}</em>
                    </span>
                    <span className="searchExportGranthMeta">
                      {granth.matched_pages} page(s)
                      {granth.exportable ? null : (
                        <em title={granth.unavailable_reason || "No uploaded PDF"}>CSV / word list only</em>
                      )}
                    </span>
                  </label>
                ))
              )}
            </div>

            <footer className="searchDownloadFooter">
              {pdfPagesOverLimit ? (
                <span className="searchDownloadErrorText">
                  Nearby pages may push this past {maxDownloadPages} pages. Lower it if the download is rejected.
                </span>
              ) : null}
              <button
                type="button"
                onClick={() => setDeliveryFormat("csv")}
                title={csvBlockedReason || "Export page and line numbers as CSV"}
                disabled={busyFormat !== null || csvBlockedReason !== null}
              >
                {busyFormat === "csv" ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Building CSV
                  </span>
                ) : (
                  "Export CSV"
                )}
              </button>
              <button
                type="button"
                onClick={() => setDeliveryFormat("wordlist")}
                title={csvBlockedReason || "PDF table of every matched word with its granth page number"}
                disabled={busyFormat !== null || csvBlockedReason !== null}
              >
                {busyFormat === "wordlist" ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Building word list
                  </span>
                ) : (
                  "Export word list (PDF)"
                )}
              </button>
              <button
                type="button"
                onClick={() => setDeliveryFormat("linelist")}
                title={csvBlockedReason || "PDF table of every matched line: granth page, line number, the line and the word"}
                disabled={busyFormat !== null || csvBlockedReason !== null}
              >
                {busyFormat === "linelist" ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Building line list
                  </span>
                ) : (
                  "Export line list (PDF)"
                )}
              </button>
              <button
                type="button"
                onClick={() => setDeliveryFormat("pdf")}
                title={pdfBlockedReason || "Download one PDF with matched pages from every selected granth"}
                disabled={busyFormat !== null || pdfBlockedReason !== null}
              >
                {busyFormat === "pdf" ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Building PDF
                  </span>
                ) : (
                  "Download combined PDF"
                )}
              </button>
            </footer>

            {pdfBlockedReason || csvBlockedReason ? (
              <div className="searchExportHint">{pdfBlockedReason || csvBlockedReason}</div>
            ) : null}

            <DownloadDeliveryDialog
              open={deliveryFormat !== null}
              title={
                deliveryFormat === "csv"
                  ? "Choose CSV delivery"
                  : deliveryFormat === "wordlist"
                    ? "Choose word list delivery"
                    : deliveryFormat === "linelist"
                      ? "Choose line list delivery"
                      : "Choose PDF delivery"
              }
              fileLabel={
                deliveryFormat === "csv" || deliveryFormat === "wordlist" || deliveryFormat === "linelist"
                  ? `${selectedGranths.length} granth(s), ${selectedMatchedPages} matching page(s)`
                  : `${pdfGranths.length} granth(s), about ${pdfEstimate.min}${
                      pdfEstimate.max > pdfEstimate.min ? `-${pdfEstimate.max}` : ""
                    } PDF page(s)`
              }
              busy={busyFormat !== null}
              error={error}
              onClose={() => setDeliveryFormat(null)}
              onDownload={() => void runExport(deliveryFormat ?? "pdf", "download")}
              onEmail={(email) => void runExport(deliveryFormat ?? "pdf", "email", email)}
            />
          </>
        ) : null}
      </div>
    </div>
  );
}
