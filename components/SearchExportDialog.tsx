import { useEffect, useMemo, useState } from "react";
import { DownloadDeliveryDialog, type DeliveryMode } from "@/components/DownloadDeliveryDialog";
import { LightTableIcon } from "@/components/LightTableIcon";
import { ExportActions, Sheet, Stepper } from "@/components/Sheet";
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

  const choice = (format: ExportFormat) => ({
    onSelect: () => setDeliveryFormat(format),
    busy: busyFormat === format,
    disabled: busyFormat !== null || (format === "pdf" ? pdfBlockedReason !== null : csvBlockedReason !== null),
  });
  const pdfRange = pdfEstimate.max > pdfEstimate.min ? `${pdfEstimate.min}–${pdfEstimate.max}` : String(pdfEstimate.min);
  const noPdfCount = preview ? preview.total_granths - preview.exportable_granths : 0;

  return (
    <Sheet
      open
      title="Export results"
      subtitle={`${queries.join(", ")} · ${scopeLabel}`}
      onClose={onClose}
      busy={busyFormat !== null}
      footer={
        preview ? (
          <ExportActions
            hint={
              // The size limit already has its own note, with the fix, above the list.
              pdfBlockedReason && !csvBlockedReason && !tooManyPdfPages && !tooManyPdfGranths
                ? `Full PDF: ${pdfBlockedReason}`
                : csvBlockedReason ?? (pdfPagesOverLimit ? `Nearby pages may push the PDF past ${maxDownloadPages} pages.` : null)
            }
            secondary={{
              id: "linelist",
              label: "Line list",
              detail: "A PDF table of every matching line: page, line number, the line and the word",
              busyLabel: "Building",
              ...choice("linelist"),
            }}
            primary={{
              id: "pdf",
              label: "Full PDF",
              detail: "One PDF with the matching pages of every chosen book",
              busyLabel: "Building",
              ...choice("pdf"),
            }}
            more={[
              {
                id: "wordlist",
                label: "Word list (PDF)",
                detail: "Each matching word with the book's page number",
                busyLabel: "Building word list",
                ...choice("wordlist"),
              },
              {
                id: "csv",
                label: "Spreadsheet (CSV)",
                detail: "Page and line numbers, to open in Excel or Sheets",
                busyLabel: "Building CSV",
                ...choice("csv"),
              },
            ]}
          />
        ) : null
      }
    >
      {loading ? (
        <div className="sheetLoading" role="status">
          <span className="sheetSpinner" aria-hidden="true" /> Finding every book that matches…
        </div>
      ) : null}
      {error ? <p className="sheetNote is-error" role="alert">{error}</p> : null}
      {notice ? <p className="sheetNote is-ok" role="status">{notice}</p> : null}

      {preview ? (
        <>
          <div className="sheetStats">
            <div className="sheetStat">
              <strong>{selectedGranths.length}<small style={{ fontSize: 13, fontWeight: 500 }}> / {preview.total_granths}</small></strong>
              <span>books chosen</span>
            </div>
            <div className="sheetStat">
              <strong>{selectedMatchedPages}</strong>
              <span>matching pages</span>
            </div>
            <div className="sheetStat">
              <strong>{pdfRange}</strong>
              <span>pages in the PDF</span>
            </div>
          </div>

          {tooManyPdfPages || tooManyPdfGranths ? (
            <p className="sheetNote is-warn">
              The full PDF can hold up to {maxDownloadPages} pages from {maxCombinedGranths} books; this choice is larger.
              <button type="button" onClick={fitSelectionToPdfLimit}>Fit to limit</button>
            </p>
          ) : null}
          {noPdfCount > 0 ? (
            <p className="sheetSmall">
              {noPdfCount} {noPdfCount === 1 ? "book has" : "books have"} no uploaded PDF, so {noPdfCount === 1 ? "it goes" : "they go"} only into the line list, word list and CSV.
            </p>
          ) : null}
          {preview.truncated ? (
            <p className="sheetSmall">Only the first {preview.max_export_granth_preview} books are listed. Narrow the search to see the rest.</p>
          ) : null}

          <details className="sheetFold">
            <summary>
              <LightTableIcon name="chevron" size={14} /> PDF options
              <em>
                {contextPages ? `±${contextPages} nearby` : "Matching pages only"}
                {maxPagesPerGranth ? ` · ${maxPagesPerGranth} per book` : ""}
              </em>
            </summary>
            <div className="sheetFoldBody">
              <Stepper
                label="Nearby pages"
                hint="Pages before and after each match"
                value={contextPages}
                max={MAX_CONTEXT_PAGE_RADIUS}
                onChange={(n) => setContextPages(normalizeContextPageRadius(n))}
              />
              <Stepper
                label="Pages per book"
                hint="0 keeps every matching page"
                value={maxPagesPerGranth}
                max={maxDownloadPages}
                onChange={setMaxPagesPerGranth}
              />
            </div>
          </details>

          <section className="sheetSection">
            <div className="sheetSectionHead">
              <h3>Books</h3>
              <div className="sheetLinks">
                <button type="button" onClick={() => selectAll("all")}>All</button>
                <button type="button" onClick={() => selectAll("pdf")}>With PDF</button>
                <button type="button" onClick={() => selectAll("none")}>None</button>
              </div>
            </div>
            <div className="sheetList">
              {preview.granths.length === 0 ? (
                <div className="sheetEmpty">No book matched this search.</div>
              ) : (
                preview.granths.map((granth) => (
                  <label key={granth.granth_key} className="sheetRow">
                    <input
                      type="checkbox"
                      checked={selectedSet.has(granth.granth_key)}
                      onChange={(event) => setGranthSelected(granth.granth_key, event.target.checked)}
                    />
                    <span className="sheetRowMain">
                      <strong>{granth.granth_name || granth.pdf_name || granth.custom_id}</strong>
                    </span>
                    <span className="sheetRowSide">
                      {granth.matched_pages} {granth.matched_pages === 1 ? "page" : "pages"}
                      {granth.exportable ? null : (
                        <span className="sheetBadge" title={granth.unavailable_reason || "No uploaded PDF"}>No PDF</span>
                      )}
                    </span>
                  </label>
                ))
              )}
            </div>
          </section>

          <DownloadDeliveryDialog
            open={deliveryFormat !== null}
            title={
              deliveryFormat === "csv"
                ? "Spreadsheet (CSV)"
                : deliveryFormat === "wordlist"
                  ? "Word list"
                  : deliveryFormat === "linelist"
                    ? "Line list"
                    : "Full PDF"
            }
            fileLabel={
              deliveryFormat === "pdf" || deliveryFormat === null
                ? `${pdfGranths.length} ${pdfGranths.length === 1 ? "book" : "books"}, about ${pdfRange} PDF pages`
                : `${selectedGranths.length} ${selectedGranths.length === 1 ? "book" : "books"}, ${selectedMatchedPages} matching pages`
            }
            busy={busyFormat !== null}
            error={error}
            onClose={() => setDeliveryFormat(null)}
            onDownload={() => void runExport(deliveryFormat ?? "pdf", "download")}
            onEmail={(email) => void runExport(deliveryFormat ?? "pdf", "email", email)}
          />
        </>
      ) : null}
    </Sheet>
  );
}
