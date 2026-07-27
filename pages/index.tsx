import Link from "next/link";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { getDocumentScanLabel, getDocumentStatusLabel, type DocumentScanState } from "@/lib/document-scan-state";
import { useEffect, useMemo, useState } from "react";

type GranthItem = {
  id: number;
  file_name: string | null;
  ufs_url: string | null;
  file_size: number | null;
  custom_id: string | null;
  collection: string | null;
  subcollection: string | null;
  original_rel_path: string | null;
  cover_image_url: string | null;
  cover_image_key: string | null;
  document_status: string | null;
  scan_state: DocumentScanState;
};

type ApiResponse = {
  items: GranthItem[];
  meta: {
    count: number;
    total: number;
    pageCount: number;
    limit: number;
    offset: number;
    page: number;
    totalPages: number;
    hasNextPage: boolean;
    hasPreviousPage: boolean;
    q: string | null;
    collection: string | null;
    coverColumnAvailable: boolean;
  };
};

type DocumentStats = {
  total_documents: number;
  processed_documents: number;
  ready_documents?: number;
  review_documents?: number;
  searchable_documents?: number;
  remaining_documents?: number;
};

const BOOKS_PER_PAGE = 10;

function toMB(sizeBytes: number | null) {
  if (sizeBytes == null || !Number.isFinite(sizeBytes)) return null;
  return `${(sizeBytes / 1024 / 1024).toFixed(1)} MB`;
}

function displayTitle(row: GranthItem) {
  const raw = row.file_name ?? row.original_rel_path ?? row.custom_id ?? `Granth ${row.id}`;
  const base = raw.split(/[\\/]/).pop() ?? raw;
  let cleaned = base
    .replace(/\.pdf$/i, "")
    .replace(/\s+OCR$/i, "")
    .replace(/\s+-\s+Copy$/i, "")
    .replace(/^\d{1,4}(?:-\d{1,4})?[_\s.-]+/, "")
    .replace(/^[A-Za-z]\d{4,}[_\s.-]+/, "");

  for (let i = 0; i < 6; i += 1) {
    cleaned = cleaned.replace(/[_\s.-]+(?:\d{4,}|hr\d*|std|ocr|ocred|needs[_\s-]*ocr)$/i, "");
  }

  return cleaned
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function pageButtons(currentPage: number, totalPages: number) {
  const start = Math.max(1, currentPage - 2);
  const end = Math.min(totalPages, currentPage + 2);
  const pages: number[] = [];
  for (let page = start; page <= end; page += 1) pages.push(page);
  return pages;
}

export default function HomePage() {
  const [items, setItems] = useState<GranthItem[]>([]);
  const [meta, setMeta] = useState<ApiResponse["meta"] | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [coverColumnAvailable, setCoverColumnAvailable] = useState(true);
  const [brokenCoverIds, setBrokenCoverIds] = useState<Record<number, boolean>>({});
  const [documentStats, setDocumentStats] = useState<DocumentStats | null>(null);
  const [nameQuery, setNameQuery] = useState("");
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    const timer = window.setTimeout(() => {
      async function load() {
        setLoading(true);
        setError(null);
        try {
          const params = new URLSearchParams();
          params.set("limit", String(BOOKS_PER_PAGE));
          params.set("page", String(page));
          if (nameQuery.trim()) params.set("q", nameQuery.trim());

          const granthRes = await fetch(`/api/granths?${params.toString()}`, {
            signal: controller.signal,
          });
          const granthJson = (await granthRes.json()) as ApiResponse | { error?: string };

          if (!granthRes.ok) {
            throw new Error(
              ("error" in granthJson && granthJson.error) || `Request failed (${granthRes.status})`
            );
          }

          if (!active) return;
          const payload = granthJson as ApiResponse;
          setItems(payload.items ?? []);
          setMeta(payload.meta ?? null);
          setCoverColumnAvailable(payload.meta?.coverColumnAvailable ?? true);
        } catch (loadError) {
          if (!active || controller.signal.aborted) return;
          setError(loadError instanceof Error ? loadError.message : String(loadError));
        } finally {
          if (active && !controller.signal.aborted) setLoading(false);
        }
      }

      void load();
    }, 220);

    return () => {
      active = false;
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [nameQuery, page]);

  useEffect(() => {
    let active = true;

    async function loadStats() {
      try {
        const statsRes = await fetch("/api/document-stats");
        const statsJson = (await statsRes.json()) as DocumentStats | { error?: string };
        if (statsRes.ok && active) {
          setDocumentStats(statsJson as DocumentStats);
        }
      } catch (statsError) {
        console.error(statsError);
      }
    }

    void loadStats();
    return () => {
      active = false;
    };
  }, []);

  const total = meta?.total ?? 0;
  const totalPages = meta?.totalPages ?? 1;
  const currentPage = meta?.page ?? page;
  const rangeStart = total === 0 ? 0 : (meta?.offset ?? 0) + 1;
  const rangeEnd = total === 0 ? 0 : Math.min((meta?.offset ?? 0) + items.length, total);
  const searchableDocuments = documentStats?.searchable_documents ?? documentStats?.processed_documents ?? 0;
  const remainingDocuments =
    documentStats?.remaining_documents ??
    (documentStats ? Math.max(0, documentStats.total_documents - searchableDocuments) : 0);

  const pagination = useMemo(
    () => pageButtons(currentPage, totalPages),
    [currentPage, totalPages]
  );

  function goToPage(targetPage: number) {
    const nextPage = Math.max(1, Math.min(totalPages, targetPage));
    if (nextPage !== page) setPage(nextPage);
  }

  function onQueryChange(value: string) {
    setNameQuery(value);
    setPage(1);
  }

  return (
    <main className="libraryShell">
      <div className="libraryFrame">
        <header className="libraryHeader">
          <div className="libraryHeaderText">
            <h1 className="libraryTitle">Granth Library</h1>
            <div className="libraryStatusLine">
              {error ? "Could not load granths" : loading && items.length === 0 ? "Loading granths..." : `${rangeStart}-${rangeEnd} of ${total}`}
              {documentStats ? (
                <span className="libraryStats">
                  Searchable {searchableDocuments}/{documentStats.total_documents}
                  {remainingDocuments > 0 ? `, needs scan ${remainingDocuments}` : ""}
                </span>
              ) : null}
            </div>
          </div>
          <nav className="libraryNav" aria-label="Library tools">
            <Link href="/search">Search pages</Link>
            <Link href="/granth-extractor">Extractor</Link>
            <Link href="/scannable-documents">Scan status</Link>
          </nav>
        </header>

        <section className="libraryToolbar" aria-label="Library search and pagination">
          <div className="librarySearchBox">
            <input
              value={nameQuery}
              onChange={(event) => onQueryChange(event.target.value)}
              placeholder="Search granth name"
              aria-label="Search granth name"
              className="librarySearchInput"
            />
            {nameQuery ? (
              <button
                type="button"
                className="libraryClearButton"
                onClick={() => onQueryChange("")}
                aria-label="Clear search"
              >
                Clear
              </button>
            ) : null}
          </div>

          <div className="libraryPager" aria-label="Book pages">
            <button
              type="button"
              onClick={() => goToPage(currentPage - 1)}
              disabled={loading || currentPage <= 1}
              className="libraryPageButton"
            >
              Previous
            </button>
            <div className="libraryPageNumbers">
              {pagination.map((pageNumber) => (
                <button
                  key={pageNumber}
                  type="button"
                  onClick={() => goToPage(pageNumber)}
                  disabled={loading && currentPage === pageNumber}
                  className={`libraryPageButton libraryNumberButton${
                    currentPage === pageNumber ? " isActive" : ""
                  }`}
                >
                  {pageNumber}
                </button>
              ))}
            </div>
            <button
              type="button"
              onClick={() => goToPage(currentPage + 1)}
              disabled={loading || currentPage >= totalPages}
              className="libraryPageButton"
            >
              Next
            </button>
          </div>
        </section>

        {error ? <div className="libraryError">{error}</div> : null}

        <section className="libraryGrid" aria-busy={loading}>
          {!error && items.length === 0 && !loading ? (
            <div className="libraryEmpty">No granths found.</div>
          ) : null}

          {items.map((row) => {
            const showCover = Boolean(row.cover_image_url) && !brokenCoverIds[row.id];
            const title = displayTitle(row);
            const sizeLabel = toMB(row.file_size);

            return (
              <article key={row.id} className="libraryCard">
                <div className="libraryCover">
                  {showCover ? (
                    <img
                      src={row.cover_image_url ?? ""}
                      alt={`${title} cover`}
                      className="libraryCoverImage"
                      loading="lazy"
                      onError={() => setBrokenCoverIds((prev) => ({ ...prev, [row.id]: true }))}
                    />
                  ) : (
                    <div className="libraryCoverFallback">No cover</div>
                  )}
                </div>

                <div className="libraryCardBody">
                  <div className="libraryCardTop">
                    <div title={title} className="libraryCardTitle">
                      {title || `Granth ${row.id}`}
                    </div>
                    <span
                      className={`libraryScanBadge is-${row.scan_state}`}
                      title={getDocumentStatusLabel(row.document_status, row.scan_state)}
                    >
                      {getDocumentScanLabel(row.scan_state)}
                    </span>
                  </div>
                  <div className="libraryCardMeta">
                    <span>{row.collection ?? "-"}</span>
                    <span>{row.subcollection ?? "-"}</span>
                    <span>{sizeLabel ?? "-"}</span>
                  </div>
                  {row.ufs_url ? (
                    <button
                      type="button"
                      className="libraryPdfLink inlinePdfButton"
                      onClick={() => setPdfTarget({ pdfUrl: row.ufs_url ?? "", title, page: 1 })}
                    >
                      Open PDF
                    </button>
                  ) : null}
                </div>
              </article>
            );
          })}
        </section>

        <footer className="libraryFooter">
          <span>Page {currentPage} of {totalPages}</span>
          {!coverColumnAvailable ? (
            <span className="libraryWarning">Cover columns missing in DB.</span>
          ) : null}
        </footer>
      </div>
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </main>
  );
}
