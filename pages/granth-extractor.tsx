import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import type { MappingSegment } from "@/lib/granth-mapping";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type BookItem = {
  id: number;
  title_english: string | null;
  title_display: string | null;
  author_text: string | null;
  details_text: string | null;
  book_codes: string[];
};

type BooksResponse = {
  items: BookItem[];
};

type ContextFile = {
  id: number;
  book_code: string | null;
  code_label: string;
  pdf_file_name: string | null;
  pdf_url: string | null;
  custom_id: string | null;
  page_count: number | null;
  cover_image_url: string | null;
  file_size: number | null;
  collection: string | null;
  subcollection: string | null;
};

type PageRangeSummary = {
  book_code: string | null;
  code_label: string;
  pdf_file_name: string;
  page_start: number;
  page_end: number;
  min_gatha: number | null;
  max_gatha: number | null;
  count: number;
};

type IdentifierSummary = {
  adhikar: number | null;
  label: string;
  total_gathas: number;
  min_gatha: number | null;
  max_gatha: number | null;
  book_codes: string[];
  page_ranges: PageRangeSummary[];
};

type BookContext = {
  book: BookItem;
  files: ContextFile[];
  identifiers: IdentifierSummary[];
  page_ranges: PageRangeSummary[];
  meta: {
    bookCode: string | null;
    file_count: number;
    identifier_count: number;
    mapped_row_count: number;
    total_gathas: number;
  };
};

type ResolveResponse = {
  error?: string;
  conflicts?: Array<{ gatha: number; adhikars: string[] }>;
  ranges?: Array<{ adhikar: number | null; minGatha: number; maxGatha: number }>;
  segments?: MappingSegment[];
};

type BuildMode = "combined" | "separate";

function titleForBook(book: BookItem | null) {
  if (!book) return "Selected Granth";
  return book.title_display || book.title_english || `Granth ${book.id}`;
}

function codeLabel(value: string | null | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return "All";
  return /^\d+$/.test(raw) ? raw.padStart(3, "0") : raw;
}

function fileSafe(value: string) {
  return value
    .replace(/[^a-z0-9\u0900-\u097f\u0a80-\u0aff._-]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
}

function toMB(sizeBytes: number | null) {
  if (sizeBytes == null || !Number.isFinite(sizeBytes)) return null;
  return `${(sizeBytes / 1024 / 1024).toFixed(1)} MB`;
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function rangeLabel(range: PageRangeSummary) {
  const pages = range.page_end === range.page_start ? `p.${range.page_start}` : `p.${range.page_start}-${range.page_end}`;
  const gathas =
    range.min_gatha == null
      ? ""
      : range.max_gatha === range.min_gatha
        ? ` | gatha ${range.min_gatha}`
        : ` | gatha ${range.min_gatha}-${range.max_gatha}`;
  return `${range.code_label} | ${pages}${gathas}`;
}

export default function GranthExtractorPage() {
  const [books, setBooks] = useState<BookItem[]>([]);
  const [loadingBooks, setLoadingBooks] = useState(true);
  const [bookError, setBookError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [bookId, setBookId] = useState<number | null>(null);
  const [bookCode, setBookCode] = useState("");
  const [kind, setKind] = useState<"gathas" | "pages">("gathas");
  const [spec, setSpec] = useState("");
  const [adhikar, setAdhikar] = useState("");
  const [includeCover, setIncludeCover] = useState(kind === "gathas");
  const [resolving, setResolving] = useState(false);
  const [buildingMode, setBuildingMode] = useState<BuildMode | null>(null);
  const [result, setResult] = useState<ResolveResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [context, setContext] = useState<BookContext | null>(null);
  const [contextLoading, setContextLoading] = useState(false);
  const [contextError, setContextError] = useState<string | null>(null);
  const [brokenCoverIds, setBrokenCoverIds] = useState<Record<number, boolean>>({});
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);

  useEffect(() => {
    let active = true;
    async function load() {
      setLoadingBooks(true);
      setBookError(null);
      try {
        const res = await fetch("/api/granth-mapping/books?limit=1000");
        const json = (await res.json()) as BooksResponse | { error?: string };
        if (!res.ok) throw new Error(("error" in json && json.error) || `Request failed (${res.status})`);
        if (!active) return;
        const items = (json as BooksResponse).items || [];
        setBooks(items);
        if (items[0]) {
          setBookId(items[0].id);
          setBookCode(items[0].book_codes?.[0] || "");
        }
      } catch (loadError) {
        if (active) setBookError(loadError instanceof Error ? loadError.message : String(loadError));
      } finally {
        if (active) setLoadingBooks(false);
      }
    }
    void load();
    return () => {
      active = false;
    };
  }, []);

  const filteredBooks = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return books;
    return books.filter((book) =>
      [book.title_english, book.title_display, book.author_text, book.details_text, ...(book.book_codes || [])]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(q)
    );
  }, [books, query]);

  const selectedBook = useMemo(
    () => books.find((book) => book.id === bookId) || filteredBooks[0] || null,
    [bookId, books, filteredBooks]
  );

  const selectedCodes = selectedBook?.book_codes || [];
  const segments = result?.segments || [];
  const totalPages = segments.reduce((sum, segment) => sum + segment.pages.length, 0);
  const selectedTitle = titleForBook(selectedBook);

  useEffect(() => {
    if (!selectedBook) {
      setContext(null);
      return;
    }

    let active = true;
    const controller = new AbortController();

    async function loadContext() {
      setContextLoading(true);
      setContextError(null);
      try {
        const params = new URLSearchParams({ bookId: String(selectedBook.id) });
        if (bookCode) params.set("bookCode", bookCode);
        const res = await fetch(`/api/granth-mapping/context?${params.toString()}`, {
          signal: controller.signal,
        });
        const json = (await res.json()) as BookContext | { error?: string };
        if (!res.ok) throw new Error(("error" in json && json.error) || `Request failed (${res.status})`);
        if (!active) return;
        setContext(json as BookContext);
      } catch (loadError) {
        if (!active || controller.signal.aborted) return;
        setContextError(loadError instanceof Error ? loadError.message : String(loadError));
      } finally {
        if (active && !controller.signal.aborted) setContextLoading(false);
      }
    }

    void loadContext();
    return () => {
      active = false;
      controller.abort();
    };
  }, [bookCode, selectedBook]);

  const currentIdentifier = useMemo(() => {
    if (!context || !adhikar.trim()) return null;
    const parsed = Number(adhikar);
    if (!Number.isFinite(parsed)) return null;
    return context.identifiers.find((item) => item.adhikar === parsed) || null;
  }, [adhikar, context]);

  const visibleRanges = useMemo(() => {
    if (currentIdentifier) return currentIdentifier.page_ranges;
    return context?.page_ranges || [];
  }, [context?.page_ranges, currentIdentifier]);

  const primaryFile = context?.files.find((file) => file.cover_image_url && !brokenCoverIds[file.id]) || context?.files[0] || null;

  async function resolveSelection() {
    if (!selectedBook) return;
    setResolving(true);
    setError(null);
    setResult(null);
    try {
      const params = new URLSearchParams({
        bookId: String(selectedBook.id),
        kind,
        spec,
        includeCover: includeCover ? "1" : "0",
      });
      if (bookCode) params.set("bookCode", bookCode);
      if (kind === "gathas" && adhikar.trim()) params.set("adhikar", adhikar.trim());
      const res = await fetch(`/api/granth-mapping/resolve?${params.toString()}`);
      const json = (await res.json()) as ResolveResponse;
      if (!res.ok) {
        setResult(json);
        throw new Error(json.error || `Request failed (${res.status})`);
      }
      setResult(json);
    } catch (resolveError) {
      setError(resolveError instanceof Error ? resolveError.message : String(resolveError));
    } finally {
      setResolving(false);
    }
  }

  async function buildDownload(mode: BuildMode) {
    if (!selectedBook) return;
    setBuildingMode(mode);
    setError(null);
    try {
      const res = await fetch("/api/granth-mapping/build-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bookId: selectedBook.id,
          bookCode,
          kind,
          spec,
          adhikar: kind === "gathas" && adhikar.trim() ? adhikar.trim() : null,
          includeCover,
          mode,
          title: selectedTitle,
        }),
      });

      if (!res.ok) {
        const body = await res.json().catch(async () => ({ error: await res.text() }));
        throw new Error(body?.error || `Build failed (${res.status})`);
      }

      const blob = await res.blob();
      const suffix = mode === "separate" ? "separate.zip" : "combined.pdf";
      downloadBlob(blob, `granth_${fileSafe(selectedTitle)}_${suffix}`);
    } catch (downloadError) {
      setError(downloadError instanceof Error ? downloadError.message : String(downloadError));
    } finally {
      setBuildingMode(null);
    }
  }

  function previewSegment(segment: MappingSegment, page = segment.pages[0] || 1) {
    setPdfTarget({
      pdfUrl: segment.pdfUrl,
      page,
      title: segment.pdfFileName,
    });
  }

  return (
    <main className="extractorShell">
      <div className="extractorFrame">
        <header className="extractorHeader">
          <div className="extractorHeaderText">
            <h1>Granth Page Extractor</h1>
            <div className="extractorNav">
              <Link href="/">Library</Link>
              <Link href="/search">Search pages</Link>
              <Link href="/scannable-documents">Scan status</Link>
            </div>
          </div>
          <div className="extractorHeaderStats">
            <span>{context?.meta.file_count ?? 0} PDF</span>
            <span>{context?.meta.total_gathas ?? 0} gathas</span>
            <span>{segments.length ? `${segments.length} output PDF / ${totalPages} pages` : "No output yet"}</span>
          </div>
        </header>

        {bookError ? <div className="extractorError">{bookError}</div> : null}

        <section className="extractorLayout">
          <aside className="extractorControlPanel">
            <div className="extractorPanelHeader">
              <span>Selection</span>
              {loadingBooks ? <strong>Loading</strong> : <strong>{filteredBooks.length}</strong>}
            </div>

            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search granth name"
              aria-label="Search granth name"
              className="extractorInput"
            />

            <select
              value={selectedBook?.id || ""}
              onChange={(event) => {
                const nextId = Number(event.target.value);
                const next = books.find((book) => book.id === nextId) || null;
                setBookId(nextId);
                setBookCode(next?.book_codes?.[0] || "");
                setAdhikar("");
                setResult(null);
              }}
              disabled={loadingBooks}
              className="extractorInput"
            >
              {filteredBooks.map((book) => (
                <option key={book.id} value={book.id}>
                  {titleForBook(book)}
                </option>
              ))}
            </select>

            <div className="extractorCodeGrid" aria-label="Book codes">
              <button
                type="button"
                onClick={() => {
                  setBookCode("");
                  setResult(null);
                }}
                className={!bookCode ? "isActive" : ""}
              >
                All
              </button>
              {selectedCodes.map((code) => (
                <button
                  key={code}
                  type="button"
                  onClick={() => {
                    setBookCode(code);
                    setResult(null);
                  }}
                  className={bookCode === code ? "isActive" : ""}
                >
                  {codeLabel(code)}
                </button>
              ))}
            </div>

            <div className="extractorModeGrid">
              <button
                type="button"
                onClick={() => setKind("gathas")}
                className={kind === "gathas" ? "isActive" : ""}
              >
                Gathas
              </button>
              <button
                type="button"
                onClick={() => setKind("pages")}
                className={kind === "pages" ? "isActive" : ""}
              >
                Pages
              </button>
            </div>

            <input
              value={spec}
              onChange={(event) => setSpec(event.target.value)}
              placeholder={kind === "gathas" ? "5 or 3-6, 10" : "2, 5-7, 10"}
              aria-label={kind === "gathas" ? "Gatha numbers" : "Page numbers"}
              className="extractorInput"
            />

            {kind === "gathas" ? (
              <select
                value={adhikar}
                onChange={(event) => setAdhikar(event.target.value)}
                className="extractorInput"
                aria-label="Identifier"
              >
                <option value="">Any identifier</option>
                {(context?.identifiers || []).map((item) => (
                  <option key={item.adhikar ?? "none"} value={item.adhikar ?? ""}>
                    {item.label} | {item.total_gathas} gathas
                  </option>
                ))}
              </select>
            ) : null}

            <label className="extractorCheckbox">
              <input
                type="checkbox"
                checked={includeCover}
                onChange={(event) => setIncludeCover(event.target.checked)}
              />
              <span>Include cover page</span>
            </label>

            <div className="extractorActions">
              <button
                type="button"
                onClick={resolveSelection}
                disabled={resolving || !selectedBook || !spec.trim()}
                className="extractorPrimaryButton"
              >
                {resolving ? "Resolving..." : "Resolve"}
              </button>
              <button
                type="button"
                onClick={() => void buildDownload("combined")}
                disabled={Boolean(buildingMode) || !selectedBook || !spec.trim()}
              >
                {buildingMode === "combined" ? "Building..." : "Combined PDF"}
              </button>
              <button
                type="button"
                onClick={() => void buildDownload("separate")}
                disabled={Boolean(buildingMode) || !selectedBook || !spec.trim()}
              >
                {buildingMode === "separate" ? "Building..." : "Separate ZIP"}
              </button>
            </div>

            {error ? <div className="extractorError">{error}</div> : null}
            {result?.ranges?.length ? (
              <div className="extractorRangeHint">
                {result.ranges.map((range) => (
                  <button
                    key={`${range.adhikar ?? "none"}_${range.minGatha}_${range.maxGatha}`}
                    type="button"
                    onClick={() => setAdhikar(range.adhikar == null ? "" : String(range.adhikar))}
                  >
                    id {range.adhikar ?? "none"}: {range.minGatha}-{range.maxGatha}
                  </button>
                ))}
              </div>
            ) : null}
          </aside>

          <section className="extractorContextPanel">
            <div className="extractorBookHero">
              <div className="extractorCoverPreview">
                {primaryFile?.cover_image_url && !brokenCoverIds[primaryFile.id] ? (
                  <img
                    src={primaryFile.cover_image_url}
                    alt={`${selectedTitle} cover`}
                    onError={() => setBrokenCoverIds((prev) => ({ ...prev, [primaryFile.id]: true }))}
                  />
                ) : (
                  <span>No cover</span>
                )}
              </div>
              <div className="extractorBookCopy">
                <div className="extractorEyebrow">{bookCode ? codeLabel(bookCode) : "All codes"}</div>
                <h2>{selectedTitle}</h2>
                <p>{selectedBook?.author_text || selectedBook?.details_text || "Mapped granth selection"}</p>
                <div className="extractorBookMeta">
                  <span>{context?.meta.identifier_count ?? 0} identifiers</span>
                  <span>{context?.meta.mapped_row_count ?? 0} mapped gathas</span>
                  <span>{contextLoading ? "Loading context" : contextError || "Context ready"}</span>
                </div>
              </div>
            </div>

            {context?.files.length ? (
              <div className="extractorCoverStrip" aria-label="Available PDFs">
                {context.files.map((file) => {
                  const isActive = !bookCode || file.book_code === bookCode;
                  return (
                    <button
                      key={file.id}
                      type="button"
                      onClick={() => {
                        setBookCode(file.book_code || "");
                        setResult(null);
                      }}
                      className={isActive && bookCode ? "isActive" : ""}
                    >
                      <span className="extractorTinyCover">
                        {file.cover_image_url && !brokenCoverIds[file.id] ? (
                          <img
                            src={file.cover_image_url}
                            alt=""
                            onError={() => setBrokenCoverIds((prev) => ({ ...prev, [file.id]: true }))}
                          />
                        ) : (
                          <strong>{file.code_label}</strong>
                        )}
                      </span>
                      <span>
                        <strong>{file.code_label}</strong>
                        <em>{file.page_count ? `${file.page_count} pages` : toMB(file.file_size) || "PDF"}</em>
                      </span>
                    </button>
                  );
                })}
              </div>
            ) : null}

            <div className="extractorInfoGrid">
              <section className="extractorInfoPanel">
                <div className="extractorPanelHeader">
                  <span>Identifiers</span>
                  <strong>{context?.identifiers.length ?? 0}</strong>
                </div>
                <div className="extractorIdentifierGrid">
                  {(context?.identifiers || []).slice(0, 12).map((item) => (
                    <button
                      key={item.adhikar ?? "none"}
                      type="button"
                      className={currentIdentifier?.adhikar === item.adhikar ? "isActive" : ""}
                      onClick={() => setAdhikar(item.adhikar == null ? "" : String(item.adhikar))}
                    >
                      <span>{item.label}</span>
                      <strong>{item.total_gathas}</strong>
                      <em>
                        {item.min_gatha}-{item.max_gatha}
                      </em>
                    </button>
                  ))}
                  {!contextLoading && !context?.identifiers.length ? (
                    <div className="extractorMuted">No identifier map available.</div>
                  ) : null}
                </div>
              </section>

              <section className="extractorInfoPanel">
                <div className="extractorPanelHeader">
                  <span>Mapped Page Ranges</span>
                  <strong>{visibleRanges.length}</strong>
                </div>
                <div className="extractorRangeList">
                  {visibleRanges.slice(0, 14).map((range, index) => (
                    <div key={`${range.pdf_file_name}_${range.page_start}_${index}`}>
                      <strong>{rangeLabel(range)}</strong>
                      <span>{range.pdf_file_name}</span>
                    </div>
                  ))}
                  {!contextLoading && visibleRanges.length === 0 ? (
                    <div className="extractorMuted">No mapped page ranges available.</div>
                  ) : null}
                </div>
              </section>
            </div>

            <section className="extractorResultPanel">
              <div className="extractorPanelHeader">
                <span>Resolved Output</span>
                <strong>{segments.length ? `${totalPages} pages` : "Pending"}</strong>
              </div>

              {segments.length ? (
                <div className="extractorSegmentGrid">
                  {segments.map((segment) => (
                    <article key={segment.pdfUrl}>
                      <div className="extractorSegmentHead">
                        <strong>{segment.pdfFileName}</strong>
                        <button type="button" className="inlinePdfButton" onClick={() => previewSegment(segment)}>
                          Preview
                        </button>
                      </div>
                      <div className="extractorSegmentMeta">
                        {codeLabel(segment.bookCode)} | {segment.pages.length} page{segment.pages.length === 1 ? "" : "s"}
                      </div>
                      <div className="extractorSegmentRanges">
                        {segment.ranges.slice(0, 18).map((range, index) => (
                          <button
                            key={`${range.pageStart}_${range.pageEnd}_${index}`}
                            type="button"
                            onClick={() => previewSegment(segment, range.pageStart)}
                          >
                            {range.gatha ? `id ${range.adhikar ?? "none"} / gatha ${range.gatha}: ` : ""}
                            page {range.pageStart}
                            {range.pageEnd !== range.pageStart ? `-${range.pageEnd}` : ""}
                          </button>
                        ))}
                        {segment.ranges.length > 18 ? <span>{segment.ranges.length - 18} more ranges</span> : null}
                      </div>
                    </article>
                  ))}
                </div>
              ) : (
                <div className="extractorMuted">No pages resolved yet.</div>
              )}
            </section>
          </section>
        </section>
      </div>
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </main>
  );
}
