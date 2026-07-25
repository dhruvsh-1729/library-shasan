import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { PDFDocument } from "pdf-lib";
import type { MappingSegment } from "@/lib/granth-mapping";

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

type ResolveResponse = {
  error?: string;
  conflicts?: Array<{ gatha: number; adhikars: string[] }>;
  ranges?: Array<{ adhikar: number | null; minGatha: number; maxGatha: number }>;
  segments?: MappingSegment[];
};

function titleForBook(book: BookItem) {
  return book.title_display || book.title_english || `Granth ${book.id}`;
}

function fileSafe(value: string) {
  return value
    .replace(/[^a-z0-9\u0900-\u097f\u0a80-\u0aff._-]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
}

function downloadBytes(bytes: Uint8Array, filename: string) {
  const payload = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(payload).set(bytes);
  const blob = new Blob([payload], { type: "application/pdf" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

async function fetchPdf(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Could not fetch PDF (${res.status})`);
  return await res.arrayBuffer();
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
  const [building, setBuilding] = useState(false);
  const [result, setResult] = useState<ResolveResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

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

  function openLinks() {
    const opened = new Set<string>();
    for (const segment of segments) {
      for (const range of segment.ranges) {
        const url = `${segment.pdfUrl}#page=${range.pageStart}`;
        if (opened.has(url)) continue;
        opened.add(url);
        window.open(url, "_blank", "noreferrer");
      }
    }
  }

  async function downloadCombined() {
    if (!selectedBook || segments.length === 0) return;
    setBuilding(true);
    setError(null);
    try {
      const out = await PDFDocument.create();
      for (const segment of segments) {
        const source = await PDFDocument.load(await fetchPdf(segment.pdfUrl));
        const pageCount = source.getPageCount();
        const indices = segment.pages
          .filter((page) => page >= 1 && page <= pageCount)
          .map((page) => page - 1);
        const copied = await out.copyPages(source, indices);
        copied.forEach((page) => out.addPage(page));
      }
      const bytes = await out.save();
      downloadBytes(bytes, `granth_${fileSafe(titleForBook(selectedBook))}_${Date.now()}.pdf`);
    } catch (downloadError) {
      setError(downloadError instanceof Error ? downloadError.message : String(downloadError));
    } finally {
      setBuilding(false);
    }
  }

  async function downloadSeparate() {
    if (!selectedBook || segments.length === 0) return;
    setBuilding(true);
    setError(null);
    try {
      for (const segment of segments) {
        const source = await PDFDocument.load(await fetchPdf(segment.pdfUrl));
        const pageCount = source.getPageCount();
        for (const range of segment.ranges) {
          const start = Math.max(1, range.pageStart);
          const end = Math.max(start, range.pageEnd);
          const pages = [];
          for (let page = start; page <= end; page += 1) {
            if (page >= 1 && page <= pageCount) pages.push(page - 1);
          }
          if (pages.length === 0) continue;
          const out = await PDFDocument.create();
          const copied = await out.copyPages(source, pages);
          copied.forEach((page) => out.addPage(page));
          const bytes = await out.save();
          const label = range.gatha
            ? `id-${range.adhikar ?? "NA"}_gatha-${range.gatha}`
            : `pages-${start}-${end}`;
          downloadBytes(bytes, `granth_${fileSafe(segment.pdfFileName)}_${label}.pdf`);
        }
      }
    } catch (downloadError) {
      setError(downloadError instanceof Error ? downloadError.message : String(downloadError));
    } finally {
      setBuilding(false);
    }
  }

  return (
    <main
      style={{
        minHeight: "100vh",
        padding: "24px 18px 44px",
        background: "#f7f4ed",
        color: "#202321",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ maxWidth: 1180, margin: "0 auto" }}>
        <header style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", marginBottom: 20 }}>
          <div>
            <h1 style={{ margin: 0, fontSize: 28 }}>Granth Page Extractor</h1>
            <div style={{ marginTop: 8, display: "flex", gap: 14, flexWrap: "wrap" }}>
              <Link href="/">Library</Link>
              <Link href="/search">Search inside pages</Link>
              <Link href="/ocrsearch">OCR search</Link>
            </div>
          </div>
          {segments.length ? (
            <div style={{ fontWeight: 700 }}>
              {segments.length} PDF{segments.length === 1 ? "" : "s"} / {totalPages} page{totalPages === 1 ? "" : "s"}
            </div>
          ) : null}
        </header>

        {bookError ? <p style={{ color: "#9d1c1c" }}>{bookError}</p> : null}

        <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 320px), 1fr))", gap: 18 }}>
          <div style={{ display: "grid", gap: 12, alignContent: "start" }}>
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search granth name"
              aria-label="Search granth name"
              style={{ padding: 10, border: "1px solid #c9c4b8", borderRadius: 8, fontSize: 15 }}
            />

            <select
              value={selectedBook?.id || ""}
              onChange={(event) => {
                const nextId = Number(event.target.value);
                const next = books.find((book) => book.id === nextId) || null;
                setBookId(nextId);
                setBookCode(next?.book_codes?.[0] || "");
              }}
              disabled={loadingBooks}
              style={{ padding: 10, border: "1px solid #c9c4b8", borderRadius: 8, fontSize: 15 }}
            >
              {filteredBooks.map((book) => (
                <option key={book.id} value={book.id}>
                  {titleForBook(book)}
                </option>
              ))}
            </select>

            <select
              value={bookCode}
              onChange={(event) => setBookCode(event.target.value)}
              style={{ padding: 10, border: "1px solid #c9c4b8", borderRadius: 8, fontSize: 15 }}
            >
              <option value="">All book codes</option>
              {selectedCodes.map((code) => (
                <option key={code} value={code}>
                  {code}
                </option>
              ))}
            </select>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
              <button
                type="button"
                onClick={() => setKind("gathas")}
                style={{
                  padding: 10,
                  border: "1px solid #1f2120",
                  borderRadius: 8,
                  background: kind === "gathas" ? "#1f2120" : "#fff",
                  color: kind === "gathas" ? "#fff" : "#1f2120",
                }}
              >
                Gathas
              </button>
              <button
                type="button"
                onClick={() => setKind("pages")}
                style={{
                  padding: 10,
                  border: "1px solid #1f2120",
                  borderRadius: 8,
                  background: kind === "pages" ? "#1f2120" : "#fff",
                  color: kind === "pages" ? "#fff" : "#1f2120",
                }}
              >
                Pages
              </button>
            </div>

            <input
              value={spec}
              onChange={(event) => setSpec(event.target.value)}
              placeholder={kind === "gathas" ? "Gatha numbers, e.g. 5 or 3-6" : "Pages, e.g. 2,5-7,10"}
              aria-label={kind === "gathas" ? "Gatha numbers" : "Page numbers"}
              style={{ padding: 10, border: "1px solid #c9c4b8", borderRadius: 8, fontSize: 15 }}
            />

            {kind === "gathas" ? (
              <input
                value={adhikar}
                onChange={(event) => setAdhikar(event.target.value)}
                placeholder="Identifier"
                aria-label="Identifier"
                style={{ padding: 10, border: "1px solid #c9c4b8", borderRadius: 8, fontSize: 15 }}
              />
            ) : null}

            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 14 }}>
              <input
                type="checkbox"
                checked={includeCover}
                onChange={(event) => setIncludeCover(event.target.checked)}
              />
              Include cover page
            </label>

            <button
              type="button"
              onClick={resolveSelection}
              disabled={resolving || !selectedBook || !spec.trim()}
              style={{
                padding: 11,
                border: 0,
                borderRadius: 8,
                background: "#2f5f50",
                color: "#fff",
                fontWeight: 700,
              }}
            >
              {resolving ? "Resolving..." : "Resolve"}
            </button>

            {error ? <div style={{ color: "#9d1c1c", whiteSpace: "pre-wrap" }}>{error}</div> : null}
            {result?.ranges?.length ? (
              <div style={{ fontSize: 13, lineHeight: 1.5, color: "#4d4f52" }}>
                {result.ranges.map((range) => (
                  <div key={`${range.adhikar ?? "none"}_${range.minGatha}_${range.maxGatha}`}>
                    id {range.adhikar ?? "none"}: gatha {range.minGatha}-{range.maxGatha}
                  </div>
                ))}
              </div>
            ) : null}
          </div>

          <div style={{ minWidth: 0 }}>
            {segments.length ? (
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 14 }}>
                <button type="button" onClick={openLinks} style={{ padding: "9px 11px", borderRadius: 8 }}>
                  Open Links
                </button>
                <button
                  type="button"
                  onClick={downloadCombined}
                  disabled={building}
                  style={{ padding: "9px 11px", borderRadius: 8 }}
                >
                  {building ? "Building..." : "Download Combined PDF"}
                </button>
                <button
                  type="button"
                  onClick={downloadSeparate}
                  disabled={building}
                  style={{ padding: "9px 11px", borderRadius: 8 }}
                >
                  Download Separate PDFs
                </button>
              </div>
            ) : null}

            <div style={{ display: "grid", gap: 12 }}>
              {segments.map((segment) => (
                <article
                  key={segment.pdfUrl}
                  style={{
                    background: "#fff",
                    border: "1px solid #ddd8cc",
                    borderRadius: 8,
                    padding: 12,
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                    <strong>{segment.pdfFileName}</strong>
                    <a href={segment.pdfUrl} target="_blank" rel="noreferrer">
                      Open PDF
                    </a>
                  </div>
                  <div style={{ marginTop: 8, fontSize: 13, color: "#4d4f52" }}>
                    Pages: {segment.pages.join(", ")}
                  </div>
                  <div style={{ marginTop: 10, display: "grid", gap: 6 }}>
                    {segment.ranges.slice(0, 30).map((range, index) => (
                      <div key={`${range.pageStart}_${range.pageEnd}_${index}`} style={{ fontSize: 13 }}>
                        {range.gatha ? (
                          <>
                            id {range.adhikar ?? "none"} / gatha {range.gatha}:{" "}
                          </>
                        ) : null}
                        page {range.pageStart}
                        {range.pageEnd !== range.pageStart ? `-${range.pageEnd}` : ""}
                        {range.anchorText ? <span style={{ opacity: 0.72 }}> · {range.anchorText}</span> : null}
                      </div>
                    ))}
                    {segment.ranges.length > 30 ? (
                      <div style={{ fontSize: 13, opacity: 0.72 }}>
                        {segment.ranges.length - 30} more range{segment.ranges.length - 30 === 1 ? "" : "s"}
                      </div>
                    ) : null}
                  </div>
                </article>
              ))}
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
