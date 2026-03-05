import OcrReplacePanel from "@/components/OcrReplacePanel";
import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useMemo, useRef, useState } from "react";

type GranthMeta = {
  granth_key: string;
  book_number: string;
  library_code: string | null;
  granth_name: string;
  xlsx_url: string | null;
};

type PageRow = {
  page_number: number;
  content: string;
};

type OccurrenceItem = {
  pageNumber: number;
  count: number;
  excerpt: string;
};

function readSingleQuery(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value ?? "";
}

function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildOccurrenceSummary(content: string, terms: string[], maxChars = 120) {
  if (!content || terms.length === 0) return null;

  const pattern = new RegExp(`(${terms.map(escapeRegExp).join("|")})`, "gi");
  let firstIndex = -1;
  let firstLength = 0;
  let count = 0;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(content)) !== null) {
    count += 1;
    if (firstIndex < 0) {
      firstIndex = typeof match.index === "number" ? match.index : 0;
      firstLength = match[0]?.length ?? 0;
    }
  }

  if (count === 0 || firstIndex < 0) return null;

  const normalized = content.replace(/\s+/g, " ").trim();
  const compactIndex = normalized.toLowerCase().indexOf((content.slice(firstIndex, firstIndex + firstLength) || "").toLowerCase());
  const effectiveIndex = compactIndex >= 0 ? compactIndex : firstIndex;
  const start = Math.max(0, effectiveIndex - Math.floor((maxChars - firstLength) / 2));
  const end = Math.min(normalized.length, effectiveIndex + firstLength + Math.floor((maxChars - firstLength) / 2));
  let excerpt = normalized.slice(start, end).trim();
  if (start > 0) excerpt = `…${excerpt}`;
  if (end < normalized.length) excerpt = `${excerpt}…`;

  return { count, excerpt };
}

export default function OCRTextViewerPage() {
  const router = useRouter();
  const rowRefs = useRef<Record<number, HTMLTableRowElement | null>>({});

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [granth, setGranth] = useState<GranthMeta | null>(null);
  const [rows, setRows] = useState<PageRow[]>([]);
  const [activePageNumber, setActivePageNumber] = useState<number | null>(null);
  const [refreshToken, setRefreshToken] = useState(0);

  const granthKey = readSingleQuery(router.query.granthKey);
  const pageRaw = readSingleQuery(router.query.page);
  const targetPage = pageRaw ? Number(pageRaw) : null;
  const q = readSingleQuery(router.query.q);

  const queryTerms = useMemo(() => {
    const terms = q
      .trim()
      .split(/\s+/)
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean);
    return Array.from(new Set(terms)).sort((a, b) => b.length - a.length);
  }, [q]);

  const occurrenceItems = useMemo<OccurrenceItem[]>(() => {
    if (queryTerms.length === 0) return [];
    return rows
      .map((row) => {
        const summary = buildOccurrenceSummary(row.content, queryTerms);
        if (!summary) return null;
        return {
          pageNumber: row.page_number,
          count: summary.count,
          excerpt: summary.excerpt,
        };
      })
      .filter((value): value is OccurrenceItem => Boolean(value));
  }, [queryTerms, rows]);

  const totalOccurrenceCount = useMemo(
    () => occurrenceItems.reduce((sum, item) => sum + item.count, 0),
    [occurrenceItems]
  );

  const currentRowIndex = useMemo(() => {
    if (activePageNumber == null) return null;
    const index = rows.findIndex((row) => row.page_number === activePageNumber);
    return index >= 0 ? index : null;
  }, [activePageNumber, rows]);

  function renderHighlightedText(text: string) {
    if (!text || queryTerms.length === 0) return text;
    const pattern = new RegExp(`(${queryTerms.map(escapeRegExp).join("|")})`, "gi");
    const lowerTermSet = new Set(queryTerms);
    const parts = text.split(pattern);

    return parts.map((part, idx) => {
      if (!part) return null;
      const isMatch = lowerTermSet.has(part.toLowerCase());
      if (!isMatch) return <span key={idx}>{part}</span>;
      return (
        <mark
          key={idx}
          style={{
            background: "#fff100",
            color: "#111",
            padding: "0 2px",
            borderRadius: 3,
            fontWeight: 700,
          }}
        >
          {part}
        </mark>
      );
    });
  }

  function scrollToPage(pageNumber: number, updateUrl = true) {
    setActivePageNumber(pageNumber);
    if (updateUrl) {
      void router.replace(
        {
          pathname: router.pathname,
          query: {
            ...router.query,
            granthKey,
            page: String(pageNumber),
            ...(q ? { q } : {}),
          },
        },
        undefined,
        { shallow: true, scroll: false }
      );
    }
  }

  useEffect(() => {
    if (!router.isReady) return;
    if (!granthKey) {
      setError("Missing granthKey query parameter.");
      return;
    }

    let active = true;
    async function loadRows() {
      setLoading(true);
      setError(null);

      try {
        const res = await fetch(`/api/ocr-granth-pages?granthKey=${encodeURIComponent(granthKey)}`);
        const body = await res.json();
        if (!res.ok) {
          throw new Error(body?.error || `Failed to load OCR rows (${res.status})`);
        }

        const rowsData = Array.isArray(body.rows) ? (body.rows as PageRow[]) : [];
        if (!active) return;
        setGranth(body.granth as GranthMeta);
        setRows(rowsData);
      } catch (loadError) {
        if (!active) return;
        setError(loadError instanceof Error ? loadError.message : String(loadError));
      } finally {
        if (active) setLoading(false);
      }
    }

    void loadRows();
    return () => {
      active = false;
    };
  }, [router.isReady, granthKey, refreshToken]);

  useEffect(() => {
    if (rows.length === 0) return;
    if (Number.isFinite(targetPage) && rows.some((row) => row.page_number === targetPage)) {
      setActivePageNumber(targetPage);
      return;
    }
    if (activePageNumber == null) {
      setActivePageNumber(rows[0].page_number);
    }
  }, [activePageNumber, rows, targetPage]);

  useEffect(() => {
    if (activePageNumber == null) return;
    const element = rowRefs.current[activePageNumber];
    if (element) {
      element.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }, [activePageNumber, rows.length]);

  const title = useMemo(() => {
    if (!granth) return granthKey || "OCR Text Viewer";
    return `${granth.book_number}${granth.library_code ? ` ${granth.library_code}` : ""} ${granth.granth_name}`;
  }, [granth, granthKey]);

  return (
    <main
      style={{
        minHeight: "100vh",
        background: "radial-gradient(circle at 8% 0%, #f8efe0 0%, #f2f4ec 40%, #e9edf2 100%)",
        color: "#1f2120",
        padding: "20px 16px 28px",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ maxWidth: 1520, margin: "0 auto" }}>
        <header style={{ marginBottom: 16 }}>
          <h1 style={{ margin: 0, fontSize: 30, lineHeight: 1.15 }}>OCR Granth Text Viewer</h1>
          <div style={{ marginTop: 10, display: "flex", gap: 14, flexWrap: "wrap", fontSize: 16 }}>
            <Link href="/ocrsearch">Back to OCR search</Link>
            {granth?.xlsx_url ? (
              <a href={granth.xlsx_url} target="_blank" rel="noreferrer">
                Open XLSX
              </a>
            ) : null}
          </div>
          <div style={{ marginTop: 10, fontSize: 16, opacity: 0.85, lineHeight: 1.6 }}>
            Granth: <strong>{title}</strong>
            {activePageNumber != null ? (
              <>
                {" "}| Focus page: <strong>{activePageNumber}</strong>
              </>
            ) : null}
            {queryTerms.length > 0 ? (
              <>
                {" "}| Matching pages in this granth: <strong>{occurrenceItems.length}</strong>
                {" "}| Total visible matches: <strong>{totalOccurrenceCount}</strong>
              </>
            ) : null}
          </div>
        </header>

        <div style={{ marginBottom: 18 }}>
          <OcrReplacePanel
            currentPageTarget={
              granthKey && activePageNumber != null
                ? {
                    granthKey,
                    pageNumber: activePageNumber,
                    title,
                  }
                : null
            }
            currentGranthTarget={granthKey ? { granthKey, title } : null}
            initialWord={q.trim()}
            title="Replace In OCR Text"
            onApplied={async () => {
              setRefreshToken((value) => value + 1);
            }}
          />
        </div>

        {loading ? <p style={{ fontSize: 16 }}>Loading granth pages...</p> : null}
        {error ? <p style={{ color: "#9f1f1f", fontWeight: 700, fontSize: 16 }}>{error}</p> : null}

        {!loading && !error && rows.length > 0 ? (
          <div style={{ display: "flex", flexWrap: "wrap", gap: 16, alignItems: "flex-start" }}>
            <aside
              style={{
                flex: "0 0 320px",
                display: "grid",
                gap: 14,
                alignSelf: "stretch",
              }}
            >
              <section
                style={{
                  border: "1px solid #d4d9e2",
                  borderRadius: 14,
                  background: "#fff",
                  boxShadow: "0 8px 20px rgba(35, 42, 51, 0.08)",
                  overflow: "hidden",
                }}
              >
                <div style={{ padding: "12px 14px", borderBottom: "1px solid #e5e7eb" }}>
                  <div style={{ fontWeight: 700, fontSize: 18 }}>Occurrences In This Granth</div>
                  <div style={{ marginTop: 6, fontSize: 14, opacity: 0.78, lineHeight: 1.55 }}>
                    {queryTerms.length > 0
                      ? `Click a page to jump directly to that occurrence inside ${title}.`
                      : "Open this viewer from OCR search with a word query to see matching pages here."}
                  </div>
                </div>

                <div style={{ maxHeight: "75vh", overflow: "auto", padding: 10, display: "grid", gap: 8 }}>
                  {queryTerms.length === 0 ? (
                    <div style={{ fontSize: 15, opacity: 0.72 }}>No search word was provided for occurrence navigation.</div>
                  ) : occurrenceItems.length === 0 ? (
                    <div style={{ fontSize: 15, opacity: 0.72 }}>This granth has no visible occurrence for the current query.</div>
                  ) : (
                    occurrenceItems.map((item) => {
                      const isActive = item.pageNumber === activePageNumber;
                      return (
                        <button
                          key={item.pageNumber}
                          type="button"
                          onClick={() => scrollToPage(item.pageNumber)}
                          style={{
                            textAlign: "left",
                            padding: "10px 12px",
                            borderRadius: 12,
                            border: isActive ? "1px solid #b7791f" : "1px solid #d8dde6",
                            background: isActive ? "#fff4d5" : "#fff",
                            cursor: "pointer",
                            display: "grid",
                            gap: 6,
                          }}
                        >
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center" }}>
                            <strong style={{ fontSize: 15 }}>Page {item.pageNumber}</strong>
                            <span style={{ fontSize: 13, opacity: 0.78 }}>
                              {item.count} match{item.count === 1 ? "" : "es"}
                            </span>
                          </div>
                          <div style={{ fontSize: 15, lineHeight: 1.6, color: "#344054" }}>{renderHighlightedText(item.excerpt)}</div>
                        </button>
                      );
                    })
                  )}
                </div>
              </section>
            </aside>

            <section
              style={{
                flex: "1 1 780px",
                minWidth: 0,
                border: "1px solid #d4d9e2",
                borderRadius: 14,
                background: "#fff",
                overflow: "hidden",
                boxShadow: "0 8px 20px rgba(35, 42, 51, 0.08)",
              }}
            >
              <div style={{ padding: "12px 14px", borderBottom: "1px solid #e0e4ec", fontSize: 15, lineHeight: 1.6 }}>
                Rows: {rows.length}. {currentRowIndex != null ? `Focused row ${currentRowIndex + 1}.` : "Requested page not found."}
              </div>

              <div style={{ maxHeight: "78vh", overflow: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", tableLayout: "fixed" }}>
                  <thead>
                    <tr style={{ background: "#f7f9fc" }}>
                      <th
                        style={{
                          borderBottom: "1px solid #d9deea",
                          padding: "10px 12px",
                          textAlign: "left",
                          width: 96,
                          fontSize: 14,
                        }}
                      >
                        Page
                      </th>
                      <th
                        style={{
                          borderBottom: "1px solid #d9deea",
                          padding: "10px 12px",
                          textAlign: "left",
                          fontSize: 14,
                        }}
                      >
                        Text
                      </th>
                    </tr>
                  </thead>

                  <tbody>
                    {rows.map((row, idx) => {
                      const isActive = row.page_number === activePageNumber;
                      return (
                        <tr
                          key={`${row.page_number}_${idx}`}
                          ref={(element) => {
                            rowRefs.current[row.page_number] = element;
                          }}
                          style={{
                            background: isActive ? "#fff0c4" : idx % 2 === 0 ? "#fff" : "#fcfdff",
                          }}
                        >
                          <td
                            style={{
                              borderBottom: "1px solid #eef1f6",
                              padding: "10px 12px",
                              verticalAlign: "top",
                              fontSize: 15,
                              fontWeight: isActive ? 700 : 500,
                              color: "#2d3748",
                            }}
                          >
                            <button
                              type="button"
                              onClick={() => scrollToPage(row.page_number)}
                              style={{
                                padding: 0,
                                border: "none",
                                background: "transparent",
                                font: "inherit",
                                color: "inherit",
                                cursor: "pointer",
                              }}
                            >
                              {row.page_number}
                            </button>
                          </td>
                          <td
                            style={{
                              borderBottom: "1px solid #eef1f6",
                              padding: "10px 12px",
                              whiteSpace: "pre-wrap",
                              lineHeight: 1.75,
                              fontSize: 20,
                              color: "#111827",
                            }}
                          >
                            {renderHighlightedText(row.content)}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        ) : null}
      </div>
    </main>
  );
}
