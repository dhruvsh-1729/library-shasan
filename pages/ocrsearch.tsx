import OcrReplacePanel from "@/components/OcrReplacePanel";
import Link from "next/link";
import type { KeyboardEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

type OCRSearchResult = {
  granth_key: string;
  book_number: string;
  library_code: string | null;
  granth_name: string;
  page_number: number;
  snippet: string;
  score?: number;
  xlsx_url?: string | null;
};

type OCRGranthOption = {
  granth_key: string;
  book_number: string;
  library_code: string | null;
  granth_name: string;
  display_name: string;
  page_count: number;
  xlsx_url: string | null;
};

type GranthGroup = {
  name: string;
  granthKeys: string[];
};

type SelectionMode = "all" | "single" | "multi";

type OCRDocumentStats = {
  total_granths: number;
  total_pages: number;
};

type ReplacePageTarget = {
  granthKey: string;
  pageNumber: number;
  title: string;
};

type ReplaceGranthTarget = {
  granthKey: string;
  title: string;
};

const RESULTS_PER_PAGE = 48;

function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildResultTitle(result: Pick<OCRSearchResult, "book_number" | "library_code" | "granth_name">) {
  return `${result.book_number}${result.library_code ? ` ${result.library_code}` : ""} ${result.granth_name}`;
}

export default function OCRSearchPage() {
  const replacePanelRef = useRef<HTMLDivElement | null>(null);

  const [q, setQ] = useState("");
  const [results, setResults] = useState<OCRSearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [hasSearched, setHasSearched] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [granthOptions, setGranthOptions] = useState<OCRGranthOption[]>([]);
  const [loadingGranths, setLoadingGranths] = useState(true);
  const [selectionMode, setSelectionMode] = useState<SelectionMode>("all");
  const [nameFilter, setNameFilter] = useState("");
  const [selectedNames, setSelectedNames] = useState<string[]>([]);
  const [documentStats, setDocumentStats] = useState<OCRDocumentStats | null>(null);

  const [activePageTarget, setActivePageTarget] = useState<ReplacePageTarget | null>(null);
  const [activeGranthTarget, setActiveGranthTarget] = useState<ReplaceGranthTarget | null>(null);

  useEffect(() => {
    let active = true;

    async function loadInitialData() {
      setLoadingGranths(true);
      try {
        const granthsRes = await fetch("/api/ocr-granths?limit=10000");
        const granthsJson = (await granthsRes.json()) as { items?: OCRGranthOption[]; error?: string };
        if (!granthsRes.ok) {
          throw new Error(granthsJson.error || `Failed to load OCR granths (${granthsRes.status})`);
        }
        if (!active) return;
        setGranthOptions(granthsJson.items ?? []);

        try {
          const statsRes = await fetch("/api/ocr-document-stats");
          const statsJson = (await statsRes.json()) as OCRDocumentStats | { error?: string };
          if (statsRes.ok && active) {
            setDocumentStats(statsJson as OCRDocumentStats);
          }
        } catch (statsError) {
          console.error(statsError);
        }
      } catch (loadError) {
        if (!active) return;
        setError(loadError instanceof Error ? loadError.message : String(loadError));
      } finally {
        if (active) setLoadingGranths(false);
      }
    }

    void loadInitialData();
    return () => {
      active = false;
    };
  }, []);

  const groups = useMemo<GranthGroup[]>(() => {
    const byName = new Map<string, GranthGroup>();
    for (const row of granthOptions) {
      const name = row.display_name || row.granth_name || row.granth_key;
      const existing = byName.get(name);
      if (existing) {
        if (!existing.granthKeys.includes(row.granth_key)) existing.granthKeys.push(row.granth_key);
      } else {
        byName.set(name, { name, granthKeys: [row.granth_key] });
      }
    }

    return Array.from(byName.values()).sort((a, b) => a.name.localeCompare(b.name, "en", { sensitivity: "base" }));
  }, [granthOptions]);

  const filteredGroups = useMemo(() => {
    const keyword = nameFilter.trim().toLowerCase();
    if (!keyword) return groups;
    return groups.filter((group) => group.name.toLowerCase().includes(keyword));
  }, [groups, nameFilter]);

  const selectedGranthKeys = useMemo(() => {
    if (selectionMode === "all") return [];
    const selectedSet = new Set(selectedNames);
    const keys: string[] = [];
    for (const group of groups) {
      if (!selectedSet.has(group.name)) continue;
      keys.push(...group.granthKeys);
    }
    return Array.from(new Set(keys));
  }, [groups, selectedNames, selectionMode]);

  const selectedLabel = useMemo(() => {
    if (selectionMode === "all") return "All OCR granths";
    return `${selectedNames.length} granth name(s), ${selectedGranthKeys.length} file(s)`;
  }, [selectedGranthKeys.length, selectedNames.length, selectionMode]);

  const queryTerms = useMemo(() => {
    const terms = q
      .trim()
      .split(/\s+/)
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean);
    return Array.from(new Set(terms)).sort((a, b) => b.length - a.length);
  }, [q]);

  const totalPages = useMemo(() => {
    if (total <= 0) return 1;
    return Math.max(1, Math.ceil(total / RESULTS_PER_PAGE));
  }, [total]);

  const paginationItems = useMemo(() => {
    if (totalPages <= 1) return [] as number[];
    const start = Math.max(1, currentPage - 2);
    const end = Math.min(totalPages, currentPage + 2);
    const pages: number[] = [];
    for (let page = start; page <= end; page += 1) pages.push(page);
    return pages;
  }, [currentPage, totalPages]);

  function renderHighlightedSnippet(text: string) {
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

  function setMode(mode: SelectionMode) {
    setSelectionMode(mode);
    setError(null);
    if (mode === "all") {
      setSelectedNames([]);
      return;
    }
    if (mode === "single" && selectedNames.length > 1) {
      setSelectedNames(selectedNames.slice(0, 1));
    }
  }

  function toggleGroup(name: string) {
    if (selectionMode === "all") return;
    if (selectionMode === "single") {
      setSelectedNames((prev) => (prev[0] === name ? [] : [name]));
      return;
    }
    setSelectedNames((prev) => (prev.includes(name) ? prev.filter((value) => value !== name) : [...prev, name]));
  }

  function clearSelection() {
    setSelectedNames([]);
    setError(null);
  }

  function selectAllFiltered() {
    if (selectionMode !== "multi") return;
    setSelectedNames(filteredGroups.map((group) => group.name));
  }

  function onSearchInputKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter" && !loading && q.trim().length >= 2) {
      void runSearch(1);
    }
  }

  async function runSearch(page: number) {
    setError(null);
    if (selectionMode !== "all" && selectedGranthKeys.length === 0) {
      setError("Select at least one granth before searching.");
      return;
    }

    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set("q", q);
      params.set("limit", String(RESULTS_PER_PAGE));
      params.set("page", String(page));
      if (selectionMode !== "all" && selectedGranthKeys.length > 0) {
        params.set("granths", selectedGranthKeys.join(","));
      }

      const res = await fetch(`/api/ocr-search?${params.toString()}`);
      const json = (await res.json()) as {
        results?: OCRSearchResult[];
        total?: number;
        page?: number;
        error?: string;
      };
      if (!res.ok) {
        throw new Error(json.error || `Search failed (${res.status})`);
      }

      setResults(json.results ?? []);
      setTotal(Number(json.total ?? 0));
      setCurrentPage(Number(json.page ?? page));
      setHasSearched(true);
    } catch (searchError) {
      setError(searchError instanceof Error ? searchError.message : String(searchError));
    } finally {
      setLoading(false);
    }
  }

  function focusReplacePanel() {
    window.requestAnimationFrame(() => {
      replacePanelRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }

  function selectPageTarget(result: OCRSearchResult) {
    const title = buildResultTitle(result);
    setActivePageTarget({
      granthKey: result.granth_key,
      pageNumber: result.page_number,
      title,
    });
    setActiveGranthTarget({
      granthKey: result.granth_key,
      title,
    });
    focusReplacePanel();
  }

  function selectGranthTarget(result: OCRSearchResult) {
    const title = buildResultTitle(result);
    setActivePageTarget(null);
    setActiveGranthTarget({
      granthKey: result.granth_key,
      title,
    });
    focusReplacePanel();
  }

  return (
    <main
      style={{
        minHeight: "100vh",
        background: "radial-gradient(circle at 14% 0%, #fcefd9 0%, #f5f6ea 36%, #e8edf2 100%)",
        color: "#1f2120",
        padding: "24px 16px 40px",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ width: "100%", maxWidth: 1520, margin: "0 auto" }}>
        <header style={{ marginBottom: 18 }}>
          <h1 style={{ margin: 0, fontSize: 34, letterSpacing: "0.01em", lineHeight: 1.15 }}>OCR XLSX Search</h1>
          <div style={{ marginTop: 10, display: "flex", flexWrap: "wrap", gap: 14, fontSize: 16 }}>
            <Link href="/">Back to library</Link>
            <Link href="/search">Open PDF search</Link>
            {documentStats ? (
              <span style={{ fontWeight: 700 }}>
                OCR granths: {documentStats.total_granths} (pages: {documentStats.total_pages})
              </span>
            ) : null}
            <span style={{ opacity: 0.8 }}>48 results per page with highlighted OCR excerpts.</span>
          </div>
        </header>

        <section
          style={{
            border: "1px solid #d7d3c8",
            borderRadius: 18,
            background: "#fffefb",
            boxShadow: "0 12px 28px rgba(36, 36, 31, 0.08)",
            padding: 18,
          }}
        >
          <div style={{ display: "grid", gap: 16 }}>
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <input
                value={q}
                onChange={(event) => setQ(event.target.value)}
                onKeyDown={onSearchInputKeyDown}
                placeholder="Search OCR text..."
                style={{
                  flex: 1,
                  minWidth: 280,
                  padding: "14px 16px",
                  fontSize: 19,
                  borderRadius: 12,
                  border: "1px solid #b9c0cb",
                  background: "#fff",
                }}
              />
              <button
                onClick={() => void runSearch(1)}
                disabled={loading || q.trim().length < 2}
                style={{
                  padding: "14px 18px",
                  borderRadius: 12,
                  border: "1px solid #1f2120",
                  background: "#1f2120",
                  color: "#fff",
                  fontWeight: 700,
                  fontSize: 16,
                  cursor: loading ? "default" : "pointer",
                }}
              >
                {loading ? "Searching..." : "Search"}
              </button>
            </div>

            <div style={{ display: "grid", gap: 8 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                <strong style={{ fontSize: 16 }}>Filter mode:</strong>
                {[
                  { key: "all", label: "All granths" },
                  { key: "single", label: "Single granth" },
                  { key: "multi", label: "Multi granth" },
                ].map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    onClick={() => setMode(item.key as SelectionMode)}
                    style={{
                      padding: "8px 12px",
                      borderRadius: 999,
                      border: "1px solid #bcc4ce",
                      background: selectionMode === item.key ? "#1f2120" : "#fff",
                      color: selectionMode === item.key ? "#fff" : "#222",
                      cursor: "pointer",
                      fontSize: 15,
                    }}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
              <div style={{ fontSize: 15, opacity: 0.82 }}>{selectedLabel}</div>
            </div>

            {selectionMode !== "all" ? (
              <div style={{ border: "1px solid #d5dae2", borderRadius: 14, padding: 14, background: "#fafbfc" }}>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <input
                    value={nameFilter}
                    onChange={(event) => setNameFilter(event.target.value)}
                    placeholder="Filter granth names..."
                    style={{
                      flex: 1,
                      minWidth: 220,
                      padding: "11px 12px",
                      fontSize: 16,
                      borderRadius: 10,
                      border: "1px solid #c7cfd9",
                      background: "#fff",
                    }}
                  />
                  <button
                    type="button"
                    onClick={clearSelection}
                    disabled={selectedNames.length === 0}
                    style={{
                      padding: "10px 12px",
                      borderRadius: 10,
                      border: "1px solid #c7cfd9",
                      background: "#fff",
                      fontSize: 15,
                      cursor: "pointer",
                    }}
                  >
                    Clear
                  </button>
                  {selectionMode === "multi" ? (
                    <button
                      type="button"
                      onClick={selectAllFiltered}
                      disabled={filteredGroups.length === 0}
                      style={{
                        padding: "10px 12px",
                        borderRadius: 10,
                        border: "1px solid #c7cfd9",
                        background: "#fff",
                        fontSize: 15,
                        cursor: "pointer",
                      }}
                    >
                      Select all shown
                    </button>
                  ) : null}
                </div>

                <div
                  style={{
                    marginTop: 12,
                    maxHeight: 300,
                    overflow: "auto",
                    display: "grid",
                    gap: 8,
                    paddingRight: 4,
                  }}
                >
                  {loadingGranths ? (
                    <div style={{ opacity: 0.75, fontSize: 15 }}>Loading granth names...</div>
                  ) : filteredGroups.length === 0 ? (
                    <div style={{ opacity: 0.75, fontSize: 15 }}>No matching names.</div>
                  ) : (
                    filteredGroups.map((group) => {
                      const isSelected = selectedNames.includes(group.name);
                      return (
                        <button
                          key={group.name}
                          type="button"
                          onClick={() => toggleGroup(group.name)}
                          style={{
                            textAlign: "left",
                            padding: "12px 14px",
                            borderRadius: 12,
                            border: isSelected ? "1px solid #1f2120" : "1px solid #ced4df",
                            background: isSelected ? "#edf0f5" : "#fff",
                            cursor: "pointer",
                          }}
                        >
                          <div style={{ fontWeight: 700, fontSize: 16 }}>{group.name}</div>
                          <div style={{ fontSize: 14, opacity: 0.74 }}>
                            Includes {group.granthKeys.length} file{group.granthKeys.length > 1 ? "s" : ""}
                          </div>
                        </button>
                      );
                    })
                  )}
                </div>
              </div>
            ) : null}

            {error ? <div style={{ color: "#9f1f1f", fontWeight: 700, fontSize: 15 }}>{error}</div> : null}
          </div>
        </section>

        <section style={{ marginTop: 20 }}>
          {hasSearched ? (
            <div style={{ marginBottom: 14, fontWeight: 700, fontSize: 18 }}>
              Showing page {currentPage} of {totalPages} ({results.length} result(s) on this page, total {total}).
            </div>
          ) : null}

          {hasSearched && totalPages > 1 ? (
            <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap", alignItems: "center" }}>
              <button
                type="button"
                onClick={() => void runSearch(Math.max(1, currentPage - 1))}
                disabled={loading || currentPage <= 1}
                style={{
                  padding: "9px 12px",
                  borderRadius: 10,
                  border: "1px solid #c7cfd9",
                  background: "#fff",
                  fontSize: 15,
                  cursor: "pointer",
                }}
              >
                Previous
              </button>
              {paginationItems.map((page) => (
                <button
                  key={page}
                  type="button"
                  onClick={() => void runSearch(page)}
                  disabled={loading && currentPage === page}
                  style={{
                    minWidth: 42,
                    padding: "9px 12px",
                    borderRadius: 10,
                    border: "1px solid #c7cfd9",
                    background: currentPage === page ? "#1f2120" : "#fff",
                    color: currentPage === page ? "#fff" : "#222",
                    fontSize: 15,
                    cursor: "pointer",
                  }}
                >
                  {page}
                </button>
              ))}
              <button
                type="button"
                onClick={() => void runSearch(Math.min(totalPages, currentPage + 1))}
                disabled={loading || currentPage >= totalPages}
                style={{
                  padding: "9px 12px",
                  borderRadius: 10,
                  border: "1px solid #c7cfd9",
                  background: "#fff",
                  fontSize: 15,
                  cursor: "pointer",
                }}
              >
                Next
              </button>
            </div>
          ) : null}

          {results.length > 0 ? (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
                gap: 12,
              }}
            >
              {results.map((result, index) => {
                const title = buildResultTitle(result);
                const textViewerHref = `/ocr-text-viewer?granthKey=${encodeURIComponent(
                  result.granth_key
                )}&page=${encodeURIComponent(String(result.page_number))}&q=${encodeURIComponent(q)}`;
                const isActivePageTarget =
                  activePageTarget?.granthKey === result.granth_key && activePageTarget.pageNumber === result.page_number;
                const isActiveGranthTarget = activeGranthTarget?.granthKey === result.granth_key;

                return (
                  <article
                    key={`${result.granth_key}_${result.page_number}_${index}`}
                    style={{
                      padding: 14,
                      border: isActivePageTarget ? "2px solid #b7791f" : "1px solid #d4d9e2",
                      borderRadius: 14,
                      background: "#fff",
                      boxShadow: "0 5px 14px rgba(35, 42, 51, 0.05)",
                      display: "grid",
                      gap: 10,
                    }}
                  >
                    <div>
                      <div style={{ fontWeight: 700, lineHeight: 1.35, fontSize: 18 }}>{title}</div>
                      <div style={{ fontSize: 15, opacity: 0.78 }}>Page {result.page_number}</div>
                      {isActiveGranthTarget ? (
                        <div style={{ marginTop: 6, fontSize: 13, color: "#8b5e1a", fontWeight: 700 }}>
                          Active replace target
                        </div>
                      ) : null}
                    </div>

                    <div
                      style={{
                        whiteSpace: "pre-wrap",
                        lineHeight: 1.7,
                        fontSize: 18,
                        color: "#182230",
                      }}
                    >
                      {renderHighlightedSnippet(result.snippet)}
                    </div>

                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap", fontSize: 15 }}>
                      <a href={textViewerHref} target="_blank" rel="noreferrer">
                        Open text at page
                      </a>
                      {result.xlsx_url ? (
                        <a href={result.xlsx_url} target="_blank" rel="noreferrer">
                          Open XLSX
                        </a>
                      ) : null}
                      <button
                        type="button"
                        onClick={() => selectPageTarget(result)}
                        style={{
                          padding: "8px 10px",
                          borderRadius: 10,
                          border: "1px solid #c7cfd9",
                          background: "#fff",
                          fontSize: 15,
                          cursor: "pointer",
                        }}
                      >
                        Replace on this page
                      </button>
                      <button
                        type="button"
                        onClick={() => selectGranthTarget(result)}
                        style={{
                          padding: "8px 10px",
                          borderRadius: 10,
                          border: "1px solid #c7cfd9",
                          background: "#fff",
                          fontSize: 15,
                          cursor: "pointer",
                        }}
                      >
                        Replace in this granth
                      </button>
                    </div>
                  </article>
                );
              })}
            </div>
          ) : null}

          {hasSearched && !loading && results.length === 0 ? (
            <div style={{ opacity: 0.8, fontSize: 16 }}>No results found for this query/filter.</div>
          ) : null}
        </section>

        <div ref={replacePanelRef} style={{ marginTop: 24 }}>
          <OcrReplacePanel
            availableGranths={granthOptions}
            currentPageTarget={activePageTarget}
            currentGranthTarget={activeGranthTarget}
            initialSelectedGranthKeys={selectedGranthKeys}
            initialWord={q.trim()}
            title="Exact Word Replace Workflow"
            onApplied={async () => {
              if (hasSearched) {
                await runSearch(currentPage);
              }
            }}
          />
        </div>
      </div>
    </main>
  );
}
