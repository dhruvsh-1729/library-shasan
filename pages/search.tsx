import {
  OCR_SEARCH_MODE_OPTIONS,
  type OCRSearchMode,
  findOCRSearchMatches,
  getOCRSearchModeLabel,
  parseOCRSearchMode,
} from "@/lib/ocr-search";
import Link from "next/link";
import type { KeyboardEvent, ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";

type SearchResult = {
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  page_number: number;
  snippet: string;
  score?: number;
  occurrence_count?: number;
  open_pdf_url: string;
  csv_url?: string | null;
};

type GranthOption = {
  custom_id: string;
  pdf_name: string | null;
  display_name: string;
};

type GranthGroup = {
  name: string;
  customIds: string[];
  pdfNames: string[];
};

type SelectionMode = "all" | "single" | "multi";

type DocumentStats = {
  total_documents: number;
  processed_documents: number;
  ready_documents?: number;
  review_documents?: number;
  searchable_documents?: number;
  remaining_documents?: number;
};

const RESULTS_PER_PAGE = 20;

export default function SearchPage() {
  const [q, setQ] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalIsExact, setTotalIsExact] = useState(true);
  const [hasSearched, setHasSearched] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchMode, setSearchMode] = useState<OCRSearchMode>("exact_word");

  const [granthOptions, setGranthOptions] = useState<GranthOption[]>([]);
  const [loadingGranths, setLoadingGranths] = useState(true);
  const [selectionMode, setSelectionMode] = useState<SelectionMode>("all");
  const [nameFilter, setNameFilter] = useState("");
  const [selectedNames, setSelectedNames] = useState<string[]>([]);
  const [documentStats, setDocumentStats] = useState<DocumentStats | null>(null);

  useEffect(() => {
    let active = true;

    async function loadInitialData() {
      setLoadingGranths(true);
      try {
        const granthsRes = await fetch("/api/search-granths?limit=5000");
        const granthsJson = (await granthsRes.json()) as { items?: GranthOption[]; error?: string };

        if (!granthsRes.ok) {
          throw new Error(granthsJson.error || `Failed to load granths (${granthsRes.status})`);
        }
        if (!active) return;
        setGranthOptions(granthsJson.items ?? []);

        try {
          const statsRes = await fetch("/api/document-stats");
          const statsJson = (await statsRes.json()) as DocumentStats | { error?: string };
          if (statsRes.ok) {
            setDocumentStats(statsJson as DocumentStats);
          } else {
            console.error(("error" in statsJson && statsJson.error) || "Failed to load document stats");
          }
        } catch (statsErr) {
          console.error(statsErr);
        }
      } catch (e) {
        if (!active) return;
        console.error(e);
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
      const name = row.display_name || row.pdf_name || row.custom_id;
      const existing = byName.get(name);
      if (existing) {
        if (!existing.customIds.includes(row.custom_id)) existing.customIds.push(row.custom_id);
        if (row.pdf_name && !existing.pdfNames.includes(row.pdf_name)) existing.pdfNames.push(row.pdf_name);
      } else {
        byName.set(name, {
          name,
          customIds: [row.custom_id],
          pdfNames: row.pdf_name ? [row.pdf_name] : [],
        });
      }
    }

    return Array.from(byName.values()).sort((a, b) => a.name.localeCompare(b.name, "en", { sensitivity: "base" }));
  }, [granthOptions]);

  const filteredGroups = useMemo(() => {
    const keyword = nameFilter.trim().toLowerCase();
    if (!keyword) return groups;
    return groups.filter((g) => g.name.toLowerCase().includes(keyword));
  }, [groups, nameFilter]);

  const selectedCustomIds = useMemo(() => {
    if (selectionMode === "all") return [];
    const selectedSet = new Set(selectedNames);
    const ids: string[] = [];

    for (const group of groups) {
      if (!selectedSet.has(group.name)) continue;
      ids.push(...group.customIds);
    }

    return Array.from(new Set(ids));
  }, [groups, selectedNames, selectionMode]);

  const selectedLabel = useMemo(() => {
    if (selectionMode === "all") return "All granths";
    return `${selectedNames.length} granth name(s), ${selectedCustomIds.length} PDF(s)`;
  }, [selectedCustomIds.length, selectedNames.length, selectionMode]);

  const totalPages = useMemo(() => {
    if (total <= 0) return 1;
    return Math.max(1, Math.ceil(total / RESULTS_PER_PAGE));
  }, [total]);
  const searchableDocuments = documentStats?.searchable_documents ?? documentStats?.processed_documents ?? 0;
  const remainingDocuments =
    documentStats?.remaining_documents ??
    (documentStats ? Math.max(0, documentStats.total_documents - searchableDocuments) : 0);

  const paginationItems = useMemo(() => {
    if (totalPages <= 1) return [] as number[];
    const start = Math.max(1, currentPage - 2);
    const end = Math.min(totalPages, currentPage + 2);
    const pages: number[] = [];
    for (let page = start; page <= end; page += 1) pages.push(page);
    return pages;
  }, [currentPage, totalPages]);

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

    setSelectedNames((prev) => (prev.includes(name) ? prev.filter((x) => x !== name) : [...prev, name]));
  }

  function clearSelection() {
    setSelectedNames([]);
    setError(null);
  }

  function selectAllFiltered() {
    if (selectionMode !== "multi") return;
    const all = filteredGroups.map((g) => g.name);
    setSelectedNames(all);
  }

  function onSearchInputKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter" && !loading && q.trim().length >= 2) {
      void run(1);
    }
  }

  function renderHighlightedSnippet(text: string) {
    const matches = findOCRSearchMatches(text, q, searchMode);
    if (!text || matches.length === 0) return text;

    const parts: ReactNode[] = [];
    let cursor = 0;

    matches.forEach((match, idx) => {
      if (match.start > cursor) {
        parts.push(text.slice(cursor, match.start));
      }
      parts.push(
        <mark
          key={`${match.start}_${idx}`}
          style={{
            background: "#fff100",
            color: "#111",
            padding: "0 2px",
            borderRadius: 2,
            fontWeight: 700,
          }}
        >
          {text.slice(match.start, match.end)}
        </mark>
      );
      cursor = match.end;
    });

    if (cursor < text.length) {
      parts.push(text.slice(cursor));
    }

    return parts.map((part, idx) => <span key={idx}>{part}</span>);
  }

  async function run(page: number) {
    setError(null);
    if (selectionMode !== "all" && selectedCustomIds.length === 0) {
      setError("Select at least one granth name before searching.");
      return;
    }

    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set("q", q);
      params.set("limit", String(RESULTS_PER_PAGE));
      params.set("page", String(page));
      params.set("matchMode", searchMode);
      if (selectionMode !== "all" && selectedCustomIds.length > 0) {
        params.set("granths", selectedCustomIds.join(","));
      }

      const res = await fetch(`/api/search?${params.toString()}`);
      const json = (await res.json()) as {
        results?: SearchResult[];
        total?: number;
        page?: number;
        total_is_exact?: boolean;
        match_mode?: string;
        error?: string;
      };
      if (!res.ok) {
        throw new Error(json.error || `Search failed (${res.status})`);
      }
      setResults(json.results ?? []);
      setTotal(Number(json.total ?? (json.results?.length ?? 0)));
      setCurrentPage(Number(json.page ?? page));
      setTotalIsExact(json.total_is_exact !== false);
      setSearchMode(parseOCRSearchMode(json.match_mode));
      setHasSearched(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <main
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at 14% 0%, #fcefd9 0%, #f5f6ea 36%, #e8edf2 100%)",
        color: "#1f2120",
        padding: "24px 16px 40px",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ width: "100%", margin: "0 auto" }}>
        <header style={{ marginBottom: 16 }}>
          <h1 style={{ margin: 0, fontSize: 30, letterSpacing: "0.01em" }}>Granth Search</h1>
          <div style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 12 }}>
            <Link href="/">Back to library</Link>
            <Link href="/scannable-documents">Scan status</Link>
            {documentStats ? (
              <span style={{ fontWeight: 700 }}>
                Searchable documents: {searchableDocuments}/{documentStats.total_documents}
                {remainingDocuments > 0 ? `, needs scan ${remainingDocuments}` : ""}
              </span>
            ) : null}
            <span style={{ opacity: 0.78 }}>Filter by granth names and search inside selected PDFs.</span>
          </div>
        </header>

        <section
          style={{
            border: "1px solid #d7d3c8",
            borderRadius: 16,
            background: "#fffefb",
            boxShadow: "0 12px 28px rgba(36, 36, 31, 0.08)",
            padding: 16,
          }}
        >
          <div style={{ display: "grid", gap: 14 }}>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                onKeyDown={onSearchInputKeyDown}
                placeholder="Search text..."
                style={{
                  flex: 1,
                  minWidth: 260,
                  padding: "12px 14px",
                  fontSize: 16,
                  borderRadius: 10,
                  border: "1px solid #b9c0cb",
                  background: "#fff",
                }}
              />
              <button
                onClick={() => void run(1)}
                disabled={loading || q.trim().length < 2}
                style={{
                  padding: "12px 16px",
                  borderRadius: 10,
                  border: "1px solid #1f2120",
                  background: "#1f2120",
                  color: "#fff",
                  fontWeight: 700,
                  cursor: loading ? "default" : "pointer",
                }}
              >
                {loading ? "Searching..." : "Search"}
              </button>
            </div>

            <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
              <strong style={{ fontSize: 15 }}>Match:</strong>
              {OCR_SEARCH_MODE_OPTIONS.map((option) => (
                <button
                  key={option.mode}
                  type="button"
                  onClick={() => setSearchMode(option.mode)}
                  title={option.description}
                  style={{
                    padding: "8px 12px",
                    borderRadius: 999,
                    border: "1px solid #bcc4ce",
                    background: searchMode === option.mode ? "#1f2120" : "#fff",
                    color: searchMode === option.mode ? "#fff" : "#222",
                    cursor: "pointer",
                    fontSize: 14,
                  }}
                >
                  {option.label}
                </button>
              ))}
              <span style={{ fontSize: 14, opacity: 0.78 }}>
                {OCR_SEARCH_MODE_OPTIONS.find((option) => option.mode === searchMode)?.description}
              </span>
            </div>

            <div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                <strong>Filter mode:</strong>
                <button
                  type="button"
                  onClick={() => setMode("all")}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 999,
                    border: "1px solid #bcc4ce",
                    background: selectionMode === "all" ? "#1f2120" : "#fff",
                    color: selectionMode === "all" ? "#fff" : "#222",
                    cursor: "pointer",
                  }}
                >
                  All granths
                </button>
                <button
                  type="button"
                  onClick={() => setMode("single")}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 999,
                    border: "1px solid #bcc4ce",
                    background: selectionMode === "single" ? "#1f2120" : "#fff",
                    color: selectionMode === "single" ? "#fff" : "#222",
                    cursor: "pointer",
                  }}
                >
                  Single granth
                </button>
                <button
                  type="button"
                  onClick={() => setMode("multi")}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 999,
                    border: "1px solid #bcc4ce",
                    background: selectionMode === "multi" ? "#1f2120" : "#fff",
                    color: selectionMode === "multi" ? "#fff" : "#222",
                    cursor: "pointer",
                  }}
                >
                  Multi granth
                </button>
              </div>

              <div style={{ marginTop: 8, fontSize: 13, opacity: 0.8 }}>{selectedLabel}</div>
            </div>

            {selectionMode !== "all" ? (
              <div style={{ border: "1px solid #d5dae2", borderRadius: 12, padding: 12, background: "#fafbfc" }}>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <input
                    value={nameFilter}
                    onChange={(e) => setNameFilter(e.target.value)}
                    placeholder="Filter granth names..."
                    style={{
                      flex: 1,
                      minWidth: 220,
                      padding: "9px 10px",
                      fontSize: 14,
                      borderRadius: 8,
                      border: "1px solid #c7cfd9",
                      background: "#fff",
                    }}
                  />

                  <button
                    type="button"
                    onClick={clearSelection}
                    disabled={selectedNames.length === 0}
                    style={{ padding: "8px 10px", borderRadius: 8, border: "1px solid #c7cfd9", background: "#fff" }}
                  >
                    Clear
                  </button>

                  {selectionMode === "multi" ? (
                    <button
                      type="button"
                      onClick={selectAllFiltered}
                      disabled={filteredGroups.length === 0}
                      style={{
                        padding: "8px 10px",
                        borderRadius: 8,
                        border: "1px solid #c7cfd9",
                        background: "#fff",
                      }}
                    >
                      Select all shown
                    </button>
                  ) : null}
                </div>

                <div
                  style={{
                    marginTop: 10,
                    maxHeight: 280,
                    overflow: "auto",
                    display: "grid",
                    gap: 8,
                    paddingRight: 4,
                  }}
                >
                  {loadingGranths ? (
                    <div style={{ opacity: 0.75 }}>Loading granth names...</div>
                  ) : filteredGroups.length === 0 ? (
                    <div style={{ opacity: 0.75 }}>No matching names.</div>
                  ) : (
                    filteredGroups.map((g) => {
                      const isSelected = selectedNames.includes(g.name);
                      return (
                        <button
                          key={g.name}
                          type="button"
                          onClick={() => toggleGroup(g.name)}
                          style={{
                            textAlign: "left",
                            padding: "10px 12px",
                            borderRadius: 10,
                            border: isSelected ? "1px solid #1f2120" : "1px solid #ced4df",
                            background: isSelected ? "#edf0f5" : "#fff",
                            cursor: "pointer",
                          }}
                        >
                          <div style={{ fontWeight: 700 }}>{g.name}</div>
                          <div style={{ fontSize: 12, opacity: 0.72 }}>
                            Includes {g.customIds.length} PDF{g.customIds.length > 1 ? "s" : ""}
                          </div>
                        </button>
                      );
                    })
                  )}
                </div>
              </div>
            ) : null}

            {error ? (
              <div style={{ color: "#9f1f1f", fontWeight: 600, marginTop: 2 }}>
                {error}
              </div>
            ) : null}
          </div>
        </section>

        <section style={{ marginTop: 18 }}>
          {hasSearched ? (
            <div style={{ marginBottom: 12, fontWeight: 700, fontSize: 16 }}>
              Showing page {currentPage} of {totalPages} ({results.length} result(s) on this page, total{" "}
              {totalIsExact ? total : `at least ${total}`}). Match: <strong>{getOCRSearchModeLabel(searchMode)}</strong>.
            </div>
          ) : null}

          {hasSearched && totalPages > 1 ? (
            <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap", alignItems: "center" }}>
              <button
                type="button"
                onClick={() => void run(Math.max(1, currentPage - 1))}
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
                  onClick={() => void run(page)}
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
                onClick={() => void run(Math.min(totalPages, currentPage + 1))}
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
                gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
                gap: 10,
              }}
            >
              {results.map((r, i) => {
                const csvViewerHref = r.csv_url
                  ? `/csv-viewer?csvUrl=${encodeURIComponent(r.csv_url)}&customId=${encodeURIComponent(
                      r.custom_id
                    )}&page=${encodeURIComponent(String(r.page_number))}`
                  : null;
                const pdfViewerHref = `${r.pdf_url}#page=${encodeURIComponent(String(r.page_number))}`;

                return (
                  <article
                    key={`${r.custom_id}_${r.page_number}_${i}`}
                    style={{
                      padding: 12,
                      border: "1px solid #d4d9e2",
                      borderRadius: 12,
                      background: "#fff",
                      boxShadow: "0 5px 14px rgba(35, 42, 51, 0.05)",
                      minHeight: 210,
                      display: "grid",
                      gridTemplateRows: "auto 1fr auto",
                      gap: 8,
                    }}
                  >
                    <div>
                      <div style={{ fontWeight: 700, lineHeight: 1.35 }}>
                        {r.pdf_name}
                      </div>
                      <div style={{ fontSize: 13, opacity: 0.76 }}>
                        Page {r.page_number}
                        {typeof r.occurrence_count === "number" ? ` | ${r.occurrence_count} match(es)` : ""}
                      </div>
                    </div>

                    <div
                      style={{
                        opacity: 0.88,
                        whiteSpace: "pre-wrap",
                        lineHeight: 1.55,
                        fontSize: 21,
                        maxHeight: 240,
                        overflow: "auto",
                        paddingRight: 2,
                      }}
                    >
                      {renderHighlightedSnippet(r.snippet)}
                    </div>

                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap", fontSize: 13 }}>
                      <a href={pdfViewerHref} target="_blank" rel="noreferrer">
                        Open PDF
                      </a>
                      {csvViewerHref ? (
                        <a href={csvViewerHref} target="_blank" rel="noreferrer">
                          Open CSV at row
                        </a>
                      ) : null}
                    </div>
                  </article>
                );
              })}
            </div>
          ) : null}
          {hasSearched && !loading && results.length === 0 ? (
            <div style={{ opacity: 0.8 }}>No results found for this query/filter.</div>
          ) : null}
        </section>
      </div>
    </main>
  );
}
