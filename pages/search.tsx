import {
  OCR_SEARCH_MODE_OPTIONS,
  type OCRSearchMode,
  findOCRSearchMatchesForQueries,
  getOCRSearchModeLabel,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
} from "@/lib/ocr-search";
import { buildIndicQueryOptions } from "@/lib/phonetic-transliteration";
import {
  DEFAULT_CONTEXT_PAGE_RADIUS,
  MAX_CONTEXT_PAGE_RADIUS,
  expandPagesWithContext,
  normalizeContextPageRadius,
} from "@/lib/page-context";
import { PageJumpPager } from "@/components/PageJumpPager";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { DownloadDeliveryDialog, type DeliveryMode } from "@/components/DownloadDeliveryDialog";
import Link from "next/link";
import { useRouter } from "next/router";
import type { KeyboardEvent, ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

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
  source_rel_path?: string;
  source_page_number?: number;
  matched_queries?: string[];
};

type SearchMatchPage = {
  page_number: number;
  occurrence_count: number;
  snippet: string;
};

type SearchMatchPreview = {
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  cover_image_url?: string | null;
  cover_page: number;
  pages: SearchMatchPage[];
  total_matched_pages: number;
  truncated?: boolean;
  max_download_pages: number;
  match_mode: OCRSearchMode;
  queries?: string[];
};

type DownloadPreviewState = {
  result: SearchResult;
  query: string;
  queries: string[];
  matchMode: OCRSearchMode;
  loading: boolean;
  downloading: boolean;
  error: string | null;
  notice: string | null;
  preview: SearchMatchPreview | null;
  selectedPages: number[];
  contextPages: number;
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

function readSingleQuery(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value ?? "";
}

function isValidHttpUrl(value: string | null | undefined) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  try {
    const url = new URL(raw);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function fileSafe(value: string) {
  return String(value || "download")
    .replace(/\.pdf$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 110) || "download";
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function contextRangeLabel(pageNumber: number, contextPages: number) {
  const radius = normalizeContextPageRadius(contextPages);
  if (radius === 0) return `PDF page ${pageNumber}`;
  const start = Math.max(1, pageNumber - radius);
  const end = pageNumber + radius;
  return `PDF pages ${start}-${end}`;
}

export default function SearchPage() {
  const router = useRouter();
  const [q, setQ] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalIsExact, setTotalIsExact] = useState(true);
  const [hasSearched, setHasSearched] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchMode, setSearchMode] = useState<OCRSearchMode>("exact_word");
  const [selectedQueryOptionIds, setSelectedQueryOptionIds] = useState<string[]>([]);
  const [lastSearchQueries, setLastSearchQueries] = useState<string[]>([]);

  const [granthOptions, setGranthOptions] = useState<GranthOption[]>([]);
  const [loadingGranths, setLoadingGranths] = useState(true);
  const [selectionMode, setSelectionMode] = useState<SelectionMode>("all");
  const [nameFilter, setNameFilter] = useState("");
  const [selectedNames, setSelectedNames] = useState<string[]>([]);
  const [documentStats, setDocumentStats] = useState<DocumentStats | null>(null);
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);
  const [downloadPreview, setDownloadPreview] = useState<DownloadPreviewState | null>(null);
  const [deliveryDialogOpen, setDeliveryDialogOpen] = useState(false);
  const previewCacheRef = useRef(new Map<string, SearchMatchPreview>());
  const [routePrefillApplied, setRoutePrefillApplied] = useState(false);

  const queryOptions = useMemo(() => buildIndicQueryOptions(q), [q]);
  const activeQueries = useMemo(() => {
    const selected = new Set(selectedQueryOptionIds);
    const values = queryOptions
      .filter((option) => selected.has(option.id))
      .map((option) => option.value);
    return normalizeOCRSearchQueries(queryOptions.length ? values : q);
  }, [q, queryOptions, selectedQueryOptionIds]);

  useEffect(() => {
    setSelectedQueryOptionIds(buildIndicQueryOptions(q).map((option) => option.id));
  }, [q]);

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

  useEffect(() => {
    if (!router.isReady || routePrefillApplied || groups.length === 0) return;

    const customId = readSingleQuery(router.query.customId).trim();
    const initialQuery = readSingleQuery(router.query.q).trim();
    if (initialQuery) setQ(initialQuery);

    if (customId) {
      const match = groups.find((group) => group.customIds.includes(customId));
      if (match) {
        setSelectionMode("single");
        setSelectedNames([match.name]);
        setNameFilter(match.name);
      }
    }

    setRoutePrefillApplied(true);
  }, [groups, routePrefillApplied, router.isReady, router.query.customId, router.query.q]);

  const totalPages = useMemo(() => {
    if (total <= 0) return 1;
    return Math.max(1, Math.ceil(total / RESULTS_PER_PAGE));
  }, [total]);
  const searchReady = activeQueries.some((query) => Array.from(query).length >= (searchMode === "contains" ? 3 : 2));
  const searchableDocuments = documentStats?.searchable_documents ?? documentStats?.processed_documents ?? 0;
  const remainingDocuments =
    documentStats?.remaining_documents ??
    (documentStats ? Math.max(0, documentStats.total_documents - searchableDocuments) : 0);

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
    if (e.key === "Enter" && !loading && searchReady) {
      void run(1);
    }
  }

  function renderHighlightedText(text: string, queries: string[], mode: OCRSearchMode) {
    const matches = findOCRSearchMatchesForQueries(text, queries, mode);
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

  function renderHighlightedSnippet(text: string, queries?: string[]) {
    return renderHighlightedText(text, queries?.length ? queries : lastSearchQueries, searchMode);
  }

  async function run(page: number) {
    setError(null);
    const queriesForSearch = activeQueries;
    if (queriesForSearch.length === 0) {
      setError("Enter a search word or select at least one generated language option.");
      return;
    }
    if (selectionMode !== "all" && selectedCustomIds.length === 0) {
      setError("Select at least one granth name before searching.");
      return;
    }

    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set("q", queriesForSearch[0]);
      for (const queryVariant of queriesForSearch.slice(1)) params.append("queryVariant", queryVariant);
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
        queries?: string[];
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
      setLastSearchQueries(json.queries?.length ? json.queries : queriesForSearch);
      setHasSearched(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  async function openDownloadPreview(result: SearchResult) {
    const queries = result.matched_queries?.length ? result.matched_queries : lastSearchQueries;
    if (queries.length === 0) {
      setError("Enter a search word before building a matched-page PDF.");
      return;
    }

    const cacheKey = `${result.custom_id}\n${result.source_rel_path || ""}\n${queries.join("\n")}\n${searchMode}`;
    setDownloadPreview({
      result,
      query: queries[0],
      queries,
      matchMode: searchMode,
      loading: true,
      downloading: false,
      error: null,
      notice: null,
      preview: null,
      selectedPages: [],
      contextPages: DEFAULT_CONTEXT_PAGE_RADIUS,
    });

    try {
      let preview = previewCacheRef.current.get(cacheKey);
      if (!preview) {
        const params = new URLSearchParams();
        params.set("customId", result.custom_id);
        params.set("q", queries[0]);
        for (const queryVariant of queries.slice(1)) params.append("queryVariant", queryVariant);
        params.set("matchMode", searchMode);
        if (result.source_rel_path) params.set("sourceRelPath", result.source_rel_path);

        const res = await fetch(`/api/search-match-pages?${params.toString()}`);
        const json = (await res.json()) as SearchMatchPreview & { error?: string };
        if (!res.ok) throw new Error(json.error || `Could not load page preview (${res.status})`);
        preview = json;
        previewCacheRef.current.set(cacheKey, preview);
      }

      setDownloadPreview((prev) =>
        prev && prev.result.custom_id === result.custom_id
          ? {
              ...prev,
              loading: false,
              preview: preview ?? null,
              queries: preview?.queries?.length ? preview.queries : prev.queries,
              selectedPages: (preview?.pages ?? []).map((page) => page.page_number),
            }
          : prev
      );
    } catch (previewError) {
      setDownloadPreview((prev) =>
        prev && prev.result.custom_id === result.custom_id
          ? {
              ...prev,
              loading: false,
              error: previewError instanceof Error ? previewError.message : String(previewError),
            }
          : prev
      );
    }
  }

  function setPreviewPageSelected(pageNumber: number, selected: boolean) {
    setDownloadPreview((prev) => {
      if (!prev) return prev;
      const pages = new Set(prev.selectedPages);
      if (selected) pages.add(pageNumber);
      else pages.delete(pageNumber);
      return { ...prev, selectedPages: [...pages].sort((a, b) => a - b) };
    });
  }

  function setAllPreviewPages(selected: boolean) {
    setDownloadPreview((prev) => {
      if (!prev?.preview) return prev;
      return {
        ...prev,
        selectedPages: selected ? prev.preview.pages.map((page) => page.page_number) : [],
      };
    });
  }

  function setDownloadContextPages(value: string) {
    setDownloadPreview((prev) =>
      prev
        ? {
            ...prev,
            contextPages: value === "" ? 0 : normalizeContextPageRadius(value),
          }
        : prev
    );
  }

  async function downloadMatchedPdf(delivery: DeliveryMode, email?: string) {
    if (!downloadPreview?.preview || downloadPreview.selectedPages.length === 0) return;

    setDownloadPreview((prev) => (prev ? { ...prev, downloading: true, error: null, notice: null } : prev));
    try {
      const res = await fetch("/api/search-match-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          customId: downloadPreview.preview.custom_id,
          sourceRelPath: downloadPreview.result.source_rel_path || "",
          q: downloadPreview.query,
          queryVariants: downloadPreview.queries.slice(1),
          matchMode: downloadPreview.matchMode,
          pages: downloadPreview.selectedPages,
          contextPages: downloadPreview.contextPages,
          delivery,
          email,
          title: downloadPreview.preview.pdf_name,
        }),
      });

      if (!res.ok) {
        const json = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(json.error || `PDF download failed (${res.status})`);
      }

      if (delivery === "email") {
        const json = (await res.json()) as { email?: string };
        setDeliveryDialogOpen(false);
        setDownloadPreview((prev) =>
          prev
            ? {
                ...prev,
                downloading: false,
                notice: `Email sent to ${json.email || email}.`,
              }
            : prev
        );
        return;
      }

      const blob = await res.blob();
      downloadBlob(blob, `${fileSafe(downloadPreview.preview.pdf_name)}_${fileSafe(downloadPreview.queries.join("_"))}_matched_pages.pdf`);
      setDeliveryDialogOpen(false);
      setDownloadPreview(null);
    } catch (downloadError) {
      setDownloadPreview((prev) =>
        prev
          ? {
              ...prev,
              downloading: false,
              error: downloadError instanceof Error ? downloadError.message : String(downloadError),
            }
          : prev
      );
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
          <div className="appPillNav" style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 12 }}>
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
                id="search-query"
                value={q}
                onChange={(e) => setQ(e.target.value)}
                onKeyDown={onSearchInputKeyDown}
                placeholder="Search text or English phonetic..."
                aria-label="Search word"
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
                disabled={loading || !searchReady}
                aria-busy={loading}
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
                {loading ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Searching
                  </span>
                ) : (
                  "Search"
                )}
              </button>
            </div>

            {queryOptions.length > 1 ? (
              <fieldset className="queryVariantFieldset">
                <legend>Language queries</legend>
                <div className="queryVariantGrid">
                  {queryOptions.map((option) => {
                    const checked = selectedQueryOptionIds.includes(option.id);
                    return (
                      <label key={option.id} className="queryVariantOption">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={(event) => {
                            setSelectedQueryOptionIds((prev) => {
                              if (event.target.checked) return Array.from(new Set([...prev, option.id]));
                              return prev.filter((id) => id !== option.id);
                            });
                          }}
                        />
                        <span>
                          <strong>{option.label}</strong>
                          <span>{option.value}</span>
                        </span>
                      </label>
                    );
                  })}
                </div>
              </fieldset>
            ) : null}

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
                    <div className="buttonSpinnerLabel" style={{ opacity: 0.75 }} role="status">
                      <span className="loadingSpinner" aria-hidden="true" />
                      Loading granth names
                    </div>
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
              <div role="alert" style={{ color: "#9f1f1f", fontWeight: 600, marginTop: 2 }}>
                {error}
              </div>
            ) : null}
          </div>
        </section>

        <section style={{ marginTop: 18 }} aria-busy={loading}>
          {hasSearched ? (
            <div style={{ marginBottom: 12, fontWeight: 700, fontSize: 16 }}>
              Showing page {currentPage} of {totalPages} ({results.length} result(s) on this page, total{" "}
              {totalIsExact ? total : `at least ${total}`}). Match: <strong>{getOCRSearchModeLabel(searchMode)}</strong>.
            </div>
          ) : null}

          {hasSearched ? (
            <div style={{ marginBottom: 14 }}>
              <PageJumpPager
                currentPage={currentPage}
                totalPages={totalPages}
                loading={loading}
                ariaLabel="Search result pages"
                onPageChange={(page) => void run(page)}
              />
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
                const rowQueries = r.matched_queries?.length ? r.matched_queries : lastSearchQueries;
                const csvViewerHref = r.csv_url
                  ? `/csv-viewer?csvUrl=${encodeURIComponent(r.csv_url)}&customId=${encodeURIComponent(
                      r.custom_id
                    )}&page=${encodeURIComponent(String(r.page_number))}`
                  : null;
                const canOpenPdf = isValidHttpUrl(r.pdf_url);
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
                      {renderHighlightedSnippet(r.snippet, rowQueries)}
                    </div>

                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap", fontSize: 13 }}>
                      {canOpenPdf ? (
                        <button
                          type="button"
                          className="inlinePdfButton"
                          onClick={() =>
                            setPdfTarget({
                              pdfUrl: r.pdf_url,
                              page: r.page_number,
                              title: r.pdf_name,
                              searchTerm: rowQueries[0] || q,
                              searchTerms: rowQueries,
                              searchMode,
                            })
                          }
                        >
                          Open PDF
                        </button>
                      ) : (
                        <span style={{ color: "#7b8784", fontWeight: 700 }} title="No uploaded PDF URL is linked for this result.">
                          PDF unavailable
                        </span>
                      )}
                      {canOpenPdf ? (
                        <button
                          type="button"
                          className="inlinePdfButton"
                          onClick={() => void openDownloadPreview(r)}
                          disabled={rowQueries.length === 0}
                        >
                          Download matched pages
                        </button>
                      ) : null}
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
      {downloadPreview
        ? (() => {
            const selectedSet = new Set(downloadPreview.selectedPages);
            const selectedCount = downloadPreview.selectedPages.length;
            const maxPages = downloadPreview.preview?.max_download_pages ?? 0;
            const expandedSelectedPages = expandPagesWithContext(downloadPreview.selectedPages, downloadPreview.contextPages);
            const finalDownloadPages = selectedCount > 0 ? [1, ...expandedSelectedPages.filter((page) => page !== 1)] : [];
            const finalPageCount = finalDownloadPages.length;
            const tooManyPages = Boolean(maxPages && finalPageCount > maxPages);
            return (
              <div className="searchDownloadOverlay" role="dialog" aria-modal="true" aria-label="Matched page PDF preview">
                <div className="searchDownloadPanel">
                  <header className="searchDownloadHeader">
                    <div>
                      <h2>Preview pages</h2>
                      <p>{downloadPreview.preview?.pdf_name || downloadPreview.result.pdf_name}</p>
                    </div>
                    <button
                      type="button"
                      onClick={() => {
                        setDeliveryDialogOpen(false);
                        setDownloadPreview(null);
                      }}
                    >
                      Close
                    </button>
                  </header>

                  {downloadPreview.loading ? (
                    <div className="searchDownloadNotice" role="status">
                      <span className="buttonSpinnerLabel">
                        <span className="loadingSpinner" aria-hidden="true" />
                        Loading pages
                      </span>
                    </div>
                  ) : null}
                  {downloadPreview.error ? <div className="searchDownloadError" role="alert">{downloadPreview.error}</div> : null}
                  {downloadPreview.notice ? <div className="searchDownloadNotice" role="status">{downloadPreview.notice}</div> : null}

                  {downloadPreview.preview ? (
                    <>
                      <div className="searchDownloadSummary">
                        <strong>{selectedCount}</strong> of{" "}
                        <strong>{downloadPreview.preview.total_matched_pages}</strong> matching page(s) selected.
                        <span> Download includes up to {finalPageCount} PDF page(s), with cover page 1 first.</span>
                        {downloadPreview.preview.truncated ? <span> Preview is capped; narrow the search if needed.</span> : null}
                      </div>

                      <div className="searchDownloadToolbar">
                        <button type="button" onClick={() => setAllPreviewPages(true)}>
                          Select all
                        </button>
                        <button type="button" onClick={() => setAllPreviewPages(false)}>
                          Clear
                        </button>
                        <label className="searchDownloadContextInput">
                          <span>Nearby pages</span>
                          <input
                            type="number"
                            min={0}
                            max={MAX_CONTEXT_PAGE_RADIUS}
                            inputMode="numeric"
                            value={downloadPreview.contextPages}
                            onChange={(event) => setDownloadContextPages(event.target.value)}
                          />
                        </label>
                      </div>

                      <div className="searchDownloadPageList">
                        {downloadPreview.preview.pages.map((page) => {
                          const isCover = page.page_number === 1;
                          return (
                            <label key={page.page_number} className="searchDownloadPageRow">
                              <input
                                type="checkbox"
                                checked={isCover || selectedSet.has(page.page_number)}
                                disabled={isCover}
                                onChange={(event) => setPreviewPageSelected(page.page_number, event.target.checked)}
                              />
                              <span className="searchDownloadPageMeta">
                                Page {page.page_number}
                                {isCover ? " | cover" : ""} | {page.occurrence_count} match(es)
                                <em>{contextRangeLabel(page.page_number, downloadPreview.contextPages)}</em>
                              </span>
                              <span className="searchDownloadSnippet">
                                {renderHighlightedText(page.snippet, downloadPreview.queries, downloadPreview.matchMode)}
                              </span>
                            </label>
                          );
                        })}
                      </div>

                      <footer className="searchDownloadFooter">
                        {tooManyPages ? (
                          <span className="searchDownloadErrorText">
                            Reduce the selection to {maxPages} PDF pages or fewer.
                          </span>
                        ) : null}
                        <button
                          type="button"
                          onClick={() => setDeliveryDialogOpen(true)}
                          aria-busy={downloadPreview.downloading}
                          disabled={downloadPreview.downloading || selectedCount === 0 || tooManyPages}
                        >
                          {downloadPreview.downloading ? (
                            <span className="buttonSpinnerLabel">
                              <span className="loadingSpinner" aria-hidden="true" />
                              Building PDF
                            </span>
                          ) : (
                            "Download PDF"
                          )}
                        </button>
                      </footer>
                      <DownloadDeliveryDialog
                        open={deliveryDialogOpen}
                        title="Choose download method"
                        fileLabel={`${finalPageCount} PDF page(s), cover first`}
                        busy={downloadPreview.downloading}
                        error={downloadPreview.error}
                        onClose={() => setDeliveryDialogOpen(false)}
                        onDownload={() => void downloadMatchedPdf("download")}
                        onEmail={(email) => void downloadMatchedPdf("email", email)}
                      />
                    </>
                  ) : null}
                </div>
              </div>
            );
          })()
        : null}
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </main>
  );
}
