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
  type SearchRequest,
  type SearchScope,
  buildSearchUrl,
  parseSearchUrl,
  resolveGranthValues,
  sameIds,
} from "@/lib/search-url";
import {
  DEFAULT_CONTEXT_PAGE_RADIUS,
  MAX_CONTEXT_PAGE_RADIUS,
  expandPagesWithContext,
  normalizeContextPageRadius,
} from "@/lib/page-context";
import { PageJumpPager } from "@/components/PageJumpPager";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { DownloadDeliveryDialog, type DeliveryMode } from "@/components/DownloadDeliveryDialog";
import { SearchExportDialog, type ExportFormat } from "@/components/SearchExportDialog";
import { downloadBlob, fileSafe, filenameFromResponse } from "@/lib/download-file";
import Link from "next/link";
import { useRouter } from "next/router";
import type { CSSProperties, KeyboardEvent, ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

type SearchOccurrence = {
  snippet: string;
  matchStart: number;
  matchEnd: number;
  text: string;
};

type SearchResult = {
  granth_key?: string;
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  page_number: number;
  snippet: string;
  occurrences?: SearchOccurrence[];
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
  /** Short id used in the URL (?in=215,296). */
  key: string;
  kind: "pdf" | "text";
  pdf_name: string | null;
  display_name: string;
};

const chipRowStyle: CSSProperties = { display: "flex", flexWrap: "wrap", gap: 6, marginTop: 8 };
const chipStyle: CSSProperties = {
  display: "inline-flex",
  alignItems: "center",
  gap: 6,
  maxWidth: "100%",
  padding: "3px 4px 3px 8px",
  borderRadius: 999,
  border: "1px solid #c7cfd9",
  background: "#fff",
  fontSize: 13,
};
const chipKeyStyle: CSSProperties = {
  fontVariantNumeric: "tabular-nums",
  fontWeight: 700,
  color: "#4a5561",
  flexShrink: 0,
};
const chipNameStyle: CSSProperties = { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 260 };
const chipRemoveStyle: CSSProperties = {
  border: "none",
  background: "#eef1f5",
  borderRadius: 999,
  width: 22,
  height: 22,
  cursor: "pointer",
  lineHeight: 1,
};
const optionRowStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 10,
  padding: "8px 10px",
  borderRadius: 10,
  cursor: "pointer",
  textAlign: "left",
};
const textOnlyTagStyle: CSSProperties = {
  marginLeft: "auto",
  fontSize: 11,
  fontWeight: 700,
  padding: "2px 8px",
  borderRadius: 999,
  background: "#f3ecd9",
  color: "#6b5316",
  flexShrink: 0,
};
const staleNoteStyle: CSSProperties = { flexBasis: "100%", color: "#8a5a00", fontWeight: 600, fontSize: 13 };

// The API caps a scoped search at this many granths.
const MAX_SELECTED_GRANTHS = 250;

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
  const [totalOccurrences, setTotalOccurrences] = useState(0);
  const [occurrencesExact, setOccurrencesExact] = useState(true);
  const [occurrenceScannedPages, setOccurrenceScannedPages] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalIsExact, setTotalIsExact] = useState(true);
  const [hasSearched, setHasSearched] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchMode, setSearchMode] = useState<OCRSearchMode>("exact_word");
  const [selectedQueryOptionIds, setSelectedQueryOptionIds] = useState<string[]>([]);
  const [lastSearchQueries, setLastSearchQueries] = useState<string[]>([]);
  // The export dialog covers the search that produced the results on screen,
  // even if the granth filter is edited afterwards.
  const [lastSearchGranthIds, setLastSearchGranthIds] = useState<string[]>([]);
  const [lastSearchScopeLabel, setLastSearchScopeLabel] = useState("All granths");
  const [lastSearchMode, setLastSearchMode] = useState<OCRSearchMode>("exact_word");
  // The request behind the results on screen, to tell when the form has moved on.
  const [lastRequest, setLastRequest] = useState<SearchRequest | null>(null);

  const [granthOptions, setGranthOptions] = useState<GranthOption[]>([]);
  const [loadingGranths, setLoadingGranths] = useState(true);
  const [granthLoadError, setGranthLoadError] = useState<string | null>(null);
  const [scope, setScope] = useState<SearchScope>("all");
  const [nameFilter, setNameFilter] = useState("");
  // Kept while switching to "All granths" and back, so a selection is never lost.
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [linkWarning, setLinkWarning] = useState<string | null>(null);
  const [documentStats, setDocumentStats] = useState<DocumentStats | null>(null);
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);
  const [downloadPreview, setDownloadPreview] = useState<DownloadPreviewState | null>(null);
  const [deliveryFormat, setDeliveryFormat] = useState<ExportFormat | null>(null);
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const previewCacheRef = useRef(new Map<string, SearchMatchPreview>());
  // Language options to restore for the next typed query (from a URL), instead of
  // the default of all of them.
  const pendingLangsRef = useRef<string[] | null>(null);
  // Guards against an older search response overwriting a newer one.
  const searchSeqRef = useRef(0);

  const queryOptions = useMemo(() => buildIndicQueryOptions(q), [q]);
  const activeQueries = useMemo(() => {
    const selected = new Set(selectedQueryOptionIds);
    const values = queryOptions
      .filter((option) => selected.has(option.id))
      .map((option) => option.value);
    return normalizeOCRSearchQueries(queryOptions.length ? values : q);
  }, [q, queryOptions, selectedQueryOptionIds]);

  useEffect(() => {
    const ids = buildIndicQueryOptions(q).map((option) => option.id);
    const pending = pendingLangsRef.current;
    pendingLangsRef.current = null;
    setSelectedQueryOptionIds(pending ? ids.filter((id) => pending.includes(id)) : ids);
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
        setGranthLoadError(null);

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
        setGranthLoadError(e instanceof Error ? e.message : String(e));
      } finally {
        if (active) setLoadingGranths(false);
      }
    }

    void loadInitialData();
    return () => {
      active = false;
    };
  }, []);

  const optionById = useMemo(() => new Map(granthOptions.map((row) => [row.custom_id, row])), [granthOptions]);
  const keyById = useMemo(() => new Map(granthOptions.map((row) => [row.custom_id, row.key])), [granthOptions]);

  const sortedOptions = useMemo(
    () =>
      [...granthOptions].sort((a, b) =>
        a.key.localeCompare(b.key, "en", { numeric: true, sensitivity: "base" })
      ),
    [granthOptions]
  );

  const filteredOptions = useMemo(() => {
    const keyword = nameFilter.trim().toLowerCase();
    if (!keyword) return sortedOptions;
    return sortedOptions.filter((row) => `${row.key} ${row.display_name}`.toLowerCase().includes(keyword));
  }, [sortedOptions, nameFilter]);

  const searchIds = scope === "all" ? [] : selectedIds;
  const selectedLabel =
    scope === "all" ? "All granths" : `${selectedIds.length} selected granth${selectedIds.length === 1 ? "" : "s"}`;

  // The URL is the source of truth: on load, and on back/forward, the form is set
  // from it and the search it describes is run.
  const appliedUrlRef = useRef<string | null>(null);
  useEffect(() => {
    if (!router.isReady || loadingGranths) return;
    if (appliedUrlRef.current === router.asPath) return;
    appliedUrlRef.current = router.asPath;

    const { q: text, langs, matchMode, page: pageNumber, granthValues: values } = parseSearchUrl(router.query);
    const { ids, missing } = granthOptions.length
      ? resolveGranthValues(values, granthOptions)
      : { ids: values, missing: [] as string[] };
    const nextScope: SearchScope = values.length ? "selected" : "all";

    setLinkWarning(
      missing.length
        ? `${missing.length} granth${missing.length === 1 ? "" : "s"} in this link could not be found (${missing
            .slice(0, 5)
            .join(", ")}).`
        : null
    );
    if (text !== q) {
      pendingLangsRef.current = langs;
      setQ(text);
    } else {
      const ids = buildIndicQueryOptions(text).map((o) => o.id);
      setSelectedQueryOptionIds(langs ? ids.filter((id) => langs.includes(id)) : ids);
    }
    setSearchMode(matchMode);
    setScope(nextScope);
    if (values.length) setSelectedIds(ids);
    setNameFilter("");

    const request: SearchRequest = { q: text, langs, matchMode, scope: nextScope, granthIds: ids, page: pageNumber };
    // Old links (?customId=...) are rewritten to the current form, once the
    // granth list is there to turn ids into short keys.
    const canonical = buildSearchUrl(request, keyById);
    if (granthOptions.length && canonical !== router.asPath) {
      appliedUrlRef.current = canonical;
      void router.replace(canonical, undefined, { shallow: true });
    }
    if (text && !(nextScope === "selected" && ids.length === 0)) {
      void executeSearch(request);
    } else {
      setResults([]);
      setHasSearched(false);
      setLastRequest(null);
    }
    // Runs when the URL changes or the granth list arrives; the form state it
    // writes is intentionally not a dependency.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [router.isReady, router.asPath, loadingGranths, granthOptions]);

  const totalPages = useMemo(() => {
    if (total <= 0) return 1;
    return Math.max(1, Math.ceil(total / RESULTS_PER_PAGE));
  }, [total]);
  const searchReady = activeQueries.some((query) => Array.from(query).length >= (searchMode === "contains" ? 3 : 2));
  const searchableDocuments = documentStats?.searchable_documents ?? documentStats?.processed_documents ?? 0;
  const remainingDocuments =
    documentStats?.remaining_documents ??
    (documentStats ? Math.max(0, documentStats.total_documents - searchableDocuments) : 0);

  function toggleGranth(id: string) {
    setError(null);
    setSelectedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  }

  function clearSelection() {
    setSelectedIds([]);
    setError(null);
  }

  function selectAllShown() {
    setSelectedIds((prev) => Array.from(new Set([...prev, ...filteredOptions.map((row) => row.custom_id)])));
  }

  function onSearchInputKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter" && !loading && searchReady) {
      run(1);
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

  /** Highlights exactly one match, so each tile marks its own occurrence only. */
  // One tile per occurrence: a page with five hits becomes five results, each
  // marking its own hit, rather than one row the reader has to re-scan.
  type OccurrenceTile = {
    result: SearchResult;
    resultIndex: number;
    occurrence: SearchOccurrence | null;
    indexOnPage: number;
    countOnPage: number;
  };

  const occurrenceTiles = useMemo<OccurrenceTile[]>(
    () =>
      results.flatMap((result, resultIndex): OccurrenceTile[] => {
        const list = result.occurrences?.length ? result.occurrences : null;
        if (!list) {
          return [{ result, resultIndex, occurrence: null, indexOnPage: 0, countOnPage: 1 }];
        }
        return list.map((occurrence, indexOnPage) => ({
          result,
          resultIndex,
          occurrence,
          indexOnPage,
          countOnPage: list.length,
        }));
      }),
    [results]
  );

  function renderSingleOccurrence(text: string, start: number, end: number) {
    if (!text) return text;
    const a = Math.max(0, Math.min(start, text.length));
    const b = Math.max(a, Math.min(end, text.length));
    if (b <= a) return text;
    return (
      <>
        {text.slice(0, a)}
        <mark
          style={{
            background: "#fff100",
            color: "#111",
            padding: "0 2px",
            borderRadius: 2,
            fontWeight: 700,
          }}
        >
          {text.slice(a, b)}
        </mark>
        {text.slice(b)}
      </>
    );
  }

  function renderHighlightedSnippet(text: string, queries?: string[]) {
    return renderHighlightedText(text, queries?.length ? queries : lastSearchQueries, lastSearchMode);
  }

  /** Starts a search from the form: validates it, records it in the URL, runs it. */
  function run(page: number) {
    setError(null);
    if (activeQueries.length === 0) {
      setError("Enter a search word or select at least one generated language option.");
      return;
    }
    if (scope === "selected" && selectedIds.length === 0) {
      setError("Tick at least one granth, or choose All granths.");
      return;
    }
    if (scope === "selected" && selectedIds.length > MAX_SELECTED_GRANTHS) {
      setError(`Select at most ${MAX_SELECTED_GRANTHS} granths, or choose All granths.`);
      return;
    }

    const allLangs = queryOptions.map((option) => option.id);
    const langs =
      queryOptions.length > 1 && !sameIds(allLangs, selectedQueryOptionIds)
        ? allLangs.filter((id) => selectedQueryOptionIds.includes(id))
        : null;
    const request: SearchRequest = { q: q.trim(), langs, matchMode: searchMode, scope, granthIds: [...searchIds], page };
    const url = buildSearchUrl(request, keyById);
    if (url !== router.asPath) {
      // Each new search adds a history entry, so Back returns to the previous one.
      appliedUrlRef.current = url;
      void router.push(url, undefined, { shallow: true, scroll: false });
    }
    void executeSearch(request, activeQueries);
  }

  /** Another page of the results on screen, even if the form has been edited since. */
  function goToResultPage(page: number) {
    if (!lastRequest) return;
    const request = { ...lastRequest, page };
    const url = buildSearchUrl(request, keyById);
    if (url !== router.asPath) {
      appliedUrlRef.current = url;
      void router.replace(url, undefined, { shallow: true, scroll: false });
    }
    void executeSearch(request, lastSearchQueries);
  }

  async function executeSearch(request: SearchRequest, queriesOverride?: string[]) {
    const options = buildIndicQueryOptions(request.q);
    const queriesForSearch =
      queriesOverride ??
      normalizeOCRSearchQueries(
        options.length
          ? options.filter((option) => !request.langs || request.langs.includes(option.id)).map((option) => option.value)
          : request.q
      );
    if (queriesForSearch.length === 0) return;
    const seq = ++searchSeqRef.current;

    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("q", queriesForSearch[0]);
      for (const queryVariant of queriesForSearch.slice(1)) params.append("queryVariant", queryVariant);
      params.set("limit", String(RESULTS_PER_PAGE));
      params.set("page", String(request.page));
      params.set("matchMode", request.matchMode);
      if (request.scope === "selected") params.set("granths", request.granthIds.join(","));

      const res = await fetch(`/api/search?${params.toString()}`);
      const json = (await res.json()) as {
        results?: SearchResult[];
        total?: number;
        total_occurrences?: number;
        total_occurrences_exact?: boolean;
        occurrence_scanned_pages?: number;
        page?: number;
        total_is_exact?: boolean;
        match_mode?: string;
        queries?: string[];
        error?: string;
      };
      if (seq !== searchSeqRef.current) return;
      if (!res.ok) {
        throw new Error(json.error || `Search failed (${res.status})`);
      }
      const scopeLabel =
        request.scope === "all"
          ? "All granths"
          : `${request.granthIds.length} selected granth${request.granthIds.length === 1 ? "" : "s"}`;
      setResults(json.results ?? []);
      setTotal(Number(json.total ?? (json.results?.length ?? 0)));
      setTotalOccurrences(Number(json.total_occurrences ?? 0));
      setOccurrencesExact(json.total_occurrences_exact !== false);
      setOccurrenceScannedPages(Number(json.occurrence_scanned_pages ?? 0));
      setCurrentPage(Number(json.page ?? request.page));
      setTotalIsExact(json.total_is_exact !== false);
      setLastSearchQueries(json.queries?.length ? json.queries : queriesForSearch);
      setLastSearchGranthIds(request.scope === "all" ? [] : request.granthIds);
      setLastSearchScopeLabel(scopeLabel);
      setLastSearchMode(parseOCRSearchMode(json.match_mode ?? request.matchMode));
      setLastRequest(request);
      setHasSearched(true);
    } catch (e) {
      if (seq === searchSeqRef.current) setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (seq === searchSeqRef.current) setLoading(false);
    }
  }

  // True once the form no longer describes the results on screen.
  const resultsStale = Boolean(
    lastRequest &&
      (lastRequest.q !== q.trim() ||
        lastRequest.matchMode !== searchMode ||
        lastRequest.scope !== scope ||
        (scope === "selected" && !sameIds(lastRequest.granthIds, selectedIds)))
  );

  async function openDownloadPreview(result: SearchResult) {
    const queries = result.matched_queries?.length ? result.matched_queries : lastSearchQueries;
    if (queries.length === 0) {
      setError("Enter a search word before building a matched-page PDF.");
      return;
    }

    const cacheKey = `${result.custom_id}\n${result.source_rel_path || ""}\n${queries.join("\n")}\n${lastSearchMode}`;
    setDownloadPreview({
      result,
      query: queries[0],
      queries,
      matchMode: lastSearchMode,
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
        params.set("matchMode", lastSearchMode);
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

  async function downloadMatchedPages(format: ExportFormat, delivery: DeliveryMode, email?: string) {
    if (!downloadPreview?.preview || downloadPreview.selectedPages.length === 0) return;

    const preview = downloadPreview.preview;
    setDownloadPreview((prev) => (prev ? { ...prev, downloading: true, error: null, notice: null } : prev));
    try {
      const endpoint = format === "pdf" ? "/api/search-match-pdf" : "/api/search-match-csv";
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          customId: preview.custom_id,
          sourceRelPath: downloadPreview.result.source_rel_path || "",
          granthName: preview.pdf_name,
          q: downloadPreview.query,
          queryVariants: downloadPreview.queries.slice(1),
          matchMode: downloadPreview.matchMode,
          pages: downloadPreview.selectedPages,
          ...(format === "pdf" ? { contextPages: downloadPreview.contextPages } : {}),
          delivery,
          email,
          title: preview.pdf_name,
        }),
      });

      if (!res.ok) {
        const json = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(json.error || `${format === "pdf" ? "PDF" : "CSV"} export failed (${res.status})`);
      }

      if (delivery === "email") {
        const json = (await res.json()) as { email?: string; row_count?: number };
        setDeliveryFormat(null);
        setDownloadPreview((prev) =>
          prev
            ? {
                ...prev,
                downloading: false,
                notice: `Email sent to ${json.email || email}${
                  typeof json.row_count === "number" ? ` with ${json.row_count} CSV row(s)` : ""
                }.`,
              }
            : prev
        );
        return;
      }

      const blob = await res.blob();
      const fallbackName =
        format === "pdf"
          ? `${fileSafe(preview.pdf_name)}_${fileSafe(downloadPreview.queries.join("_"))}_matched_pages.pdf`
          : `${fileSafe(preview.pdf_name)}_${fileSafe(downloadPreview.queries.join("_"))}_page_lines.csv`;
      downloadBlob(blob, filenameFromResponse(res, fallbackName));
      setDeliveryFormat(null);
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
            <Link href="/ask">Ask the library</Link>
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
                onClick={() => run(1)}
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
                <strong>Search in:</strong>
                {(["all", "selected"] as const).map((value) => (
                  <button
                    key={value}
                    type="button"
                    aria-pressed={scope === value}
                    onClick={() => {
                      setScope(value);
                      setError(null);
                    }}
                    style={{
                      padding: "6px 10px",
                      borderRadius: 999,
                      border: "1px solid #bcc4ce",
                      background: scope === value ? "#1f2120" : "#fff",
                      color: scope === value ? "#fff" : "#222",
                      cursor: "pointer",
                    }}
                  >
                    {value === "all"
                      ? "All granths"
                      : `Selected granths${selectedIds.length ? ` (${selectedIds.length})` : ""}`}
                  </button>
                ))}
              </div>

              {scope === "selected" && selectedIds.length > 0 ? (
                <div style={chipRowStyle} aria-label="Selected granths">
                  {selectedIds.map((id) => {
                    const row = optionById.get(id);
                    return (
                      <span key={id} style={chipStyle}>
                        <span style={chipKeyStyle}>{row?.key ?? "?"}</span>
                        <span style={chipNameStyle} title={row?.display_name ?? id}>{row?.display_name ?? id}</span>
                        <button type="button" style={chipRemoveStyle} onClick={() => toggleGranth(id)} aria-label={`Remove ${row?.display_name ?? id}`}>
                          ×
                        </button>
                      </span>
                    );
                  })}
                  <button type="button" style={{ ...chipStyle, padding: "3px 10px", cursor: "pointer" }} onClick={clearSelection}>
                    Clear all
                  </button>
                </div>
              ) : null}
              {linkWarning ? (
                <div role="status" style={{ marginTop: 8, fontSize: 13, color: "#8a5a00" }}>
                  {linkWarning}
                </div>
              ) : null}
            </div>

            {scope === "selected" ? (
              <div style={{ border: "1px solid #d5dae2", borderRadius: 12, padding: 12, background: "#fafbfc" }}>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <input
                    value={nameFilter}
                    onChange={(e) => setNameFilter(e.target.value)}
                    placeholder="Find a granth by name or number..."
                    aria-label="Find a granth"
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
                    onClick={selectAllShown}
                    disabled={filteredOptions.length === 0}
                    style={{ padding: "8px 10px", borderRadius: 8, border: "1px solid #c7cfd9", background: "#fff" }}
                  >
                    Tick all shown ({filteredOptions.length})
                  </button>
                </div>

                <div
                  style={{
                    marginTop: 10,
                    maxHeight: 280,
                    overflow: "auto",
                    display: "grid",
                    gap: 6,
                    paddingRight: 4,
                  }}
                >
                  {loadingGranths ? (
                    <div className="buttonSpinnerLabel" style={{ opacity: 0.75 }} role="status">
                      <span className="loadingSpinner" aria-hidden="true" />
                      Loading granths
                    </div>
                  ) : granthLoadError ? (
                    <div role="alert" style={{ color: "#9f1f1f" }}>
                      Could not load the granth list: {granthLoadError}
                    </div>
                  ) : filteredOptions.length === 0 ? (
                    <div style={{ opacity: 0.75 }}>No granth matches “{nameFilter}”.</div>
                  ) : (
                    filteredOptions.map((row) => {
                      const checked = selectedIds.includes(row.custom_id);
                      return (
                        <label
                          key={row.custom_id}
                          style={{
                            ...optionRowStyle,
                            border: checked ? "1px solid #1f2120" : "1px solid #ced4df",
                            background: checked ? "#edf0f5" : "#fff",
                          }}
                        >
                          <input type="checkbox" checked={checked} onChange={() => toggleGranth(row.custom_id)} />
                          <span style={chipKeyStyle}>{row.key}</span>
                          <span style={{ fontWeight: 600 }}>{row.display_name}</span>
                          {row.kind === "text" ? <span style={textOnlyTagStyle}>text only</span> : null}
                        </label>
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
            <div className="searchCountsRow">
              <span className="searchCountPrimary">
                <strong>{occurrencesExact ? totalOccurrences : `${totalOccurrences}+`}</strong> total occurrence
                {totalOccurrences === 1 ? "" : "s"}
                {occurrencesExact ? null : (
                  <span className="searchCountNote">
                    {" "}counted in the first {occurrenceScannedPages.toLocaleString()} pages
                  </span>
                )}
              </span>
              <span className="searchCountDivider" aria-hidden="true">
                /
              </span>
              <span className="searchCountSecondary">
                across <strong>{totalIsExact ? total : `${total}+`}</strong> page{total === 1 ? "" : "s"}
              </span>
              <span className="searchCountMeta">
                in <strong>{lastSearchScopeLabel.toLowerCase()}</strong> · showing page {currentPage} of {totalPages} ·
                match: <strong>{getOCRSearchModeLabel(lastSearchMode)}</strong>
              </span>
              {resultsStale ? (
                <span style={staleNoteStyle} role="status">
                  The search settings above have changed. Press Search to update these results.
                </span>
              ) : null}
            </div>
          ) : null}

          {hasSearched && results.length > 0 && lastSearchQueries.length > 0 ? (
            <div className="searchExportBar">
              <div>
                <strong>Export this whole search</strong>
                <span>
                  One combined PDF with the matched pages of every granth this search found, or a CSV of every match
                  with its PDF page number and line number.
                </span>
              </div>
              <button type="button" onClick={() => setExportDialogOpen(true)}>
                Export all matched granths
              </button>
            </div>
          ) : null}

          {hasSearched ? (
            <div style={{ marginBottom: 14 }}>
              <PageJumpPager
                currentPage={currentPage}
                totalPages={totalPages}
                loading={loading}
                ariaLabel="Search result pages"
                onPageChange={(page) => goToResultPage(page)}
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
              {occurrenceTiles.map((tile, i) => {
                const r = tile.result;
                const rowQueries = r.matched_queries?.length ? r.matched_queries : lastSearchQueries;
                const csvViewerHref = r.csv_url
                  ? `/csv-viewer?csvUrl=${encodeURIComponent(r.csv_url)}&customId=${encodeURIComponent(
                      r.custom_id
                    )}&page=${encodeURIComponent(String(r.page_number))}`
                  : null;
                const canOpenPdf = isValidHttpUrl(r.pdf_url);
                return (
                  <article
                    key={`${r.custom_id}_${r.page_number}_${tile.indexOnPage}_${i}`}
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
                        {tile.occurrence
                          ? ` | occurrence ${tile.indexOnPage + 1} of ${tile.countOnPage} on this page`
                          : typeof r.occurrence_count === "number"
                            ? ` | ${r.occurrence_count} match(es)`
                            : ""}
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
                      {tile.occurrence
                        ? renderSingleOccurrence(
                            tile.occurrence.snippet,
                            tile.occurrence.matchStart,
                            tile.occurrence.matchEnd
                          )
                        : renderHighlightedSnippet(r.snippet, rowQueries)}
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
                              searchMode: lastSearchMode,
                            })
                          }
                        >
                          Open PDF
                        </button>
                      ) : r.granth_key ? (
                        <Link
                          href={`/ocr-text-viewer?granthKey=${encodeURIComponent(r.granth_key)}&page=${encodeURIComponent(
                            String(r.source_page_number ?? r.page_number)
                          )}&q=${encodeURIComponent(rowQueries[0] || q)}&matchMode=${encodeURIComponent(lastSearchMode)}`}
                          title="This granth has no uploaded PDF; open its OCR text on this page."
                        >
                          Open text page
                        </Link>
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
                        setDeliveryFormat(null);
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
                          onClick={() => setDeliveryFormat("csv")}
                          title="Export the page number and line number of every match as CSV"
                          aria-busy={downloadPreview.downloading}
                          disabled={downloadPreview.downloading || selectedCount === 0}
                        >
                          {downloadPreview.downloading && deliveryFormat === "csv" ? (
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
                          onClick={() => setDeliveryFormat("pdf")}
                          aria-busy={downloadPreview.downloading}
                          disabled={downloadPreview.downloading || selectedCount === 0 || tooManyPages}
                        >
                          {downloadPreview.downloading && deliveryFormat === "pdf" ? (
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
                        open={deliveryFormat !== null}
                        title={deliveryFormat === "csv" ? "Choose CSV delivery" : "Choose download method"}
                        fileLabel={
                          deliveryFormat === "csv"
                            ? `${selectedCount} matched page(s), one row per matched line`
                            : `${finalPageCount} PDF page(s), cover first`
                        }
                        busy={downloadPreview.downloading}
                        error={downloadPreview.error}
                        onClose={() => setDeliveryFormat(null)}
                        onDownload={() => void downloadMatchedPages(deliveryFormat ?? "pdf", "download")}
                        onEmail={(email) => void downloadMatchedPages(deliveryFormat ?? "pdf", "email", email)}
                      />
                    </>
                  ) : null}
                </div>
              </div>
            );
          })()
        : null}
      <SearchExportDialog
        open={exportDialogOpen}
        queries={lastSearchQueries}
        matchMode={lastSearchMode}
        granthIds={lastSearchGranthIds}
        scopeLabel={lastSearchScopeLabel}
        onClose={() => setExportDialogOpen(false)}
      />
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </main>
  );
}
