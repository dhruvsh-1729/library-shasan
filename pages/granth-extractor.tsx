import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { DownloadDeliveryDialog, type DeliveryMode } from "@/components/DownloadDeliveryDialog";
import type { MappingSegment } from "@/lib/granth-mapping";
import {
  DEFAULT_CONTEXT_PAGE_RADIUS,
  MAX_CONTEXT_PAGE_RADIUS,
  expandPagesWithContext,
  normalizeContextPageRadius,
} from "@/lib/page-context";
import Link from "next/link";
import { useRouter } from "next/router";
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

type PageSourceItem = {
  id: number;
  file_name: string | null;
  display_name: string;
  pdf_url: string | null;
  file_size: number | null;
  custom_id: string | null;
  collection: string | null;
  subcollection: string | null;
  original_rel_path: string | null;
  cover_image_url: string | null;
  page_count: number | null;
  ocr_granth_key: string | null;
  text_row_count: number | null;
  xlsx_url: string | null;
  mapping_book_id: number | null;
  mapping_book_code: string | null;
};

type PageSourcesResponse = {
  items: PageSourceItem[];
  meta?: {
    total?: number;
  };
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

type DeliveryRequest = {
  mode: BuildMode;
};

type PreviewRange = {
  key: string;
  index: number;
  range: MappingSegment["ranges"][number];
  pages: number[];
};

type PreviewSegment = {
  key: string;
  segment: MappingSegment;
  ranges: PreviewRange[];
  pages: number[];
};

type PreviewSelectionState = {
  disabledSegments: Record<string, boolean>;
  disabledRanges: Record<string, boolean>;
  disabledPages: Record<string, Record<number, boolean>>;
};

type BuildSelectionPayload = {
  pagesByPdf: Array<{ pdfUrl: string; pages: number[] }>;
  rangeIndexesByPdf: Array<{ pdfUrl: string; rangeIndexes: number[] }>;
};

type PreparedSelection = {
  selection: BuildSelectionPayload;
  combinedPages: number;
  separatePages: number;
  selectedSegments: number;
  selectedRanges: number;
};

function titleForBook(book: BookItem | null) {
  if (!book) return "Selected Granth";
  return book.title_display || book.title_english || `Granth ${book.id}`;
}

function titleForPageSource(source: PageSourceItem | null) {
  if (!source) return "Selected PDF";
  return source.display_name || source.file_name || source.original_rel_path || `PDF ${source.id}`;
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

function pagesInRange(start: number, end: number) {
  const first = Math.max(1, Math.floor(Number(start)));
  const last = Math.max(first, Math.floor(Number(end || start)));
  const pages: number[] = [];
  for (let page = first; page <= last; page += 1) pages.push(page);
  return pages;
}

function uniqueSortedPages(pages: number[]) {
  return [...new Set(pages.map((page) => Math.floor(Number(page))).filter((page) => page > 0))].sort((a, b) => a - b);
}

function segmentDownloadPages(segment: MappingSegment, contextPages: number, includeCover: boolean) {
  const mappedPages = segment.ranges.flatMap((range) => pagesInRange(range.pageStart, range.pageEnd));
  return uniqueSortedPages([
    ...(includeCover ? [1] : []),
    ...expandPagesWithContext(mappedPages, contextPages),
  ]);
}

function countCombinedDownloadPages(segments: MappingSegment[], contextPages: number, includeCover: boolean) {
  const seenPagesByPdf = new Map<string, Set<number>>();

  for (const segment of segments) {
    const seenPages = seenPagesByPdf.get(segment.pdfUrl) ?? new Set<number>();
    for (const page of segmentDownloadPages(segment, contextPages, includeCover)) {
      seenPages.add(page);
    }
    seenPagesByPdf.set(segment.pdfUrl, seenPages);
  }

  return [...seenPagesByPdf.values()].reduce((sum, pages) => sum + pages.size, 0);
}

function rangeDownloadPages(range: MappingSegment["ranges"][number], contextPages: number, includeCover: boolean) {
  return uniqueSortedPages([
    ...(includeCover ? [1] : []),
    ...expandPagesWithContext(pagesInRange(range.pageStart, range.pageEnd), contextPages),
  ]);
}

function buildPreviewSegments(segments: MappingSegment[], contextPages: number, includeCover: boolean): PreviewSegment[] {
  return segments.map((segment, segmentIndex) => {
    const ranges = segment.ranges.map((range, rangeIndex) => ({
      key: `${segmentIndex}:${rangeIndex}`,
      index: rangeIndex,
      range,
      pages: rangeDownloadPages(range, contextPages, includeCover),
    }));
    return {
      key: segment.pdfUrl,
      segment,
      ranges,
      pages: uniqueSortedPages(ranges.flatMap((range) => range.pages)),
    };
  });
}

function emptyPreviewSelection(): PreviewSelectionState {
  return {
    disabledSegments: {},
    disabledRanges: {},
    disabledPages: {},
  };
}

function isPageDisabled(selection: PreviewSelectionState, segmentKey: string, page: number) {
  return Boolean(selection.disabledPages[segmentKey]?.[page]);
}

function selectedRangePages(
  previewSegment: PreviewSegment,
  range: PreviewRange,
  selection: PreviewSelectionState
) {
  if (selection.disabledSegments[previewSegment.key] || selection.disabledRanges[range.key]) return [];
  return range.pages.filter((page) => !isPageDisabled(selection, previewSegment.key, page));
}

function selectedSegmentPages(previewSegment: PreviewSegment, selection: PreviewSelectionState) {
  if (selection.disabledSegments[previewSegment.key]) return [];
  return uniqueSortedPages(
    previewSegment.ranges
      .filter((range) => !selection.disabledRanges[range.key])
      .flatMap((range) => selectedRangePages(previewSegment, range, selection))
  );
}

function prepareBuildSelection(previewSegments: PreviewSegment[], selection: PreviewSelectionState): PreparedSelection {
  const pagesByPdf: BuildSelectionPayload["pagesByPdf"] = [];
  const rangeIndexesByPdf: BuildSelectionPayload["rangeIndexesByPdf"] = [];
  let combinedPages = 0;
  let separatePages = 0;
  let selectedSegments = 0;
  let selectedRanges = 0;

  for (const previewSegment of previewSegments) {
    const pages = selectedSegmentPages(previewSegment, selection);
    const rangeIndexes = previewSegment.ranges
      .filter((range) => selectedRangePages(previewSegment, range, selection).length > 0)
      .map((range) => range.index + 1);

    if (pages.length > 0) {
      selectedSegments += 1;
      combinedPages += pages.length;
      pagesByPdf.push({ pdfUrl: previewSegment.segment.pdfUrl, pages });
    }

    if (rangeIndexes.length > 0) {
      selectedRanges += rangeIndexes.length;
      rangeIndexesByPdf.push({ pdfUrl: previewSegment.segment.pdfUrl, rangeIndexes });
      separatePages += previewSegment.ranges.reduce(
        (sum, range) => sum + selectedRangePages(previewSegment, range, selection).length,
        0
      );
    }
  }

  return {
    selection: { pagesByPdf, rangeIndexesByPdf },
    combinedPages,
    separatePages,
    selectedSegments,
    selectedRanges,
  };
}

function readSingleQuery(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value ?? "";
}

export default function GranthExtractorPage() {
  const router = useRouter();
  const [books, setBooks] = useState<BookItem[]>([]);
  const [loadingBooks, setLoadingBooks] = useState(true);
  const [bookError, setBookError] = useState<string | null>(null);
  const [pageSources, setPageSources] = useState<PageSourceItem[]>([]);
  const [loadingPageSources, setLoadingPageSources] = useState(true);
  const [pageSourceError, setPageSourceError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [bookId, setBookId] = useState<number | null>(null);
  const [pageSourceId, setPageSourceId] = useState<number | null>(null);
  const [bookCode, setBookCode] = useState("");
  const [kind, setKind] = useState<"gathas" | "pages">("gathas");
  const [spec, setSpec] = useState("");
  const [adhikar, setAdhikar] = useState("");
  const [includeAllIdentifiers, setIncludeAllIdentifiers] = useState(false);
  const [includeCover, setIncludeCover] = useState(kind === "gathas");
  const [downloadContextPages, setDownloadContextPages] = useState(DEFAULT_CONTEXT_PAGE_RADIUS);
  const [resolving, setResolving] = useState(false);
  const [buildingMode, setBuildingMode] = useState<BuildMode | null>(null);
  const [deliveryRequest, setDeliveryRequest] = useState<DeliveryRequest | null>(null);
  const [previewMode, setPreviewMode] = useState<BuildMode | null>(null);
  const [previewSelection, setPreviewSelection] = useState<PreviewSelectionState>(() => emptyPreviewSelection());
  const [previewingKey, setPreviewingKey] = useState<string | null>(null);
  const [previewObjectUrl, setPreviewObjectUrl] = useState<string | null>(null);
  const [result, setResult] = useState<ResolveResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [context, setContext] = useState<BookContext | null>(null);
  const [contextLoading, setContextLoading] = useState(false);
  const [contextError, setContextError] = useState<string | null>(null);
  const [brokenCoverIds, setBrokenCoverIds] = useState<Record<number, boolean>>({});
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);

  useEffect(() => {
    if (!router.isReady) return;
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
        const routeBookId = Number(readSingleQuery(router.query.bookId));
        const routeBookCode = readSingleQuery(router.query.bookCode).trim();
        const routeBook = Number.isFinite(routeBookId)
          ? items.find((item) => item.id === routeBookId)
          : null;
        const initialBook = routeBook || items[0] || null;

        setBooks(items);
        if (initialBook) {
          setBookId(initialBook.id);
          setBookCode(routeBookCode || initialBook.book_codes?.[0] || "");
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
  }, [router.isReady, router.query.bookCode, router.query.bookId]);

  useEffect(() => {
    if (!router.isReady) return;
    let active = true;
    async function load() {
      setLoadingPageSources(true);
      setPageSourceError(null);
      try {
        const res = await fetch("/api/granth-mapping/page-sources?limit=5000");
        const json = (await res.json()) as PageSourcesResponse | { error?: string };
        if (!res.ok) throw new Error(("error" in json && json.error) || `Request failed (${res.status})`);
        if (!active) return;
        const items = (json as PageSourcesResponse).items || [];
        const routeRawFileId = Number(readSingleQuery(router.query.rawFileId));
        const routeSource = Number.isFinite(routeRawFileId)
          ? items.find((item) => item.id === routeRawFileId)
          : null;
        const initialSource = routeSource || items[0] || null;

        setPageSources(items);
        if (initialSource) setPageSourceId(initialSource.id);
        if (routeSource) {
          setKind("pages");
          setIncludeCover(false);
          setDownloadContextPages(0);
        }
      } catch (loadError) {
        if (active) setPageSourceError(loadError instanceof Error ? loadError.message : String(loadError));
      } finally {
        if (active) setLoadingPageSources(false);
      }
    }
    void load();
    return () => {
      active = false;
    };
  }, [router.isReady, router.query.rawFileId]);

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

  const filteredPageSources = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return pageSources;
    return pageSources.filter((source) =>
      [
        source.display_name,
        source.file_name,
        source.custom_id,
        source.collection,
        source.subcollection,
        source.original_rel_path,
        source.ocr_granth_key,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(q)
    );
  }, [pageSources, query]);

  const selectedBook = useMemo(
    () => books.find((book) => book.id === bookId) || filteredBooks[0] || null,
    [bookId, books, filteredBooks]
  );

  const selectedPageSource = useMemo(
    () => pageSources.find((source) => source.id === pageSourceId) || filteredPageSources[0] || null,
    [filteredPageSources, pageSourceId, pageSources]
  );

  const isPageMode = kind === "pages";
  const selectedCodes = isPageMode ? [] : selectedBook?.book_codes || [];
  const activeSourceReady = isPageMode ? Boolean(selectedPageSource) : Boolean(selectedBook);
  const segments = result?.segments || [];
  const totalPages = segments.reduce((sum, segment) => sum + segment.pages.length, 0);
  const totalDownloadPages = segments.reduce(
    (sum, segment) => sum + segmentDownloadPages(segment, downloadContextPages, includeCover).length,
    0
  );
  const combinedDownloadPages = countCombinedDownloadPages(segments, downloadContextPages, includeCover);
  const previewSegments = useMemo(
    () => buildPreviewSegments(segments, downloadContextPages, includeCover),
    [downloadContextPages, includeCover, segments]
  );
  const preparedSelection = useMemo(
    () => prepareBuildSelection(previewSegments, previewSelection),
    [previewSegments, previewSelection]
  );
  const selectedTitle = isPageMode ? titleForPageSource(selectedPageSource) : titleForBook(selectedBook);
  const activePreviewPages = previewMode === "separate" ? preparedSelection.separatePages : preparedSelection.combinedPages;
  const previewReady = Boolean(previewMode && previewObjectUrl && activePreviewPages > 0);

  useEffect(() => {
    if (!selectedBook || isPageMode) {
      setContext(null);
      setContextError(null);
      setContextLoading(false);
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
  }, [bookCode, isPageMode, selectedBook]);

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
  const rawCoverKey = selectedPageSource ? -selectedPageSource.id : 0;

  useEffect(() => {
    return () => {
      if (previewObjectUrl) URL.revokeObjectURL(previewObjectUrl);
    };
  }, [previewObjectUrl]);

  async function resolveSelection(forceIncludeAllIdentifiers = includeAllIdentifiers) {
    if (!activeSourceReady) return null;
    setResolving(true);
    setError(null);
    setNotice(null);
    setResult(null);
    resetPreviewState();
    try {
      const params = new URLSearchParams({
        kind,
        spec,
        includeCover: includeCover ? "1" : "0",
      });
      if (isPageMode && selectedPageSource) {
        params.set("rawFileId", String(selectedPageSource.id));
      } else if (selectedBook) {
        params.set("bookId", String(selectedBook.id));
        if (bookCode) params.set("bookCode", bookCode);
      }
      if (kind === "gathas" && adhikar.trim()) params.set("adhikar", adhikar.trim());
      if (kind === "gathas" && forceIncludeAllIdentifiers && !adhikar.trim()) {
        params.set("includeAllIdentifiers", "1");
      }
      const res = await fetch(`/api/granth-mapping/resolve?${params.toString()}`);
      const json = (await res.json()) as ResolveResponse;
      if (!res.ok) {
        setResult(json);
        throw new Error(json.error || `Request failed (${res.status})`);
      }
      setIncludeAllIdentifiers(forceIncludeAllIdentifiers && !adhikar.trim());
      setResult(json);
      setPreviewSelection(emptyPreviewSelection());
      return json;
    } catch (resolveError) {
      setError(resolveError instanceof Error ? resolveError.message : String(resolveError));
      return null;
    } finally {
      setResolving(false);
    }
  }

  function invalidateProcessedPreview() {
    setPdfTarget(null);
    setPreviewObjectUrl((current) => {
      if (current) URL.revokeObjectURL(current);
      return null;
    });
  }

  function resetPreviewState() {
    setPreviewMode(null);
    setDeliveryRequest(null);
    setPreviewSelection(emptyPreviewSelection());
    setPreviewingKey(null);
    invalidateProcessedPreview();
  }

  function buildRequestBody(
    mode: BuildMode,
    delivery: DeliveryMode,
    email: string | undefined,
    selection: BuildSelectionPayload,
    title = selectedTitle
  ) {
    const rawFileId = isPageMode ? selectedPageSource?.id : null;
    if (!rawFileId && !selectedBook) return null;
    return {
      bookId: rawFileId ? null : selectedBook?.id,
      bookCode: rawFileId ? "" : bookCode,
      rawFileId,
      kind,
      spec,
      adhikar: kind === "gathas" && adhikar.trim() ? adhikar.trim() : null,
      includeCover,
      includeAllIdentifiers: kind === "gathas" && includeAllIdentifiers && !adhikar.trim(),
      contextPages: downloadContextPages,
      delivery,
      email,
      mode,
      title,
      selection,
    };
  }

  async function openProcessedPreview(
    previewKey: string,
    title: string,
    selection: BuildSelectionPayload = preparedSelection.selection
  ) {
    if (!activeSourceReady) return;
    const pageCount = selection.pagesByPdf.reduce((sum, item) => sum + item.pages.length, 0);
    if (pageCount <= 0) {
      setError("Select at least one page before generating the preview.");
      return;
    }

    setPreviewingKey(previewKey);
    setError(null);
    setNotice(null);
    try {
      const body = buildRequestBody("combined", "download", undefined, selection, `${selectedTitle}_preview`);
      if (!body) return;
      const res = await fetch("/api/granth-mapping/build-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const body = await res.json().catch(async () => ({ error: await res.text() }));
        throw new Error(body?.error || `Preview failed (${res.status})`);
      }

      const blob = await res.blob();
      const objectUrl = URL.createObjectURL(blob);
      setPreviewObjectUrl((current) => {
        if (current) URL.revokeObjectURL(current);
        return objectUrl;
      });
      setPdfTarget({
        pdfUrl: objectUrl,
        page: 1,
        title,
      });
    } catch (previewError) {
      invalidateProcessedPreview();
      setError(previewError instanceof Error ? previewError.message : String(previewError));
    } finally {
      setPreviewingKey(null);
    }
  }

  async function openDownloadPreview(mode: BuildMode) {
    setError(null);
    setNotice(null);
    setPreviewMode(mode);

    if (segments.length > 0) {
      await openProcessedPreview("download-preview", `${selectedTitle} selected pages`);
      return;
    }

    const resolved = await resolveSelection();
    const resolvedSegments = resolved?.segments || [];
    if (resolvedSegments.length === 0) return;
    const nextPreviewSegments = buildPreviewSegments(resolvedSegments, downloadContextPages, includeCover);
    const nextPrepared = prepareBuildSelection(nextPreviewSegments, emptyPreviewSelection());
    setPreviewMode(mode);
    await openProcessedPreview("download-preview", `${selectedTitle} selected pages`, nextPrepared.selection);
  }

  function updatePreviewSelection(update: (current: PreviewSelectionState) => PreviewSelectionState) {
    invalidateProcessedPreview();
    setPreviewSelection(update);
  }

  function setSegmentSelected(segmentKey: string, selected: boolean) {
    updatePreviewSelection((current) => {
      const disabledSegments = { ...current.disabledSegments };
      if (selected) {
        delete disabledSegments[segmentKey];
      } else {
        disabledSegments[segmentKey] = true;
      }
      return { ...current, disabledSegments };
    });
  }

  function setRangeSelected(rangeKey: string, selected: boolean) {
    updatePreviewSelection((current) => {
      const disabledRanges = { ...current.disabledRanges };
      if (selected) {
        delete disabledRanges[rangeKey];
      } else {
        disabledRanges[rangeKey] = true;
      }
      return { ...current, disabledRanges };
    });
  }

  function setPageSelected(segmentKey: string, page: number, selected: boolean) {
    updatePreviewSelection((current) => {
      const segmentPages = { ...(current.disabledPages[segmentKey] || {}) };
      if (selected) {
        delete segmentPages[page];
      } else {
        segmentPages[page] = true;
      }

      const disabledPages = { ...current.disabledPages };
      if (Object.keys(segmentPages).length === 0) {
        delete disabledPages[segmentKey];
      } else {
        disabledPages[segmentKey] = segmentPages;
      }
      return { ...current, disabledPages };
    });
  }

  function selectAllPreviewParts() {
    resetPreviewState();
    setPreviewMode(previewMode);
  }

  function clearAllPreviewParts() {
    updatePreviewSelection(() => ({
      disabledSegments: Object.fromEntries(previewSegments.map((segment) => [segment.key, true])),
      disabledRanges: {},
      disabledPages: {},
    }));
  }

  function segmentSelectionPayload(previewSegment: PreviewSegment): BuildSelectionPayload {
    return {
      pagesByPdf: [{ pdfUrl: previewSegment.segment.pdfUrl, pages: previewSegment.pages }],
      rangeIndexesByPdf: [
        {
          pdfUrl: previewSegment.segment.pdfUrl,
          rangeIndexes: previewSegment.ranges.map((range) => range.index + 1),
        },
      ],
    };
  }

  function rangeSelectionPayload(previewSegment: PreviewSegment, range: PreviewRange): BuildSelectionPayload {
    return {
      pagesByPdf: [{ pdfUrl: previewSegment.segment.pdfUrl, pages: range.pages }],
      rangeIndexesByPdf: [{ pdfUrl: previewSegment.segment.pdfUrl, rangeIndexes: [range.index + 1] }],
    };
  }

  async function buildDownload(mode: BuildMode, delivery: DeliveryMode, email?: string) {
    if (!activeSourceReady) return;
    const activeSelection = prepareBuildSelection(previewSegments, previewSelection);
    const selectedPageCount = mode === "separate" ? activeSelection.separatePages : activeSelection.combinedPages;
    if (selectedPageCount <= 0) {
      setError("Select at least one page before downloading.");
      return;
    }

    setBuildingMode(mode);
    setError(null);
    setNotice(null);
    try {
      const body = buildRequestBody(mode, delivery, email, activeSelection.selection);
      if (!body) return;
      const res = await fetch("/api/granth-mapping/build-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const body = await res.json().catch(async () => ({ error: await res.text() }));
        throw new Error(body?.error || `Build failed (${res.status})`);
      }

      if (delivery === "email") {
        const json = (await res.json()) as { email?: string };
        setDeliveryRequest(null);
        setNotice(`Email sent to ${json.email || email}.`);
        return;
      }

      const blob = await res.blob();
      const suffix = mode === "separate" ? "separate.zip" : "combined.pdf";
      downloadBlob(blob, `granth_${fileSafe(selectedTitle)}_${suffix}`);
      setDeliveryRequest(null);
    } catch (downloadError) {
      setError(downloadError instanceof Error ? downloadError.message : String(downloadError));
    } finally {
      setBuildingMode(null);
    }
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
            <span>{isPageMode ? `${pageSources.length} PDF` : `${context?.meta.file_count ?? 0} PDF`}</span>
            <span>
              {isPageMode
                ? selectedPageSource?.page_count
                  ? `${selectedPageSource.page_count} pages`
                  : "Page source"
                : `${context?.meta.total_gathas ?? 0} gathas`}
            </span>
            <span>
              {segments.length
                ? `${segments.length} output PDF / ${combinedDownloadPages} download pages`
                : "No output yet"}
            </span>
          </div>
        </header>

        {bookError ? <div className="extractorError" role="alert">{bookError}</div> : null}
        {pageSourceError ? <div className="extractorError" role="alert">{pageSourceError}</div> : null}

        <section className="extractorLayout">
          <aside className="extractorControlPanel">
            <div className="extractorPanelHeader">
              <span>Selection</span>
              {isPageMode
                ? loadingPageSources
                  ? <strong>Loading</strong>
                  : <strong>{filteredPageSources.length}</strong>
                : loadingBooks
                  ? <strong>Loading</strong>
                  : <strong>{filteredBooks.length}</strong>}
            </div>

            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search granth name or PDF"
              aria-label="Search granth name or PDF"
              className="extractorInput"
            />

            {isPageMode ? (
              <select
                value={selectedPageSource?.id || ""}
                onChange={(event) => {
                  const nextId = Number(event.target.value);
                  setPageSourceId(nextId);
                  setIncludeAllIdentifiers(false);
                  setResult(null);
                  resetPreviewState();
                }}
                disabled={loadingPageSources}
                className="extractorInput"
              >
                {filteredPageSources.map((source) => (
                  <option key={source.id} value={source.id}>
                    {titleForPageSource(source)}
                  </option>
                ))}
              </select>
            ) : (
              <select
                value={selectedBook?.id || ""}
                onChange={(event) => {
                  const nextId = Number(event.target.value);
                  const next = books.find((book) => book.id === nextId) || null;
                  setBookId(nextId);
                  setBookCode(next?.book_codes?.[0] || "");
                  setAdhikar("");
                  setIncludeAllIdentifiers(false);
                  setResult(null);
                  resetPreviewState();
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
            )}

            {isPageMode ? null : (
              <div className="extractorCodeGrid" aria-label="Book codes">
                <button
                  type="button"
                  onClick={() => {
                    setBookCode("");
                    setIncludeAllIdentifiers(false);
                    setResult(null);
                    resetPreviewState();
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
                      setIncludeAllIdentifiers(false);
                      setResult(null);
                      resetPreviewState();
                    }}
                    className={bookCode === code ? "isActive" : ""}
                  >
                    {codeLabel(code)}
                  </button>
                ))}
              </div>
            )}

            <div className="extractorModeGrid">
              <button
                type="button"
                onClick={() => {
                  setKind("gathas");
                  setIncludeCover(true);
                  setDownloadContextPages(DEFAULT_CONTEXT_PAGE_RADIUS);
                  setIncludeAllIdentifiers(false);
                  setResult(null);
                  resetPreviewState();
                }}
                className={kind === "gathas" ? "isActive" : ""}
              >
                Gathas
              </button>
              <button
                type="button"
                onClick={() => {
                  setKind("pages");
                  setIncludeCover(false);
                  setDownloadContextPages(0);
                  if (!pageSourceId && pageSources[0]) setPageSourceId(pageSources[0].id);
                  setIncludeAllIdentifiers(false);
                  setResult(null);
                  resetPreviewState();
                }}
                className={kind === "pages" ? "isActive" : ""}
              >
                Pages
              </button>
            </div>

            <input
              value={spec}
              onChange={(event) => {
                setSpec(event.target.value);
                setIncludeAllIdentifiers(false);
                setResult(null);
                resetPreviewState();
              }}
              placeholder={kind === "gathas" ? "5 or 3-6, 10" : "2, 5-7, 10"}
              aria-label={kind === "gathas" ? "Gatha numbers" : "Page numbers"}
              className="extractorInput"
            />

            {kind === "gathas" ? (
              <select
                value={adhikar}
                onChange={(event) => {
                  setAdhikar(event.target.value);
                  setIncludeAllIdentifiers(false);
                  setResult(null);
                  resetPreviewState();
                }}
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
                onChange={(event) => {
                  setIncludeCover(event.target.checked);
                  setPreviewSelection(emptyPreviewSelection());
                  resetPreviewState();
                }}
              />
              <span>Include cover page</span>
            </label>

            <label className="extractorFieldLabel">
              <span>Nearby pages</span>
              <input
                type="number"
                min={0}
                max={MAX_CONTEXT_PAGE_RADIUS}
                inputMode="numeric"
                value={downloadContextPages}
                onChange={(event) => {
                  setDownloadContextPages(event.target.value === "" ? 0 : normalizeContextPageRadius(event.target.value));
                  setPreviewSelection(emptyPreviewSelection());
                  resetPreviewState();
                }}
                className="extractorInput"
              />
            </label>

            <div className="extractorActions">
              <button
                type="button"
                onClick={() => void resolveSelection()}
                disabled={resolving || !activeSourceReady || !spec.trim()}
                className="extractorPrimaryButton"
                aria-busy={resolving}
              >
                {resolving ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Resolving
                  </span>
                ) : (
                  "Resolve"
                )}
              </button>
              <button
                type="button"
                onClick={() => void openDownloadPreview("combined")}
                disabled={Boolean(buildingMode) || resolving || Boolean(previewingKey) || !activeSourceReady || !spec.trim()}
                aria-busy={previewingKey === "download-preview"}
              >
                {previewingKey === "download-preview" && previewMode === "combined" ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Previewing
                  </span>
                ) : (
                  "Preview combined"
                )}
              </button>
              <button
                type="button"
                onClick={() => void openDownloadPreview("separate")}
                disabled={Boolean(buildingMode) || resolving || Boolean(previewingKey) || !activeSourceReady || !spec.trim()}
                aria-busy={previewingKey === "download-preview"}
              >
                {previewingKey === "download-preview" && previewMode === "separate" ? (
                  <span className="buttonSpinnerLabel">
                    <span className="loadingSpinner" aria-hidden="true" />
                    Previewing
                  </span>
                ) : (
                  "Preview separate"
                )}
              </button>
            </div>

            {error ? <div className="extractorError" role="alert">{error}</div> : null}
            {notice ? <div className="extractorNotice" role="status">{notice}</div> : null}
            {kind === "gathas" && result?.conflicts?.length ? (
              <div className="extractorConflictPanel" role="status" aria-live="polite">
                <strong>Gatha appears in multiple identifiers</strong>
                <div className="extractorConflictList">
                  {result.conflicts.map((conflict) => (
                    <div key={conflict.gatha}>
                      <span>Gatha {conflict.gatha}</span>
                      <div>
                        {conflict.adhikars.map((id) => (
                          <button
                            key={`${conflict.gatha}_${id}`}
                            type="button"
                            onClick={() => {
                              setAdhikar(id === "none" ? "" : id);
                              setIncludeAllIdentifiers(false);
                              setResult(null);
                              resetPreviewState();
                            }}
                          >
                            id {id}
                          </button>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
                <button
                  type="button"
                  className="extractorPrimaryButton"
                  onClick={() => {
                    setAdhikar("");
                    setIncludeAllIdentifiers(true);
                    void resolveSelection(true);
                  }}
                  disabled={resolving}
                  aria-busy={resolving}
                >
                  {resolving ? (
                    <span className="buttonSpinnerLabel">
                      <span className="loadingSpinner" aria-hidden="true" />
                      Resolving
                    </span>
                  ) : (
                    "Include all identifiers"
                  )}
                </button>
              </div>
            ) : null}
            {result?.ranges?.length ? (
              <div className="extractorRangeHint">
                {result.ranges.map((range) => (
                  <button
                    key={`${range.adhikar ?? "none"}_${range.minGatha}_${range.maxGatha}`}
                    type="button"
                    onClick={() => {
                      setAdhikar(range.adhikar == null ? "" : String(range.adhikar));
                      setResult(null);
                      resetPreviewState();
                    }}
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
                {isPageMode && selectedPageSource?.cover_image_url && !brokenCoverIds[rawCoverKey] ? (
                  <img
                    src={selectedPageSource.cover_image_url}
                    alt={`${selectedTitle} cover`}
                    onError={() => setBrokenCoverIds((prev) => ({ ...prev, [rawCoverKey]: true }))}
                  />
                ) : !isPageMode && primaryFile?.cover_image_url && !brokenCoverIds[primaryFile.id] ? (
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
                <div className="extractorEyebrow">
                  {isPageMode ? selectedPageSource?.collection || "PDF" : bookCode ? codeLabel(bookCode) : "All codes"}
                </div>
                <h2>{selectedTitle}</h2>
                <p>
                  {isPageMode
                    ? selectedPageSource?.file_name || selectedPageSource?.original_rel_path || "Uploaded PDF"
                    : selectedBook?.author_text || selectedBook?.details_text || "Mapped granth selection"}
                </p>
                <div className="extractorBookMeta">
                  <span>
                    {isPageMode
                      ? selectedPageSource?.page_count
                        ? `${selectedPageSource.page_count} pages`
                        : toMB(selectedPageSource?.file_size ?? null) || "PDF"
                      : `${context?.meta.identifier_count ?? 0} identifiers`}
                  </span>
                  <span>
                    {isPageMode
                      ? selectedPageSource?.text_row_count
                        ? `${selectedPageSource.text_row_count} OCR pages`
                        : selectedPageSource?.subcollection || "PDF"
                      : `${context?.meta.mapped_row_count ?? 0} mapped gathas`}
                  </span>
                  <span>
                    {isPageMode ? (
                      selectedPageSource?.mapping_book_id ? "Mapping available" : "Ready"
                    ) : contextLoading ? (
                      <span className="buttonSpinnerLabel">
                        <span className="loadingSpinner" aria-hidden="true" />
                        Loading context
                      </span>
                    ) : (
                      contextError || "Context ready"
                    )}
                  </span>
                </div>
              </div>
            </div>

            {!isPageMode && context?.files.length ? (
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
                        resetPreviewState();
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

            {isPageMode ? (
              <div className="extractorInfoGrid">
                <section className="extractorInfoPanel">
                  <div className="extractorPanelHeader">
                    <span>PDF Source</span>
                    <strong>{selectedPageSource?.page_count ? `${selectedPageSource.page_count}` : "PDF"}</strong>
                  </div>
                  <div className="extractorRangeList">
                    {selectedPageSource?.file_name ? (
                      <div>
                        <strong>{selectedPageSource.file_name}</strong>
                        <span>{selectedPageSource.original_rel_path || selectedPageSource.custom_id || "Uploaded file"}</span>
                      </div>
                    ) : null}
                    {selectedPageSource?.collection || selectedPageSource?.subcollection ? (
                      <div>
                        <strong>{[selectedPageSource.collection, selectedPageSource.subcollection].filter(Boolean).join(" / ")}</strong>
                        <span>{toMB(selectedPageSource.file_size) || "PDF"}</span>
                      </div>
                    ) : null}
                  </div>
                </section>

                <section className="extractorInfoPanel">
                  <div className="extractorPanelHeader">
                    <span>Linked Data</span>
                    <strong>{selectedPageSource?.ocr_granth_key ? "OCR" : "PDF"}</strong>
                  </div>
                  <div className="extractorRangeList">
                    {selectedPageSource?.ocr_granth_key ? (
                      <div>
                        <strong>{selectedPageSource.ocr_granth_key}</strong>
                        <span>{selectedPageSource.text_row_count ?? 0} OCR pages</span>
                      </div>
                    ) : null}
                    {selectedPageSource?.mapping_book_id ? (
                      <div>
                        <strong>Mapped book {selectedPageSource.mapping_book_id}</strong>
                        <span>{selectedPageSource.mapping_book_code ? `Code ${codeLabel(selectedPageSource.mapping_book_code)}` : "All codes"}</span>
                      </div>
                    ) : null}
                    {!selectedPageSource?.ocr_granth_key && !selectedPageSource?.mapping_book_id ? (
                      <div className="extractorMuted">No linked OCR or mapping data.</div>
                    ) : null}
                  </div>
                </section>
              </div>
            ) : (
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
                        onClick={() => {
                          setAdhikar(item.adhikar == null ? "" : String(item.adhikar));
                          setResult(null);
                          resetPreviewState();
                        }}
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
            )}

            <section className="extractorResultPanel">
              <div className="extractorPanelHeader">
                <span>Resolved Output</span>
                <strong>
                  {segments.length
                    ? `${totalPages} ${isPageMode ? "selected" : "mapped"} / ${combinedDownloadPages} combined pages`
                    : "Pending"}
                </strong>
              </div>

              {previewSegments.length ? (
                <div className="extractorSegmentGrid">
                  {previewSegments.map((previewSegment) => {
                    const segmentKey = `segment:${previewSegment.key}`;
                    return (
                      <article key={previewSegment.key}>
                        <div className="extractorSegmentHead">
                          <strong>{previewSegment.segment.pdfFileName}</strong>
                          <button
                            type="button"
                            className="inlinePdfButton"
                            onClick={() =>
                              void openProcessedPreview(
                                segmentKey,
                                `${previewSegment.segment.pdfFileName} selected pages`,
                                segmentSelectionPayload(previewSegment)
                              )
                            }
                            disabled={Boolean(previewingKey)}
                            aria-busy={previewingKey === segmentKey}
                          >
                            {previewingKey === segmentKey ? "Previewing" : "Preview selected"}
                          </button>
                        </div>
                        <div className="extractorSegmentMeta">
                          {codeLabel(previewSegment.segment.bookCode)} | {previewSegment.segment.pages.length}{" "}
                          {isPageMode ? "selected" : "mapped"} page
                          {previewSegment.segment.pages.length === 1 ? "" : "s"} | {previewSegment.pages.length} preview page
                          {previewSegment.pages.length === 1 ? "" : "s"}
                        </div>
                        <div className="extractorSegmentRanges">
                          {previewSegment.ranges.slice(0, 18).map((range) => {
                            const rangeKey = `range:${range.key}`;
                            return (
                              <button
                                key={range.key}
                                type="button"
                                onClick={() =>
                                  void openProcessedPreview(
                                    rangeKey,
                                    `${previewSegment.segment.pdfFileName} page ${range.range.pageStart}`,
                                    rangeSelectionPayload(previewSegment, range)
                                  )
                                }
                                disabled={Boolean(previewingKey)}
                                aria-busy={previewingKey === rangeKey}
                              >
                                {previewingKey === rangeKey ? "Previewing " : ""}
                                {range.range.gatha ? `id ${range.range.adhikar ?? "none"} / gatha ${range.range.gatha}: ` : ""}
                                page {range.range.pageStart}
                                {range.range.pageEnd !== range.range.pageStart ? `-${range.range.pageEnd}` : ""}
                              </button>
                            );
                          })}
                          {previewSegment.ranges.length > 18 ? (
                            <span>{previewSegment.ranges.length - 18} more ranges</span>
                          ) : null}
                        </div>
                      </article>
                    );
                  })}
                </div>
              ) : (
                <div className="extractorMuted">No pages resolved yet.</div>
              )}
            </section>

            {previewMode ? (
              <section className="extractorPreviewPanel">
                <div className="extractorPanelHeader">
                  <span>Processed Download Preview</span>
                  <strong>
                    {activePreviewPages} selected page{activePreviewPages === 1 ? "" : "s"}
                  </strong>
                </div>

                <div className="extractorPreviewNotice" role="status">
                  The modal preview is a generated PDF containing only the selected pages below. After changing any checkbox,
                  generate the preview again before downloading.
                </div>

                <div className="extractorPreviewToolbar">
                  <button
                    type="button"
                    className="extractorPrimaryButton"
                    onClick={() => void openProcessedPreview("download-preview", `${selectedTitle} selected pages`)}
                    disabled={activePreviewPages <= 0 || Boolean(previewingKey) || Boolean(buildingMode)}
                    aria-busy={previewingKey === "download-preview"}
                  >
                    {previewingKey === "download-preview" ? (
                      <span className="buttonSpinnerLabel">
                        <span className="loadingSpinner" aria-hidden="true" />
                        Generating preview
                      </span>
                    ) : previewObjectUrl ? (
                      "Regenerate preview"
                    ) : (
                      "Generate limited preview"
                    )}
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      if (!previewObjectUrl) return;
                      setPdfTarget({ pdfUrl: previewObjectUrl, page: 1, title: `${selectedTitle} selected pages` });
                    }}
                    disabled={!previewObjectUrl}
                  >
                    Open preview
                  </button>
                  <button type="button" onClick={selectAllPreviewParts} disabled={Boolean(previewingKey)}>
                    Select all
                  </button>
                  <button type="button" onClick={clearAllPreviewParts} disabled={Boolean(previewingKey)}>
                    Clear all
                  </button>
                  <button
                    type="button"
                    onClick={() => setDeliveryRequest({ mode: previewMode })}
                    disabled={!previewReady || Boolean(previewingKey) || Boolean(buildingMode)}
                  >
                    Continue to delivery
                  </button>
                </div>

                <div className="extractorPreviewGrid">
                  {previewSegments.map((previewSegment) => {
                    const segmentPages = selectedSegmentPages(previewSegment, previewSelection);
                    const segmentSelected = !previewSelection.disabledSegments[previewSegment.key] && segmentPages.length > 0;
                    return (
                      <article key={previewSegment.key} className={segmentSelected ? "" : "isDisabled"}>
                        <label className="extractorPreviewSegmentCheck">
                          <input
                            type="checkbox"
                            checked={segmentSelected}
                            onChange={(event) => setSegmentSelected(previewSegment.key, event.target.checked)}
                          />
                          <span>
                            <strong>{previewSegment.segment.pdfFileName}</strong>
                            <em>
                              {codeLabel(previewSegment.segment.bookCode)} | {segmentPages.length} of {previewSegment.pages.length} page
                              {previewSegment.pages.length === 1 ? "" : "s"}
                            </em>
                          </span>
                        </label>

                        <div className="extractorPreviewRanges" aria-label={`${previewSegment.segment.pdfFileName} ranges`}>
                          {previewSegment.ranges.map((range) => {
                            const rangePages = selectedRangePages(previewSegment, range, previewSelection);
                            const checked =
                              !previewSelection.disabledSegments[previewSegment.key] &&
                              !previewSelection.disabledRanges[range.key] &&
                              rangePages.length > 0;
                            return (
                              <label key={range.key}>
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  disabled={previewSelection.disabledSegments[previewSegment.key]}
                                  onChange={(event) => setRangeSelected(range.key, event.target.checked)}
                                />
                                <span>
                                  {range.range.gatha ? `Gatha ${range.range.gatha} | ` : ""}
                                  p.{range.range.pageStart}
                                  {range.range.pageEnd !== range.range.pageStart ? `-${range.range.pageEnd}` : ""}
                                  <em>{rangePages.length} pages</em>
                                </span>
                              </label>
                            );
                          })}
                        </div>

                        <div className="extractorPreviewPages" aria-label={`${previewSegment.segment.pdfFileName} pages`}>
                          {previewSegment.pages.map((page) => (
                            <label key={page} className={isPageDisabled(previewSelection, previewSegment.key, page) ? "isOff" : ""}>
                              <input
                                type="checkbox"
                                checked={!isPageDisabled(previewSelection, previewSegment.key, page)}
                                disabled={previewSelection.disabledSegments[previewSegment.key]}
                                onChange={(event) => setPageSelected(previewSegment.key, page, event.target.checked)}
                              />
                              <span>{page === 1 && includeCover ? "Cover" : `p.${page}`}</span>
                            </label>
                          ))}
                        </div>
                      </article>
                    );
                  })}
                </div>
              </section>
            ) : null}
          </section>
        </section>
      </div>
      <DownloadDeliveryDialog
        open={Boolean(deliveryRequest)}
        title="Choose download method"
        fileLabel={
          deliveryRequest?.mode === "separate"
            ? `Separate ZIP, ${preparedSelection.separatePages} selected page${preparedSelection.separatePages === 1 ? "" : "s"}`
            : `Combined PDF, ${preparedSelection.combinedPages} selected page${preparedSelection.combinedPages === 1 ? "" : "s"}`
        }
        busy={Boolean(buildingMode)}
        error={error}
        onClose={() => setDeliveryRequest(null)}
        onDownload={() => {
          if (deliveryRequest) void buildDownload(deliveryRequest.mode, "download");
        }}
        onEmail={(email) => {
          if (deliveryRequest) void buildDownload(deliveryRequest.mode, "email", email);
        }}
      />
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </main>
  );
}
