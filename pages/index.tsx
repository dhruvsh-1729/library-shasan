// The search page, the app's home page ("/"). Kept deliberately plain: one
// search box, a sentence saying what was found, and one card per page with an
// "Open this page" button. Everything else sits behind "More options".
import Link from "next/link";
import { useRouter } from "next/router";
import type { KeyboardEvent, ReactNode } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DownloadDeliveryDialog, type DeliveryMode } from "@/components/DownloadDeliveryDialog";
import { LightTableIcon } from "@/components/LightTableIcon";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { SearchExportDialog, type ExportFormat } from "@/components/SearchExportDialog";
import { downloadBlob, fileSafe, filenameFromResponse } from "@/lib/download-file";
import { prepareRow, rankRows } from "@/lib/granth-name-search";
import {
  OCR_SEARCH_MODE_OPTIONS,
  type OCRSearchMode,
  type OCRSearchScript,
  type OCRSearchScripts,
  findOCRSearchMatchesForQueries,
  isTooShortForContains,
  normalizeOCRSearchQueries,
  parseOCRSearchMode,
  parseOCRSearchScripts,
} from "@/lib/ocr-search";
import {
  DEFAULT_CONTEXT_PAGE_RADIUS,
  MAX_CONTEXT_PAGE_RADIUS,
  expandPagesWithContext,
  normalizeContextPageRadius,
} from "@/lib/page-context";
import { KOSHES } from "@/lib/koshes.mjs";
import {
  type CompoundWord,
  type QueryFormsWord,
  composeQueries,
  defaultParts,
  formsFromList,
  isRomanQuery,
  partQueries,
  partsFromList,
} from "@/lib/search-query";
import {
  type SearchRequest,
  type SearchScope,
  buildSearchUrl,
  parseSearchUrl,
  resolveGranthValues,
  sameIds,
} from "@/lib/search-url";

type SearchOccurrence = { snippet: string; matchStart: number; matchEnd: number; text: string };

type SearchResult = {
  granth_key?: string;
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
  page_number: number;
  snippet: string;
  occurrences?: SearchOccurrence[];
  occurrence_count?: number;
  verified?: boolean;
  csv_url?: string | null;
  source_rel_path?: string;
  source_page_number?: number;
  matched_queries?: string[];
};

type SearchMatchPage = { page_number: number; occurrence_count: number; snippet: string };

type SearchMatchPreview = {
  custom_id: string;
  pdf_name: string;
  pdf_url: string;
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
  title: string;
  query: string;
  queries: string[];
  matchMode: OCRSearchMode;
  scripts: OCRSearchScripts;
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
  key: string;
  granth_key: string;
  kind: "pdf" | "text";
  display_name: string;
  title: string;
  native_title: string;
  part: string | null;
  series: string | null;
  author: string | null;
  book_number: string | null;
  page_count: number | null;
  source_name: string;
};

type SearchSummary = {
  total: number;
  /** Hit pages the result list pages through (the index's count past the check cap). */
  listTotal: number;
  /** A full count is running in the background for a total marked "+". */
  counting: boolean;
  totalIsExact: boolean;
  occurrences: number;
  occurrencesExact: boolean;
  scannedPages: number;
  scanCap: number;
  formCounts: Array<{ form: string; count: number }>;
  page: number;
  queries: string[];
  scripts: OCRSearchScripts;
  matchMode: OCRSearchMode;
  scopeLabel: string;
  granthIds: string[];
  missingGranths: string[];
};


type Phase = "idle" | "spellings" | "searching" | "done";

const RESULTS_PER_PAGE = 20;
// The API caps a scoped search at this many granths.
const MAX_SELECTED_GRANTHS = 250;
const PICKER_LIMIT = 200;
const SCRIPT_LABELS: Record<OCRSearchScript, string> = { devanagari: "Devanagari", gujarati: "Gujarati" };
const nf = new Intl.NumberFormat("en-IN");
// The match modes in everyday words.
const PLAIN_MODE_LABELS: Record<OCRSearchMode, string> = {
  sanskrit_forms: "The word and all its forms",
  exact_word: "Only this exact word",
  begins_with: "Words starting with it",
  ends_with: "Words ending with it",
  contains: "Anywhere inside a word",
};

function isValidHttpUrl(value: string | null | undefined) {
  try {
    const url = new URL(String(value || ""));
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

/** A granth's short key when it is a book number; hash keys ("g6f79…") mean nothing to a reader. */
function bookNo(key: string | undefined) {
  return key && !/^g[0-9a-f]{6,}$/.test(key) ? key : "";
}

/** The found word and a little of its line, magnified, so the exact form reads at a glance. */
function Loupe({ occurrence }: { occurrence: SearchOccurrence }) {
  const { snippet, matchStart, matchEnd } = occurrence;
  return (
    <div className="ltLoupe" aria-hidden="true">
      <span className="ltLoupeSide isBefore">
        <span>{snippet.slice(Math.max(0, matchStart - 24), matchStart)}</span>
      </span>
      <mark className="ltRing">{snippet.slice(matchStart, matchEnd)}</mark>
      <span className="ltLoupeSide">
        <span>{snippet.slice(matchEnd, matchEnd + 24)}</span>
      </span>
    </div>
  );
}

function plural(n: number, one: string, many = `${one}s`) {
  return `${nf.format(n)} ${n === 1 ? one : many}`;
}

/** One grease-pencil ring around the text between start and end. */
function Ringed({ text, start, end }: { text: string; start: number; end: number }) {
  const a = Math.max(0, Math.min(start, text.length));
  const b = Math.max(a, Math.min(end, text.length));
  if (b <= a) return <>{text}</>;
  return (
    <>
      {text.slice(0, a)}
      <mark className="ltRing">{text.slice(a, b)}</mark>
      {text.slice(b)}
    </>
  );
}

function ringAll(text: string, queries: string[], mode: OCRSearchMode, scripts: OCRSearchScripts): ReactNode {
  const matches = findOCRSearchMatchesForQueries(text, queries, mode, scripts);
  if (!text || matches.length === 0) return text;
  const parts: ReactNode[] = [];
  let cursor = 0;
  matches.forEach((match, index) => {
    if (match.start > cursor) parts.push(text.slice(cursor, match.start));
    parts.push(
      <mark key={`${match.start}_${index}`} className="ltRing">
        {text.slice(match.start, match.end)}
      </mark>
    );
    cursor = match.end;
  });
  if (cursor < text.length) parts.push(text.slice(cursor));
  return parts;
}

export default function SearchPage() {
  const router = useRouter();
  const inputRef = useRef<HTMLInputElement>(null);

  // ---------------------------------------------------------------- form
  const [q, setQ] = useState("");
  const [searchMode, setSearchMode] = useState<OCRSearchMode>("sanskrit_forms");
  const [scriptOn, setScriptOn] = useState<Record<OCRSearchScript, boolean>>({ devanagari: true, gujarati: true });
  const [scope, setScope] = useState<SearchScope>("all");
  // Kept while switching to "All granths" and back, so a selection is never lost.
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [nameFilter, setNameFilter] = useState("");
  const [pickerOpen, setPickerOpen] = useState(false);
  // Phones show the match settings as one summary line until opened.
  const [settingsOpen, setSettingsOpen] = useState(false);

  // Spellings for a romanised query, per word, from /api/query-forms.
  const [formsWords, setFormsWords] = useState<QueryFormsWord[] | null>(null);
  const [formsFor, setFormsFor] = useState("");
  const [chosenForms, setChosenForms] = useState<string[][]>([]);
  const [formsError, setFormsError] = useState<string | null>(null);
  const formsCacheRef = useRef(new Map<string, QueryFormsWord[]>());
  // Forms named in a link, applied once the spellings for its query arrive.
  const pendingFormsRef = useRef<string[] | null>(null);

  // The words a compound query is made of, from /api/compound-parts, and the
  // ones chosen to be searched with it.
  const [compoundWords, setCompoundWords] = useState<CompoundWord[] | null>(null);
  const [compoundFor, setCompoundFor] = useState("");
  const [chosenParts, setChosenParts] = useState<string[]>([]);
  const partsCacheRef = useRef(new Map<string, CompoundWord[]>());
  const pendingPartsRef = useRef<string[] | null>(null);

  // ---------------------------------------------------------------- results
  const [results, setResults] = useState<SearchResult[]>([]);
  const [summary, setSummary] = useState<SearchSummary | null>(null);
  const [phase, setPhase] = useState<Phase>("idle");
  const [error, setError] = useState<string | null>(null);
  const [lastRequest, setLastRequest] = useState<SearchRequest | null>(null);
  const [expandedPages, setExpandedPages] = useState<Set<string>>(new Set());
  const searchSeqRef = useRef(0);

  // ---------------------------------------------------------------- catalog
  const [granthOptions, setGranthOptions] = useState<GranthOption[]>([]);
  const [granthAliases, setGranthAliases] = useState<Record<string, string>>({});
  const [loadingGranths, setLoadingGranths] = useState(true);
  const [granthLoadError, setGranthLoadError] = useState<string | null>(null);
  const [linkWarning, setLinkWarning] = useState<string | null>(null);

  // ---------------------------------------------------------------- dialogs
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);
  const [downloadPreview, setDownloadPreview] = useState<DownloadPreviewState | null>(null);
  const [deliveryFormat, setDeliveryFormat] = useState<ExportFormat | null>(null);
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const previewCacheRef = useRef(new Map<string, SearchMatchPreview>());

  const loading = phase === "spellings" || phase === "searching";
  const roman = isRomanQuery(q.trim());
  const scripts: OCRSearchScripts = parseOCRSearchScripts(
    (Object.keys(scriptOn) as OCRSearchScript[]).filter((script) => scriptOn[script])
  );

  useEffect(() => {
    let active = true;
    (async () => {
      try {
        const res = await fetch("/api/search-granths");
        const json = (await res.json()) as { items?: GranthOption[]; aliases?: Record<string, string>; error?: string };
        if (!res.ok) throw new Error(json.error || `Failed to load granths (${res.status})`);
        if (!active) return;
        setGranthOptions(json.items ?? []);
        setGranthAliases(json.aliases ?? {});
        setGranthLoadError(null);
      } catch (e) {
        if (active) setGranthLoadError(e instanceof Error ? e.message : String(e));
      } finally {
        if (active) setLoadingGranths(false);
      }
    })();
    return () => {
      active = false;
    };
  }, []);

  const optionById = useMemo(() => new Map(granthOptions.map((row) => [row.custom_id, row])), [granthOptions]);
  const keyById = useMemo(() => new Map(granthOptions.map((row) => [row.custom_id, row.key])), [granthOptions]);
  const optionByGranthKey = useMemo(() => new Map(granthOptions.map((row) => [row.granth_key, row])), [granthOptions]);

  // The picker ranks names the way the library page does: across scripts and
  // spellings ("acharang" finds आचारांग), and a bare number finds that book.
  const preparedOptions = useMemo(
    () =>
      granthOptions.map((row) =>
        prepareRow({
          source: row.kind === "pdf" ? row.source_name : `${row.granth_key} ${row.source_name}`,
          extra: `${row.key} ${row.title} ${row.native_title} ${row.series ?? ""}`,
        })
      ),
    [granthOptions]
  );
  const sortedOptions = useMemo(
    () => [...granthOptions].sort((a, b) => a.key.localeCompare(b.key, "en", { numeric: true, sensitivity: "base" })),
    [granthOptions]
  );
  const filteredOptions = useMemo(() => {
    const term = nameFilter.trim();
    if (!term) return sortedOptions;
    return rankRows(preparedOptions, term).map((index) => granthOptions[index]);
  }, [granthOptions, nameFilter, preparedOptions, sortedOptions]);

  // ---------------------------------------------------------------- spellings
  const loadForms = useCallback(async (text: string) => {
    const key = text.trim();
    const cached = formsCacheRef.current.get(key);
    if (cached) return cached;
    const res = await fetch(`/api/query-forms?q=${encodeURIComponent(key)}`);
    const json = (await res.json()) as { words?: QueryFormsWord[]; error?: string };
    if (!res.ok) throw new Error(json.error || `Could not read the spellings (${res.status})`);
    const words = json.words ?? [];
    formsCacheRef.current.set(key, words);
    return words;
  }, []);

  useEffect(() => {
    const text = q.trim();
    if (!isRomanQuery(text)) {
      setFormsWords(null);
      setFormsFor("");
      setChosenForms([]);
      setFormsError(null);
      return;
    }
    let active = true;
    const timer = window.setTimeout(async () => {
      try {
        const words = await loadForms(text);
        if (!active) return;
        const pending = pendingFormsRef.current;
        pendingFormsRef.current = null;
        setFormsWords(words);
        setFormsFor(text);
        setChosenForms(formsFromList(words, pending));
        setFormsError(null);
      } catch (e) {
        if (active) setFormsError(e instanceof Error ? e.message : String(e));
      }
    }, formsCacheRef.current.has(text) ? 0 : 260);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [q, loadForms]);

  function toggleForm(wordIndex: number, form: string) {
    setChosenForms((prev) =>
      prev.map((forms, index) => {
        if (index !== wordIndex) return forms;
        if (!forms.includes(form)) return [...forms, form];
        // A word always keeps at least one spelling.
        return forms.length > 1 ? forms.filter((f) => f !== form) : forms;
      })
    );
  }

  const loadParts = useCallback(async (text: string) => {
    const key = text.trim();
    const cached = partsCacheRef.current.get(key);
    if (cached) return cached;
    const res = await fetch(`/api/compound-parts?q=${encodeURIComponent(key)}`);
    const json = (await res.json()) as { words?: CompoundWord[]; error?: string };
    if (!res.ok) throw new Error(json.error || `Could not split the word (${res.status})`);
    const words = json.words ?? [];
    partsCacheRef.current.set(key, words);
    return words;
  }, []);

  /** The strings a request is searched as, reading the spellings and compound parts if needed. */
  async function queriesFor(request: SearchRequest) {
    let base: string[];
    let forms: string[] | null = null;
    if (!isRomanQuery(request.q)) base = normalizeOCRSearchQueries(request.q);
    else {
      const chosen = formsFromList(await loadForms(request.q), request.forms);
      base = composeQueries(chosen);
      forms = chosen.flat();
    }
    if (!base.length || (request.parts && request.parts.length === 0)) return { queries: base, forms, parts: request.parts };
    // A failed split only loses the parts; the word itself is still searched.
    const words = await loadParts(base[0]).catch(() => [] as CompoundWord[]);
    const parts = partsFromList(words, request.parts);
    return { queries: normalizeOCRSearchQueries([...base, ...partQueries(words, parts)]), forms, parts: request.parts };
  }

  const baseQueries = useMemo(() => {
    const text = q.trim();
    if (!text) return [];
    if (!roman) return normalizeOCRSearchQueries(text);
    return formsFor === text ? composeQueries(chosenForms) : [];
  }, [q, roman, formsFor, chosenForms]);

  // The word the compound parts are read from: the query as typed, or for a
  // romanised query its first chosen Devanagari spelling.
  const compoundBase = baseQueries[0] ?? "";
  useEffect(() => {
    const text = compoundBase.trim();
    if (Array.from(text).length < 4 || isRomanQuery(text)) {
      setCompoundWords(null);
      setCompoundFor("");
      setChosenParts([]);
      return;
    }
    let active = true;
    const timer = window.setTimeout(async () => {
      try {
        const words = await loadParts(text);
        if (!active) return;
        const pending = pendingPartsRef.current;
        pendingPartsRef.current = null;
        setCompoundWords(words);
        setCompoundFor(text);
        setChosenParts(partsFromList(words, pending));
      } catch {
        if (active) setCompoundWords(null);
      }
    }, partsCacheRef.current.has(text) ? 0 : 260);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [compoundBase, loadParts]);

  const partsReady = compoundWords !== null && compoundFor === compoundBase.trim() && compoundWords.length > 0;
  const currentQueries = useMemo(
    () =>
      partsReady && compoundWords
        ? normalizeOCRSearchQueries([...baseQueries, ...partQueries(compoundWords, chosenParts)])
        : baseQueries,
    [baseQueries, partsReady, compoundWords, chosenParts]
  );

  /** The parts as a request stores them: null while they are the default choice. */
  function requestParts(): string[] | null {
    if (!partsReady || !compoundWords) return null;
    return sameIds(chosenParts, defaultParts(compoundWords)) ? null : [...chosenParts];
  }

  function togglePart(term: string) {
    setChosenParts((prev) => (prev.includes(term) ? prev.filter((t) => t !== term) : [...prev, term]));
  }

  // ---------------------------------------------------------------- URL
  // The URL is the source of truth: on load and on back/forward the form is set
  // from it and the search it describes is run.
  const appliedUrlRef = useRef<string | null>(null);
  useEffect(() => {
    if (!router.isReady || loadingGranths) return;
    if (appliedUrlRef.current === router.asPath) return;
    appliedUrlRef.current = router.asPath;

    const parsed = parseSearchUrl(router.query);
    const { ids, missing } = resolveGranthValues(parsed.granthValues, granthOptions, granthAliases);
    const nextScope: SearchScope = parsed.granthValues.length ? "selected" : "all";
    setLinkWarning(
      missing.length
        ? `${plural(missing.length, "granth")} in this link could not be found (${missing.slice(0, 5).join(", ")}).`
        : null
    );
    pendingFormsRef.current = parsed.forms;
    pendingPartsRef.current = parsed.parts;
    setQ(parsed.q);
    setSearchMode(parsed.matchMode);
    setScriptOn({
      devanagari: !parsed.scripts || parsed.scripts.includes("devanagari"),
      gujarati: !parsed.scripts || parsed.scripts.includes("gujarati"),
    });
    setScope(nextScope);
    if (parsed.granthValues.length) setSelectedIds(ids);

    const request: SearchRequest = { ...parsed, scope: nextScope, granthIds: ids };
    // Old links (?customId=, ?langs=) are rewritten to the current form.
    const canonical = buildSearchUrl(request, keyById);
    if (granthOptions.length && canonical !== router.asPath && !isRomanQuery(parsed.q)) {
      appliedUrlRef.current = canonical;
      void router.replace(canonical, undefined, { shallow: true });
    }
    if (parsed.q && !(nextScope === "selected" && ids.length === 0)) void executeSearch(request);
    else {
      setResults([]);
      setSummary(null);
      setLastRequest(null);
      setPhase("idle");
    }
    // Runs when the URL changes or the granth list arrives; the form state it
    // writes is intentionally not a dependency.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [router.isReady, router.asPath, loadingGranths, granthOptions]);

  // "/" jumps to the search box, as in most search tools.
  useEffect(() => {
    function onKey(event: globalThis.KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      if (event.key !== "/" || event.metaKey || event.ctrlKey) return;
      if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable)) return;
      event.preventDefault();
      inputRef.current?.focus();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Escape closes the matched-pages sheet (the delivery dialog, when open, handles its own).
  useEffect(() => {
    if (!downloadPreview || deliveryFormat) return;
    function onKey(event: globalThis.KeyboardEvent) {
      if (event.key === "Escape") setDownloadPreview(null);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [downloadPreview, deliveryFormat]);

  // ---------------------------------------------------------------- search
  function formRequest(page: number): SearchRequest {
    return {
      q: q.trim(),
      forms: roman ? chosenForms.flat() : null,
      parts: requestParts(),
      scripts,
      matchMode: searchMode,
      scope,
      granthIds: scope === "all" ? [] : [...selectedIds],
      page,
    };
  }

  const formProblem = (() => {
    const text = q.trim();
    if (!text) return "Type a word to search.";
    if (Array.from(text).length < 2) return "Type at least 2 letters.";
    if (roman && formsFor !== text && !formsError) return null; // spellings still loading; run() waits for them
    if (searchMode === "contains" && currentQueries.some(isTooShortForContains)) {
      return "Contains search needs at least 3 letters once conjuncts are folded (न्द counts as 2).";
    }
    if (scope === "selected" && selectedIds.length === 0) return "Choose at least one granth, or search all granths.";
    if (scope === "selected" && selectedIds.length > MAX_SELECTED_GRANTHS) {
      return `Choose at most ${MAX_SELECTED_GRANTHS} granths, or search all granths.`;
    }
    return null;
  })();

  // The six kosh granths, for "search in the koshes".
  const koshIds = useMemo(
    () => KOSHES.map((kosh) => optionByGranthKey.get(kosh.key)?.custom_id).filter((id): id is string => Boolean(id)),
    [optionByGranthKey]
  );

  function run(page: number, only?: { granthIds: string[] }) {
    if (only) {
      setScope("selected");
      setSelectedIds(only.granthIds);
    } else if (formProblem) {
      setError(formProblem);
      return;
    }
    setError(null);
    const request = only ? { ...formRequest(page), scope: "selected" as const, granthIds: only.granthIds } : formRequest(page);
    const url = buildSearchUrl(request, keyById);
    if (url !== router.asPath) {
      // Each new search adds a history entry, so Back returns to the previous one.
      appliedUrlRef.current = url;
      void router.push(url, undefined, { shallow: true, scroll: false });
    }
    void executeSearch(request);
  }

  function goToResultPage(page: number) {
    if (!lastRequest) return;
    const request = { ...lastRequest, page };
    const url = buildSearchUrl(request, keyById);
    if (url !== router.asPath) {
      appliedUrlRef.current = url;
      void router.replace(url, undefined, { shallow: true, scroll: false });
    }
    void executeSearch(request);
    document.getElementById("lt-results")?.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  async function executeSearch(request: SearchRequest) {
    const seq = ++searchSeqRef.current;
    setError(null);
    try {
      if (isRomanQuery(request.q)) setPhase("spellings");
      const { queries, forms, parts } = await queriesFor(request);
      if (seq !== searchSeqRef.current) return;
      if (queries.length === 0) throw new Error("No spelling to search. Choose one of the spellings offered.");
      setPhase("searching");

      const params = new URLSearchParams();
      params.set("q", queries[0]);
      for (const variant of queries.slice(1)) params.append("queryVariant", variant);
      params.set("limit", String(RESULTS_PER_PAGE));
      params.set("page", String(request.page));
      params.set("matchMode", request.matchMode);
      if (request.scripts) params.set("scripts", request.scripts.join(","));
      if (request.scope === "selected") params.set("granths", request.granthIds.join(","));

      const res = await fetch(`/api/search?${params.toString()}`);
      const json = (await res.json()) as {
        results?: SearchResult[];
        total?: number;
        total_is_exact?: boolean;
        total_occurrences?: number;
        total_occurrences_exact?: boolean;
        occurrence_scanned_pages?: number;
        occurrence_scan_cap?: number;
        form_counts?: Array<{ form: string; count: number }>;
        page?: number;
        match_mode?: string;
        queries?: string[];
        missing_granths?: string[];
        error?: string;
      };
      if (seq !== searchSeqRef.current) return;
      if (!res.ok) throw new Error(json.error || `Search failed (${res.status})`);

      setResults(json.results ?? []);
      setExpandedPages(new Set());
      const exact = json.total_occurrences_exact !== false;
      setSummary({
        total: Number(json.total ?? 0),
        listTotal: Number(json.total ?? 0),
        counting: !exact,
        totalIsExact: json.total_is_exact !== false,
        occurrences: Number(json.total_occurrences ?? 0),
        occurrencesExact: json.total_occurrences_exact !== false,
        scannedPages: Number(json.occurrence_scanned_pages ?? 0),
        scanCap: Number(json.occurrence_scan_cap ?? 4000),
        formCounts: json.form_counts ?? [],
        page: Number(json.page ?? request.page),
        queries: json.queries?.length ? json.queries : queries,
        scripts: request.scripts,
        matchMode: parseOCRSearchMode(json.match_mode ?? request.matchMode),
        scopeLabel: request.scope === "all" ? "all granths" : plural(request.granthIds.length, "chosen granth"),
        granthIds: request.scope === "all" ? [] : request.granthIds,
        missingGranths: json.missing_granths ?? [],
      });
      setLastRequest({ ...request, forms, parts });
      setPhase("done");
      // More hit pages than /api/search checks at once: count them all in the
      // background and replace the "+" totals when that is done.
      if (!exact) {
        const countParams = new URLSearchParams(params);
        countParams.delete("limit");
        countParams.delete("page");
        void (async () => {
          try {
            const res = await fetch(`/api/search-count?${countParams.toString()}`);
            const count = (await res.json()) as { pages?: number; occurrences?: number; formCounts?: Array<{ form: string; count: number }>; error?: string };
            if (seq !== searchSeqRef.current) return;
            if (!res.ok) throw new Error(count.error || "count failed");
            setSummary((prev) =>
              prev
                ? {
                    ...prev,
                    counting: false,
                    total: Number(count.pages ?? prev.total),
                    totalIsExact: true,
                    occurrences: Number(count.occurrences ?? prev.occurrences),
                    occurrencesExact: true,
                    formCounts: count.formCounts ?? prev.formCounts,
                  }
                : prev
            );
          } catch {
            if (seq === searchSeqRef.current) setSummary((prev) => (prev ? { ...prev, counting: false } : prev));
          }
        })();
      }
    } catch (e) {
      if (seq !== searchSeqRef.current) return;
      setError(e instanceof Error ? e.message : String(e));
      setPhase(summary ? "done" : "idle");
    }
  }

  // True once the form no longer describes the results on screen.
  const resultsStale = Boolean(
    lastRequest &&
      phase === "done" &&
      (lastRequest.q !== q.trim() ||
        lastRequest.matchMode !== searchMode ||
        String(lastRequest.scripts) !== String(scripts) ||
        (roman && formsFor === q.trim() && !sameIds(lastRequest.forms ?? [], chosenForms.flat())) ||
        (partsReady && String(lastRequest.parts) !== String(requestParts())) ||
        lastRequest.scope !== scope ||
        (scope === "selected" && !sameIds(lastRequest.granthIds, selectedIds)))
  );

  function onSearchKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter" && !loading) run(1);
  }

  function toggleGranth(id: string) {
    setError(null);
    setSelectedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  }

  function setScriptEnabled(script: OCRSearchScript, on: boolean) {
    // At least one script always counts.
    setScriptOn((prev) => {
      const next = { ...prev, [script]: on };
      return next.devanagari || next.gujarati ? next : prev;
    });
  }

  // ---------------------------------------------------------------- preview / export
  function granthTitle(result: SearchResult) {
    const option = result.granth_key ? optionByGranthKey.get(result.granth_key) : undefined;
    return option?.display_name ?? result.pdf_name;
  }

  async function openDownloadPreview(result: SearchResult) {
    if (!summary) return;
    const queries = result.matched_queries?.length ? result.matched_queries : summary.queries;
    const cacheKey = [result.custom_id, result.source_rel_path, queries.join("|"), summary.matchMode, summary.scripts ?? ""].join("\n");
    setDownloadPreview({
      result,
      title: granthTitle(result),
      query: queries[0],
      queries,
      matchMode: summary.matchMode,
      scripts: summary.scripts,
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
        for (const variant of queries.slice(1)) params.append("queryVariant", variant);
        params.set("matchMode", summary.matchMode);
        if (summary.scripts) params.set("scripts", summary.scripts.join(","));
        if (result.source_rel_path) params.set("sourceRelPath", result.source_rel_path);
        const res = await fetch(`/api/search-match-pages?${params.toString()}`);
        const json = (await res.json()) as SearchMatchPreview & { error?: string };
        if (!res.ok) throw new Error(json.error || `Could not load the matched pages (${res.status})`);
        preview = json;
        previewCacheRef.current.set(cacheKey, preview);
      }
      const loaded = preview;
      setDownloadPreview((prev) =>
        prev && prev.result === result
          ? {
              ...prev,
              loading: false,
              preview: loaded,
              queries: loaded.queries?.length ? loaded.queries : prev.queries,
              selectedPages: loaded.pages.map((page) => page.page_number),
            }
          : prev
      );
    } catch (e) {
      setDownloadPreview((prev) =>
        prev && prev.result === result ? { ...prev, loading: false, error: e instanceof Error ? e.message : String(e) } : prev
      );
    }
  }

  function setPreviewPages(update: (pages: Set<number>, preview: SearchMatchPreview) => void) {
    setDownloadPreview((prev) => {
      if (!prev?.preview) return prev;
      const pages = new Set(prev.selectedPages);
      update(pages, prev.preview);
      return { ...prev, selectedPages: [...pages].sort((a, b) => a - b) };
    });
  }

  async function downloadMatchedPages(format: ExportFormat, delivery: DeliveryMode, email?: string) {
    if (!downloadPreview?.preview || downloadPreview.selectedPages.length === 0) return;
    const preview = downloadPreview.preview;
    setDownloadPreview((prev) => (prev ? { ...prev, downloading: true, error: null, notice: null } : prev));
    try {
      const res = await fetch(format === "pdf" ? "/api/search-match-pdf" : "/api/search-match-csv", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          customId: preview.custom_id,
          sourceRelPath: downloadPreview.result.source_rel_path || "",
          granthName: downloadPreview.title,
          q: downloadPreview.query,
          queryVariants: downloadPreview.queries.slice(1),
          matchMode: downloadPreview.matchMode,
          scripts: downloadPreview.scripts,
          pages: downloadPreview.selectedPages,
          ...(format === "pdf" ? { contextPages: downloadPreview.contextPages } : {}),
          delivery,
          email,
          title: downloadPreview.title,
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
                notice: `Sent to ${json.email || email}${typeof json.row_count === "number" ? ` with ${plural(json.row_count, "CSV row")}` : ""}.`,
              }
            : prev
        );
        return;
      }
      const blob = await res.blob();
      const base = `${fileSafe(downloadPreview.title)}_${fileSafe(downloadPreview.queries.join("_"))}`;
      downloadBlob(blob, filenameFromResponse(res, format === "pdf" ? `${base}_matched_pages.pdf` : `${base}_page_lines.csv`));
      setDeliveryFormat(null);
      setDownloadPreview(null);
    } catch (e) {
      setDownloadPreview((prev) => (prev ? { ...prev, downloading: false, error: e instanceof Error ? e.message : String(e) } : prev));
    }
  }

  // ---------------------------------------------------------------- render helpers
  const totalPages = summary ? Math.max(1, Math.ceil(summary.listTotal / RESULTS_PER_PAGE)) : 1;

  function textViewerHref(result: SearchResult) {
    if (!summary || !result.granth_key) return "";
    const params = new URLSearchParams();
    params.set("granthKey", result.granth_key);
    params.set("page", String(result.source_page_number ?? result.page_number));
    const queries = result.matched_queries?.length ? result.matched_queries : summary.queries;
    params.set("q", queries[0] ?? "");
    for (const variant of queries.slice(1)) params.append("queryVariant", variant);
    params.set("matchMode", summary.matchMode);
    if (summary.scripts) params.set("scripts", summary.scripts.join(","));
    return `/ocr-text-viewer?${params.toString()}`;
  }

  function renderResult(result: SearchResult, index: number) {
    if (!summary) return null;
    const queries = result.matched_queries?.length ? result.matched_queries : summary.queries;
    const occurrences = result.occurrences ?? [];
    const cardKey = `${result.granth_key ?? result.custom_id}:${result.page_number}`;
    const expanded = expandedPages.has(cardKey);
    const shown = expanded ? occurrences : occurrences.slice(0, 1);
    const hidden = occurrences.length - shown.length;
    const option = result.granth_key ? optionByGranthKey.get(result.granth_key) : undefined;
    const canOpenPdf = isValidHttpUrl(result.pdf_url);
    const verified = result.verified !== false;

    return (
      <article key={`${cardKey}:${index}`} className={`ltCard${verified ? "" : " isUnverified"}`}>
        <header className="ltCardHead">
          <div>
            <h3>{granthTitle(result)}</h3>
            {option?.native_title ? <p className="ltCardNative">{option.native_title}</p> : null}
          </div>
          <div className="ltFolio">
            <span>page</span>
            <strong>{result.page_number}</strong>
          </div>
        </header>
        {!verified ? <p className="ltCardNote">Not confirmed on this page</p> : null}
        {occurrences[0] ? <Loupe occurrence={occurrences[0]} /> : null}

        <div className="ltCardText">
          {shown.length ? (
            shown.map((occurrence, i) => (
              <p key={i} className="indic">
                <Ringed text={occurrence.snippet} start={occurrence.matchStart} end={occurrence.matchEnd} />
              </p>
            ))
          ) : (
            <p className="indic">{ringAll(result.snippet, queries, summary.matchMode, summary.scripts)}</p>
          )}
        </div>

        <footer className="ltCardFoot">
          {canOpenPdf ? (
            <button
              type="button"
              className="ltPrimary"
              onClick={() =>
                setPdfTarget({
                  pdfUrl: result.pdf_url,
                  page: result.page_number,
                  title: granthTitle(result),
                  searchTerm: queries[0],
                  searchTerms: queries,
                  searchMode: summary.matchMode,
                  searchScripts: summary.scripts,
                })
              }
            >
              Open this page
            </button>
          ) : result.granth_key ? (
            <Link className="ltPrimary" href={textViewerHref(result)}>
              Open this page
            </Link>
          ) : null}
          {occurrences.length > 1 ? (
            <button
              type="button"
              className="ltLink"
              onClick={() =>
                setExpandedPages((prev) => {
                  const next = new Set(prev);
                  if (next.has(cardKey)) next.delete(cardKey);
                  else next.add(cardKey);
                  return next;
                })
              }
            >
              {expanded ? "Show less" : `${hidden} more on this page`}
            </button>
          ) : null}
          {canOpenPdf ? (
            <button type="button" className="ltLink" onClick={() => void openDownloadPreview(result)}>
              Download pages
            </button>
          ) : null}
        </footer>
      </article>
    );
  }

  const pageLabel = summary ? `Page ${summary.page} of ${nf.format(totalPages)}` : "";

  // ---------------------------------------------------------------- page
  return (
    <main className="lt">
      <div className="ltFrame">
        <header className="ltTop">
          <h1>Search the granths</h1>
          <nav className="ltNav" aria-label="Other pages">
            <Link href="/library">Library</Link>
            <Link href="/ask">Ask</Link>
            <Link href="/scannable-documents">Scan status</Link>
          </nav>
        </header>

        <section className="ltSearch" aria-label="Search">
          <div className="ltBox">
            <input
              ref={inputRef}
              id="search-query"
              className="indic"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onKeyDown={onSearchKeyDown}
              placeholder="Type a word, e.g. हिंसा or hinsa"
              aria-label="Word to search"
              autoComplete="off"
              spellCheck={false}
            />
            <button type="button" className="ltGo" onClick={() => run(1)} disabled={loading} aria-busy={loading}>
              {loading ? "Searching…" : "Search"}
            </button>
          </div>

          {roman ? (
            <div className="ltSpellings" aria-live="polite">
              {formsError ? (
                <span className="ltWarn">{formsError}</span>
              ) : formsFor !== q.trim() || !formsWords ? (
                <span className="ltMuted">…</span>
              ) : (
                <>

                  {formsWords.map((word, wordIndex) =>
                    word.forms.slice(0, 4).map((form) => {
                      const on = chosenForms[wordIndex]?.includes(form.form) ?? false;
                      return (
                        <button
                          key={`${wordIndex}-${form.form}`}
                          type="button"
                          className={`ltSpelling${on ? " isOn" : ""}`}
                          aria-pressed={on}
                          onClick={() => toggleForm(wordIndex, form.form)}
                          title={on ? "Click to stop searching this spelling" : "Click to also search this spelling"}
                        >
                          {on ? <LightTableIcon name="check" size={14} /> : null}
                          <span className="indic">{form.form}</span>
                        </button>
                      );
                    })
                  )}
                </>
              )}
            </div>
          ) : null}

          {partsReady && compoundWords ? (
            <div className="ltParts" aria-live="polite">
              {compoundWords.map((word) => (
                <div key={word.word} className="ltPartsRow">
                  <span className="ltPartsLabel">
                    <span className="indic">{word.word}</span>{" "}
                    {word.inKosh.length ? "is in the koshes; its parts:" : "is made of:"}
                  </span>
                  {word.parts.map((part, index) => {
                    const key = `${word.word}-${index}-${part.text}`;
                    if (part.kind === "prefix" || part.kind === "ending") {
                      return (
                        <span
                          key={key}
                          className="ltPartFixed indic"
                          title={part.kind === "prefix" ? "A prefix (not searched on its own)" : "A case ending (not searched)"}
                        >
                          {part.kind === "prefix" ? `${part.term}-` : `-${part.text}`}
                        </span>
                      );
                    }
                    const term = part.term ?? part.text;
                    const on = chosenParts.includes(term);
                    const where =
                      part.kind === "unknown"
                        ? "Not in the koshes or the vishay list"
                        : part.vishayOnly
                          ? "A vishay term; not a kosh headword"
                          : `In ${part.koshes?.join(", ") || "the kosh text"}`;
                    const also = part.alias ? ` Also searched as ${part.alias}.` : "";
                    const pages = typeof part.pages === "number" ? ` ${plural(part.pages, "page")} in the library.` : "";
                    return (
                      <button
                        key={key}
                        type="button"
                        className={`ltSpelling ltPart${on ? " isOn" : ""}${part.kind === "word" && !part.vishayOnly ? "" : " isOutsideKosh"}`}
                        aria-pressed={on}
                        onClick={() => togglePart(term)}
                        title={`${where}.${also}${pages} Click to ${on ? "stop searching" : "also search"} this part.`}
                      >
                        {on ? <LightTableIcon name="check" size={14} /> : null}
                        <span className="indic">{term}</span>
                      </button>
                    );
                  })}
                </div>
              ))}
              {koshIds.length && chosenParts.length ? (
                <button
                  type="button"
                  className="ltLink ltPartsKosh"
                  onClick={() => run(1, { granthIds: koshIds })}
                  disabled={loading}
                  title="Search the word and the chosen parts in Shabda Ratna Mahodadhi, Abhidhan Vyutpatti Prakriya Kosh and Apte"
                >
                  Search these in the koshes
                </button>
              ) : null}
            </div>
          ) : null}

          <button
            type="button"
            className="ltMoreToggle"
            aria-expanded={settingsOpen}
            aria-controls="lt-options"
            onClick={() => setSettingsOpen((open) => !open)}
          >
            {settingsOpen ? "Hide options" : "More options"}
            <LightTableIcon name="chevron" size={14} />
          </button>

          {settingsOpen ? (
            <div id="lt-options" className="ltOptions">
              <div className="ltOption">
                <span className="ltOptionLabel">Find</span>
                <div className="ltChoices" role="radiogroup" aria-label="Find">
                  {OCR_SEARCH_MODE_OPTIONS.map((option) => (
                    <label key={option.mode} className={searchMode === option.mode ? "isOn" : ""}>
                      <input
                        type="radio"
                        name="lt-mode"
                        checked={searchMode === option.mode}
                        onChange={() => setSearchMode(option.mode)}
                      />
                      {PLAIN_MODE_LABELS[option.mode]}
                    </label>
                  ))}
                </div>
              </div>

              <div className="ltOption">
                <span className="ltOptionLabel">Script</span>
                <div className="ltChoices">
                  {(Object.keys(SCRIPT_LABELS) as OCRSearchScript[]).map((script) => (
                    <label key={script} className={scriptOn[script] ? "isOn" : ""}>
                      <input type="checkbox" checked={scriptOn[script]} onChange={() => setScriptEnabled(script, !scriptOn[script])} />
                      {script === "devanagari" ? "Sanskrit / Devanagari" : "Gujarati"}
                    </label>
                  ))}
                </div>
              </div>

              <div className="ltOption">
                <span className="ltOptionLabel">Books</span>
                <div className="ltChoices" role="radiogroup" aria-label="Search in">
                  <label className={scope === "all" ? "isOn" : ""}>
                    <input
                      type="radio"
                      name="lt-scope"
                      checked={scope === "all"}
                      onChange={() => {
                        setScope("all");
                        setPickerOpen(false);
                      }}
                    />
                    All books
                  </label>
                  <label className={scope === "selected" ? "isOn" : ""}>
                    <input
                      type="radio"
                      name="lt-scope"
                      checked={scope === "selected"}
                      onChange={() => {
                        setScope("selected");
                        setPickerOpen(true);
                      }}
                    />
                    Choose books{selectedIds.length ? ` (${selectedIds.length})` : ""}
                  </label>
                </div>
              </div>
            </div>
          ) : scope === "selected" || searchMode !== "sanskrit_forms" || scripts ? (
            <p className="ltMuted ltOptionsNow">
              {[
                searchMode !== "sanskrit_forms" ? PLAIN_MODE_LABELS[searchMode] : "",
                scripts ? `only ${scripts.map((s) => SCRIPT_LABELS[s]).join(" + ")}` : "",
                scope === "selected" ? `in ${plural(selectedIds.length, "chosen book")}` : "",
              ]
                .filter(Boolean)
                .join(" · ")}
            </p>
          ) : null}

          {scope === "selected" && selectedIds.length > 0 ? (
            <div className="ltChosen" aria-label="Chosen books">
              {selectedIds.map((id) => {
                const row = optionById.get(id);
                return (
                  <span key={id} className="ltChip">
                    <span title={row?.display_name ?? id}>{row?.display_name ?? id}</span>
                    <button type="button" onClick={() => toggleGranth(id)} aria-label={`Remove ${row?.display_name ?? id}`}>
                      <LightTableIcon name="close" size={14} />
                    </button>
                  </span>
                );
              })}
              {!pickerOpen ? (
                <button type="button" className="ltLink" onClick={() => setPickerOpen(true)}>
                  Edit
                </button>
              ) : null}
            </div>
          ) : null}
          {linkWarning ? <p className="ltWarn" role="status">{linkWarning}</p> : null}

          {scope === "selected" && pickerOpen ? (
            <div className="ltPicker">
              <div className="ltPickerBar">
                <input
                  value={nameFilter}
                  onChange={(e) => setNameFilter(e.target.value)}
                  placeholder="Book name or number"
                  aria-label="Find a book"
                  className="indic"
                />
                <button type="button" className="ltPrimary" onClick={() => setPickerOpen(false)}>
                  Done
                </button>
              </div>
              <div className="ltPickerList" aria-label="Books">
                {loadingGranths ? (
                  <p className="ltMuted" role="status">Loading books…</p>
                ) : granthLoadError ? (
                  <p className="ltError" role="alert">Could not load the list of books: {granthLoadError}</p>
                ) : filteredOptions.length === 0 ? (
                  <p className="ltMuted">No book matches “{nameFilter}”.</p>
                ) : (
                  filteredOptions.slice(0, PICKER_LIMIT).map((row) => {
                    const checked = selectedIds.includes(row.custom_id);
                    return (
                      <label key={row.custom_id} className={`ltPickRow${checked ? " isOn" : ""}`}>
                        <input type="checkbox" checked={checked} onChange={() => toggleGranth(row.custom_id)} />
                        <span className="ltPickName">
                          <strong>{row.display_name}</strong>
                          {row.native_title && row.title ? <span className="indic">{row.native_title}</span> : null}
                        </span>
                        {bookNo(row.key) ? <span className="ltPickNo">No. {bookNo(row.key)}</span> : null}
                      </label>
                    );
                  })
                )}
                {filteredOptions.length > PICKER_LIMIT ? (
                  <p className="ltMuted">Type more of the name to see the rest.</p>
                ) : null}
              </div>
            </div>
          ) : null}

          {error ? (
            <p className="ltError" role="alert">
              {error}
            </p>
          ) : null}
        </section>

        <section id="lt-results" className={`ltResults${summary || loading ? " ltGlass" : ""}${phase === "done" ? " isLit" : ""}`} aria-busy={loading} aria-live="polite">
          {loading ? (
            <p className="ltLoading" role="status">
              <span className="ltSpinner" aria-hidden="true" /> Searching…
            </p>
          ) : null}

          {summary && !loading ? (
            <div className="ltSummary">
              {summary.total > 0 ? (
                <div className="ltTally">
                  <p>
                    <strong>
                      {nf.format(summary.occurrences)}
                      {summary.occurrencesExact ? "" : "+"}
                    </strong>
                    <span>times</span>
                  </p>
                  <p className="isSecond">
                    <strong>
                      {nf.format(summary.total)}
                      {summary.totalIsExact ? "" : "+"}
                    </strong>
                    <span>pages</span>
                  </p>
                  <p className="ltTallyWord indic">{summary.queries.join(", ")}</p>
                </div>
              ) : null}
              {!summary.occurrencesExact && summary.total > 0 ? (
                <p className="ltWarn">
                  {summary.counting ? (
                    <>
                      <span className="ltSpinner" aria-hidden="true" /> Counting every page…
                    </>
                  ) : (
                    `Counted in the first ${nf.format(summary.scannedPages)} pages`
                  )}
                </p>
              ) : null}
              {summary.missingGranths.length ? (
                <p className="ltWarn">{plural(summary.missingGranths.length, "chosen book")} could not be found and were left out.</p>
              ) : null}
              {resultsStale ? (
                <p className="ltStale" role="status">
                  Settings changed.{" "}
                  <button type="button" className="ltLink" onClick={() => run(1)}>
                    Search again
                  </button>
                </p>
              ) : null}
              {summary.formCounts.length > 1 ? (
                <details className="ltForms">
                  <summary>Forms found</summary>
                  <ul>
                    {summary.formCounts.map((entry) => (
                      <li key={entry.form}>
                        <span className="indic">{entry.form}</span> <b>{nf.format(entry.count)}</b>
                      </li>
                    ))}
                  </ul>
                </details>
              ) : null}
              {results.length > 0 ? (
                <button type="button" className="ltLink ltExportAll" onClick={() => setExportDialogOpen(true)}>
                  <LightTableIcon name="export" size={16} />
                  Download all results
                </button>
              ) : null}
            </div>
          ) : null}

          {summary && results.length === 0 && !loading ? (
            <div className="ltEmpty">
              <p>
                Nothing found for <span className="indic">{summary.queries.join(", ")}</span>.
              </p>
            </div>
          ) : null}

          {results.length > 0 && summary && !loading ? <div className="ltCards">{results.map(renderResult)}</div> : null}

          {results.length > 0 && summary && totalPages > 1 && !loading ? (
            <nav className="ltPager" aria-label="Result pages">
              <button type="button" onClick={() => goToResultPage(summary.page - 1)} disabled={summary.page <= 1}>
                ← Previous
              </button>
              <span>{pageLabel}</span>
              <button type="button" onClick={() => goToResultPage(summary.page + 1)} disabled={summary.page >= totalPages}>
                Next →
              </button>
            </nav>
          ) : null}
        </section>
      </div>

      {downloadPreview
        ? (() => {
            const selectedSet = new Set(downloadPreview.selectedPages);
            const selectedCount = downloadPreview.selectedPages.length;
            const maxPages = downloadPreview.preview?.max_download_pages ?? 0;
            const expanded = expandPagesWithContext(downloadPreview.selectedPages, downloadPreview.contextPages);
            const finalPageCount = selectedCount > 0 ? [1, ...expanded.filter((page) => page !== 1)].length : 0;
            const tooManyPages = Boolean(maxPages && finalPageCount > maxPages);
            const close = () => {
              setDeliveryFormat(null);
              setDownloadPreview(null);
            };
            return (
              <div className="searchDownloadOverlay" role="dialog" aria-modal="true" aria-label="Matched pages" onClick={(e) => e.target === e.currentTarget && close()}>
                <div className="searchDownloadPanel">
                  <header className="searchDownloadHeader">
                    <div>
                      <h2>Matched pages</h2>
                      <p>{downloadPreview.title}</p>
                    </div>
                    <button type="button" onClick={close} aria-label="Close">
                      <LightTableIcon name="close" />
                    </button>
                  </header>

                  {downloadPreview.loading ? (
                    <div className="searchDownloadNotice" role="status">
                      <span className="ltSpinner" aria-hidden="true" /> Finding every matched page in this granth
                    </div>
                  ) : null}
                  {downloadPreview.error ? <div className="searchDownloadError" role="alert">{downloadPreview.error}</div> : null}
                  {downloadPreview.notice ? <div className="searchDownloadNotice" role="status">{downloadPreview.notice}</div> : null}

                  {downloadPreview.preview ? (
                    <>
                      <div className="searchDownloadSummary">
                        <strong>{selectedCount}</strong> of <strong>{downloadPreview.preview.total_matched_pages}</strong> matched pages
                        chosen. The PDF holds {plural(finalPageCount, "page")}, page 1 first as the cover.
                        {downloadPreview.preview.truncated ? " The list is capped; narrow the search to see every page." : null}
                      </div>
                      <div className="searchDownloadToolbar">
                        <button type="button" onClick={() => setPreviewPages((pages, preview) => preview.pages.forEach((p) => pages.add(p.page_number)))}>
                          Choose all
                        </button>
                        <button type="button" onClick={() => setPreviewPages((pages) => pages.clear())}>
                          Clear
                        </button>
                        <label className="searchDownloadContextInput">
                          <span>Pages either side</span>
                          <input
                            type="number"
                            min={0}
                            max={MAX_CONTEXT_PAGE_RADIUS}
                            inputMode="numeric"
                            value={downloadPreview.contextPages}
                            onChange={(event) =>
                              setDownloadPreview((prev) =>
                                prev ? { ...prev, contextPages: event.target.value === "" ? 0 : normalizeContextPageRadius(event.target.value) } : prev
                              )
                            }
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
                                onChange={(event) =>
                                  setPreviewPages((pages) => (event.target.checked ? pages.add(page.page_number) : pages.delete(page.page_number)))
                                }
                              />
                              <span className="searchDownloadPageMeta">
                                <b>p. {page.page_number}</b>
                                {isCover ? " cover" : ""} · {plural(page.occurrence_count, "match", "matches")}
                              </span>
                              <span className="searchDownloadSnippet indic">
                                {ringAll(page.snippet, downloadPreview.queries, downloadPreview.matchMode, downloadPreview.scripts)}
                              </span>
                            </label>
                          );
                        })}
                      </div>
                      <footer className="searchDownloadFooter">
                        {tooManyPages ? <span className="searchDownloadErrorText">Choose {maxPages} PDF pages or fewer.</span> : null}
                        <button type="button" onClick={() => setDeliveryFormat("csv")} disabled={downloadPreview.downloading || selectedCount === 0}>
                          <LightTableIcon name="table" size={16} /> Export CSV
                        </button>
                        <button
                          type="button"
                          className="isPrimary"
                          onClick={() => setDeliveryFormat("pdf")}
                          disabled={downloadPreview.downloading || selectedCount === 0 || tooManyPages}
                        >
                          <LightTableIcon name="export" size={16} /> Download PDF
                        </button>
                      </footer>
                      <DownloadDeliveryDialog
                        open={deliveryFormat !== null}
                        title={deliveryFormat === "csv" ? "Deliver the CSV" : "Deliver the PDF"}
                        fileLabel={
                          deliveryFormat === "csv"
                            ? `${plural(selectedCount, "matched page")}, one row per matched line`
                            : `${plural(finalPageCount, "PDF page")}, cover first`
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
        queries={summary?.queries ?? []}
        matchMode={summary?.matchMode ?? searchMode}
        scripts={summary?.scripts ?? null}
        granthIds={summary?.granthIds ?? []}
        scopeLabel={summary ? summary.scopeLabel.replace(/^./, (c) => c.toUpperCase()) : "All granths"}
        onClose={() => setExportDialogOpen(false)}
      />
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </main>
  );
}
