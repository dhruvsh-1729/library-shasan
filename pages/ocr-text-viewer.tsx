import Head from "next/head";
import OcrReplacePanel from "@/components/OcrReplacePanel";
import {
  type OCRSearchMode,
  type OCRSearchScripts,
  findOCRSearchMatchesForQueries,
  getOCRSearchModeLabel,
  parseOCRSearchMode,
  parseOCRSearchScripts,
} from "@/lib/ocr-search";
import Link from "next/link";
import { useRouter } from "next/router";
import type { ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

type GranthMeta = {
  display_name?: string | null;
  native_title?: string | null;
  pdf_url?: string | null;
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
  return Array.isArray(value) ? value[0] : (value ?? "");
}

function buildOccurrenceSummary(
  content: string,
  queries: string[],
  mode: OCRSearchMode,
  scripts: OCRSearchScripts,
  maxChars = 120,
) {
  const normalized = String(content ?? "")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized || queries.length === 0) return null;

  const matches = findOCRSearchMatchesForQueries(
    normalized,
    queries,
    mode,
    scripts,
  );
  if (matches.length === 0) return null;

  const firstMatch = matches[0];
  const matchLength = Math.max(1, firstMatch.end - firstMatch.start);
  const start = Math.max(
    0,
    firstMatch.start - Math.floor((maxChars - matchLength) / 2),
  );
  const end = Math.min(
    normalized.length,
    firstMatch.end + Math.floor((maxChars - matchLength) / 2),
  );
  let excerpt = normalized.slice(start, end).trim();
  if (start > 0) excerpt = `…${excerpt}`;
  if (end < normalized.length) excerpt = `${excerpt}…`;

  return { count: matches.length, excerpt };
}

export default function OCRTextViewerPage() {
  const router = useRouter();
  const rowRefs = useRef<Record<number, HTMLElement | null>>({});

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
  const matchMode = parseOCRSearchMode(readSingleQuery(router.query.matchMode));
  // Every spelling the search used ("hinsa" alone matches nothing in Devanagari
  // text), and the scripts it kept, so this page shows the same hits.
  const variantsKey = [q, ...[router.query.queryVariant ?? []].flat()]
    .map((v) => String(v).trim())
    .filter(Boolean)
    .join("\n");
  const queries = useMemo(
    () => variantsKey.split("\n").filter(Boolean),
    [variantsKey],
  );
  const scriptsKey = readSingleQuery(router.query.scripts);
  const scripts = useMemo(
    () => parseOCRSearchScripts(scriptsKey),
    [scriptsKey],
  );

  const occurrenceItems = useMemo<OccurrenceItem[]>(() => {
    if (queries.length === 0) return [];
    return rows
      .map((row) => {
        const summary = buildOccurrenceSummary(
          row.content,
          queries,
          matchMode,
          scripts,
        );
        if (!summary) return null;
        return {
          pageNumber: row.page_number,
          count: summary.count,
          excerpt: summary.excerpt,
        };
      })
      .filter((value): value is OccurrenceItem => Boolean(value));
  }, [matchMode, queries, rows, scripts]);

  const totalOccurrenceCount = useMemo(
    () => occurrenceItems.reduce((sum, item) => sum + item.count, 0),
    [occurrenceItems],
  );

  const currentRowIndex = useMemo(() => {
    if (activePageNumber == null) return null;
    const index = rows.findIndex((row) => row.page_number === activePageNumber);
    return index >= 0 ? index : null;
  }, [activePageNumber, rows]);

  function renderHighlightedText(text: string) {
    const matches = findOCRSearchMatchesForQueries(
      text,
      queries,
      matchMode,
      scripts,
    );
    if (!text || matches.length === 0) return text;

    const parts: ReactNode[] = [];
    let cursor = 0;

    matches.forEach((match, idx) => {
      if (match.start > cursor) {
        parts.push(text.slice(cursor, match.start));
      }
      parts.push(
        <mark key={`${match.start}_${idx}`} className="ltRing">
          {text.slice(match.start, match.end)}
        </mark>,
      );
      cursor = match.end;
    });

    if (cursor < text.length) {
      parts.push(text.slice(cursor));
    }

    return parts.map((part, idx) => <span key={idx}>{part}</span>);
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
            matchMode,
          },
        },
        undefined,
        { shallow: true, scroll: false },
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
        const res = await fetch(
          `/api/ocr-granth-pages?granthKey=${encodeURIComponent(granthKey)}`,
        );
        const body = await res.json();
        if (!res.ok) {
          throw new Error(
            body?.error || `Failed to load OCR rows (${res.status})`,
          );
        }

        const rowsData = Array.isArray(body.rows)
          ? (body.rows as PageRow[])
          : [];
        if (!active) return;
        setGranth(body.granth as GranthMeta);
        setRows(rowsData);
      } catch (loadError) {
        if (!active) return;
        setError(
          loadError instanceof Error ? loadError.message : String(loadError),
        );
      } finally {
        if (active) setLoading(false);
      }
    }

    void loadRows();
    return () => {
      active = false;
    };
  }, [router.isReady, granthKey, refreshToken]);

  // On load, open the asked-for page, else the first page with the word.
  const openedRowsRef = useRef<PageRow[] | null>(null);
  useEffect(() => {
    if (rows.length === 0 || openedRowsRef.current === rows) return;
    const reload = openedRowsRef.current != null;
    openedRowsRef.current = rows;
    if (reload && activePageNumber != null) return;
    if (Number.isFinite(targetPage) && rows.some((row) => row.page_number === targetPage)) {
      setActivePageNumber(targetPage);
      return;
    }
    setActivePageNumber(occurrenceItems[0]?.pageNumber ?? rows[0].page_number);
  }, [activePageNumber, occurrenceItems, rows, targetPage]);

  // The first jump is instant: a smooth scroll never runs in a background tab,
  // which is how pages opened from search often start.
  const scrolledOnceRef = useRef(false);
  useEffect(() => {
    if (activePageNumber == null) return;
    const element = rowRefs.current[activePageNumber];
    if (element) {
      element.scrollIntoView({ behavior: scrolledOnceRef.current ? "smooth" : "auto", block: "start" });
      scrolledOnceRef.current = true;
    }
  }, [activePageNumber, rows.length]);

  const title = useMemo(() => {
    if (!granth) return "";
    return granth.display_name || granth.granth_name;
  }, [granth, granthKey]);
  const [fixOpen, setFixOpen] = useState(false);

  return (
    <>
      <Head>
        <title>{title ? `${title} · Shasan Library` : "Shasan Library"}</title>
      </Head>
      <main className="lt tv">
        <div className="tvFrame">
          <header className="tvTop">
            <div className="tvTitle">
              <h1>{title || "\u00a0"}</h1>
              {granth?.native_title && granth.native_title !== title ? (
                <p className="indic">{granth.native_title}</p>
              ) : null}
            </div>
            <nav className="ltNav" aria-label="Pages">
              <Link href="/">Search</Link>
              <Link href="/library">Library</Link>
              {granth?.xlsx_url ? (
                <a href={granth.xlsx_url} target="_blank" rel="noreferrer">
                  Spreadsheet
                </a>
              ) : null}
            </nav>
          </header>

          {q.trim() && rows.length ? (
            <p className="tvSummary">
              <strong className="indic">{q}</strong> · {occurrenceItems.length}{" "}
              page{occurrenceItems.length === 1 ? "" : "s"} ·{" "}
              {totalOccurrenceCount} time{totalOccurrenceCount === 1 ? "" : "s"}{" "}
              · {getOCRSearchModeLabel(matchMode)}
            </p>
          ) : null}

          {loading ? (
            <p className="ltLoading" role="status">
              <span className="ltSpinner" aria-hidden="true" /> Loading pages…
            </p>
          ) : null}
          {error ? (
            <p className="ltError" role="alert">
              {error}
            </p>
          ) : null}

          {!loading && !error && rows.length > 0 ? (
            <div className="tvLayout">
              {q.trim() ? (
                <aside className="tvHits" aria-label="Pages with the word">
                  {occurrenceItems.length === 0 ? (
                    <p className="ltMuted">Not found in this granth.</p>
                  ) : (
                    occurrenceItems.map((item) => (
                      <button
                        key={item.pageNumber}
                        type="button"
                        className={`tvHit${item.pageNumber === activePageNumber ? " isOn" : ""}`}
                        onClick={() => scrollToPage(item.pageNumber)}
                      >
                        <span className="tvHitHead">
                          <b>p. {item.pageNumber}</b>
                          {item.count > 1 ? <small>{item.count}×</small> : null}
                        </span>
                        <span className="tvHitText indic">
                          {renderHighlightedText(item.excerpt)}
                        </span>
                      </button>
                    ))
                  )}
                </aside>
              ) : null}

              <section className="tvPages" aria-label="Pages">
                {rows.map((row, idx) => (
                  <article
                    key={`${row.page_number}_${idx}`}
                    ref={(element) => {
                      rowRefs.current[row.page_number] = element;
                    }}
                    className={`tvPage${row.page_number === activePageNumber ? " isOn" : ""}`}
                  >
                    <button
                      type="button"
                      className="tvFolio"
                      onClick={() => scrollToPage(row.page_number)}
                      aria-label={`Page ${row.page_number}`}
                    >
                      <span>page</span>
                      <strong>{row.page_number}</strong>
                    </button>
                    <div className="tvText indic">
                      {renderHighlightedText(row.content)}
                    </div>
                  </article>
                ))}
              </section>
            </div>
          ) : null}

          {granth ? (
            <section className="tvFix">
              <button
                type="button"
                className="ltMoreToggle"
                aria-expanded={fixOpen}
                onClick={() => setFixOpen((open) => !open)}
              >
                {fixOpen ? "Hide OCR corrections" : "Fix OCR text"}
              </button>
              {fixOpen ? (
                <OcrReplacePanel
                  currentPageTarget={
                    granthKey && activePageNumber != null
                      ? { granthKey, pageNumber: activePageNumber, title }
                      : null
                  }
                  currentGranthTarget={granthKey ? { granthKey, title } : null}
                  initialWord={q.trim()}
                  title="Replace in OCR text"
                  onApplied={async () => {
                    setRefreshToken((value) => value + 1);
                  }}
                />
              ) : null}
            </section>
          ) : null}
        </div>
      </main>
    </>
  );
}
