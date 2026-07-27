import {
  getDocumentScanLabel,
  getDocumentStatusLabel,
  type DocumentScanState,
} from "@/lib/document-scan-state";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type ScanView = "remaining" | "ready" | "review" | "searchable" | "all";

type ScannableDoc = {
  custom_id: string;
  pdf_name: string | null;
  display_name: string;
  pdf_url: string | null;
  csv_url: string | null;
  status: string | null;
  scan_state: DocumentScanState;
  updated_at: string | null;
};

type ApiResponse = {
  items: ScannableDoc[];
  meta: {
    total_documents: number;
    total_processed: number;
    ready_documents: number;
    review_documents: number;
    remaining_documents: number;
    total_for_view: number;
    limit: number;
    offset: number;
    view: ScanView;
  };
};

const PAGE_SIZE = 100;

function formatDate(value: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function viewCount(meta: ApiResponse["meta"] | null, view: ScanView) {
  if (!meta) return 0;
  if (view === "remaining") return meta.remaining_documents;
  if (view === "ready") return meta.ready_documents;
  if (view === "review") return meta.review_documents;
  if (view === "searchable") return meta.total_processed;
  return meta.total_documents;
}

export default function ScannableDocumentsPage() {
  const [items, setItems] = useState<ScannableDoc[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [meta, setMeta] = useState<ApiResponse["meta"] | null>(null);
  const [offset, setOffset] = useState(0);
  const [view, setView] = useState<ScanView>("remaining");

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function load() {
      setLoading(true);
      setError(null);

      try {
        const params = new URLSearchParams();
        params.set("limit", String(PAGE_SIZE));
        params.set("offset", String(offset));
        params.set("view", view);

        const res = await fetch(`/api/scannable-documents?${params.toString()}`, {
          signal: controller.signal,
        });
        const json = (await res.json()) as ApiResponse | { error?: string };
        if (!res.ok) {
          throw new Error(("error" in json && json.error) || `Request failed (${res.status})`);
        }

        if (!active) return;
        const payload = json as ApiResponse;
        setItems(payload.items ?? []);
        setMeta(payload.meta ?? null);
      } catch (e) {
        if (!active || controller.signal.aborted) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (active && !controller.signal.aborted) setLoading(false);
      }
    }

    void load();
    return () => {
      active = false;
      controller.abort();
    };
  }, [offset, view]);

  const tabs = useMemo(
    () =>
      [
        { view: "remaining" as const, label: "Needs scan", count: viewCount(meta, "remaining") },
        { view: "ready" as const, label: "Ready", count: viewCount(meta, "ready") },
        { view: "review" as const, label: "Review", count: viewCount(meta, "review") },
        { view: "searchable" as const, label: "Searchable", count: viewCount(meta, "searchable") },
        { view: "all" as const, label: "All", count: viewCount(meta, "all") },
      ],
    [meta]
  );

  const totalForView = meta?.total_for_view ?? 0;
  const rangeStart = totalForView === 0 ? 0 : offset + 1;
  const rangeEnd = totalForView === 0 ? 0 : Math.min(offset + items.length, totalForView);

  function changeView(nextView: ScanView) {
    setView(nextView);
    setOffset(0);
  }

  return (
    <main className="scanShell">
      <div className="scanFrame">
        <header className="scanHeader">
          <div className="scanHeaderText">
            <h1 className="scanTitle">Scan Status</h1>
            <div className="scanStatusLine">
              {loading && items.length === 0
                ? "Loading documents..."
                : `${rangeStart}-${rangeEnd} of ${totalForView}`}
            </div>
          </div>
          <nav className="scanNav" aria-label="Library tools">
            <Link href="/">Library</Link>
            <Link href="/search">Search pages</Link>
            <Link href="/granth-extractor">Extractor</Link>
          </nav>
        </header>

        <section className="scanStats" aria-label="Document scan counts">
          <div className="scanStat">
            <span>Needs scan</span>
            <strong>{meta?.remaining_documents ?? 0}</strong>
          </div>
          <div className="scanStat">
            <span>Ready</span>
            <strong>{meta?.ready_documents ?? 0}</strong>
          </div>
          <div className="scanStat">
            <span>Review</span>
            <strong>{meta?.review_documents ?? 0}</strong>
          </div>
          <div className="scanStat">
            <span>Total</span>
            <strong>{meta?.total_documents ?? 0}</strong>
          </div>
        </section>

        <section className="scanToolbar" aria-label="Scan status filters">
          <div className="scanTabs">
            {tabs.map((tab) => (
              <button
                key={tab.view}
                type="button"
                className={`scanTab${view === tab.view ? " isActive" : ""}`}
                onClick={() => changeView(tab.view)}
              >
                <span>{tab.label}</span>
                <strong>{tab.count}</strong>
              </button>
            ))}
          </div>
        </section>

        {error ? <div className="scanError">{error}</div> : null}

        <section className="scanList" aria-busy={loading}>
          {!loading && !error && items.length === 0 ? (
            <div className="scanEmpty">No documents in this view.</div>
          ) : null}

          {items.map((row) => (
            <article key={row.custom_id} className="scanRow">
              <div className="scanPrimary">
                <span
                  className={`scanBadge is-${row.scan_state}`}
                  title={getDocumentStatusLabel(row.status, row.scan_state)}
                >
                  {getDocumentScanLabel(row.scan_state)}
                </span>
                <strong title={row.display_name}>{row.display_name}</strong>
              </div>

              <div className="scanMetaBlock">
                <span className="scanMetaLabel">Custom ID</span>
                <span className="scanMono">{row.custom_id}</span>
              </div>

              <div className="scanMetaBlock">
                <span className="scanMetaLabel">Updated</span>
                <span>{formatDate(row.updated_at)}</span>
              </div>

              <div className="scanMetaBlock">
                <span className="scanMetaLabel">Status</span>
                <span>{getDocumentStatusLabel(row.status, row.scan_state)}</span>
              </div>

              <div className="scanLinks">
                {row.pdf_url ? (
                  <a href={row.pdf_url} target="_blank" rel="noreferrer">
                    PDF
                  </a>
                ) : (
                  <span>No PDF</span>
                )}
                {row.csv_url ? (
                  <a href={row.csv_url} target="_blank" rel="noreferrer">
                    CSV
                  </a>
                ) : (
                  <span>No CSV</span>
                )}
              </div>
            </article>
          ))}
        </section>

        <footer className="scanFooter">
          <span>
            Showing {rangeStart}-{rangeEnd} of {totalForView}
          </span>
          <div className="scanPager">
            <button
              type="button"
              onClick={() => setOffset((prev) => Math.max(0, prev - PAGE_SIZE))}
              disabled={loading || offset === 0}
            >
              Previous
            </button>
            <button
              type="button"
              onClick={() => setOffset((prev) => prev + PAGE_SIZE)}
              disabled={loading || offset + items.length >= totalForView}
            >
              Next
            </button>
          </div>
        </footer>
      </div>
    </main>
  );
}
