import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type ScannableDoc = {
  custom_id: string;
  pdf_name: string | null;
  display_name: string;
  pdf_url: string | null;
  csv_url: string | null;
  status: string;
  updated_at: string | null;
};

type ApiResponse = {
  items: ScannableDoc[];
  meta: {
    total_processed: number;
    limit: number;
    offset: number;
  };
};

function formatDate(value: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export default function ScannableDocumentsPage() {
  const [items, setItems] = useState<ScannableDoc[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [totalProcessed, setTotalProcessed] = useState(0);
  const [offset, setOffset] = useState(0);
  const limit = 250;

  useEffect(() => {
    let active = true;

    async function load() {
      setLoading(true);
      setError(null);

      try {
        const res = await fetch(`/api/scannable-documents?limit=${limit}&offset=${offset}`);
        const json = (await res.json()) as ApiResponse | { error?: string };
        if (!res.ok) {
          throw new Error(("error" in json && json.error) || `Request failed (${res.status})`);
        }

        if (!active) return;
        const payload = json as ApiResponse;
        setItems(payload.items ?? []);
        setTotalProcessed(payload.meta?.total_processed ?? payload.items?.length ?? 0);
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (active) setLoading(false);
      }
    }

    void load();
    return () => {
      active = false;
    };
  }, [limit, offset]);

  const heading = useMemo(() => {
    if (loading) return "Loading scannable documents...";
    if (error) return "Could not load scannable documents";
    return `Scannable Documents (${totalProcessed})`;
  }, [error, loading, totalProcessed]);

  return (
    <main
      style={{
        minHeight: "100vh",
        background: "radial-gradient(circle at 10% 0%, #f7ecdf 0%, #f3f4ea 36%, #e9edf2 100%)",
        color: "#1f2120",
        padding: "24px 16px 38px",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ maxWidth: 1200, margin: "0 auto" }}>
        <header style={{ marginBottom: 16 }}>
          <h1 style={{ margin: 0, fontSize: 30, letterSpacing: "0.01em" }}>{heading}</h1>
          <div style={{ marginTop: 8, display: "flex", gap: 14, flexWrap: "wrap" }}>
            <Link href="/">Back to library</Link>
            <Link href="/search">Open search</Link>
            <span style={{ opacity: 0.8 }}>Status filter: processed</span>
            <span style={{ opacity: 0.8 }}>
              Showing {items.length} docs (offset {offset}, page size {limit})
            </span>
          </div>
        </header>

        {error ? <p style={{ color: "#9f1f1f", fontWeight: 700 }}>{error}</p> : null}
        {loading ? <p>Fetching documents...</p> : null}

        {!loading && !error ? (
          <section
            style={{
              border: "1px solid #d4d9e2",
              borderRadius: 14,
              background: "#fff",
              overflow: "hidden",
              boxShadow: "0 10px 24px rgba(35, 42, 51, 0.08)",
            }}
          >
            <div style={{ maxHeight: "78vh", overflow: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f7f9fc" }}>
                    <th style={{ textAlign: "left", padding: "10px 12px", borderBottom: "1px solid #dde3ef" }}>Name</th>
                    <th style={{ textAlign: "left", padding: "10px 12px", borderBottom: "1px solid #dde3ef" }}>Custom ID</th>
                    <th style={{ textAlign: "left", padding: "10px 12px", borderBottom: "1px solid #dde3ef" }}>Updated</th>
                    <th style={{ textAlign: "left", padding: "10px 12px", borderBottom: "1px solid #dde3ef" }}>Links</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((row) => (
                    <tr key={row.custom_id}>
                      <td
                        style={{
                          padding: "9px 12px",
                          borderBottom: "1px solid #edf1f6",
                          verticalAlign: "top",
                          fontWeight: 700,
                        }}
                      >
                        {row.display_name}
                      </td>
                      <td
                        style={{
                          padding: "9px 12px",
                          borderBottom: "1px solid #edf1f6",
                          verticalAlign: "top",
                          fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                          fontSize: 12,
                        }}
                      >
                        {row.custom_id}
                      </td>
                      <td style={{ padding: "9px 12px", borderBottom: "1px solid #edf1f6", verticalAlign: "top" }}>
                        {formatDate(row.updated_at)}
                      </td>
                      <td style={{ padding: "9px 12px", borderBottom: "1px solid #edf1f6", verticalAlign: "top" }}>
                        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                          {row.pdf_url ? (
                            <a href={row.pdf_url} target="_blank" rel="noreferrer">
                              PDF
                            </a>
                          ) : null}
                          {row.csv_url ? (
                            <a href={row.csv_url} target="_blank" rel="noreferrer">
                              CSV
                            </a>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div
              style={{
                padding: "12px",
                borderTop: "1px solid #edf1f6",
                display: "flex",
                gap: 10,
                alignItems: "center",
                justifyContent: "space-between",
              }}
            >
              <div style={{ fontSize: 13, opacity: 0.8 }}>
                Processed total: {totalProcessed}
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  type="button"
                  onClick={() => setOffset((prev) => Math.max(0, prev - limit))}
                  disabled={loading || offset === 0}
                  style={{
                    padding: "8px 11px",
                    borderRadius: 8,
                    border: "1px solid #c7cfd9",
                    background: "#fff",
                    cursor: loading || offset === 0 ? "default" : "pointer",
                  }}
                >
                  Previous
                </button>
                <button
                  type="button"
                  onClick={() => setOffset((prev) => prev + limit)}
                  disabled={loading || offset + items.length >= totalProcessed}
                  style={{
                    padding: "8px 11px",
                    borderRadius: 8,
                    border: "1px solid #c7cfd9",
                    background: "#fff",
                    cursor: loading || offset + items.length >= totalProcessed ? "default" : "pointer",
                  }}
                >
                  Next
                </button>
              </div>
            </div>
          </section>
        ) : null}
      </div>
    </main>
  );
}
