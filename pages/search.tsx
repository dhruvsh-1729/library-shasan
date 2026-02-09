import { useState } from "react";

export default function SearchPage() {
  const [q, setQ] = useState("");
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  async function run() {
    setLoading(true);
    const res = await fetch(`/api/search?q=${encodeURIComponent(q)}&limit=50`);
    const json = await res.json();
    setResults(json.results ?? []);
    setLoading(false);
  }

  return (
    <div style={{ maxWidth: 900, margin: "40px auto", padding: 16 }}>
      <h1>Granth Search</h1>

      <div style={{ display: "flex", gap: 8 }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search text…"
          style={{ flex: 1, padding: 10, fontSize: 16 }}
        />
        <button onClick={run} disabled={loading || q.trim().length < 2} style={{ padding: "10px 14px" }}>
          {loading ? "Searching…" : "Search"}
        </button>
      </div>

      <div style={{ marginTop: 20 }}>
        {results.map((r, i) => (
          <div key={i} style={{ padding: 12, border: "1px solid #ddd", borderRadius: 10, marginBottom: 10 }}>
            <div style={{ fontWeight: 600 }}>
              {r.pdf_name} — page {r.page_number}
            </div>
            <div style={{ marginTop: 6, opacity: 0.85, whiteSpace: "pre-wrap" }}>
              {r.snippet}
            </div>
            <div style={{ marginTop: 8 }}>
              <a href={r.open_pdf_url} target="_blank" rel="noreferrer">
                Open PDF at page
              </a>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
