import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type GranthItem = {
  id: number;
  file_name: string | null;
  ufs_url: string | null;
  file_size: number | null;
  custom_id: string | null;
  collection: string | null;
  subcollection: string | null;
  original_rel_path: string | null;
  cover_image_url: string | null;
  cover_image_key: string | null;
};

type ApiResponse = {
  items: GranthItem[];
  meta: {
    count: number;
    limit: number;
    offset: number;
    collection: string | null;
    coverColumnAvailable: boolean;
  };
};

function toMB(sizeBytes: number | null) {
  if (sizeBytes == null || !Number.isFinite(sizeBytes)) return null;
  return `${(sizeBytes / 1024 / 1024).toFixed(1)} MB`;
}

function displayTitle(row: GranthItem) {
  const raw = row.file_name ?? row.original_rel_path ?? row.custom_id ?? `Granth ${row.id}`;
  return raw.replace(/\s+OCR\.pdf$/i, "").replace(/\.pdf$/i, "");
}

export default function HomePage() {
  const [items, setItems] = useState<GranthItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [coverColumnAvailable, setCoverColumnAvailable] = useState(true);
  const [brokenCoverIds, setBrokenCoverIds] = useState<Record<number, boolean>>({});

  useEffect(() => {
    let active = true;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch("/api/granths?limit=250");
        const json = (await res.json()) as ApiResponse | { error?: string };

        if (!res.ok) {
          throw new Error(("error" in json && json.error) || `Request failed (${res.status})`);
        }

        if (!active) return;
        const payload = json as ApiResponse;
        setItems(payload.items ?? []);
        setCoverColumnAvailable(payload.meta?.coverColumnAvailable ?? true);
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
  }, []);

  const heading = useMemo(() => {
    if (loading) return "Loading granths...";
    if (error) return "Could not load granths";
    return `Granth Library (${items.length})`;
  }, [error, items.length, loading]);

  return (
    <main
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at 10% 0%, #f9efdf 0%, #f8f4ea 36%, #eef1e6 100%)",
        color: "#1f2120",
        padding: "24px 18px 42px",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ maxWidth: 1180, margin: "0 auto" }}>
        <header style={{ marginBottom: 22 }}>
          <h1 style={{ margin: 0, fontSize: 30, letterSpacing: "0.01em" }}>{heading}</h1>
          <div style={{ marginTop: 8, display: "flex", gap: 16, flexWrap: "wrap" }}>
            <Link href="/search">Search inside pages</Link>
            {!coverColumnAvailable ? (
              <span style={{ color: "#a65400", fontWeight: 600 }}>
                Cover columns missing in DB; run cover migration before storing covers.
              </span>
            ) : null}
          </div>
        </header>

        {error ? <p style={{ color: "#9e1a1a" }}>{error}</p> : null}
        {loading ? <p>Fetching records...</p> : null}

        {!loading && !error ? (
          <section
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(190px, 1fr))",
              gap: 16,
            }}
          >
            {items.map((row) => {
              const showCover = Boolean(row.cover_image_url) && !brokenCoverIds[row.id];
              const title = displayTitle(row);
              const sizeLabel = toMB(row.file_size);

              return (
                <article
                  key={row.id}
                  style={{
                    background: "#fffefb",
                    borderRadius: 14,
                    overflow: "hidden",
                    border: "1px solid #dbd8ce",
                    boxShadow: "0 6px 18px rgba(58, 56, 46, 0.08)",
                  }}
                >
                  <div
                    style={{
                      aspectRatio: "3 / 4",
                      background: "linear-gradient(145deg, #e9e3d6, #f9f7f1)",
                      display: "grid",
                      placeItems: "center",
                    }}
                  >
                    {showCover ? (
                      <img
                        src={row.cover_image_url ?? ""}
                        alt={`${title} cover`}
                        style={{ width: "100%", height: "100%", objectFit: "cover" }}
                        loading="lazy"
                        onError={() => setBrokenCoverIds((prev) => ({ ...prev, [row.id]: true }))}
                      />
                    ) : (
                      <div style={{ padding: 12, textAlign: "center", fontSize: 13, color: "#4c4a44" }}>
                        No cover yet
                      </div>
                    )}
                  </div>

                  <div style={{ padding: "12px 12px 14px" }}>
                    <div
                      title={title}
                      style={{
                        fontSize: 14,
                        fontWeight: 700,
                        lineHeight: 1.35,
                        maxHeight: 56,
                        overflow: "hidden",
                      }}
                    >
                      {title}
                    </div>
                    <div style={{ marginTop: 8, fontSize: 12, color: "#4d4f52" }}>
                      <div>{row.collection ?? "-"}</div>
                      <div>{row.subcollection ?? "-"}</div>
                      <div>{sizeLabel ?? "-"}</div>
                    </div>
                    {row.ufs_url ? (
                      <a
                        href={row.ufs_url}
                        target="_blank"
                        rel="noreferrer"
                        style={{ marginTop: 10, display: "inline-block", fontSize: 12 }}
                      >
                        Open PDF
                      </a>
                    ) : null}
                  </div>
                </article>
              );
            })}
          </section>
        ) : null}
      </div>
    </main>
  );
}
