import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useMemo, useRef, useState } from "react";

function readSingleQuery(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value ?? "";
}

function parseCsvRows(input: string) {
  const csv = input.charCodeAt(0) === 0xfeff ? input.slice(1) : input;
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < csv.length; i++) {
    const ch = csv[i];

    if (inQuotes) {
      if (ch === '"') {
        if (csv[i + 1] === '"') {
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      continue;
    }
    if (ch === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    if (ch === "\r") {
      if (csv[i + 1] === "\n") i += 1;
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    field += ch;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter((r) => !(r.length === 1 && r[0] === ""));
}

export default function CsvViewerPage() {
  const router = useRouter();
  const rowRefs = useRef<Record<number, HTMLTableRowElement | null>>({});

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [headers, setHeaders] = useState<string[]>([]);
  const [rows, setRows] = useState<string[][]>([]);
  const [targetRowIndex, setTargetRowIndex] = useState<number | null>(null);

  const csvUrl = readSingleQuery(router.query.csvUrl);
  const customId = readSingleQuery(router.query.customId);
  const pageRaw = readSingleQuery(router.query.page);
  const targetPage = pageRaw ? Number(pageRaw) : null;

  useEffect(() => {
    if (!router.isReady) return;
    if (!csvUrl) {
      setError("Missing csvUrl query parameter.");
      return;
    }

    let active = true;
    async function loadCsv() {
      setLoading(true);
      setError(null);

      try {
        const res = await fetch(`/api/csv-proxy?url=${encodeURIComponent(csvUrl)}`);
        const body = await res.text();
        if (!res.ok) {
          throw new Error(body || `Failed to load CSV (${res.status})`);
        }

        const matrix = parseCsvRows(body);
        if (matrix.length === 0) {
          throw new Error("CSV appears to be empty.");
        }

        const [headerRow, ...dataRows] = matrix;
        const normalizedHeaders = headerRow.map((h) => String(h ?? "").trim());

        let foundIndex: number | null = null;
        const customIdIdx = normalizedHeaders.indexOf("custom_id");
        const pageIdx = normalizedHeaders.indexOf("page_number");

        if (customId && Number.isFinite(targetPage) && customIdIdx >= 0 && pageIdx >= 0) {
          for (let i = 0; i < dataRows.length; i++) {
            const row = dataRows[i];
            const rowCustomId = String(row[customIdIdx] ?? "");
            const rowPage = Number(row[pageIdx]);

            if (rowCustomId === customId && rowPage === targetPage) {
              foundIndex = i;
              break;
            }
          }
        }

        if (!active) return;
        setHeaders(normalizedHeaders);
        setRows(dataRows);
        setTargetRowIndex(foundIndex);
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (active) setLoading(false);
      }
    }

    void loadCsv();
    return () => {
      active = false;
    };
  }, [router.isReady, csvUrl, customId, targetPage]);

  useEffect(() => {
    if (targetRowIndex == null) return;
    const el = rowRefs.current[targetRowIndex];
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }, [rows.length, targetRowIndex]);

  const pageNumberIndex = useMemo(() => headers.indexOf("page_number"), [headers]);
  const textIndex = useMemo(() => headers.indexOf("text"), [headers]);

  function getValueByIndex(row: string[], index: number) {
    return index >= 0 ? String(row[index] ?? "") : "";
  }

  return (
    <main
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at 8% 0%, #f8efe0 0%, #f2f4ec 40%, #e9edf2 100%)",
        color: "#1f2120",
        padding: "20px 16px 28px",
        fontFamily: '"Noto Sans Gujarati","Noto Serif Devanagari","Segoe UI",sans-serif',
      }}
    >
      <div style={{ maxWidth: 1400, margin: "0 auto" }}>
        <header style={{ marginBottom: 12 }}>
          <h1 style={{ margin: 0, fontSize: 24 }}>CSV Row Viewer</h1>
          <div style={{ marginTop: 8, display: "flex", gap: 12, flexWrap: "wrap", fontSize: 14 }}>
            <Link href="/search">Back to search</Link>
            {csvUrl ? (
              <a href={csvUrl} target="_blank" rel="noreferrer">
                Open raw CSV
              </a>
            ) : null}
          </div>
          <div style={{ marginTop: 8, fontSize: 13, opacity: 0.8 }}>
            Target: <strong>{customId || "-"}</strong> page <strong>{pageRaw || "-"}</strong>
          </div>
        </header>

        {loading ? <p>Loading CSV...</p> : null}
        {error ? <p style={{ color: "#9f1f1f", fontWeight: 700 }}>{error}</p> : null}

        {!loading && !error && rows.length > 0 ? (
          <section
            style={{
              border: "1px solid #d4d9e2",
              borderRadius: 12,
              background: "#fff",
              overflow: "hidden",
              boxShadow: "0 8px 20px rgba(35, 42, 51, 0.08)",
            }}
          >
            <div style={{ padding: "10px 12px", borderBottom: "1px solid #e0e4ec", fontSize: 13 }}>
              Rows: {rows.length}.{" "}
              {targetRowIndex != null
                ? `Jumped to CSV row ${targetRowIndex + 2}.`
                : "Matching row not found for the requested custom_id/page."}
            </div>

            <div style={{ maxHeight: "78vh", overflow: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", tableLayout: "fixed" }}>
                <thead>
                  <tr style={{ background: "#f7f9fc" }}>
                    <th
                      style={{
                        borderBottom: "1px solid #d9deea",
                        padding: "8px 10px",
                        textAlign: "left",
                        width: 88,
                        fontSize: 12,
                      }}
                    >
                      Page
                    </th>
                    <th
                      style={{
                        borderBottom: "1px solid #d9deea",
                        padding: "8px 10px",
                        textAlign: "left",
                        fontSize: 12,
                      }}
                    >
                      Text
                    </th>
                  </tr>
                </thead>

                <tbody>
                  {rows.map((row, idx) => {
                    const isTarget = idx === targetRowIndex;
                    const pageValue = getValueByIndex(row, pageNumberIndex);
                    const textValue = getValueByIndex(row, textIndex);
                    return (
                      <tr
                        key={idx}
                        ref={(el) => {
                          rowRefs.current[idx] = el;
                        }}
                        style={{
                          background: isTarget ? "#fff0c4" : idx % 2 === 0 ? "#fff" : "#fcfdff",
                        }}
                      >
                        <td
                          style={{
                            borderBottom: "1px solid #eef1f6",
                            padding: "8px 10px",
                            verticalAlign: "top",
                            fontSize: 12,
                            fontWeight: isTarget ? 700 : 500,
                            color: "#2d3748",
                          }}
                        >
                          {pageValue || "-"}
                        </td>

                        <td
                          style={{
                            borderBottom: "1px solid #eef1f6",
                            padding: "8px 10px",
                            verticalAlign: "top",
                            fontSize: 13,
                            lineHeight: 1.35,
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                          }}
                        >
                          {textValue}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}
      </div>
    </main>
  );
}
