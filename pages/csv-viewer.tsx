import Head from "next/head";
import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useMemo, useRef, useState } from "react";
import { AppNav } from "@/components/AppNav";

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

/** The first of the column names this sheet has: gatha sheets and OCR sheets name them differently. */
function firstColumn(headers: string[], names: string[]) {
  for (const name of names) {
    const index = headers.indexOf(name);
    if (index >= 0) return index;
  }
  return -1;
}

export default function CsvViewerPage() {
  const router = useRouter();
  const rowRefs = useRef<Record<number, HTMLElement | null>>({});

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
      setError("No spreadsheet to show.");
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
          throw new Error(body || `Could not open the spreadsheet (${res.status}).`);
        }

        const matrix = parseCsvRows(body);
        if (matrix.length === 0) {
          throw new Error("This spreadsheet is empty.");
        }

        const [headerRow, ...dataRows] = matrix;
        const normalizedHeaders = headerRow.map((h) => String(h ?? "").trim());

        let foundIndex: number | null = null;
        const customIdIdx = firstColumn(normalizedHeaders, ["custom_id", "granth_key"]);
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
    if (targetRowIndex == null || loading) return;
    rowRefs.current[targetRowIndex]?.scrollIntoView({ block: "start" });
  }, [loading, rows.length, targetRowIndex]);

  const pageNumberIndex = useMemo(() => headers.indexOf("page_number"), [headers]);
  const textIndex = useMemo(() => firstColumn(headers, ["text", "content"]), [headers]);

  function getValueByIndex(row: string[], index: number) {
    return index >= 0 ? String(row[index] ?? "") : "";
  }

  return (
    <main className="lt tv">
      <Head>
        <title>Spreadsheet</title>
      </Head>
      <div className="tvFrame">
        <header className="tvTop">
          <div className="tvTitle">
            <h1>Spreadsheet</h1>
            {rows.length ? (
              <p className="tvSummary">
                {rows.length} row{rows.length === 1 ? "" : "s"}
                {targetPage != null && targetRowIndex == null && !loading ? ` · page ${pageRaw} not found` : ""}
              </p>
            ) : null}
          </div>
          <AppNav
            extra={
              csvUrl ? (
                <a href={csvUrl} target="_blank" rel="noreferrer">
                  Download
                </a>
              ) : null
            }
          />
        </header>

        {loading ? (
          <p className="ltLoading" role="status">
            <span className="ltSpinner" aria-hidden="true" /> Loading…
          </p>
        ) : null}
        {error ? <p className="ltError" role="alert">{error}</p> : null}

        {!loading && !error && rows.length > 0 ? (
          <section className="tvPages" aria-label="Rows">
            {rows.map((row, idx) => {
              const pageValue = getValueByIndex(row, pageNumberIndex);
              return (
                <article
                  key={idx}
                  ref={(el) => {
                    rowRefs.current[idx] = el;
                  }}
                  className={`tvPage${idx === targetRowIndex ? " isOn" : ""}`}
                >
                  <div className="tvFolio">
                    <span>Page</span>
                    <strong>{pageValue || "–"}</strong>
                  </div>
                  <div className="tvText indic">{getValueByIndex(row, textIndex)}</div>
                </article>
              );
            })}
          </section>
        ) : null}
      </div>
    </main>
  );
}
