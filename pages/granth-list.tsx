import Head from "next/head";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { AppNav } from "@/components/AppNav";

// Maharaj Saheb's granth list: the books of the Shrirang library, as her list
// gives them (Sr. No., Lib No, whether a PDF was found, the name), and for each
// the book of ours with the same Lib No. A book not linked that way has a name
// search in our library instead of a guess.

type Row = {
  sr: number;
  libNo: string;
  /** From the list: Yes / G PDF (a PDF was found), G (green sticker, no PDF), No (no PDF anywhere). */
  pdf: string;
  name: string;
  ours: { granthKey: string; customId: string; name: string; bookNumber: string } | null;
};

const PDF_LABEL: Record<string, string> = {
  Yes: "PDF found (GG)",
  "G PDF": "Green tick · PDF found",
  G: "Green tick · no PDF",
  No: "No PDF found",
};

type Show = "all" | "ours" | "notOurs" | "noPdf";

/** The name to search our library by: without "भाग 2", "[ANUYOGADVARASUTRAM]", "(अधिकार-3)", "शब्दशः विवेचन". */
const searchName = (name: string) =>
  name
    .replace(/\[[^\]]*\]|\([^)]*\)/g, " ")
    .replace(/(?:भाग|विभाग)\s*[0-9०-९]+/g, " ")
    .replace(/^श्री\s*/, "")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .slice(0, 2)
    .join(" ");

export default function GranthListPage() {
  const [rows, setRows] = useState<Row[]>([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [show, setShow] = useState<Show>("all");

  useEffect(() => {
    fetch("/api/granth-list")
      .then(async (res) => {
        const json = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(json.error || `The list could not be loaded (${res.status}).`);
        setRows(json.rows ?? []);
      })
      .catch((e) => setError(e instanceof Error ? e.message : "The list could not be loaded."))
      .finally(() => setLoading(false));
  }, []);

  const shown = useMemo(() => {
    const q = query.trim().toLowerCase();
    return rows.filter((r) => {
      if (show === "ours" && !r.ours) return false;
      if (show === "notOurs" && r.ours) return false;
      if (show === "noPdf" && !(r.pdf === "No" || r.pdf === "G")) return false;
      if (!q) return true;
      return r.name.toLowerCase().includes(q) || r.libNo.toLowerCase().includes(q) || String(r.sr) === q || (r.ours?.name.toLowerCase().includes(q) ?? false);
    });
  }, [rows, query, show]);

  const ours = rows.filter((r) => r.ours).length;

  return (
    <main className="libraryShell">
      <Head>
        <title>Granth list</title>
      </Head>
      <div className="libraryFrame">
        <header className="libraryHeader">
          <div className="libraryHeaderText">
            <h1 className="libraryTitle">Granth list</h1>
            <div className="libraryStatusLine">
              {error ? "Could not load the list" : loading ? "Loading the list…" : `${rows.length} books in Maharaj Saheb's Shrirang library list · ${ours} linked to our library by Lib No`}
            </div>
          </div>
          <AppNav current="library" extra={<Link href="/library">All our books</Link>} />
        </header>

        <section className="libraryToolbar glToolbar" aria-label="Find in the list">
          <div className="librarySearchBox">
            <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Find by name, Lib No or Sr. No." aria-label="Find in the granth list" className="librarySearchInput" />
            {query ? (
              <button type="button" className="libraryClearButton" onClick={() => setQuery("")} aria-label="Clear">
                Clear
              </button>
            ) : null}
          </div>
          <div className="glFilters" role="radiogroup" aria-label="Show">
            {(
              [
                ["all", "All"],
                ["ours", "In our library"],
                ["notOurs", "Not linked yet"],
                ["noPdf", "No PDF (to scan)"],
              ] as Array<[Show, string]>
            ).map(([key, label]) => (
              <button key={key} type="button" role="radio" aria-checked={show === key} className={`glFilter${show === key ? " isOn" : ""}`} onClick={() => setShow(key)}>
                {label}
              </button>
            ))}
          </div>
        </section>

        {error ? <div className="libraryError">{error}</div> : null}

        <div className="glTableWrap">
          <table className="glTable">
            <thead>
              <tr>
                <th>Sr.</th>
                <th>Lib No</th>
                <th>Book name with part</th>
                <th>PDF (from the list)</th>
                <th>In our library</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((r) => (
                <tr key={r.libNo}>
                  <td>{r.sr}</td>
                  <td className="glCode">{r.libNo}</td>
                  <td className="indic" lang="sa">
                    {r.name}
                  </td>
                  <td className={`glPdf is-${r.pdf.replace(/\s+/g, "")}`}>{PDF_LABEL[r.pdf] ?? r.pdf}</td>
                  <td>
                    {r.ours ? (
                      <>
                        <span>
                          {r.ours.bookNumber ? <strong>{r.ours.bookNumber}</strong> : null} {r.ours.name}
                        </span>{" "}
                        <Link href={`/?customId=${encodeURIComponent(r.ours.customId)}`}>Search in it</Link>
                      </>
                    ) : (
                      <Link href={`/library?q=${encodeURIComponent(searchName(r.name))}`}>Find in our library</Link>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!loading && !shown.length && !error ? <p className="libraryEmpty">No book in the list matches.</p> : null}
        </div>
      </div>
    </main>
  );
}
