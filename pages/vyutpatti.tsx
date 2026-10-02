import Head from "next/head";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import type { ReaderLine, TableRow } from "@/lib/vyutpatti/format";
import type { VyutpattiResult } from "@/lib/vyutpatti/pipeline";

// Vyutpatti: the reader types each vishay (in Devanagari) and gets its
// derivation from the koshes in Maharaj Saheb's order, as a reader page and an
// internal table, editable here and downloaded as one PDF.
//
// Nothing is kept: no vishay is saved on the server or in the browser
// (Sahebji's instruction), so the page warns before it is left with results.

type Engine = "claude" | "sarvam";

type Item = {
  key: number;
  number: string;
  vishay: string;
  status: "waiting" | "running" | "done" | "error";
  progress: string;
  error?: string;
  result?: VyutpattiResult;
  lines: ReaderLine[];
  rows: TableRow[];
};

const GUJARATI = /[\u0a80-\u0aff]/u;
// Letters must be Devanagari; digits, spaces, braces and punctuation may stay.
const OTHER_LETTER = /[^\P{L}\u0900-\u097f]/u;

/** "10.6.6 कायिकहिंसा" → number and vishay; same rule as the server. */
function parseLines(text: string) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const m = line.match(/^([0-9०-९]+(?:[.][0-9०-९]+)*)[.)]?\s+(.+)$/u);
      return { number: m ? m[1] : "", vishay: (m ? m[2] : line).replace(/\s+/g, " ").trim() };
    });
}

function problemWith(text: string) {
  for (const { vishay } of parseLines(text)) {
    if (GUJARATI.test(vishay)) return `“${vishay}” is in Gujarati script. Type the vishay in Devanagari (Hindi lipi).`;
    if (OTHER_LETTER.test(vishay)) return `“${vishay}” has letters that are not Devanagari.`;
  }
  return "";
}

const SOURCE_LABEL: Record<ReaderLine["source"], string> = {
  kosh: "From kosh",
  ai: "AI: verify",
  vigraha: "Vigraha: verify",
};

async function readStream(res: Response, onEvent: (event: Record<string, unknown>) => void) {
  if (!(res.headers.get("content-type") ?? "").includes("ndjson") || !res.body) {
    const json = await res.json().catch(() => ({}));
    onEvent({ type: "error", error: json.error || `Request failed (${res.status}).` });
    return;
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (!line.trim()) continue;
      try {
        onEvent(JSON.parse(line));
      } catch {
        // a torn line; the next ones still count
      }
    }
  }
}

function AutoGrow({ value, onChange, className, label }: { value: string; onChange: (v: string) => void; className?: string; label: string }) {
  const ref = useRef<HTMLTextAreaElement | null>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${el.scrollHeight}px`;
  }, [value]);
  return (
    <textarea ref={ref} className={className} value={value} rows={1} aria-label={label} onChange={(e) => onChange(e.target.value)} />
  );
}

export default function VyutpattiPage() {
  const [input, setInput] = useState("");
  const [box, setBox] = useState("1");
  const [engine, setEngine] = useState<Engine>("claude");
  const [items, setItems] = useState<Item[]>([]);
  const [running, setRunning] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState("");
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);
  const nextKey = useRef(1);

  const problem = useMemo(() => problemWith(input), [input]);
  const done = items.filter((i) => i.status === "done");

  // Nothing is saved anywhere: leaving the page loses the results.
  useEffect(() => {
    if (!done.length) return;
    const warn = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [done.length]);

  const update = (key: number, change: Partial<Item> | ((item: Item) => Partial<Item>)) =>
    setItems((list) => list.map((item) => (item.key === key ? { ...item, ...(typeof change === "function" ? change(item) : change) } : item)));

  async function runOne(item: Item) {
    update(item.key, { status: "running", progress: "Starting", error: undefined });
    try {
      const res = await fetch("/api/vyutpatti/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vishay: item.vishay, number: item.number, box, engine }),
      });
      let finished = false;
      await readStream(res, (event) => {
        if (event.type === "progress") update(item.key, { progress: String(event.message ?? "") });
        else if (event.type === "done") {
          finished = true;
          const result = event.result as VyutpattiResult;
          update(item.key, { status: "done", result, lines: result.lines, rows: result.rows, progress: "" });
        } else if (event.type === "error") {
          finished = true;
          update(item.key, { status: "error", error: String(event.error ?? "Something went wrong."), progress: "" });
        }
      });
      if (!finished) update(item.key, { status: "error", error: "The connection closed before the vyutpatti was ready. Try again.", progress: "" });
    } catch {
      update(item.key, { status: "error", error: "Could not reach the server. Check the connection and try again.", progress: "" });
    }
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (running || problem) return;
    const parsed = parseLines(input);
    if (!parsed.length) return;
    const fresh: Item[] = parsed.map((p) => ({ key: nextKey.current++, ...p, status: "waiting", progress: "Waiting", lines: [], rows: [] }));
    setItems((list) => [...fresh, ...list]);
    setRunning(true);
    // One at a time: each reads several page scans.
    for (const item of fresh) await runOne(item);
    setRunning(false);
    setInput("");
  }

  async function retry(item: Item) {
    if (running) return;
    setRunning(true);
    await runOne(item);
    setRunning(false);
  }

  async function download(only?: Item) {
    // In the order they were typed (the list shows the newest batch first).
    const chosen = only ? [only] : [...done].sort((a, b) => a.key - b.key);
    if (!chosen.length) return;
    setDownloading(true);
    setDownloadError("");
    try {
      const res = await fetch("/api/vyutpatti/pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sections: chosen.map((i) => ({ number: i.number, vishay: i.result?.vishay ?? i.vishay, box: i.result?.box ?? box, rows: i.rows, lines: i.lines })),
        }),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error || `The PDF could not be made (${res.status}).`);
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = chosen.length === 1 ? `${chosen[0].number ? `${chosen[0].number} ` : ""}${chosen[0].vishay} vyutpatti.pdf` : "vyutpatti.pdf";
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 10_000);
    } catch (error) {
      setDownloadError(error instanceof Error ? error.message : "The PDF could not be made.");
    } finally {
      setDownloading(false);
    }
  }

  const editLine = (key: number, index: number, body: string) =>
    update(key, (item) => ({ lines: item.lines.map((l, i) => (i === index ? { ...l, body } : l)) }));
  const removeLine = (key: number, index: number) => update(key, (item) => ({ lines: item.lines.filter((_, i) => i !== index) }));
  const editRow = (key: number, index: number, field: keyof TableRow, value: string) =>
    update(key, (item) => ({ rows: item.rows.map((r, i) => (i === index ? { ...r, [field]: value } : r)) }));
  const removeRow = (key: number, index: number) => update(key, (item) => ({ rows: item.rows.filter((_, i) => i !== index) }));

  return (
    <>
      <Head>
        <title>Vyutpatti</title>
      </Head>
      <div className="vyShell">
        <header className="chTop">
          <strong className="chBrand">Vyutpatti</strong>
          <nav className="chNav">
            <Link href="/">Search</Link>
            <Link href="/ask">Ask</Link>
            <Link href="/library">Library</Link>
          </nav>
        </header>

        <main className="vyMain">
          <form className="vyForm" onSubmit={onSubmit}>
            <label className="vyLabel" htmlFor="vy-input">
              Vishay
              <span>One per line, typed in Devanagari. A number before it becomes the heading (1.1 अचौर्य); a topic in braces guides the meaning ({"सकरणयोग {अयोगीकेवली}"}).</span>
            </label>
            <textarea
              id="vy-input"
              className="vyInput indic"
              value={input}
              rows={Math.min(8, Math.max(2, input.split("\n").length + 1))}
              placeholder={"1.1 अचौर्य\n10.6.6 कायिकहिंसा"}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) onSubmit(e as unknown as FormEvent);
              }}
              aria-invalid={Boolean(problem)}
              aria-describedby={problem ? "vy-problem" : undefined}
              spellCheck={false}
              autoComplete="off"
            />
            {problem ? (
              <p className="vyProblem" id="vy-problem" role="alert">
                {problem}
              </p>
            ) : null}
            <div className="vyControls">
              <label className="vyBox">
                Box No
                <input value={box} onChange={(e) => setBox(e.target.value.slice(0, 12))} inputMode="numeric" />
              </label>
              <div className="vyEngine" role="radiogroup" aria-label="Read with">
                {(["claude", "sarvam"] as Engine[]).map((e) => (
                  <button key={e} type="button" role="radio" aria-checked={engine === e} className={engine === e ? "isOn" : ""} onClick={() => setEngine(e)}>
                    {e === "claude" ? "Claude" : "Sarvam"}
                  </button>
                ))}
              </div>
              <button type="submit" className="vyGo" disabled={running || !input.trim() || Boolean(problem)}>
                {running ? "Working…" : "Find vyutpatti"}
              </button>
            </div>
            <p className="vyHint">
              Looked up in Abhidhan Vyutpatti Prakriya Kosh, Shabda Ratna Mahodadhi and Apte; a word none of them has goes to the Agamic
              Vyutpatti Kosh, Abhidhan Rajendra Kosh, Paia Sadda Mahannavo and Alpaparichit Saiddhantik Shabdakosh. Nothing you type is saved.
              {engine === "sarvam" ? " With Sarvam, Shabda Ratna Mahodadhi's Gujarati is taken from the OCR text and needs checking." : ""}
            </p>
          </form>

          {done.length ? (
            <div className="vyBar">
              <span>
                {done.length} vishay{done.length === 1 ? "" : "s"} ready
              </span>
              <button type="button" className="vyGo" onClick={() => download()} disabled={downloading}>
                {downloading ? "Making PDF…" : done.length === 1 ? "Download PDF" : `Download PDF (${done.length})`}
              </button>
              {downloadError ? <span className="vyProblem">{downloadError}</span> : null}
            </div>
          ) : null}

          <section className="vyList" aria-live="polite">
            {items.map((item) => (
              <article key={item.key} className={`vyCard is-${item.status}`}>
                <header className="vyCardHead">
                  <h2>
                    {item.number ? <span className="vyNumber">{item.number}</span> : null}
                    <span className="indic">{item.result?.vishay ?? item.vishay}</span>
                  </h2>
                  {item.status === "running" || item.status === "waiting" ? (
                    <span className="vyStatus">
                      <span className="chTyping" aria-hidden="true">
                        <span />
                        <span />
                        <span />
                      </span>
                      {item.progress}
                    </span>
                  ) : null}
                  {item.status === "done" ? (
                    <div className="vyCardActions">
                      <button type="button" onClick={() => download(item)} disabled={downloading}>
                        PDF
                      </button>
                      <button type="button" onClick={() => setItems((list) => list.filter((i) => i.key !== item.key))}>
                        Remove
                      </button>
                    </div>
                  ) : null}
                  {item.status === "error" ? (
                    <div className="vyCardActions">
                      <button type="button" onClick={() => retry(item)} disabled={running}>
                        Try again
                      </button>
                      <button type="button" onClick={() => setItems((list) => list.filter((i) => i.key !== item.key))}>
                        Remove
                      </button>
                    </div>
                  ) : null}
                </header>

                {item.status === "error" ? <p className="vyProblem">{item.error}</p> : null}

                {item.result ? (
                  <>
                    <p className="vyParts">
                      <span className="indic">
                        {item.result.parts.map((p) => (p.prefix ? `${p.word}-` : p.word)).join(" + ")}
                      </span>
                      {item.result.samasa ? <span className="vySamasa indic">{item.result.samasa}</span> : null}
                    </p>

                    <h3 className="vySection">Reader page</h3>
                    {item.lines.length ? (
                      <ol className="vyLines">
                        {item.lines.map((line, index) => (
                          <li key={index} className={`vyLine is-${line.source}`}>
                            <div className="vyLineText indic">
                              <strong>{line.head}</strong>
                              <AutoGrow
                                className="vyEdit indic"
                                label={`Line for ${line.head}`}
                                value={line.body}
                                onChange={(v) => editLine(item.key, index, v)}
                              />
                            </div>
                            <div className="vyLineMeta">
                              <span className={`vyTag is-${line.source}`}>{SOURCE_LABEL[line.source]}</span>
                              {line.note && line.source === "kosh" ? <span className="vyNote">{line.note}</span> : null}
                              <button type="button" className="vyRemove" onClick={() => removeLine(item.key, index)} aria-label={`Remove the line for ${line.head}`}>
                                Remove
                              </button>
                            </div>
                          </li>
                        ))}
                      </ol>
                    ) : (
                      <p className="vyEmpty">No line: none of the koshes has these words.</p>
                    )}

                    <details className="vyTableWrap">
                      <summary>
                        Internal table <span>{item.rows.length} rows</span>
                      </summary>
                      <div className="vyTableScroll">
                        <table className="vyTable">
                          <thead>
                            <tr>
                              <th>Sr.</th>
                              <th>Granth</th>
                              <th>ShastraPath</th>
                              <th>Pub.Rem</th>
                              <th>In.Rem</th>
                              <th aria-label="Remove" />
                            </tr>
                          </thead>
                          <tbody>
                            {item.rows.map((row, index) => (
                              <tr key={index} className={`is-${row.source}`}>
                                <td>{index + 1}</td>
                                <td className="indic">{row.granth}</td>
                                <td>
                                  <AutoGrow className="vyEdit indic" label="ShastraPath" value={row.shastraPath} onChange={(v) => editRow(item.key, index, "shastraPath", v)} />
                                </td>
                                <td>
                                  <AutoGrow className="vyEdit indic" label="Pub.Rem" value={row.pubRem} onChange={(v) => editRow(item.key, index, "pubRem", v)} />
                                </td>
                                <td>
                                  <AutoGrow className="vyEdit indic" label="In.Rem" value={row.inRem} onChange={(v) => editRow(item.key, index, "inRem", v)} />
                                </td>
                                <td>
                                  <button type="button" className="vyRemove" onClick={() => removeRow(item.key, index)} aria-label={`Remove row ${index + 1}`}>
                                    ×
                                  </button>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </details>

                    <details className="vyTableWrap">
                      <summary>
                        Kosh pages <span>{item.result.words.reduce((n, w) => n + w.entries.length, 0)} entries</span>
                      </summary>
                      <ul className="vySources">
                        {item.result.words.map((w) => (
                          <li key={w.word}>
                            <span className="vySourceWord indic">{w.word}</span>
                            {w.role === "base" && w.of ? <span className="vySourceRole indic">base of {w.of}</span> : null}
                            {w.entries.length ? (
                              w.entries.map((e) => (
                                <button
                                  key={e.id}
                                  type="button"
                                  className="vyPage"
                                  disabled={!e.pdfUrl}
                                  onClick={() =>
                                    e.pdfUrl &&
                                    setPdfTarget({ pdfUrl: e.pdfUrl, page: e.pdfPage, title: e.citation, searchTerm: e.head, searchMode: "exact_word" })
                                  }
                                  title={e.readFrom === "scan" ? "Read from the page scan" : "Read from the OCR text"}
                                >
                                  <span className="indic">{e.citation}</span>
                                  <span>{e.printedPage ? `p. ${e.printedPage}` : `PDF p. ${e.pdfPage}`}</span>
                                  {e.checked ? null : <span className="vyFlag">check</span>}
                                </button>
                              ))
                            ) : (
                              <span className="vyEmpty">{w.ai ? "in no kosh: AI" : "in no kosh"}</span>
                            )}
                          </li>
                        ))}
                      </ul>
                    </details>

                    {item.result.notes.length ? (
                      <ul className="vyNotes">
                        {item.result.notes.map((n) => (
                          <li key={n}>{n}</li>
                        ))}
                      </ul>
                    ) : null}
                  </>
                ) : null}
              </article>
            ))}
          </section>
        </main>
      </div>
      <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
    </>
  );
}

export const getServerSideProps: GetServerSideProps = async (ctx) => {
  const session = await getServerSession(ctx.req, ctx.res, authOptions);
  if (!session?.user || session.user.status !== "approved") {
    return { redirect: { destination: `/auth/signin?callbackUrl=${encodeURIComponent("/vyutpatti")}`, permanent: false } };
  }
  return { props: {} };
};
