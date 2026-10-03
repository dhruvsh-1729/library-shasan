import Head from "next/head";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { Sheet } from "@/components/Sheet";
import type { ReaderLine, TableRow } from "@/lib/vyutpatti/format";
import type { VyutpattiResult } from "@/lib/vyutpatti/pipeline";
import { needsDevanagari, toDevanagari } from "@/lib/to-devanagari";

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

// Letters may be typed in Devanagari, Gujarati or Roman; the last two are
// written in Devanagari (shown first) before anything is looked up.
const OTHER_LETTER = /[^\P{L}\u0900-\u097f\u0a80-\u0affa-zA-Zāīūṛṝḷṭḍṇśṣṃṁḥñṅ]/u;

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
    if (OTHER_LETTER.test(vishay)) return `“${vishay}” has letters that are not Hindi, Gujarati or English.`;
  }
  return "";
}

/** The input written in Devanagari by the server (kosh spellings for Roman words); the local reading if it cannot be reached. */
async function spellInDevanagari(text: string) {
  try {
    const res = await fetch("/api/vyutpatti/spell", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
    const json = (await res.json()) as { text?: string };
    if (res.ok && typeof json.text === "string") return json.text;
  } catch {
    // offline or slow: the reading made here
  }
  return toDevanagari(text);
}

type KoshPage = { id: string; granthKey: string; page: number; citation: string; printedPage: string | null; pdfUrl: string | null; head: string; words: string[] };

/** The kosh pages a result was read from, each page once, in the order the words came. */
function koshPagesOf(result: VyutpattiResult | undefined): KoshPage[] {
  const pages = new Map<string, KoshPage>();
  for (const w of result?.words ?? []) {
    for (const e of w.entries) {
      const id = `${e.granthKey}:${e.pdfPage}`;
      const known = pages.get(id);
      if (known) {
        if (!known.words.includes(w.word)) known.words.push(w.word);
        continue;
      }
      pages.set(id, { id, granthKey: e.granthKey, page: e.pdfPage, citation: e.citation, printedPage: e.printedPage, pdfUrl: e.pdfUrl, head: e.head, words: [w.word] });
    }
  }
  return [...pages.values()];
}

const pageLabel = (p: Pick<KoshPage, "printedPage" | "page">) => (p.printedPage ? `p. ${p.printedPage}` : `PDF ${p.page}`);

function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

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
  // Sized to its text; measured again once the Indic fonts arrive and whenever
  // the width changes (a phone turned sideways), or a line is cut short.
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const fit = () => {
      el.style.height = "auto";
      el.style.height = `${el.scrollHeight + 2}px`;
    };
    fit();
    let cancelled = false;
    document.fonts?.ready.then(() => !cancelled && fit());
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(fit);
    observer?.observe(el);
    return () => {
      cancelled = true;
      observer?.disconnect();
    };
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
  // Which kosh-page download is being made ("all", an item key, or a page id).
  const [pagesBusy, setPagesBusy] = useState("");
  const [pagesError, setPagesError] = useState("");
  const nextKey = useRef(1);
  // One question at a time: the vishay, then the results; the PDF in a sheet.
  const [view, setView] = useState<"input" | "results">("input");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [getOpen, setGetOpen] = useState(false);
  const [confirmClear, setConfirmClear] = useState(false);

  const problem = useMemo(() => problemWith(input), [input]);
  // Gujarati or English typed: the Devanagari it will be looked up as.
  const [spelled, setSpelled] = useState<{ from: string; text: string } | null>(null);
  const needsSpelling = needsDevanagari(input) && !problem;
  useEffect(() => {
    if (!needsSpelling) return;
    let active = true;
    const timer = window.setTimeout(async () => {
      const text = await spellInDevanagari(input);
      if (active) setSpelled({ from: input, text });
    }, 350);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [input, needsSpelling]);
  const preview = needsSpelling ? (spelled?.from === input ? spelled.text : toDevanagari(input)) : "";
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
    const text = needsDevanagari(input) ? (spelled?.from === input ? spelled.text : await spellInDevanagari(input)) : input;
    const parsed = parseLines(text);
    if (!parsed.length) return;
    const fresh: Item[] = parsed.map((p) => ({ key: nextKey.current++, ...p, status: "waiting", progress: "Waiting", lines: [], rows: [] }));
    setItems((list) => [...fresh, ...list]);
    setView("results");
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
      saveBlob(await res.blob(), chosen.length === 1 ? `${chosen[0].number ? `${chosen[0].number} ` : ""}${chosen[0].vishay} vyutpatti.pdf` : "vyutpatti.pdf");
    } catch (error) {
      setDownloadError(error instanceof Error ? error.message : "The PDF could not be made.");
    } finally {
      setDownloading(false);
    }
  }

  /** The kosh pages themselves, as scanned, in one PDF. */
  async function downloadPages(busy: string, pages: KoshPage[], name: string) {
    if (!pages.length || pagesBusy) return;
    setPagesBusy(busy);
    setPagesError("");
    try {
      const res = await fetch("/api/vyutpatti/pages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pages: pages.map((p) => ({ granthKey: p.granthKey, page: p.page })), name }),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error || `The kosh pages could not be fetched (${res.status}).`);
      }
      saveBlob(await res.blob(), `${name}.pdf`);
    } catch (error) {
      setPagesError(error instanceof Error ? error.message : "The kosh pages could not be fetched.");
    } finally {
      setPagesBusy("");
    }
  }

  const allPages = useMemo(() => {
    const seen = new Set<string>();
    return [...done]
      .sort((a, b) => a.key - b.key)
      .flatMap((i) => koshPagesOf(i.result))
      .filter((p) => (seen.has(p.id) ? false : (seen.add(p.id), true)));
  }, [done]);

  const editLine = (key: number, index: number, body: string) =>
    update(key, (item) => ({ lines: item.lines.map((l, i) => (i === index ? { ...l, body } : l)) }));
  const removeLine = (key: number, index: number) => update(key, (item) => ({ lines: item.lines.filter((_, i) => i !== index) }));
  const editRow = (key: number, index: number, field: keyof TableRow, value: string) =>
    update(key, (item) => ({ rows: item.rows.map((r, i) => (i === index ? { ...r, [field]: value } : r)) }));
  const removeRow = (key: number, index: number) => update(key, (item) => ({ rows: item.rows.filter((_, i) => i !== index) }));

  function startAgain() {
    setItems([]);
    setInput("");
    setConfirmClear(false);
    setGetOpen(false);
    setView("input");
  }

  // The kosh-pages download takes at most 80 pages at a time.
  const MAX_KOSH_PAGES = 80;
  const working = items.filter((i) => i.status === "waiting" || i.status === "running").length;

  return (
    <>
      <Head>
        <title>Vyutpatti</title>
      </Head>
      <div className={`vyShell${done.length ? " hasBar" : ""}`}>
        <header className="chTop">
          <strong className="chBrand">Vyutpatti</strong>
          <nav className="chNav">
            <Link href="/">Search</Link>
            <Link href="/ask">Ask</Link>
            <Link href="/library">Library</Link>
          </nav>
        </header>

        <main className="vyMain">
          {items.length ? (
            <ol className="exTrail" aria-label="Where you are">
              <li>
                <button type="button" onClick={() => setView("input")} aria-current={view === "input" ? "step" : undefined}>
                  {view === "input" ? "Vishay" : "Add more"}
                </button>
              </li>
              <li>
                <button type="button" onClick={() => setView("results")} aria-current={view === "results" ? "step" : undefined}>
                  Results ({items.length}){working ? ` · ${working} working` : ""}
                </button>
              </li>
              <li className="exTrailEnd">
                {confirmClear ? (
                  <span className="vyConfirm">
                    Nothing is saved. Clear all?
                    <button type="button" className="exGhost" onClick={startAgain} disabled={running}>
                      Clear
                    </button>
                    <button type="button" className="exGhost" onClick={() => setConfirmClear(false)}>
                      Keep
                    </button>
                  </span>
                ) : (
                  <button type="button" className="exGhost" onClick={() => setConfirmClear(true)} disabled={running}>
                    Start again
                  </button>
                )}
              </li>
            </ol>
          ) : null}

          {view === "input" ? (
          <form className="vyForm exStep" onSubmit={onSubmit}>
            <h1 className="exQuestion">
              <label htmlFor="vy-input">Which vishay?</label>
            </h1>
            <p className="vyLabel">
              <span>One per line · Hindi, ગુજરાતી or English · a number first if it has one</span>
            </p>
            <textarea
              id="vy-input"
              className="vyInput indic"
              value={input}
              autoFocus
              rows={Math.min(8, Math.max(2, input.split("\n").length + 1))}
              placeholder={"1.1 अचौर्य\n10.6.6 कायिकहिंसा"}
              lang="hi"
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) onSubmit(e as unknown as FormEvent);
              }}
              aria-invalid={Boolean(problem)}
              aria-describedby={problem ? "vy-problem" : undefined}
              spellCheck={false}
              autoComplete="off"
              autoCapitalize="off"
            />
            {problem ? (
              <p className="vyProblem" id="vy-problem" role="alert">
                {problem}
              </p>
            ) : null}
            {preview ? (
              <div className="vyPreview" aria-live="polite">
                <span>Will look up as</span>
                <span className="vyPreviewText indic">{preview}</span>
                <button type="button" className="vyLink" onClick={() => setInput(preview)}>
                  Use this
                </button>
              </div>
            ) : null}
            {settingsOpen ? (
              <div className="vyControls">
                <div className="vyEngine" role="radiogroup" aria-label="Read with">
                  {(["claude", "sarvam"] as Engine[]).map((e) => (
                    <button key={e} type="button" role="radio" aria-checked={engine === e} className={engine === e ? "isOn" : ""} onClick={() => setEngine(e)}>
                      {e === "claude" ? "Claude" : "Sarvam"}
                    </button>
                  ))}
                </div>
                <label className="vyBox">
                  Box
                  <input value={box} onChange={(e) => setBox(e.target.value.slice(0, 12))} inputMode="numeric" aria-label="Box No" />
                </label>
                <button type="button" className="vyLink" onClick={() => setSettingsOpen(false)}>
                  Done
                </button>
              </div>
            ) : (
              <p className="vySettingsLine">
                Read with {engine === "claude" ? "Claude" : "Sarvam"} · Box {box || "—"}{" "}
                <button type="button" className="vyLink" onClick={() => setSettingsOpen(true)}>
                  Change
                </button>
              </p>
            )}
            {engine === "sarvam" ? <p className="vyHint">Sarvam cannot read the page scans: check the Gujarati meanings.</p> : null}
            <button type="submit" className="vyGo exWide" disabled={running || !input.trim() || Boolean(problem)}>
              {running ? "Working…" : items.length ? "Find these too" : "Find vyutpatti"}
            </button>
          </form>
          ) : null}

          {view === "results" && done.length ? (
            <div className="vyBar">
              <span className="vyBarCount">
                {done.length} ready{working ? ` · ${working} working` : ""}
              </span>
              <button type="button" className="exGhost" onClick={() => setView("input")} disabled={running}>
                Add more
              </button>
              <button type="button" className="vyGo" onClick={() => setGetOpen(true)}>
                Get the PDF
              </button>
            </div>
          ) : null}

          {view === "results" && !getOpen && (downloadError || pagesError) ? (
            <p className="exError" role="alert">{downloadError || pagesError}</p>
          ) : null}

          {view === "results" ? (
          <section className="vyList" aria-live="polite">
            {items.map((item) => (
              <article key={item.key} className={`vyCard is-${item.status}`}>
                <header className="vyCardHead">
                  <h2>
                    {item.number ? <span className="vyNumber">{item.number}</span> : null}
                    <span className="indic">{item.result?.vishay ?? item.vishay}</span>
                  </h2>
                  {item.status === "done" || item.status === "error" ? (
                    <button
                      type="button"
                      className="vyClose"
                      onClick={() => setItems((list) => list.filter((i) => i.key !== item.key))}
                      aria-label={`Remove ${item.vishay}`}
                    >
                      ×
                    </button>
                  ) : null}
                </header>

                {item.status === "running" || item.status === "waiting" ? (
                  <p className="vyStatus">
                    <span className="chTyping" aria-hidden="true">
                      <span />
                      <span />
                      <span />
                    </span>
                    {item.progress}
                  </p>
                ) : null}

                {item.status === "error" ? (
                  <div className="vyErrorRow">
                    <p className="vyProblem">{item.error}</p>
                    <button type="button" className="vyLink" onClick={() => retry(item)} disabled={running}>
                      Try again
                    </button>
                  </div>
                ) : null}

                {item.result ? (
                  <>
                    {item.result.parts.length > 1 ? (
                      <p className="vyParts indic">
                        {item.result.parts
                          .filter((p) => !p.skip)
                          .map((p) => (p.prefix ? `${p.word}-` : p.word))
                          .join(" + ")}
                      </p>
                    ) : null}

                    {item.lines.length ? (
                      <ol className="vyLines">
                        {item.lines.map((line, index) => (
                          <li key={index} className={`vyLine is-${line.source}`}>
                            <div className="vyLineText indic">
                              <strong>{line.head}</strong>
                              {/* The line's leading " - " is drawn by the page, kept in the PDF. */}
                              <AutoGrow
                                className="vyEdit indic"
                                label={`Line for ${line.head}`}
                                value={line.body.replace(/^\s*-\s*/, "")}
                                onChange={(v) => editLine(item.key, index, ` - ${v.replace(/^\s*-\s*/, "")}`)}
                              />
                            </div>
                            <div className="vyLineMeta">
                              {line.source !== "kosh" ? <span className="vyTag">AI · check</span> : null}
                              {line.source === "kosh" && line.note ? <span className="vyTag">check page</span> : null}
                              <button type="button" className="vyX" onClick={() => removeLine(item.key, index)} aria-label={`Remove the line for ${line.head}`}>
                                ×
                              </button>
                            </div>
                          </li>
                        ))}
                      </ol>
                    ) : (
                      <p className="vyEmpty">None of the koshes has these words.</p>
                    )}

                    <div className="vyCardFoot">
                      <button type="button" className="vyLink" onClick={() => download(item)} disabled={downloading}>
                        PDF
                      </button>
                    </div>

                    {koshPagesOf(item.result).length ? (
                      <section className="vyKosh" aria-label="Kosh pages">
                        <div className="vyKoshHead">
                          <h3 className="vySection">Kosh pages</h3>
                          <button
                            type="button"
                            className="vyLink"
                            disabled={Boolean(pagesBusy)}
                            onClick={() =>
                              downloadPages(String(item.key), koshPagesOf(item.result), `${item.number ? `${item.number} ` : ""}${item.result?.vishay ?? item.vishay} kosh pages`)
                            }
                          >
                            {pagesBusy === String(item.key) ? "Fetching…" : `Download all (${koshPagesOf(item.result).length})`}
                          </button>
                        </div>
                        <ul className="vyKoshList">
                          {koshPagesOf(item.result).map((p) => (
                            <li key={p.id}>
                              <span className="vyKoshName">
                                <span className="indic">{p.citation}</span> <span className="vyKoshPg">{pageLabel(p)}</span>
                                <span className="vyKoshWords indic">{p.words.join(", ")}</span>
                              </span>
                              <span className="vyKoshActions">
                                <button
                                  type="button"
                                  className="vyLink"
                                  disabled={!p.pdfUrl}
                                  onClick={() =>
                                    p.pdfUrl && setPdfTarget({ pdfUrl: p.pdfUrl, page: p.page, title: p.citation, searchTerm: p.head, searchMode: "exact_word" })
                                  }
                                >
                                  View
                                </button>
                                <button
                                  type="button"
                                  className="vyLink"
                                  disabled={!p.pdfUrl || Boolean(pagesBusy)}
                                  onClick={() => downloadPages(p.id, [p], `${p.citation} ${pageLabel(p)}`)}
                                >
                                  {pagesBusy === p.id ? "…" : "Download"}
                                </button>
                              </span>
                            </li>
                          ))}
                        </ul>
                      </section>
                    ) : null}

                    <details className="vyDetails">
                      <summary>Details</summary>

                      <h3 className="vySection">Internal table</h3>
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
                                <td data-label="Sr.">{index + 1}</td>
                                <td data-label="Granth" className="indic">
                                  {row.granth}
                                </td>
                                <td data-label="ShastraPath">
                                  <AutoGrow className="vyEdit indic" label="ShastraPath" value={row.shastraPath} onChange={(v) => editRow(item.key, index, "shastraPath", v)} />
                                </td>
                                <td data-label="Pub.Rem">
                                  <AutoGrow className="vyEdit indic" label="Pub.Rem" value={row.pubRem} onChange={(v) => editRow(item.key, index, "pubRem", v)} />
                                </td>
                                <td data-label="In.Rem">
                                  <AutoGrow className="vyEdit indic" label="In.Rem" value={row.inRem} onChange={(v) => editRow(item.key, index, "inRem", v)} />
                                </td>
                                <td>
                                  <button type="button" className="vyX" onClick={() => removeRow(item.key, index)} aria-label={`Remove row ${index + 1}`}>
                                    ×
                                  </button>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>

                      {item.result.notes.length ? (
                        <ul className="vyNotes">
                          {item.result.notes.map((n) => (
                            <li key={n}>{n}</li>
                          ))}
                        </ul>
                      ) : null}
                    </details>
                  </>
                ) : null}
              </article>
            ))}
          </section>
          ) : null}
        </main>
      </div>
      {getOpen ? (
        <Sheet open title="Get the PDF" subtitle={`${done.length} vishay${done.length === 1 ? "" : "s"} ready`} onClose={() => setGetOpen(false)} busy={downloading || Boolean(pagesBusy)} size="narrow">
          <div className="exExport">
            <section className="exExportBlock">
              <h3>Vyutpatti</h3>
              <p className="vyHint">Every vishay in the order typed, with your edits, as the internal table.</p>
              <button type="button" className="vyGo exWide" onClick={() => download()} disabled={downloading || !done.length}>
                {downloading ? "Making the PDF…" : done.length === 1 ? "Download PDF" : `Download PDF (${done.length})`}
              </button>
            </section>
            {allPages.length ? (
              <section className="exExportBlock">
                <h3>Kosh pages</h3>
                <p className="vyHint">
                  The scanned kosh pages these came from, each page once
                  {allPages.length > MAX_KOSH_PAGES ? ` — ${allPages.length} is over the ${MAX_KOSH_PAGES}-page limit, so the first ${MAX_KOSH_PAGES} are included; download the rest from each card.` : "."}
                </p>
                <button
                  type="button"
                  className="exSecondary exWide"
                  onClick={() => downloadPages("all", allPages.slice(0, MAX_KOSH_PAGES), "kosh pages")}
                  disabled={Boolean(pagesBusy)}
                >
                  {pagesBusy === "all" ? "Fetching pages…" : `Download kosh pages (${Math.min(allPages.length, MAX_KOSH_PAGES)})`}
                </button>
              </section>
            ) : null}
            {downloadError ? <p className="exError">{downloadError}</p> : null}
            {pagesError ? <p className="exError">{pagesError}</p> : null}
          </div>
        </Sheet>
      ) : null}
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
