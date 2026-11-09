import { useCallback, useEffect, useRef, useState } from "react";
import { fileType, readStream } from "@/lib/anuvaad/browser";

// "Saved words" on /anuvaad (Maharaj Saheb, 9 Nov 2026): the śāstra words of
// the box files with the Gujarati his anuvaad gives them, kept for everyone.
// Vyutpatti shows a saved word's Gujarati first, and the box and page anuvaad
// use it for that word.
//
//   Add from a box: the box pages and their Gujarati anuvaad pages (one file or
//   several) are read; the words the anuvaad explains are listed with its
//   Gujarati, word for word; the reader ticks and corrects them, then saves.
//   The list: search, correct or remove any word.

type Saved = { id: number; word: string; gujarati: string; source: string; addedAt: string };
type Proposal = { word: string; gujarati: string; inBox: boolean; onPage: number | null; take: boolean };

export function AnuvaadWords({ onBack }: { onBack: () => void }) {
  const [words, setWords] = useState<Saved[]>([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState("");
  const [query, setQuery] = useState("");
  const [editing, setEditing] = useState<{ id: number; word: string; gujarati: string } | null>(null);

  const [adding, setAdding] = useState(false);
  const [files, setFiles] = useState<File[]>([]);
  const [source, setSource] = useState("");
  const [progress, setProgress] = useState("");
  const [addError, setAddError] = useState("");
  const [proposals, setProposals] = useState<Proposal[] | null>(null);
  const [saving, setSaving] = useState(false);
  const [notice, setNotice] = useState("");
  const picker = useRef<HTMLInputElement | null>(null);

  const load = useCallback(async (q: string) => {
    setLoading(true);
    setListError("");
    try {
      const res = await fetch(`/api/anuvaad/words?q=${encodeURIComponent(q)}`);
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "The saved words could not be read.");
      setWords(json.words ?? []);
    } catch (error) {
      setListError(error instanceof Error ? error.message : "The saved words could not be read.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => void load(query.trim()), query ? 300 : 0);
    return () => window.clearTimeout(timer);
  }, [query, load]);

  /** Reads every page of every file: box pages as Sanskrit, then the same file as Gujarati for the anuvaad pages. */
  async function readFiles() {
    if (!files.length) return;
    setAddError("");
    setProposals(null);
    const pages: string[] = [];
    try {
      for (const [i, file] of files.entries()) {
        const type = fileType(file);
        if (!/pdf|jpeg|png/.test(type)) throw new Error(`${file.name} is not a PDF, JPG or PNG.`);
        setProgress(`Reading ${file.name} (${i + 1} of ${files.length})`);
        const res = await fetch("/api/anuvaad/ocr?language=gu-IN", { method: "POST", headers: { "Content-Type": type }, body: await file.arrayBuffer() });
        let got: string[] | null = null;
        let failed = "";
        await readStream(res, (event) => {
          if (event.type === "progress") setProgress(`Reading ${file.name}: ${event.done} of ${event.total} pages`);
          else if (event.type === "done") got = (event.pages as string[]) ?? [];
          else if (event.type === "error") failed = String(event.error ?? "The pages could not be read.");
        });
        if (!got) throw new Error(failed || `${file.name} could not be read.`);
        pages.push(...(got as string[]));
      }
      setProgress("Finding the words the anuvaad explains");
      const res = await fetch("/api/anuvaad/learn", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ pages }) });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "The words could not be read.");
      const list = (json.proposals ?? []) as Omit<Proposal, "take">[];
      // the words the box itself prints are ticked; the rest are offered unticked
      setProposals(list.map((p) => ({ ...p, take: p.inBox })));
      if (!list.length) setAddError("The anuvaad pages explain no words that could be read. Check that the anuvaad pages were added.");
    } catch (error) {
      setAddError(error instanceof Error ? error.message : "The pages could not be read.");
    } finally {
      setProgress("");
    }
  }

  async function saveChosen() {
    const chosen = (proposals ?? []).filter((p) => p.take && p.word.trim() && p.gujarati.trim());
    if (!chosen.length) return;
    setSaving(true);
    setAddError("");
    try {
      const res = await fetch("/api/anuvaad/words", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ words: chosen.map(({ word, gujarati }) => ({ word, gujarati })), source: source.trim() || files.map((f) => f.name).join(", ") }),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "The words could not be saved.");
      setNotice(`${json.saved} word${json.saved === 1 ? "" : "s"} saved.`);
      setAdding(false);
      setFiles([]);
      setProposals(null);
      setSource("");
      void load(query.trim());
    } catch (error) {
      setAddError(error instanceof Error ? error.message : "The words could not be saved.");
    } finally {
      setSaving(false);
    }
  }

  async function saveEdit() {
    if (!editing) return;
    const res = await fetch("/api/anuvaad/words", { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(editing) });
    if (res.ok) {
      setWords((list) => list.map((w) => (w.id === editing.id ? { ...w, word: editing.word, gujarati: editing.gujarati } : w)));
      setEditing(null);
    } else setListError((await res.json().catch(() => ({}))).error || "The change could not be saved.");
  }

  async function remove(id: number) {
    const res = await fetch(`/api/anuvaad/words?id=${id}`, { method: "DELETE" });
    if (res.ok) setWords((list) => list.filter((w) => w.id !== id));
    else setListError("The word could not be removed.");
  }

  const setProposal = (i: number, change: Partial<Proposal>) => setProposals((list) => (list ? list.map((p, k) => (k === i ? { ...p, ...change } : p)) : list));

  return (
    <main className="vyMain">
      <button type="button" className="anBack" onClick={onBack}>
        ← Anuvaad
      </button>
      <section className="vyForm exStep">
        <h1 className="exQuestion">Saved words</h1>
        <p className="vyHint">
          Maharaj Saheb&apos;s own Gujarati for the words of the box files. Vyutpatti shows it first for these words, and the Anuvaad uses it when the word is
          in a passage.
        </p>
        {notice ? <p className="anNote">{notice}</p> : null}

        {!adding ? (
          <button type="button" className="vyGo" onClick={() => { setAdding(true); setNotice(""); }}>
            Add words from a box and its anuvaad
          </button>
        ) : (
          <div className="awAdd">
            <p className="vyLabel">
              <span>The box pages and Maharaj Saheb&apos;s Gujarati anuvaad pages for them (in one file or several, PDF or photos).</span>
            </p>
            <input
              ref={picker}
              type="file"
              accept="application/pdf,image/jpeg,image/png"
              multiple
              hidden
              onChange={(e) => {
                setFiles((list) => [...list, ...Array.from(e.target.files ?? [])]);
                e.target.value = "";
              }}
            />
            <div className="awRow">
              <button type="button" className="anChip" onClick={() => picker.current?.click()} disabled={Boolean(progress)}>
                Choose files
              </button>
              {files.map((f, i) => (
                <span key={`${f.name}-${i}`} className="anChip">
                  {f.name}{" "}
                  <button type="button" className="vyLink" aria-label={`Remove ${f.name}`} onClick={() => setFiles((list) => list.filter((_, k) => k !== i))} disabled={Boolean(progress)}>
                    ×
                  </button>
                </span>
              ))}
            </div>
            <label className="vyLabel">
              <span>Which box (optional)</span>
              <input className="vyInput" value={source} onChange={(e) => setSource(e.target.value)} placeholder="Box 1 — प्राणातिपातविरमणमहाव्रत" />
            </label>
            <div className="awRow">
              <button type="button" className="vyGo" onClick={() => void readFiles()} disabled={!files.length || Boolean(progress)}>
                {progress ? "Reading…" : "Find the words"}
              </button>
              <button type="button" className="vyLink" onClick={() => { setAdding(false); setFiles([]); setProposals(null); setAddError(""); }} disabled={Boolean(progress)}>
                Cancel
              </button>
            </div>
            {progress ? <p className="vyHint" role="status">{progress}</p> : null}
            {addError ? <p className="vyProblem">{addError}</p> : null}

            {proposals?.length ? (
              <div className="awProposals">
                <p className="vyHint">
                  Tick the words to save and correct them if needed. The Gujarati is copied word for word from the anuvaad pages; pen corrections on the
                  pages are not read, so put them in here. Words printed in the box are ticked.
                </p>
                <ul className="awList">
                  {proposals.map((p, i) => (
                    <li key={i} className={p.take ? "awItem isOn" : "awItem"}>
                      <label className="awTake">
                        <input type="checkbox" checked={p.take} onChange={() => setProposal(i, { take: !p.take })} />
                        <span className="vyTag">{p.inBox ? "in the box" : "only in the anuvaad"}</span>
                      </label>
                      <input className="vyInput indic awWord" value={p.word} aria-label="Word" onChange={(e) => setProposal(i, { word: e.target.value })} />
                      <textarea className="vyInput indic awGu" value={p.gujarati} aria-label="Gujarati" rows={2} onChange={(e) => setProposal(i, { gujarati: e.target.value })} />
                    </li>
                  ))}
                </ul>
                <button type="button" className="vyGo" onClick={() => void saveChosen()} disabled={saving || !proposals.some((p) => p.take)}>
                  {saving ? "Saving…" : `Save ${proposals.filter((p) => p.take).length} words`}
                </button>
              </div>
            ) : null}
          </div>
        )}
      </section>

      <section className="vyForm exStep">
        <label className="vyLabel">
          <span>Find a saved word</span>
          <input className="vyInput indic" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="अहिंसा" />
        </label>
        {listError ? <p className="vyProblem">{listError}</p> : null}
        {loading ? <p className="vyHint">Loading…</p> : null}
        {!loading && !words.length ? <p className="vyEmpty">{query ? "No saved word matches." : "No words saved yet."}</p> : null}
        <ul className="awList">
          {words.map((w) =>
            editing?.id === w.id ? (
              <li key={w.id} className="awItem isOn">
                <input className="vyInput indic awWord" value={editing.word} aria-label="Word" onChange={(e) => setEditing({ ...editing, word: e.target.value })} />
                <textarea className="vyInput indic awGu" value={editing.gujarati} aria-label="Gujarati" rows={3} onChange={(e) => setEditing({ ...editing, gujarati: e.target.value })} />
                <div className="awRow">
                  <button type="button" className="vyGo" onClick={() => void saveEdit()}>
                    Save
                  </button>
                  <button type="button" className="vyLink" onClick={() => setEditing(null)}>
                    Cancel
                  </button>
                </div>
              </li>
            ) : (
              <li key={w.id} className="awItem">
                <strong className="indic awHead">{w.word}</strong>
                <span className="indic">{w.gujarati}</span>
                <span className="vyLineMeta">{w.source}</span>
                <div className="awRow">
                  <button type="button" className="vyLink" onClick={() => setEditing({ id: w.id, word: w.word, gujarati: w.gujarati })}>
                    Correct
                  </button>
                  <button type="button" className="vyLink" onClick={() => void remove(w.id)}>
                    Remove
                  </button>
                </div>
              </li>
            )
          )}
        </ul>
      </section>
    </main>
  );
}
