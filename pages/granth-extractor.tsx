import Head from "next/head";
import Link from "next/link";
import { useRouter } from "next/router";
import { useEffect, useMemo, useRef, useState } from "react";
import { LightTableIcon } from "@/components/LightTableIcon";
import { PageThumb } from "@/components/PageThumb";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { Sheet, Stepper } from "@/components/Sheet";
import type { ExtractChapter, ExtractGatha, ExtractWork } from "@/lib/extract-data";
import {
  buildParts,
  estimateBytes,
  pageTotal,
  parseAsked,
  passagePages,
  planGathas,
  planPages,
  splitForEmail,
  type Passage,
  type Plan,
} from "@/lib/extract-plan";
import { prepareRow, rankRows } from "@/lib/granth-name-search";

type Mode = "gathas" | "pages";
type Recent = {
  id: number;
  mode: Mode;
  work_id: string;
  work_title: string;
  volume_key: string | null;
  chapter_id: string | null;
  chapter_label: string | null;
  spec: string;
};

const EMAIL_LIMIT = 15 * 1024 * 1024;
// Size estimates are rough, so email parts aim well under the limit.
const EMAIL_TARGET = 12 * 1024 * 1024;
const MAX_ADDED = 20;
const THUMBS_PER_PASSAGE = 48;

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `Request failed (${res.status})`);
  return json as T;
}

function unitWord(chapter: ExtractChapter | null) {
  const label = chapter?.unitLabel ?? "";
  if (/सूत्र|સૂત્ર|सुत्त/.test(label)) return "Sutra";
  if (/श्लोक|શ્લોક/.test(label)) return "Shlok";
  if (/कारिका|કારિકા/.test(label)) return "Karika";
  return "Gatha";
}

function volumeName(work: ExtractWork, key: string) {
  const index = work.volumes.findIndex((v) => v.key === key);
  const volume = work.volumes[index];
  if (!volume) return key;
  if (volume.part && !/^\d+$/.test(volume.part.trim())) return volume.part;
  if (work.volumes.length === 1) return "The book";
  return `Volume ${volume.part?.trim() || index + 1}`;
}

function rangesText(ranges: Array<[number, number]>, max = 6) {
  const parts = ranges.slice(0, max).map(([a, b]) => (a === b ? `${a}` : `${a}–${b}`));
  return parts.join(", ") + (ranges.length > max ? ` … (${ranges.length - max} more runs)` : "");
}

function plural(n: number, word: string) {
  return `${n} ${word}${n === 1 ? "" : "s"}`;
}

function mb(bytes: number) {
  return bytes >= 1048576 ? `${(bytes / 1048576).toFixed(1)} MB` : `${Math.max(1, Math.round(bytes / 1024))} KB`;
}

export default function GranthExtractor() {
  const router = useRouter();
  const [works, setWorks] = useState<ExtractWork[] | null>(null);
  const [worksError, setWorksError] = useState<string | null>(null);
  const [recents, setRecents] = useState<Recent[]>([]);

  const [mode, setMode] = useState<Mode | null>(null);
  const [work, setWork] = useState<ExtractWork | null>(null);
  const [volumeKey, setVolumeKey] = useState<string | null>(null);
  const [chapters, setChapters] = useState<ExtractChapter[] | null>(null);
  const [chapter, setChapter] = useState<ExtractChapter | null>(null);
  const [gathas, setGathas] = useState<ExtractGatha[] | null>(null);
  const [spec, setSpec] = useState("");
  const [passages, setPassages] = useState<Passage[] | null>(null);
  const [missing, setMissing] = useState<number[]>([]);
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [query, setQuery] = useState("");
  const [chapterQuery, setChapterQuery] = useState("");
  const [viewer, setViewer] = useState<PdfDialogTarget | null>(null);
  const [exportOpen, setExportOpen] = useState(false);
  const restoring = useRef<Recent | null>(null);

  // ------------------------------------------------------------ data
  useEffect(() => {
    getJson<{ works: ExtractWork[] }>("/api/extract/works")
      .then((r) => setWorks(r.works))
      .catch((e) => setWorksError(e.message));
    loadRecents();
  }, []);

  function loadRecents() {
    getJson<{ items: Recent[] }>("/api/extract/history")
      .then((r) => setRecents(r.items))
      .catch(() => setRecents([]));
  }

  // A link from the library opens on that volume (?bookCode=…).
  useEffect(() => {
    if (!works || !router.isReady) return;
    const code = String(router.query.bookCode ?? router.query.volume ?? "");
    if (!code) return;
    const found = works.find((w) => w.volumes.some((v) => v.key === code || v.key === code.replace(/^0+/, "")));
    if (!found) return;
    const vol = found.volumes.find((v) => v.key === code || v.key === code.replace(/^0+/, ""))!;
    setMode(vol.gathas > 0 ? "gathas" : "pages");
    chooseWork(found, vol.gathas > 0 ? "gathas" : "pages", vol.key);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [works, router.isReady]);

  const prepared = useMemo(
    () =>
      (works ?? []).map((w) =>
        prepareRow({ source: `${w.bookNumbers[0] ?? ""}_${w.title}`, extra: [w.nativeTitle, w.author, ...w.bookNumbers].filter(Boolean).join(" ") })
      ),
    [works]
  );
  const listed = useMemo(() => {
    const pool = (works ?? []).map((w, i) => ({ w, i })).filter(({ w }) => (mode === "gathas" ? w.gathas > 0 : true));
    if (!query.trim()) return pool.map(({ w }) => w);
    const order = rankRows(prepared, query.trim());
    const allowed = new Set(pool.map(({ i }) => i));
    return order.filter((i) => allowed.has(i)).map((i) => works![i]);
  }, [works, prepared, query, mode]);

  function chooseWork(next: ExtractWork, forMode: Mode | null = mode, volume: string | null = null) {
    setWork(next);
    setError(null);
    setPassages(null);
    setChapter(null);
    setGathas(null);
    setChapters(null);
    setChapterQuery("");
    if (forMode === "pages") {
      setVolumeKey(volume ?? (next.volumes.length === 1 ? next.volumes[0].key : null));
      return;
    }
    setVolumeKey(null);
    setLoading("chapters");
    getJson<{ chapters: ExtractChapter[] }>(`/api/extract/chapters?work=${encodeURIComponent(next.id)}`)
      .then((r) => {
        let list = r.chapters;
        if (volume) {
          const inVolume = list.filter((c) => c.volumeKeys.includes(volume));
          if (inVolume.length) list = inVolume;
        }
        setChapters(r.chapters);
        // A book numbered straight through has nothing to choose.
        if (list.length === 1) chooseChapter(next, list[0]);
        else if (restoring.current?.chapter_id) {
          const again = r.chapters.find((c) => c.id === restoring.current?.chapter_id);
          if (again) chooseChapter(next, again);
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(null));
  }

  function chooseChapter(forWork: ExtractWork, next: ExtractChapter) {
    setChapter(next);
    setPassages(null);
    setGathas(null);
    setError(null);
    setLoading("gathas");
    getJson<{ gathas: ExtractGatha[] }>(`/api/extract/gathas?work=${encodeURIComponent(forWork.id)}&chapter=${encodeURIComponent(next.id)}`)
      .then((r) => {
        setGathas(r.gathas);
        const again = restoring.current;
        if (again && again.chapter_id === next.id) {
          restoring.current = null;
          setSpec(again.spec);
          showPlan(planFor("gathas", forWork, next, r.gathas, null, again.spec), forWork, next, null, again.spec, false);
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(null));
  }

  // ------------------------------------------------------------ planning
  const pageCounts = useMemo(() => new Map((work?.volumes ?? []).map((v) => [v.key, v.pageCount])), [work]);
  const bytesPerPage = useMemo(
    // A copied page carries its own fonts and images, so the built file runs
    // larger than the source's average page: 7 pages of Bruhatkalpa measured
    // 2.1 MB against 1.2 MB from the source's size.
    () => new Map((work?.volumes ?? []).filter((v) => v.sizeBytes && v.pageCount).map((v) => [v.pdfUrl, (v.sizeBytes! / v.pageCount!) * 1.8])),
    [work]
  );
  const volume = work?.volumes.find((v) => v.key === volumeKey) ?? null;

  function planFor(m: Mode, w: ExtractWork, ch: ExtractChapter | null, rows: ExtractGatha[] | null, vol: string | null, text: string): Plan | null {
    if (m === "gathas") return rows && ch ? planGathas(text, rows, new Map(w.volumes.map((v) => [v.key, v.pageCount])), unitWord(ch)) : null;
    const v = w.volumes.find((x) => x.key === vol);
    return v ? planPages(text, v.key, v.pdfUrl, v.pageCount) : null;
  }

  const parsed = parseAsked(spec);
  const draft = useMemo(
    () => (mode && work ? planFor(mode, work, chapter, gathas, volumeKey, spec) : null),
    [mode, work, chapter, gathas, volumeKey, spec]
  );

  function showPlan(plan: Plan | null, w = work, ch = chapter, vol = volumeKey, text = spec, remember = true) {
    if (!plan || !w) return;
    if (!plan.passages.length) {
      setError(plan.asked.length ? `None of these exist here: ${plan.missing.slice(0, 12).join(", ")}${plan.missing.length > 12 ? "…" : ""}` : "Type the numbers first.");
      return;
    }
    setError(null);
    setMissing(plan.missing);
    setPassages(plan.passages);
    if (remember) {
      void fetch("/api/extract/history", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode: mode ?? "gathas",
          workId: w.id,
          workTitle: w.title,
          volumeKey: vol,
          chapterId: ch?.id ?? null,
          chapterLabel: ch?.label ?? null,
          spec: text.trim(),
        }),
      }).then(loadRecents);
    }
  }

  const pendingRecent = useRef<Recent | null>(null);
  useEffect(() => {
    if (works && pendingRecent.current) {
      const item = pendingRecent.current;
      pendingRecent.current = null;
      openRecent(item);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [works]);

  function openRecent(item: Recent) {
    if (!works) {
      // Still loading the library: open it as soon as it arrives.
      pendingRecent.current = item;
      setLoading("works");
      return;
    }
    const w = works.find((x) => x.id === item.work_id);
    if (!w) {
      setError("That granth is no longer in the library.");
      return;
    }
    setMode(item.mode);
    setSpec(item.spec);
    if (item.mode === "pages") {
      setWork(w);
      setVolumeKey(item.volume_key);
      setChapter(null);
      const plan = planFor("pages", w, null, null, item.volume_key, item.spec);
      if (plan?.passages.length) {
        setMissing(plan.missing);
        setPassages(plan.passages);
        void fetch("/api/extract/history", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode: "pages", workId: w.id, workTitle: w.title, volumeKey: item.volume_key, spec: item.spec }),
        }).then(loadRecents);
      }
      return;
    }
    restoring.current = item;
    chooseWork(w, "gathas");
  }

  function forgetRecent(id: number) {
    setRecents((list) => list.filter((r) => r.id !== id));
    void fetch(`/api/extract/history?id=${id}`, { method: "DELETE" });
  }

  function restart() {
    setMode(null);
    setWork(null);
    setVolumeKey(null);
    setChapters(null);
    setChapter(null);
    setGathas(null);
    setSpec("");
    setPassages(null);
    setMissing([]);
    setError(null);
    setQuery("");
    restoring.current = null;
    if (router.query.bookCode) void router.replace("/granth-extractor", undefined, { shallow: true });
  }

  const step = !mode ? "mode" : !work ? "granth" : mode === "gathas" && !chapter ? "chapter" : mode === "pages" && !volumeKey ? "volume" : !passages ? "numbers" : "preview";
  const total = passages ? pageTotal(passages) : 0;
  const estimate = passages ? estimateBytes(buildParts(passages), bytesPerPage) : 0;
  const multiChapter = (chapters?.length ?? 0) > 1;

  const shownChapters = useMemo(() => {
    const list = chapters ?? [];
    const q = chapterQuery.trim().toLowerCase();
    if (!q) return list;
    const n = Number(q);
    return list.filter((c) => (c.label ?? "").toLowerCase().includes(q) || (Number.isFinite(n) && (c.adhikar === n || (c.first <= n && n <= c.last))));
  }, [chapters, chapterQuery]);

  function updatePassage(id: string, patch: Partial<Passage>) {
    setPassages((list) => (list ? list.map((p) => (p.id === id ? { ...p, ...patch } : p)) : list));
  }

  // ------------------------------------------------------------ view
  return (
    <>
      <Head>
        <title>Extract pages</title>
      </Head>
      <div className="exShell">
        <header className="chTop">
          <strong className="chBrand">Extract</strong>
          <nav className="chNav">
            <Link href="/">Search</Link>
            <Link href="/ask">Ask</Link>
            <Link href="/library">Library</Link>
          </nav>
        </header>

        <main className="exMain">
          {mode ? (
            <ol className="exTrail" aria-label="Your choices">
              <li>
                <button type="button" onClick={restart}>
                  {mode === "gathas" ? "By gatha" : "By page"}
                </button>
              </li>
              {work ? (
                <li>
                  <button type="button" onClick={() => { setWork(null); setPassages(null); setChapter(null); setVolumeKey(null); }}>
                    {work.title}
                  </button>
                </li>
              ) : null}
              {chapter && multiChapter ? (
                <li>
                  <button type="button" onClick={() => { setChapter(null); setPassages(null); setGathas(null); }}>
                    {chapter.label}
                  </button>
                </li>
              ) : null}
              {volume && mode === "pages" && work && work.volumes.length > 1 ? (
                <li>
                  <button type="button" onClick={() => { setVolumeKey(null); setPassages(null); }}>
                    {volumeName(work, volume.key)}
                  </button>
                </li>
              ) : null}
              {passages ? (
                <li>
                  <button type="button" onClick={() => setPassages(null)}>{spec}</button>
                </li>
              ) : null}
              <li className="exTrailEnd">
                <button type="button" className="exGhost" onClick={restart}>
                  Start again
                </button>
              </li>
            </ol>
          ) : null}

          {error ? <p className="exError" role="alert">{error}</p> : null}

          {/* ---------------------------------------------- 1. what to look up */}
          {step === "mode" ? (
            <section className="exStep" aria-labelledby="ex-mode">
              <h1 id="ex-mode" className="exQuestion">What do you want to take out of a granth?</h1>
              <div className="exChoices">
                <button type="button" className="exChoice" onClick={() => setMode("gathas")}>
                  <strong>Gathas</strong>
                  <span>Choose a granth, a chapter and the gatha numbers</span>
                </button>
                <button type="button" className="exChoice" onClick={() => setMode("pages")}>
                  <strong>Pages</strong>
                  <span>Choose a granth and the PDF page numbers</span>
                </button>
              </div>
              {recents.length ? (
                <div className="exRecents">
                  <h2>Recent</h2>
                  <ul>
                    {recents.map((r) => (
                      <li key={r.id}>
                        <button type="button" className="exRecent" onClick={() => openRecent(r)}>
                          <span className="exRecentWhat">
                            {r.mode === "gathas" ? "Gathas" : "Pages"} {r.spec}
                          </span>
                          <span className="exRecentWhere">
                            {r.work_title}
                            {r.chapter_label ? ` · ${r.chapter_label}` : ""}
                          </span>
                        </button>
                        <button type="button" className="exRecentForget" aria-label={`Remove ${r.work_title} ${r.spec}`} onClick={() => forgetRecent(r.id)}>
                          <LightTableIcon name="close" size={14} />
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </section>
          ) : null}

          {/* ---------------------------------------------- 2. which granth */}
          {step === "granth" ? (
            <section className="exStep" aria-labelledby="ex-granth">
              <h1 id="ex-granth" className="exQuestion">Which granth?</h1>
              <input
                className="exInput"
                type="search"
                autoFocus
                value={query}
                placeholder="Name or book number"
                onChange={(e) => setQuery(e.target.value)}
                aria-label="Find a granth"
              />
              {worksError ? <p className="exError">{worksError}</p> : null}
              {!works && !worksError ? <p className="exQuiet">Loading the library…</p> : null}
              {works ? (
                <ul className="exWorks">
                  {listed.slice(0, 60).map((w) => (
                    <li key={w.id}>
                      <button type="button" className="exWork" onClick={() => chooseWork(w)}>
                        {w.cover ? <img src={w.cover} alt="" loading="lazy" /> : <span className="exWorkNoCover" />}
                        <span className="exWorkText">
                          <strong>{w.title}</strong>
                          {w.nativeTitle ? <span className="indic">{w.nativeTitle}</span> : null}
                          <em>
                            {[w.bookNumbers.length ? `No. ${w.bookNumbers.join(", ")}` : null, w.volumes.length > 1 ? `${w.volumes.length} volumes` : null, mode === "gathas" ? `${w.gathas} gathas` : null]
                              .filter(Boolean)
                              .join(" · ")}
                          </em>
                        </span>
                      </button>
                    </li>
                  ))}
                  {!listed.length ? <li className="exQuiet">No granth matches “{query}”.</li> : null}
                  {listed.length > 60 ? <li className="exQuiet">{listed.length - 60} more — type more of the name.</li> : null}
                </ul>
              ) : null}
            </section>
          ) : null}

          {/* ---------------------------------------------- 3a. which chapter */}
          {step === "chapter" && work ? (
            <section className="exStep" aria-labelledby="ex-chapter">
              <h1 id="ex-chapter" className="exQuestion">Which chapter of {work.title}?</h1>
              {loading === "chapters" ? <p className="exQuiet">Reading the chapters…</p> : null}
              {chapters && chapters.length > 12 ? (
                <input
                  className="exInput"
                  type="search"
                  value={chapterQuery}
                  placeholder="Chapter name or number, or a gatha number"
                  onChange={(e) => setChapterQuery(e.target.value)}
                  aria-label="Find a chapter"
                />
              ) : null}
              {chapters && !chapters.length ? <p className="exQuiet">No gathas are mapped for this granth yet. Choose it by pages instead.</p> : null}
              <ul className="exChapters">
                {shownChapters.map((c) => (
                  <li key={c.id}>
                    <button type="button" className="exChapter" onClick={() => chooseChapter(work, c)}>
                      <strong className="indic">{c.label ?? "Whole granth"}</strong>
                      <span>
                        {c.unitLabel ? <span className="indic">{c.unitLabel} </span> : null}
                        {rangesText(c.ranges, 3)}
                      </span>
                      <em>
                        {plural(c.count, unitWord(c).toLowerCase())} · {work.volumes.length > 1 ? `${c.volumeKeys.map((k) => volumeName(work, k)).join(", ")} · ` : ""}pages {c.pageStart}–{c.pageEnd}
                      </em>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          ) : null}

          {/* ---------------------------------------------- 3b. which volume */}
          {step === "volume" && work ? (
            <section className="exStep" aria-labelledby="ex-volume">
              <h1 id="ex-volume" className="exQuestion">Which volume of {work.title}?</h1>
              <ul className="exChapters">
                {work.volumes.map((v) => (
                  <li key={v.key}>
                    <button type="button" className="exChapter" onClick={() => setVolumeKey(v.key)}>
                      <strong>{volumeName(work, v.key)}</strong>
                      <em>
                        {v.pageCount ? `${v.pageCount} pages` : "Pages unknown"}
                        {/^\d+$/.test(v.key) ? ` · No. ${v.key}` : ""}
                      </em>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          ) : null}

          {/* ---------------------------------------------- 4. which numbers */}
          {step === "numbers" && work ? (
            <section className="exStep" aria-labelledby="ex-numbers">
              <h1 id="ex-numbers" className="exQuestion">
                {mode === "gathas" ? `Which ${unitWord(chapter).toLowerCase()}s?` : "Which pages?"}
              </h1>
              {mode === "gathas" && chapter ? (
                <p className="exHave">
                  {chapter.label ? <span className="indic">{chapter.label}: </span> : null}
                  {chapter.unitLabel ? <span className="indic">{chapter.unitLabel} </span> : null}
                  {rangesText(chapter.ranges, 12)}
                </p>
              ) : null}
              {mode === "pages" && volume ? (
                <p className="exHave">
                  {volumeName(work, volume.key)}: pages 1–{volume.pageCount ?? "?"}
                </p>
              ) : null}
              <form
                className="exNumbers"
                onSubmit={(e) => {
                  e.preventDefault();
                  showPlan(draft);
                }}
              >
                <input
                  className="exInput exInputBig"
                  autoFocus
                  inputMode="text"
                  value={spec}
                  onChange={(e) => setSpec(e.target.value)}
                  placeholder={mode === "gathas" ? "e.g. 12 or 12-18, 25" : "e.g. 45 or 45-60, 72"}
                  aria-label={mode === "gathas" ? "Gatha numbers" : "Page numbers"}
                />
                <button type="submit" className="exPrimary" disabled={!draft?.passages.length || Boolean(parsed.error)}>
                  Show the pages
                </button>
              </form>
              {loading === "gathas" ? <p className="exQuiet">Reading the gathas…</p> : null}
              {parsed.error ? <p className="exError">{parsed.error}</p> : null}
              {draft && spec.trim() && !parsed.error ? (
                <p className="exLive" aria-live="polite">
                  {draft.passages.length
                    ? `${draft.asked.length - draft.missing.length} found · ${plural(pageTotal(draft.passages), "page")}`
                    : "Nothing found for these numbers."}
                  {draft.missing.length ? ` · not here: ${draft.missing.slice(0, 10).join(", ")}${draft.missing.length > 10 ? "…" : ""}` : ""}
                </p>
              ) : null}
            </section>
          ) : null}

          {/* ---------------------------------------------- 5. preview */}
          {step === "preview" && work && passages ? (
            <section className="exStep exPreview" aria-labelledby="ex-preview">
              <h1 id="ex-preview" className="exQuestion">
                {total} page{total === 1 ? "" : "s"} from {work.title}
              </h1>
              {missing.length ? <p className="exWarn">Not in this granth, so left out: {missing.slice(0, 20).join(", ")}{missing.length > 20 ? "…" : ""}</p> : null}
              {passages.map((p) => {
                const pages = passagePages(p);
                return (
                  <article key={p.id} className="exPassage">
                    <header className="exPassageHead">
                      <div>
                        <h2>{p.label}</h2>
                        <p>
                          {work.volumes.length > 1 ? `${volumeName(work, p.volumeKey)} · ` : ""}
                          PDF page{p.first === p.last ? ` ${p.first}` : `s ${p.first}–${p.last}`}
                          {p.before || p.after ? ` · with ${p.before + p.after} added` : ""}
                        </p>
                      </div>
                      {passages.length > 1 ? (
                        <button type="button" className="exGhost" onClick={() => setPassages((list) => (list ? list.filter((x) => x.id !== p.id) : list))}>
                          Leave out
                        </button>
                      ) : null}
                    </header>
                    <div className="exAround">
                      <Stepper label="Pages before" value={p.before} max={Math.min(MAX_ADDED, p.first - 1)} onChange={(v) => updatePassage(p.id, { before: v })} />
                      <Stepper
                        label="Pages after"
                        value={p.after}
                        max={p.pageCount ? Math.min(MAX_ADDED, p.pageCount - p.last) : MAX_ADDED}
                        onChange={(v) => updatePassage(p.id, { after: v })}
                      />
                    </div>
                    <ul className="exThumbs">
                      {pages.slice(0, THUMBS_PER_PASSAGE).map((n) => (
                        <li key={n} className={n < p.first || n > p.last ? "isAdded" : ""}>
                          <button type="button" onClick={() => setViewer({ pdfUrl: p.pdfUrl, page: n, title: `${work.title} · ${p.label}`, pageCount: p.pageCount })}>
                            <PageThumb pdfUrl={p.pdfUrl} page={n} eager={pages.indexOf(n) < 8} />
                            <span>
                              {n}
                              {n < p.first || n > p.last ? " · added" : ""}
                            </span>
                          </button>
                        </li>
                      ))}
                      {pages.length > THUMBS_PER_PASSAGE ? <li className="exQuiet">and {pages.length - THUMBS_PER_PASSAGE} more pages</li> : null}
                    </ul>
                  </article>
                );
              })}
              <div className="exBar">
                <span>
                  {plural(total, "page")} · about {mb(estimate)}
                </span>
                <button type="button" className="exGhost" onClick={() => setPassages(null)}>
                  Change numbers
                </button>
                <button type="button" className="exPrimary" onClick={() => setExportOpen(true)} disabled={!total}>
                  <LightTableIcon name="export" size={16} /> Get the PDF
                </button>
              </div>
            </section>
          ) : null}
        </main>
      </div>

      {exportOpen && work && passages ? (
        <ExportSheet
          work={work}
          chapter={chapter}
          mode={mode ?? "gathas"}
          passages={passages}
          bytesPerPage={bytesPerPage}
          onClose={() => setExportOpen(false)}
          onRestart={() => {
            setExportOpen(false);
            restart();
          }}
        />
      ) : null}
      <PdfPageDialog target={viewer} onClose={() => setViewer(null)} />
    </>
  );
}

// ------------------------------------------------------------ export
function ExportSheet({
  work,
  chapter,
  mode,
  passages,
  bytesPerPage,
  onClose,
  onRestart,
}: {
  work: ExtractWork;
  chapter: ExtractChapter | null;
  mode: Mode;
  passages: Passage[];
  bytesPerPage: Map<string, number>;
  onClose: () => void;
  onRestart: () => void;
}) {
  // Named after what is in the file, not what was typed (numbers that do not exist are left out).
  const included = passages.map((p) => p.label.replace(/^\S+\s/, "")).join(", ");
  const [title, setTitle] = useState(() =>
    [work.title, chapter?.label && chapter.label !== String(chapter.adhikar ?? "") ? chapter.label.split(" › ").pop() : chapter?.adhikar, mode === "gathas" ? `gatha ${included}` : `pages ${included}`]
      .filter(Boolean)
      .join(" ")
      .replace(/\s+/g, " ")
      .trim()
  );
  const [email, setEmail] = useState("");
  const [saved, setSaved] = useState<string[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const total = pageTotal(passages);
  const estimate = estimateBytes(buildParts(passages), bytesPerPage);
  const groups = useMemo(() => (estimate > EMAIL_TARGET ? splitForEmail(passages, bytesPerPage, EMAIL_TARGET) : [passages]), [estimate, passages, bytesPerPage]);

  useEffect(() => {
    getJson<{ emails?: string[] }>("/api/download-email-recipients")
      .then((r) => setSaved(r.emails ?? []))
      .catch(() => setSaved([]));
  }, []);

  async function build(list: Passage[], delivery: "download" | "email", name: string) {
    const res = await fetch("/api/extract/build", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: name, parts: buildParts(list), delivery, email: delivery === "email" ? email.trim() : undefined }),
    });
    if (!res.ok) {
      const json = await res.json().catch(() => ({}));
      throw new Error(json.error || `The PDF could not be made (${res.status})`);
    }
    return res;
  }

  async function download() {
    setBusy("Making the PDF…");
    setError(null);
    setDone(null);
    try {
      const res = await build(passages, "download", title);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${title.replace(/[\\/:*?"<>|]+/g, "_") || "granth_pages"}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
      setDone(`Downloaded ${res.headers.get("X-Extract-Pages") ?? total} pages (${mb(blob.size)}).`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  async function send() {
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) {
      setError("Type an email address.");
      return;
    }
    setError(null);
    setDone(null);
    try {
      for (let i = 0; i < groups.length; i += 1) {
        setBusy(groups.length > 1 ? `Sending part ${i + 1} of ${groups.length}…` : "Making and sending the PDF…");
        await build(groups[i], "email", groups.length > 1 ? `${title} (part ${i + 1} of ${groups.length})` : title);
      }
      setDone(groups.length > 1 ? `Sent to ${email.trim()} in ${groups.length} emails.` : `Sent to ${email.trim()}.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  return (
    <Sheet
      open
      title="Get the PDF"
      subtitle={`${plural(total, "page")} · about ${mb(estimate)}`}
      onClose={onClose}
      busy={Boolean(busy)}
      size="narrow"
      footer={
        done ? (
          <div className="exSheetActions">
            <button type="button" className="exGhost" onClick={onClose}>
              Back to the pages
            </button>
            <button type="button" className="exPrimary" onClick={onRestart}>
              Extract something else
            </button>
          </div>
        ) : null
      }
    >
      <div className="exExport">
        <label className="exField">
          <span>File name</span>
          <input className="exInput" value={title} onChange={(e) => setTitle(e.target.value)} disabled={Boolean(busy)} />
        </label>

        <section className="exExportBlock">
          <h3>On this device</h3>
          <button type="button" className="exPrimary exWide" onClick={download} disabled={Boolean(busy)}>
            <LightTableIcon name="export" size={16} /> Download PDF
          </button>
        </section>

        <section className="exExportBlock">
          <h3>By email</h3>
          <input
            className="exInput"
            type="email"
            list="ex-saved-emails"
            value={email}
            placeholder="name@example.com"
            onChange={(e) => setEmail(e.target.value)}
            disabled={Boolean(busy)}
            aria-label="Email address"
          />
          <datalist id="ex-saved-emails">
            {saved.map((s) => (
              <option key={s} value={s} />
            ))}
          </datalist>
          {saved.length ? (
            <div className="exSaved">
              {saved.slice(0, 4).map((s) => (
                <button key={s} type="button" className="exChip" onClick={() => setEmail(s)} disabled={Boolean(busy)}>
                  {s}
                </button>
              ))}
            </div>
          ) : null}
          {groups.length > 1 ? (
            <p className="exWarn">
              About {mb(estimate)} is over the {EMAIL_LIMIT / 1048576} MB email limit, so it goes as {groups.length} emails, each under it.
            </p>
          ) : null}
          <button type="button" className="exSecondary exWide" onClick={send} disabled={Boolean(busy)}>
            <LightTableIcon name="mail" size={16} /> {groups.length > 1 ? `Send as ${groups.length} emails` : "Send"}
          </button>
        </section>

        {busy ? <p className="exQuiet" aria-live="polite">{busy}</p> : null}
        {done ? <p className="exDone" aria-live="polite"><LightTableIcon name="check" size={16} /> {done}</p> : null}
        {error ? <p className="exError" role="alert">{error}</p> : null}
      </div>
    </Sheet>
  );
}
