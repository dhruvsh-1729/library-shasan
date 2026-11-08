import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import type { PDFDocument } from "pdf-lib";
import { AnuvaadView } from "@/components/AnuvaadView";
import { PageThumb } from "@/components/PageThumb";
import { addImagePage, fileType, readStream, saveBlob } from "@/lib/anuvaad/browser";
import { parseAnuvaad } from "@/lib/anuvaad/markup";
import { MAX_PAGES_PER_RUN, describePages, gujaratiNumber, pageText, parsePageList } from "@/lib/anuvaad/pages";

// "Pages of a PDF" on /anuvaad (Maharaj Saheb, 2 Nov 2026: "we have a PDF
// directly; from it we need some fixed pages"). One question at a time:
//
//   1. Which PDF?     one PDF, or photos of the pages
//   2. Which pages?   "12-15 20", counted as the PDF viewer counts them
//   3. The Gujarati   each page on its own: read (/api/anuvaad/ocr, sent as a
//                     one-page PDF cut here in the browser), then translated
//                     (/api/anuvaad/page); progress and "Try again" per page;
//                     then one PDF, each original page before its Gujarati.
//
// Nothing is kept on the server or in the browser.

type Photo = { id: number; name: string; type: string; bytes: ArrayBuffer };

type Source =
  | { kind: "pdf"; name: string; bytes: ArrayBuffer; total: number; url: string }
  | { kind: "photos"; photos: Photo[] };

type Item = {
  key: string;
  /** The page number in the PDF, or the photo's place in the list. */
  n: number;
  photoId?: number;
  status: "waiting" | "reading" | "translating" | "done" | "error";
  text?: string;
  markup?: string;
  literal?: string;
  error?: string;
};

const PARALLEL_PAGES = 2;
const IMAGE_TYPES = ["image/jpeg", "image/png"];

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

export function AnuvaadPages({ onBack }: { onBack: () => void }) {
  const [source, setSource] = useState<Source | null>(null);
  const [view, setView] = useState<"file" | "pages" | "results">("file");
  const [opening, setOpening] = useState(false);
  const [fileError, setFileError] = useState("");
  const [spec, setSpec] = useState("");
  const [items, setItems] = useState<Item[]>([]);
  const [withOriginal, setWithOriginal] = useState(true);
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState("");
  const [saved, setSaved] = useState("");
  const [confirmClear, setConfirmClear] = useState(false);
  const [dragging, setDragging] = useState(false);
  const pdfPicker = useRef<HTMLInputElement | null>(null);
  const photoPicker = useRef<HTMLInputElement | null>(null);
  const pdfDoc = useRef<PDFDocument | null>(null);
  const itemsRef = useRef<Item[]>([]);
  const nextPhoto = useRef(1);

  useEffect(() => {
    itemsRef.current = items;
  }, [items]);

  useEffect(() => {
    if (!source) return;
    const warn = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [source]);

  const sourceUrl = source?.kind === "pdf" ? source.url : "";
  useEffect(() => () => (sourceUrl ? URL.revokeObjectURL(sourceUrl) : undefined), [sourceUrl]);

  const choice = useMemo(() => (source?.kind === "pdf" ? parsePageList(spec, source.total) : { pages: [], problem: "" }), [source, spec]);
  const chosenCount = source?.kind === "photos" ? source.photos.length : choice.pages.length;
  const ready = items.filter((i) => i.status === "done" && i.markup);
  const working = items.filter((i) => i.status === "waiting" || i.status === "reading" || i.status === "translating").length;
  const sourceName = source?.kind === "pdf" ? source.name : source?.photos[0]?.name ?? "";

  const updateItem = (key: string, change: Partial<Item>) => {
    itemsRef.current = itemsRef.current.map((i) => (i.key === key ? { ...i, ...change } : i));
    setItems(itemsRef.current);
  };

  // ------------------------------------------------------------ 1. the file

  async function openPdf(file: File) {
    setOpening(true);
    setFileError("");
    try {
      const bytes = await file.arrayBuffer();
      const { PDFDocument } = await import("pdf-lib");
      const doc = await PDFDocument.load(bytes, { ignoreEncryption: true, updateMetadata: false });
      const total = doc.getPageCount();
      if (!total) throw new Error("empty");
      pdfDoc.current = doc;
      setSource({ kind: "pdf", name: file.name, bytes, total, url: URL.createObjectURL(new Blob([bytes], { type: "application/pdf" })) });
      setSpec(total === 1 ? "1" : total <= 10 ? `1-${total}` : "");
      setItems([]);
      setView("pages");
    } catch {
      setFileError("This file could not be opened as a PDF. Try another copy of it, or photos of the pages.");
    } finally {
      setOpening(false);
    }
  }

  async function addPhotos(files: File[]) {
    const fresh: Photo[] = [];
    for (const file of files) {
      const type = fileType(file);
      if (IMAGE_TYPES.includes(type)) fresh.push({ id: nextPhoto.current++, name: file.name, type, bytes: await file.arrayBuffer() });
    }
    if (!fresh.length) {
      setFileError("Choose photos in JPG or PNG.");
      return;
    }
    setFileError("");
    pdfDoc.current = null;
    setSource((s) => {
      const before = s?.kind === "photos" ? s.photos : [];
      return { kind: "photos", photos: [...before, ...fresh].slice(0, MAX_PAGES_PER_RUN) };
    });
    setItems([]);
    setView("pages");
  }

  function takeFiles(list: FileList | File[]) {
    const files = Array.from(list);
    const pdf = files.find((f) => fileType(f) === "application/pdf");
    if (pdf) void openPdf(pdf);
    else void addPhotos(files);
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    if (e.dataTransfer.files?.length) takeFiles(e.dataTransfer.files);
  }

  // ------------------------------------------------------------ 3. reading and translating

  async function pageBytes(item: Item): Promise<{ body: BlobPart; type: string }> {
    if (source?.kind === "photos") {
      const photo = source.photos.find((p) => p.id === item.photoId);
      if (!photo) throw new Error("The photo is no longer here.");
      return { body: photo.bytes, type: photo.type };
    }
    const src = pdfDoc.current;
    if (!src) throw new Error("The PDF is no longer open. Start again.");
    const { PDFDocument } = await import("pdf-lib");
    const one = await PDFDocument.create();
    const [page] = await one.copyPages(src, [item.n - 1]);
    one.addPage(page);
    return { body: (await one.save()) as BlobPart, type: "application/pdf" };
  }

  /** The page's text, or null (the item then shows the error). */
  async function readPage(item: Item): Promise<string | null> {
    updateItem(item.key, { status: "reading", error: undefined });
    try {
      const { body, type } = await pageBytes(item);
      const res = await fetch("/api/anuvaad/ocr", { method: "POST", headers: { "Content-Type": type }, body });
      let pages: string[] | null = null;
      let problem = "";
      await readStream(res, (event) => {
        if (event.type === "done") pages = (event.pages as string[]) ?? [];
        else if (event.type === "error") problem = String(event.error ?? "The page could not be read.");
      });
      if (pages === null) throw new Error(problem || "The connection closed before the page was read.");
      const text = pageText((pages as string[]).join("\n\n"));
      if (!text) throw new Error("No text was found on this page.");
      updateItem(item.key, { text });
      return text;
    } catch (error) {
      updateItem(item.key, { status: "error", error: error instanceof TypeError ? "Could not reach the server. Check the connection and try again." : error instanceof Error ? error.message : "The page could not be read." });
      return null;
    }
  }

  async function processOne(key: string) {
    const item = itemsRef.current.find((i) => i.key === key);
    if (!item) return;
    const text = item.text?.trim() ? item.text : await readPage(item);
    if (!text) return;
    updateItem(key, { status: "translating", error: undefined });
    try {
      const res = await fetch("/api/anuvaad/page", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text, source: sourceName.replace(/\.[a-z0-9]+$/i, "") }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || "The Gujarati could not be prepared. Please try again.");
      updateItem(key, { status: "done", markup: String(json.markup ?? ""), literal: String(json.literal ?? "") });
    } catch (error) {
      updateItem(key, { status: "error", error: error instanceof TypeError ? "Could not reach the server. Check the connection and try again." : error instanceof Error ? error.message : "The Gujarati could not be prepared." });
    }
  }

  async function run(keys: string[]) {
    if (!keys.length) return;
    for (const key of keys) updateItem(key, { status: "waiting", error: undefined });
    const queue = [...keys];
    await Promise.all(
      Array.from({ length: Math.min(PARALLEL_PAGES, queue.length) }, async () => {
        for (let key = queue.shift(); key; key = queue.shift()) await processOne(key);
      })
    );
  }

  function start() {
    if (!source || !chosenCount) return;
    const fresh: Item[] =
      source.kind === "pdf"
        ? choice.pages.map((n) => itemsRef.current.find((i) => i.n === n && i.status === "done") ?? { key: `p${n}`, n, status: "waiting" })
        : source.photos.map((p, i) => itemsRef.current.find((x) => x.photoId === p.id && x.status === "done") ?? { key: `f${p.id}`, n: i + 1, photoId: p.id, status: "waiting" });
    itemsRef.current = fresh;
    setItems(fresh);
    setSaved("");
    setView("results");
    void run(fresh.filter((i) => i.status !== "done").map((i) => i.key));
  }

  // ------------------------------------------------------------ the PDF

  const labelOf = (item: Item) => (source?.kind === "photos" ? `Photo ${item.n}` : `Page ${item.n}`);

  async function download() {
    const done = items.filter((i) => i.status === "done" && i.markup && parseAnuvaad(i.markup).length);
    if (!done.length || exporting || !source) return;
    setExporting(true);
    setExportError("");
    setSaved("");
    try {
      const res = await fetch("/api/anuvaad/pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          numbers: !withOriginal,
          sections: done.map((i) => ({ caption: `પાનું ${gujaratiNumber(i.n)} · ${sourceName}`, markup: i.markup })),
        }),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error || `The PDF could not be made (${res.status}).`);
      }
      const anuvaad = await res.arrayBuffer();
      let bytes: Uint8Array = new Uint8Array(anuvaad);
      if (withOriginal) {
        const starts = (res.headers.get("X-Section-Starts") ?? "").split(",").filter(Boolean).map(Number);
        if (starts.length !== done.length) throw new Error("The PDF could not be put together. Untick “Original page before each Gujarati page” and try again.");
        const { PDFDocument } = await import("pdf-lib");
        const out = await PDFDocument.create();
        out.setTitle("ગુજરાતી અનુવાદ");
        const translated = await PDFDocument.load(anuvaad);
        const count = translated.getPageCount();
        for (let k = 0; k < done.length; k += 1) {
          const item = done[k];
          if (source.kind === "pdf" && pdfDoc.current) {
            const [original] = await out.copyPages(pdfDoc.current, [item.n - 1]);
            out.addPage(original);
          } else if (source.kind === "photos") {
            const photo = source.photos.find((p) => p.id === item.photoId);
            if (photo) await addImagePage(out, photo.bytes, photo.type);
          }
          const own = Array.from({ length: (starts[k + 1] ?? count) - starts[k] }, (_, j) => starts[k] + j);
          for (const page of await out.copyPages(translated, own)) out.addPage(page);
        }
        bytes = await out.save();
      }
      const base = sourceName.replace(/\.[a-z0-9]+$/i, "").replace(/[\\/:*?"<>|]+/g, " ").trim() || "pages";
      const which = source.kind === "pdf" ? ` p${describePages(done.map((i) => i.n)).replace(/, /g, "_")}` : "";
      const name = `${base}${which} anuvaad.pdf`;
      saveBlob(new Blob([bytes as BlobPart], { type: "application/pdf" }), name);
      setSaved(`Saved “${name}” to your downloads (${plural(done.length, "page")} translated${withOriginal ? ", each after its original page" : ""}).`);
    } catch (error) {
      setExportError(error instanceof Error ? error.message : "The PDF could not be made.");
    } finally {
      setExporting(false);
    }
  }

  function startAgain() {
    setSource(null);
    pdfDoc.current = null;
    itemsRef.current = [];
    setItems([]);
    setSpec("");
    setSaved("");
    setExportError("");
    setFileError("");
    setConfirmClear(false);
    setView("file");
  }

  const openOriginal = (item: Item) => {
    if (source?.kind === "pdf") window.open(`${source.url}#page=${item.n}`, "_blank");
    else {
      const photo = source?.photos.find((p) => p.id === item.photoId);
      if (photo) window.open(URL.createObjectURL(new Blob([photo.bytes], { type: photo.type })), "_blank");
    }
  };

  // ------------------------------------------------------------ screens

  const progress = items.length ? Math.round((ready.length / items.length) * 100) : 0;

  return (
    <main className="vyMain">
      {source ? (
        <ol className="exTrail" aria-label="Where you are">
          <li>
            <button type="button" onClick={() => setView("file")} aria-current={view === "file" ? "step" : undefined}>
              {source.kind === "pdf" ? source.name : plural(source.photos.length, "photo")}
            </button>
          </li>
          <li>
            <button type="button" onClick={() => setView("pages")} aria-current={view === "pages" ? "step" : undefined}>
              {chosenCount ? plural(chosenCount, "page") : "Pages"}
            </button>
          </li>
          <li>
            <button type="button" onClick={() => setView("results")} aria-current={view === "results" ? "step" : undefined} disabled={!items.length}>
              Gujarati ({ready.length}){working ? ` · ${working} working` : ""}
            </button>
          </li>
          <li className="exTrailEnd">
            {confirmClear ? (
              <span className="vyConfirm">
                Nothing is saved. Clear all?
                <button type="button" className="exGhost" onClick={startAgain}>
                  Clear
                </button>
                <button type="button" className="exGhost" onClick={() => setConfirmClear(false)}>
                  Keep
                </button>
              </span>
            ) : (
              <button type="button" className="exGhost" onClick={() => setConfirmClear(true)} disabled={working > 0}>
                Start again
              </button>
            )}
          </li>
        </ol>
      ) : (
        <button type="button" className="exGhost anBack" onClick={onBack}>
          ‹ Back
        </button>
      )}

      <input
        ref={pdfPicker}
        type="file"
        accept="application/pdf"
        hidden
        onChange={(e) => {
          if (e.target.files?.[0]) void openPdf(e.target.files[0]);
          e.target.value = "";
        }}
      />
      <input
        ref={photoPicker}
        type="file"
        accept="image/jpeg,image/png"
        multiple
        hidden
        onChange={(e) => {
          if (e.target.files?.length) void addPhotos(Array.from(e.target.files));
          e.target.value = "";
        }}
      />

      {view === "file" ? (
        <section
          className={`vyForm exStep anPagesStep${dragging ? " isOver" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <p className="anStepNo">Step 1 of 3</p>
          <h1 className="exQuestion">Which PDF?</h1>
          <p className="exQuiet">Choose the PDF that has the pages you want in Gujarati. You choose the page numbers next.</p>
          <button type="button" className="vyGo exWide anBig" onClick={() => pdfPicker.current?.click()} disabled={opening}>
            {opening ? "Opening the PDF…" : source?.kind === "pdf" ? "Choose a different PDF" : "Choose a PDF"}
          </button>
          <button type="button" className="exSecondary exWide anBig" onClick={() => photoPicker.current?.click()} disabled={opening}>
            {source?.kind === "photos" ? "Add more photos" : "Or choose photos of the pages"}
          </button>
          {fileError ? (
            <p className="exError" role="alert">
              {fileError}
            </p>
          ) : null}
          {source ? (
            <button type="button" className="exGhost" onClick={() => setView("pages")}>
              Keep {source.kind === "pdf" ? `“${source.name}”` : plural(source.photos.length, "photo")} and go on ›
            </button>
          ) : null}
        </section>
      ) : null}

      {view === "pages" && source?.kind === "pdf" ? (
        <section className="vyForm exStep">
          <p className="anStepNo">Step 2 of 3</p>
          <h1 className="exQuestion">Which pages?</h1>
          <p className="exQuiet">
            <strong className="anFileTitle">{source.name}</strong> has {plural(source.total, "page")}.
          </p>
          <label className="anField">
            <span>Page numbers</span>
            <input
              className="exInput exInputBig"
              value={spec}
              onChange={(e) => setSpec(e.target.value)}
              placeholder="e.g. 12-15 20"
              inputMode="text"
              autoComplete="off"
              enterKeyHint="go"
              onKeyDown={(e) => {
                if (e.key === "Enter" && choice.pages.length) start();
              }}
            />
          </label>
          <p className="vyHint">
            Use the page number your PDF viewer shows, not the number printed on the page. A dash gives a run of pages: <strong>12-15 20</strong> is pages 12, 13, 14, 15 and 20. At most {MAX_PAGES_PER_RUN} at a time.
          </p>
          {source.total <= MAX_PAGES_PER_RUN && source.total > 1 ? (
            <button type="button" className="exChip anChip" onClick={() => setSpec(`1-${source.total}`)}>
              All {source.total} pages
            </button>
          ) : null}
          {choice.problem ? (
            <p className="exError" role="alert">
              {choice.problem}
            </p>
          ) : null}
          {choice.pages.length ? (
            <>
              <p className="anChosen">
                {plural(choice.pages.length, "page")} chosen: {describePages(choice.pages)}
              </p>
              <ul className="exThumbs anThumbs" aria-label="The chosen pages">
                {choice.pages.slice(0, 6).map((n) => (
                  <li key={n}>
                    <button type="button" onClick={() => window.open(`${source.url}#page=${n}`, "_blank")} title="Open this page in a new tab">
                      <PageThumb pdfUrl={source.url} page={n} width={150} />
                      Page {n}
                    </button>
                  </li>
                ))}
              </ul>
              {choice.pages.length > 6 ? <p className="vyHint">Check that these are the right pages. (The first 6 are shown.)</p> : <p className="vyHint">Check that these are the right pages.</p>}
            </>
          ) : null}
          <button type="button" className="vyGo exWide anBig" onClick={start} disabled={!choice.pages.length || working > 0}>
            {choice.pages.length ? `Translate ${plural(choice.pages.length, "page")} into Gujarati` : "Write the page numbers first"}
          </button>
        </section>
      ) : null}

      {view === "pages" && source?.kind === "photos" ? (
        <section className="vyForm exStep">
          <p className="anStepNo">Step 2 of 3</p>
          <h1 className="exQuestion">These pages, in this order?</h1>
          <ul className="anFiles">
            {source.photos.map((p, i) => (
              <li key={p.id}>
                <span className="anFileName">
                  {i + 1}. {p.name}
                </span>
                <button
                  type="button"
                  className="vyX"
                  aria-label={`Remove ${p.name}`}
                  disabled={working > 0}
                  onClick={() => setSource((s) => (s?.kind === "photos" ? { ...s, photos: s.photos.filter((x) => x.id !== p.id) } : s))}
                >
                  ×
                </button>
              </li>
            ))}
          </ul>
          <button type="button" className="exSecondary exWide" onClick={() => photoPicker.current?.click()} disabled={working > 0 || source.photos.length >= MAX_PAGES_PER_RUN}>
            Add more photos
          </button>
          <button type="button" className="vyGo exWide anBig" onClick={start} disabled={!source.photos.length || working > 0}>
            {source.photos.length ? `Translate ${plural(source.photos.length, "page")} into Gujarati` : "Add a photo first"}
          </button>
        </section>
      ) : null}

      {view === "results" ? (
        <section className="exStep anResults">
          <div className="vyBar anBar anPagesBar">
            <span className="vyBarCount">
              {ready.length} of {plural(items.length, "page")} ready{working ? ` · ${working} working` : ""}
            </span>
            <label className="anCheck">
              <input type="checkbox" checked={withOriginal} onChange={(e) => setWithOriginal(e.target.checked)} />
              Original page before each Gujarati page
            </label>
            <button type="button" className="vyGo" onClick={download} disabled={exporting || !ready.length}>
              {exporting ? "Making the PDF…" : working && ready.length ? `Download PDF (${ready.length} ready)` : "Download PDF"}
            </button>
            <div className="anProgress" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress} aria-label="Pages ready">
              <span style={{ width: `${progress}%` }} />
            </div>
          </div>
          {exportError ? (
            <p className="exError" role="alert">
              {exportError}
            </p>
          ) : null}
          {saved && !exportError ? (
            <p className="anSaved" role="status">
              {saved}
            </p>
          ) : null}
          <p className="anNote">Each page takes a minute or two. The Gujarati is a first draft: read it through and correct it (“Edit the Gujarati”) before printing.</p>

          {items.map((item) => (
            <article key={item.key} className={`vyCard anResult is-${item.status}`}>
              <div className="anPageHead">
                <h2 className="anPageLabel">{labelOf(item)}</h2>
                <button type="button" className="vyLink" onClick={() => openOriginal(item)} title="Opens the original page in a new tab">
                  See the page
                </button>
              </div>
              {item.status === "waiting" || item.status === "reading" || item.status === "translating" ? (
                <p className="vyStatus">
                  <span className="chTyping" aria-hidden="true">
                    <span />
                    <span />
                    <span />
                  </span>
                  {item.status === "waiting" ? "Waiting its turn" : item.status === "reading" ? "Reading the page (1 of 2)" : "Writing the Gujarati (2 of 2)"}
                </p>
              ) : null}
              {item.status === "error" ? (
                <div className="anPageError">
                  <p className="vyProblem">{item.error}</p>
                  <button type="button" className="exSecondary" onClick={() => void run([item.key])}>
                    Try again
                  </button>
                </div>
              ) : null}
              {item.status === "done" && item.markup !== undefined ? <AnuvaadView markup={item.markup} literal={item.literal} onChange={(markup) => updateItem(item.key, { markup })} /> : null}
              {item.text !== undefined && item.status !== "reading" ? (
                <details className="vyDetails anOriginal">
                  <summary>Original text (as read from the page)</summary>
                  <p className="vyHint">If a word was misread, correct it here and press “Translate again”.</p>
                  <textarea
                    className="indic"
                    lang="sa"
                    value={item.text}
                    rows={Math.min(16, Math.max(4, Math.ceil(item.text.length / 70)))}
                    onChange={(e) => updateItem(item.key, { text: e.target.value })}
                  />
                  <button
                    type="button"
                    className="exSecondary"
                    disabled={item.status === "waiting" || item.status === "translating"}
                    onClick={() => void run([item.key])}
                  >
                    Translate again
                  </button>
                </details>
              ) : null}
            </article>
          ))}
        </section>
      ) : null}
    </main>
  );
}
