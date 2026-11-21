import Head from "next/head";
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import type { GetServerSideProps } from "next";
import { useRouter } from "next/router";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import { AppNav } from "@/components/AppNav";
import { AnuvaadView } from "@/components/AnuvaadView";
import { AnuvaadWords } from "@/components/AnuvaadWords";
import { AnuvaadPages } from "@/components/AnuvaadPages";
import { type BoxRow, parseBoxPages } from "@/lib/anuvaad/box";
import { addImagePage, fileType, readStream, saveBlob } from "@/lib/anuvaad/browser";
import type { LocatedPassage } from "@/lib/anuvaad/locate";
import { type BookText, bookGujaratiText, passageWords } from "@/lib/anuvaad/book-text";
import { gujaratiNumber, pageText } from "@/lib/anuvaad/pages";

// Anuvaad, two ways, chosen first: "Pages of a PDF" (components/AnuvaadPages:
// any PDF or page photos, pages picked by number, each page translated) and
// box files, below.
//
// Box files (the printed Sr | V.T | Granth | ShastraPath | … tables,
// scanned or photographed) go in; each page is read, each passage is given its
// Gujarati anuvaad in the form of Maharaj Saheb's pages (29 Oct 2026), shown
// here to read and correct, and exported as one PDF: the box pages as
// uploaded, then the anuvaad pages, ready to print.
//
// Nothing is kept on the server or in the browser: the page warns before it
// is left with work on it.

type Upload = {
  id: number;
  batch: number;
  name: string;
  type: string;
  bytes: ArrayBuffer;
  status: "waiting" | "reading" | "done" | "error";
  progress: string;
  error?: string;
  pages: string[];
};

type Row = BoxRow & {
  key: number;
  vishay: string;
  files: string[];
  /** "box1.pdf p. 2" for each page, counted within its own file. */
  where: string[];
  include: boolean;
  status: "idle" | "waiting" | "running" | "done" | "error";
  error?: string;
  markup?: string;
  literal?: string;
  /**
   * Where the passage is printed in the library (lib/anuvaad/locate): found,
   * null when it was looked for and not found, undefined while not yet looked for.
   */
  place?: LocatedPassage | null;
  /** The book's own Gujarati for the passage, cut from the pages around `place` (lib/anuvaad/book-text). */
  book?: BookText | null;
  finding?: boolean;
  /** What the anuvaad is made from; chosen by default as book → page → box. */
  source?: Source;
};

/**
 * Maharaj Saheb, 9-13 Nov 2026: the book's own Gujarati first, if the book has
 * it; else the AI translates our library page (not the box scan); the box's
 * own text only when the passage is not in the library.
 */
type Source = "book" | "page" | "box";

const defaultSource = (row: Row): Source => (row.book ? "book" : row.place ? "page" : "box");
const sourceOf = (row: Row): Source => row.source ?? defaultSource(row);

/** "પાનું ૧૮૦–૧૮૧" from the printed page numbers (PDF numbers when a page has none). */
function gujaratiPages(place: LocatedPassage, pdfPages: number[]) {
  const printed = pdfPages.map((n) => Number(place.pages.find((p) => p.page === n)?.printedPage) || n);
  const a = printed[0];
  const b = printed[printed.length - 1];
  return a === b ? `પાનું ${gujaratiNumber(a)}` : `પાનાં ${gujaratiNumber(a)}–${gujaratiNumber(b)}`;
}

/** Book name, number and pages, as the anuvaad's first line: every printed passage says where it is from. */
function whereFrom(place: LocatedPassage, pdfPages: number[]) {
  return `${place.granthName}${place.bookNumber ? ` (ગ્રંથ નં. ${place.bookNumber})` : ""}, ${gujaratiPages(place, pdfPages)}`;
}

/** The book's own Gujarati, the passage's part in bold, with where it is from. */
function bookMarkup(place: LocatedPassage, book: BookText) {
  return [`(પુસ્તકમાંથી: ${whereFrom(place, book.pages)})`, book.markup].join("\n");
}

/**
 * Of the places found, the one to use: the first whose pages hold the book's
 * own Gujarati for the passage, as long as the box does not name another
 * book found; else the first.
 */
function choosePlace(places: LocatedPassage[], row: Pick<Row, "granth" | "shastraPath">) {
  const words = passageWords(row.shastraPath);
  const withBook = places.map((place) => ({ place, book: bookGujaratiText(place.pages, place.page, row.granth, words) }));
  const named = places.some((p) => p.named);
  const pick = withBook.find((x) => x.book && (x.place.named || !named)) ?? withBook[0];
  return pick ? { place: pick.place, book: pick.book } : { place: null, book: null };
}

const ACCEPT = "application/pdf,image/jpeg,image/png";
const PARALLEL_ROWS = 2;

/** The uploaded files, then the anuvaad pages, as one PDF (made in the browser). */
async function mergePdf(uploads: Upload[], anuvaad: ArrayBuffer) {
  const { PDFDocument } = await import("pdf-lib");
  const out = await PDFDocument.create();
  out.setTitle("ગુજરાતી અનુવાદ");
  for (const upload of uploads) {
    if (upload.type === "application/pdf") {
      const src = await PDFDocument.load(upload.bytes, { ignoreEncryption: true });
      for (const page of await out.copyPages(src, src.getPageIndices())) out.addPage(page);
    } else {
      await addImagePage(out, upload.bytes, upload.type);
    }
  }
  const translated = await PDFDocument.load(anuvaad);
  for (const page of await out.copyPages(translated, translated.getPageIndices())) out.addPage(page);
  return out.save();
}

const SOURCE_LABEL: Record<Source, string> = {
  book: "the book's own Gujarati (no AI); the passage's part is in bold",
  page: "AI anuvaad of our library page (the book has no Gujarati there)",
  box: "AI anuvaad of the box text (the passage was not found in our library)",
};

/** "Download it with 4 pages before and after": fetched here, so the phone shows that it is coming and when it is saved. */
function NearbyPages({ place, first, last }: { place: LocatedPassage; first: number; last: number }) {
  const [state, setState] = useState<"" | "busy" | "done" | "error">("");
  async function get() {
    if (state === "busy") return;
    setState("busy");
    try {
      const res = await fetch(`/api/anuvaad/located-pdf?granth=${encodeURIComponent(place.granthKey)}&from=${first}&to=${last}`);
      if (!res.ok) throw new Error();
      saveBlob(await res.blob(), `${place.granthName} p${first}-${last}.pdf`.replace(/[\\/:*?"<>|]+/g, " "));
      setState("done");
    } catch {
      setState("error");
    }
  }
  return (
    <>
      <button type="button" className="vyLink" onClick={() => void get()} disabled={state === "busy"}>
        {state === "busy" ? "Getting the pages…" : `Download it with 4 pages before and after (PDF ${first}-${last})`}
      </button>
      {state === "done" ? <span className="vyHint"> Saved to your downloads.</span> : null}
      {state === "error" ? <span className="vyProblem"> The pages could not be fetched. Try again.</span> : null}
    </>
  );
}

/** Where a passage is in the library, its ±4 pages, and what the anuvaad is made from. */
function PlaceBox({ row, onFind, onSource }: { row: Row; onFind: () => void; onSource: (source: Source) => void }) {
  if (!row.shastraPath.trim()) return null;
  if (row.finding || row.place === undefined) {
    return <p className="anPlace vyHint">{row.finding ? "Finding this passage in our library…" : "Waiting to find this passage in our library…"}</p>;
  }
  const place = row.place;
  if (!place) {
    return (
      <div className="anPlace">
        <p className="vyHint">
          Not found in our library; the anuvaad is made from the box text.{" "}
          <button type="button" className="vyLink" onClick={onFind}>
            Look again
          </button>
        </p>
      </div>
    );
  }
  const book = row.book;
  const first = place.pages[0]?.page ?? place.page;
  const last = place.pages[place.pages.length - 1]?.page ?? place.page;
  const source = sourceOf(row);
  const pageName = (p: { page: number; printedPage: string | null }) => (p.printedPage ? `page ${p.printedPage} (PDF ${p.page})` : `PDF page ${p.page}`);
  return (
    <div className="anPlace">
      <p>
        <strong>In our library:</strong> <span className="indic">{place.granthName}</span>, {pageName(place)}
        {place.score < 0.6 ? <small> · a partial match, check it</small> : null}
      </p>
      <p className="anPlaceLinks">
        {place.pdfUrl ? (
          // only this page, cut from the book (16-17 Nov 2026: the whole book's PDF did not open on the phone)
          <a className="vyLink" href={`/api/anuvaad/located-pdf?granth=${encodeURIComponent(place.granthKey)}&from=${place.page}&to=${place.page}&inline=1`} target="_blank" rel="noreferrer">
            Open this page
          </a>
        ) : null}
        {place.pdfUrl ? <NearbyPages place={place} first={first} last={last} /> : null}
      </p>
      {book ? (
        <p className="vyHint">
          The book has its own Gujarati for it on {book.pages.length > 1 ? `PDF pages ${book.pages[0]}-${book.pages[book.pages.length - 1]}` : `PDF page ${book.pages[0]}`}
          {book.by === "gatha" ? " (found by the gatha number)" : ""}.
        </p>
      ) : (
        <p className="vyHint">The book has no Gujarati of its own for this passage in our library.</p>
      )}
      <fieldset className="anSource">
        <legend>Make the anuvaad from</legend>
        {book ? (
          <label>
            <input type="radio" checked={source === "book"} onChange={() => onSource("book")} /> The book&apos;s own Gujarati (no AI)
          </label>
        ) : null}
        <label>
          <input type="radio" checked={source === "page"} onChange={() => onSource("page")} /> AI anuvaad of our library page
        </label>
        <label>
          <input type="radio" checked={source === "box"} onChange={() => onSource("box")} /> AI anuvaad of the box text
        </label>
      </fieldset>
    </div>
  );
}

function BoxFlow({ onBack }: { onBack: () => void }) {
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [rows, setRows] = useState<Row[]>([]);
  const [view, setView] = useState<"upload" | "rows" | "results">("upload");
  const [reading, setReading] = useState(false);
  const [translating, setTranslating] = useState(false);
  const [exporting, setExporting] = useState<"" | "download" | "print">("");
  const [exportError, setExportError] = useState("");
  const [withPages, setWithPages] = useState(true);
  const [dragging, setDragging] = useState(false);
  const [confirmClear, setConfirmClear] = useState(false);
  // Pages with no box table (a cover, an anuvaad page already in the scan), by file.
  const [loose, setLoose] = useState<Array<{ file: string; page: number }>>([]);
  const [saved, setSaved] = useState("");
  const nextId = useRef(1);
  const nextBatch = useRef(1);
  const nextRow = useRef(1);
  const picker = useRef<HTMLInputElement | null>(null);
  // the rows as they are now: a lookup finishing after "Make the anuvaad" was pressed must still count
  const rowsRef = useRef<Row[]>([]);
  rowsRef.current = rows;
  const lookups = useRef(new Map<number, Promise<{ place: LocatedPassage | null; book: BookText | null }>>());

  const waiting = uploads.filter((u) => u.status === "waiting");
  const translated = rows.filter((r) => r.status === "done" && r.markup);
  const chosen = rows.filter((r) => r.include);

  useEffect(() => {
    if (!uploads.length && !rows.length) return;
    const warn = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [uploads.length, rows.length]);

  const updateUpload = (id: number, change: Partial<Upload>) => setUploads((list) => list.map((u) => (u.id === id ? { ...u, ...change } : u)));
  const updateRow = (key: number, change: Partial<Row>) => setRows((list) => list.map((r) => (r.key === key ? { ...r, ...change } : r)));

  async function addFiles(files: FileList | File[]) {
    const batch = nextBatch.current;
    const fresh: Upload[] = [];
    for (const file of Array.from(files)) {
      const type = fileType(file);
      if (!ACCEPT.split(",").includes(type)) continue;
      fresh.push({ id: nextId.current++, batch, name: file.name, type, bytes: await file.arrayBuffer(), status: "waiting", progress: "", pages: [] });
    }
    if (fresh.length) setUploads((list) => [...list, ...fresh]);
  }

  async function readOne(upload: Upload): Promise<string[] | null> {
    updateUpload(upload.id, { status: "reading", progress: "Sending", error: undefined });
    try {
      const res = await fetch("/api/anuvaad/ocr", { method: "POST", headers: { "Content-Type": upload.type }, body: upload.bytes });
      let pages: string[] | null = null;
      await readStream(res, (event) => {
        if (event.type === "progress") updateUpload(upload.id, { progress: `Read ${event.done} of ${event.total} pages` });
        else if (event.type === "done") pages = (event.pages as string[]) ?? [];
        else if (event.type === "error") updateUpload(upload.id, { status: "error", error: String(event.error ?? "The pages could not be read."), progress: "" });
      });
      if (pages) {
        updateUpload(upload.id, { status: "done", pages, progress: "" });
        return pages;
      }
      setUploads((list) => list.map((u) => (u.id === upload.id && u.status === "reading" ? { ...u, status: "error", error: "The connection closed before the pages were read.", progress: "" } : u)));
    } catch {
      updateUpload(upload.id, { status: "error", error: "Could not reach the server. Check the connection and try again.", progress: "" });
    }
    return null;
  }

  /** Reads the files not yet read, as one batch: a passage may run from one file into the next. */
  async function readPages() {
    if (reading || !waiting.length) return;
    setReading(true);
    const batch = [...waiting];
    nextBatch.current += 1;
    const pages: Array<{ page: number; markdown: string; file: string; own: number }> = [];
    let pageNo = uploads.filter((u) => u.status === "done").reduce((n, u) => n + u.pages.length, 0);
    for (const upload of batch) {
      const read = await readOne(upload);
      (read ?? []).forEach((markdown, i) => pages.push({ page: ++pageNo, markdown, file: upload.name, own: i + 1 }));
    }
    const parsed = parseBoxPages(pages);
    const fileOf = new Map(pages.map((p) => [p.page, p.file]));
    const ownOf = new Map(pages.map((p) => [p.page, p.own]));
    const fresh: Row[] = parsed.rows.map((r) => ({
      ...r,
      key: nextRow.current++,
      vishay: parsed.vishay,
      files: [...new Set(r.pages.map((p) => fileOf.get(p) ?? ""))].filter(Boolean),
      // "box1.pdf p. 1, 2 · box2.pdf p. 1"
      where: [...new Set(r.pages.map((p) => fileOf.get(p) ?? ""))].map(
        (file) => `${file} p. ${r.pages.filter((p) => fileOf.get(p) === file).map((p) => ownOf.get(p) ?? p).join(", ")}`
      ),
      include: r.shastraPath.length > 0,
      status: "idle",
    }));
    setRows((list) => [...list, ...fresh]);
    void findAll(fresh);
    setLoose((list) => [...list, ...parsed.loose.map((l) => ({ file: fileOf.get(l.page) ?? "", page: ownOf.get(l.page) ?? l.page }))]);
    setReading(false);
    if (fresh.length) setView("rows");
  }

  async function translateOne(given: Row) {
    updateRow(given.key, { status: "running", error: undefined });
    // Maharaj Saheb, 20 Nov 2026: "only 5% was taken from the books, the rest went straight to AI". The
    // anuvaad was started before each passage had been looked up in the library, and a passage not yet
    // looked up went to the AI with the box text. Now each one waits for its lookup.
    let row = rowsRef.current.find((r) => r.key === given.key) ?? given;
    if (row.shastraPath.trim() && (row.place === undefined || row.finding)) {
      const found = await findOne(row);
      row = { ...row, ...found };
    }
    const source = sourceOf(row);
    try {
      const place = row.place;
      const book = row.book;
      if (source === "book" && place && book) {
        updateRow(row.key, { status: "done", markup: bookMarkup(place, book), literal: "" });
        return;
      }
      if (source === "page" && place) {
        // our library page, with the pages on either side for context
        const at = place.pages.findIndex((p) => p.page === place.page);
        const res = await fetch("/api/anuvaad/page", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            text: pageText(place.pages[at]?.text ?? ""),
            before: pageText(place.pages[at - 1]?.text ?? ""),
            after: pageText(place.pages[at + 1]?.text ?? ""),
            source: `${place.granthName}, page ${place.printedPage ?? place.page}`,
          }),
        });
        const json = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(json.error || "The anuvaad could not be prepared.");
        const head = `(અમારી લાઇબ્રેરીના પાનાનો AI અનુવાદ: ${whereFrom(place, [place.page])})`;
        updateRow(row.key, { status: "done", markup: `${head}\n${String(json.markup ?? "")}`, literal: String(json.literal ?? "") });
        return;
      }
      const res = await fetch("/api/anuvaad/translate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vishay: row.vishay, granth: row.granth, passage: row.shastraPath }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || "The anuvaad could not be prepared.");
      const head = `(બોક્સના પાઠનો AI અનુવાદ${row.granth ? `: ${row.granth}` : ""})`;
      updateRow(row.key, { status: "done", markup: `${head}\n${String(json.markup ?? "")}`, literal: String(json.literal ?? "") });
    } catch (error) {
      updateRow(row.key, { status: "error", error: error instanceof Error ? error.message : "The anuvaad could not be prepared." });
    }
  }

  /** Looks each passage up in the library, one after another (about a second each). */
  async function findAll(list: Row[]) {
    for (const row of list) {
      if (!row.shastraPath.trim()) continue;
      await findOne(row);
    }
  }

  /** Looks the passage up (once at a time per row) and keeps where it is and the book's Gujarati for it. */
  function findOne(row: Row) {
    const running = lookups.current.get(row.key);
    if (running) return running;
    updateRow(row.key, { finding: true });
    const job = (async () => {
      try {
        const res = await fetch("/api/anuvaad/locate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ passage: row.shastraPath, granth: row.granth }),
        });
        const json = await res.json().catch(() => ({}));
        const places = res.ok && Array.isArray(json.places) ? (json.places as LocatedPassage[]) : [];
        return places.length ? choosePlace(places, row) : { place: null, book: null };
      } catch {
        return { place: null, book: null };
      }
    })();
    lookups.current.set(row.key, job);
    void job.then((found) => {
      lookups.current.delete(row.key);
      updateRow(row.key, { finding: false, ...found });
    });
    return job;
  }

  async function translate(list: Row[]) {
    if (translating || !list.length) return;
    setTranslating(true);
    setView("results");
    const queue = [...list];
    for (const r of queue) updateRow(r.key, { status: "waiting" });
    await Promise.all(
      Array.from({ length: Math.min(PARALLEL_ROWS, queue.length) }, async () => {
        for (let r = queue.shift(); r; r = queue.shift()) await translateOne(r);
      })
    );
    setTranslating(false);
  }

  async function makePdf(mode: "download" | "print") {
    const done = rows.filter((r) => r.include && r.status === "done" && r.markup);
    if (!done.length || exporting) return;
    setExporting(mode);
    setExportError("");
    setSaved("");
    // a print tab is opened now, while the click still counts, and filled when the PDF is ready
    const tab = mode === "print" ? window.open("", "_blank") : null;
    try {
      const res = await fetch("/api/anuvaad/pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sections: done.map((r) => ({ caption: [r.sr ? `Sr. ${r.sr}` : "", r.vishay, r.granth].filter(Boolean).join(" · "), markup: r.markup })),
        }),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error || `The PDF could not be made (${res.status}).`);
      }
      const anuvaad = await res.arrayBuffer();
      const used = withPages ? uploads.filter((u) => u.status === "done") : [];
      const bytes = used.length ? await mergePdf(used, anuvaad) : new Uint8Array(anuvaad);
      const blob = new Blob([bytes as BlobPart], { type: "application/pdf" });
      const vishay = done[0].vishay;
      const name = `${vishay ? `${vishay} ` : ""}anuvaad.pdf`;
      if (tab) tab.location.href = URL.createObjectURL(blob);
      else saveBlob(blob, name);
      const { PDFDocument } = await import("pdf-lib");
      const count = (await PDFDocument.load(bytes)).getPageCount();
      setSaved(tab ? `Opened in a new tab (${count} pages) — print it from there.` : `Saved “${name}” to your downloads (${count} pages${used.length ? ", box scans first" : ""}).`);
    } catch (error) {
      tab?.close();
      setExportError(error instanceof Error ? error.message : "The PDF could not be made.");
    } finally {
      setExporting("");
    }
  }

  function startAgain() {
    setUploads([]);
    setRows([]);
    setLoose([]);
    setConfirmClear(false);
    setView("upload");
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    if (e.dataTransfer.files?.length) void addFiles(e.dataTransfer.files);
  }

  const working = rows.filter((r) => r.status === "running" || r.status === "waiting").length;
  const pageCount = useMemo(() => uploads.reduce((n, u) => n + u.pages.length, 0), [uploads]);
  const viewFile = (name: string, page?: number) => {
    const upload = uploads.find((u) => u.name === name);
    if (!upload) return;
    const first = uploads.filter((u) => u.status === "done" && u.id < upload.id).reduce((n, u) => n + u.pages.length, 0);
    const url = URL.createObjectURL(new Blob([upload.bytes], { type: upload.type }));
    window.open(page && upload.type === "application/pdf" ? `${url}#page=${page - first}` : url, "_blank");
  };

  return (
    <main className="vyMain">
      {!uploads.length ? (
        <button type="button" className="exGhost anBack" onClick={onBack}>
          ‹ Back
        </button>
      ) : null}
      {uploads.length ? (
        <ol className="exTrail" aria-label="Where you are">
          <li>
            <button type="button" onClick={() => setView("upload")} aria-current={view === "upload" ? "step" : undefined}>
              Files ({uploads.length})
            </button>
          </li>
          <li>
            <button type="button" onClick={() => setView("rows")} aria-current={view === "rows" ? "step" : undefined} disabled={!rows.length}>
              Passages ({chosen.length})
            </button>
          </li>
          <li>
            <button type="button" onClick={() => setView("results")} aria-current={view === "results" ? "step" : undefined} disabled={!translated.length && !working}>
              Anuvaad ({translated.length}){working ? ` · ${working} working` : ""}
            </button>
          </li>
          <li className="exTrailEnd">
            {confirmClear ? (
              <span className="vyConfirm">
                Nothing is saved. Clear all?
                <button type="button" className="exGhost" onClick={startAgain} disabled={reading || translating}>
                  Clear
                </button>
                <button type="button" className="exGhost" onClick={() => setConfirmClear(false)}>
                  Keep
                </button>
              </span>
            ) : (
              <button type="button" className="exGhost" onClick={() => setConfirmClear(true)} disabled={reading || translating}>
                Start again
              </button>
            )}
          </li>
        </ol>
      ) : null}

      {view === "upload" ? (
        <section className="vyForm exStep">
          <h1 className="exQuestion">Which box files?</h1>
          <p className="vyLabel">
            <span>The printed box tables, as PDF scans or photos of the pages. Only three columns are read: Sr. No., Granth and ShastraPath. Several files are read as one box, in the order added.</span>
          </p>
          <div
            className={`anDrop${dragging ? " isOver" : ""}`}
            onDragOver={(e) => {
              e.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDrop={onDrop}
          >
            <input
              ref={picker}
              type="file"
              accept={ACCEPT}
              multiple
              hidden
              onChange={(e) => {
                if (e.target.files?.length) void addFiles(e.target.files);
                e.target.value = "";
              }}
            />
            <button type="button" className="vyLink" onClick={() => picker.current?.click()} disabled={reading}>
              Choose files
            </button>
            <span className="vyHint">or drop them here · PDF, JPG, PNG</span>
          </div>

          {uploads.length ? (
            <ul className="anFiles">
              {uploads.map((u) => (
                <li key={u.id} className={`is-${u.status}`}>
                  <span className="anFileName">{u.name}</span>
                  <span className="anFileState">
                    {u.status === "waiting" ? "Ready to read" : null}
                    {u.status === "reading" ? (
                      <>
                        <span className="chTyping" aria-hidden="true">
                          <span />
                          <span />
                          <span />
                        </span>
                        {u.progress || "Reading"}
                      </>
                    ) : null}
                    {u.status === "done" ? `${u.pages.length} page${u.pages.length === 1 ? "" : "s"} read` : null}
                    {u.status === "error" ? <span className="vyProblem">{u.error}</span> : null}
                  </span>
                  {u.status === "waiting" || u.status === "error" ? (
                    <button
                      type="button"
                      className="vyX"
                      aria-label={`Remove ${u.name}`}
                      onClick={() => setUploads((list) => list.filter((x) => x.id !== u.id))}
                      disabled={reading}
                    >
                      ×
                    </button>
                  ) : null}
                </li>
              ))}
            </ul>
          ) : null}

          {uploads.some((u) => u.status === "error") ? (
            <button
              type="button"
              className="exGhost"
              disabled={reading}
              onClick={() => setUploads((list) => list.map((u) => (u.status === "error" ? { ...u, status: "waiting", error: undefined } : u)))}
            >
              Try the failed files again
            </button>
          ) : null}

          <button type="button" className="vyGo exWide" onClick={readPages} disabled={reading || !waiting.length}>
            {reading ? "Reading the pages…" : waiting.length ? `Read ${waiting.length} file${waiting.length === 1 ? "" : "s"}` : rows.length ? "All files read" : "Read the pages"}
          </button>
          {!reading && uploads.length && !waiting.length && rows.length ? (
            <button type="button" className="exSecondary exWide" onClick={() => setView("rows")}>
              See the passages
            </button>
          ) : null}
          {!reading && uploads.some((u) => u.status === "done") && !rows.length ? (
            <p className="vyProblem">No box table was found on these pages. Check that they are the printed box tables.</p>
          ) : null}
        </section>
      ) : null}

      {view === "rows" ? (
        <section className="exStep anRows">
          <div className="vyBar anBar">
            <span className="vyBarCount">
              {chosen.length} of {rows.length} passages chosen · {pageCount} pages
            </span>
            <button type="button" className="vyGo" onClick={() => translate(chosen.filter((r) => r.status !== "done"))} disabled={translating || !chosen.some((r) => r.status !== "done")}>
              {translating ? "Translating…" : `Make the Gujarati anuvaad (${chosen.filter((r) => r.status !== "done").length})`}
            </button>
          </div>
          <p className="anNote">
            Check each passage against its page (<strong>View page</strong>) and correct the text if a line was missed or misread — the anuvaad is made from the text in these boxes. Untick a passage to leave it out.
          </p>
          {loose.length ? (
            <p className="anNote">
              Not translated (no box table on the page):{" "}
              {[...new Set(loose.map((l) => l.file))]
                .map((file) => {
                  const pages = loose.filter((l) => l.file === file).map((l) => l.page);
                  return `${file} p. ${pages.join(", ")}`;
                })
                .join(" · ")}
              . These pages still go into the exported PDF with the rest of the scan.
            </p>
          ) : null}
          {rows.map((row) => (
            <article key={row.key} className={`vyCard anRow${row.include ? "" : " isOff"}`}>
              <header className="vyCardHead">
                <label className="anPick">
                  <input type="checkbox" checked={row.include} onChange={(e) => updateRow(row.key, { include: e.target.checked })} />
                  <span>{row.sr ? `Sr. ${row.sr}` : "Sr. (not on the scan)"}</span>
                  {row.status === "done" ? <span className="vyTag isDone">translated</span> : null}
                </label>
                <button type="button" className="vyClose" aria-label="Remove this passage" onClick={() => setRows((list) => list.filter((r) => r.key !== row.key))}>
                  ×
                </button>
              </header>
              <label className="anField">
                <span>Granth</span>
                <input className="indic" value={row.granth} onChange={(e) => updateRow(row.key, { granth: e.target.value })} />
              </label>
              <label className="anField">
                <span>ShastraPath</span>
                <textarea
                  className="indic"
                  value={row.shastraPath}
                  rows={Math.min(14, Math.max(3, Math.ceil(row.shastraPath.length / 90)))}
                  onChange={(e) => updateRow(row.key, { shastraPath: e.target.value })}
                />
              </label>
              <div className="anRowFoot">
                <span className="vyHint">
                  {row.where.join(" · ")}
                </span>
                {row.files[0] ? (
                  <button type="button" className="vyLink" onClick={() => viewFile(row.files[0], row.pages[0])} title="Opens the scan in a new tab">
                    View page
                  </button>
                ) : null}
              </div>
              <PlaceBox row={row} onFind={() => void findOne(row)} onSource={(source) => updateRow(row.key, { source, status: row.status === "done" ? "idle" : row.status })} />
            </article>
          ))}
        </section>
      ) : null}

      {view === "results" ? (
        <section className="exStep anResults">
          <div className="vyBar anBar">
            <span className="vyBarCount">
              {translated.length} ready{working ? ` · ${working} working` : ""}
            </span>
            <label className="anCheck" title="Untick for the Gujarati pages alone">
              <input type="checkbox" checked={withPages} onChange={(e) => setWithPages(e.target.checked)} />
              Include the box scans
            </label>
            <button type="button" className="exSecondary" onClick={() => makePdf("print")} disabled={Boolean(exporting) || !translated.some((r) => r.include)}>
              {exporting === "print" ? "Making…" : "Print"}
            </button>
            <button type="button" className="vyGo" onClick={() => makePdf("download")} disabled={Boolean(exporting) || !translated.some((r) => r.include)}>
              {exporting === "download" ? "Making the PDF…" : "Export PDF"}
            </button>
          </div>
          {exportError ? <p className="exError" role="alert">{exportError}</p> : null}
          {saved && !exportError ? <p className="anSaved" role="status">{saved}</p> : null}

          {rows
            .filter((r) => r.include && r.status !== "idle")
            .map((row) => (
              <article key={row.key} className={`vyCard anResult is-${row.status}`}>
                <p className="anCardTag">
                  {row.sr ? `Sr. ${row.sr}` : "Sr. (not on the scan)"}
                  {row.granth ? <span className="indic"> · {row.granth}</span> : null}
                  <span> · {SOURCE_LABEL[sourceOf(row)]}</span>
                </p>
                {row.status === "waiting" || row.status === "running" ? (
                  <p className="vyStatus">
                    <span className="chTyping" aria-hidden="true">
                      <span />
                      <span />
                      <span />
                    </span>
                    {row.status === "waiting" ? "Waiting" : "Translating"}
                  </p>
                ) : null}
                {row.status === "error" ? (
                  <div className="vyErrorRow">
                    <p className="vyProblem">{row.error}</p>
                    <button type="button" className="vyLink" onClick={() => translate([row])} disabled={translating}>
                      Try again
                    </button>
                  </div>
                ) : null}
                {row.status === "done" && row.markup !== undefined ? (
                  <>
                    <AnuvaadView markup={row.markup} literal={row.literal} onChange={(markup) => updateRow(row.key, { markup })} />
                    <div className="anRowFoot">
                      <button type="button" className="exGhost" onClick={() => translate([row])} disabled={translating} title="Makes this passage's anuvaad again from the start; your corrections to it are replaced">
                        Make this anuvaad again
                      </button>
                    </div>
                  </>
                ) : null}
              </article>
            ))}
        </section>
      ) : null}
    </main>
  );
}

type Mode = "box" | "pages" | "words";

export default function AnuvaadPage() {
  const router = useRouter();
  // The way chosen lives in the address (?mode=pages or ?mode=box), so a link
  // can open it straight away. It replaces the address rather than adding to
  // the history: the phone's back button must not drop work on the page.
  const mode: Mode | null = router.query.mode === "box" || router.query.mode === "pages" || router.query.mode === "words" ? router.query.mode : null;

  // The action bar pins just under the page header, whose height changes with
  // the width (two rows of menu on a phone).
  useEffect(() => {
    const header = document.querySelector<HTMLElement>(".anShell > .chTop");
    if (!header) return;
    const set = () => document.documentElement.style.setProperty("--an-top", `${header.getBoundingClientRect().height}px`);
    set();
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(set);
    observer?.observe(header);
    return () => observer?.disconnect();
  }, []);

  const choose = (next: Mode | null) => {
    void router.replace({ pathname: "/anuvaad", query: next ? { mode: next } : {} }, undefined, { shallow: true });
  };

  return (
    <>
      <Head>
        <title>Anuvaad</title>
      </Head>
      <div className="vyShell anShell">
        <header className="chTop">
          <strong className="chBrand">Anuvaad</strong>
          <AppNav current="anuvaad" />
        </header>

        {mode === "box" ? <BoxFlow onBack={() => choose(null)} /> : null}
        {mode === "pages" ? <AnuvaadPages onBack={() => choose(null)} /> : null}
        {mode === "words" ? <AnuvaadWords onBack={() => choose(null)} /> : null}
        {mode === null ? (
          <main className="vyMain">
            <section className="vyForm exStep">
              <h1 className="exQuestion">What do you want to translate into Gujarati?</h1>
              <div className="exChoices anChoices">
                <button type="button" className="exChoice" onClick={() => choose("pages")}>
                  <strong>Pages of a PDF</strong>
                  <span>Any PDF (or photos of pages). You choose the page numbers; each page is translated.</span>
                </button>
                <button type="button" className="exChoice" onClick={() => choose("box")}>
                  <strong>Box files</strong>
                  <span>The printed box tables (Sr. No. · Granth · ShastraPath). Each passage is found in our books; the book&apos;s own Gujarati is used where it has one.</span>
                </button>
                <button type="button" className="exChoice" onClick={() => choose("words")}>
                  <strong>Saved words</strong>
                  <span>Maharaj Saheb&apos;s Gujarati for the words of the box files. Add words from a box and its anuvaad, or correct them.</span>
                </button>
              </div>
            </section>
          </main>
        ) : null}
      </div>
    </>
  );
}

export const getServerSideProps: GetServerSideProps = async (ctx) => {
  const session = await getServerSession(ctx.req, ctx.res, authOptions);
  if (!session?.user || session.user.status !== "approved") {
    return { redirect: { destination: `/auth/signin?callbackUrl=${encodeURIComponent(ctx.resolvedUrl || "/anuvaad")}`, permanent: false } };
  }
  return { props: {} };
};
