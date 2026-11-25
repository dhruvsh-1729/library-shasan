import { useEffect, useRef, useState, type DragEvent } from "react";
import { AnuvaadView } from "@/components/AnuvaadView";
import { fileType, readStream, saveBlob } from "@/lib/anuvaad/browser";
import { toJpeg } from "@/components/PhotoText";
import { type ApcEntry, apcEntryMarkup, parseApcPages } from "@/lib/anuvaad/apc";
import type { LocatedPassage } from "@/lib/anuvaad/locate";
import type { BookText } from "@/lib/anuvaad/book-text";
import { bookMarkup, choosePlace } from "@/lib/anuvaad/book-place";

// "APC files (no AI)" on /anuvaad (Maharaj Saheb and Diksha, 17-18 Nov 2026:
// the society does not want the Gitarth Ganga code files to touch AI; "the
// system splits them, finds the pages and prints the PDF pages").
//
//   1. The APC PDF's own text is read (a scan, which has none, is read page by
//      page as the box files are: reading only, nothing is translated).
//   2. Each (क्र.-N) entry keeps the granth, the adhyay and the śāstra text
//      (lib/anuvaad/apc); the rest is left out.
//   3. Each entry is found in our books; where the book has its own Gujarati,
//      it is shown (its part in bold).
//   4. One PDF: each entry (granth, adhyay, the text framed, the book's
//      Gujarati), then the library page(s) it is on, cut from our book.
//
// No AI writes anything here. Nothing is kept on the server.

type Entry = ApcEntry & {
  key: number;
  include: boolean;
  /** undefined: not looked up yet; null: not found in our library. */
  place?: LocatedPassage | null;
  book?: BookText | null;
  finding?: boolean;
};

const PARALLEL_LOOKUPS = 2;
const pdfName = (name: string) => name.replace(/\.[a-z0-9]+$/i, "").replace(/[\\/:*?"<>|]+/g, " ").trim() || "APC";

/** The PDF's own text, page by page, or null when it has none worth reading (a scan) or it reads badly. */
async function textLayer(bytes: ArrayBuffer): Promise<string[] | null> {
  try {
    const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
    pdfjs.GlobalWorkerOptions.workerSrc = `https://unpkg.com/pdfjs-dist@${pdfjs.version}/build/pdf.worker.min.mjs`;
    const doc = await pdfjs.getDocument({ data: new Uint8Array(bytes.slice(0)) }).promise;
    const pages: string[] = [];
    for (let n = 1; n <= doc.numPages; n += 1) {
      const page = await doc.getPage(n);
      const content = await page.getTextContent();
      // the items into lines by their height on the page, top to bottom, left to right
      const rows = new Map<number, Array<{ x: number; s: string }>>();
      for (const item of content.items) {
        if (!("str" in item) || !item.str) continue;
        const y = Math.round(item.transform[5] / 3) * 3;
        const row = rows.get(y) ?? [];
        row.push({ x: item.transform[4], s: item.str });
        rows.set(y, row);
      }
      const lines = [...rows.entries()]
        .sort((a, b) => b[0] - a[0])
        .map(([, row]) => row.sort((a, b) => a.x - b.x).map((r) => r.s).join(" ").replace(/\s+/g, " ").trim());
      pages.push(lines.join("\n"));
    }
    const all = pages.join("\n");
    const devanagari = all.match(/[ऀ-ॿ]/g)?.length ?? 0;
    // a text layer whose vowel signs come before their letters ("ि क") is not Unicode as printed
    const broken = (all.match(/(?:^|\s)[ा-ौ]/gm)?.length ?? 0) > devanagari * 0.01;
    if (devanagari < 50 * doc.numPages * 0.2 || broken) return null;
    return pages;
  } catch {
    return null;
  }
}

export function AnuvaadApc({ onBack }: { onBack: () => void }) {
  const [file, setFile] = useState<{ name: string; type: string; bytes: ArrayBuffer } | null>(null);
  const [reading, setReading] = useState("");
  const [readError, setReadError] = useState("");
  const [entries, setEntries] = useState<Entry[]>([]);
  const [nearby, setNearby] = useState(false);
  const [exporting, setExporting] = useState("");
  const [exportError, setExportError] = useState("");
  const [saved, setSaved] = useState("");
  const [dragging, setDragging] = useState(false);
  const picker = useRef<HTMLInputElement | null>(null);
  const entriesRef = useRef<Entry[]>([]);

  const update = (key: number, change: Partial<Entry>) => {
    entriesRef.current = entriesRef.current.map((e) => (e.key === key ? { ...e, ...change } : e));
    setEntries(entriesRef.current);
  };

  useEffect(() => {
    if (!file) return;
    const warn = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [file]);

  async function take(list: FileList | File[]) {
    const f = Array.from(list).find((x) => ["application/pdf", "image/jpeg", "image/png"].includes(fileType(x)));
    if (!f) {
      setReadError("Choose the APC file as a PDF (or a photo of a page).");
      return;
    }
    const next = { name: f.name, type: fileType(f), bytes: await f.arrayBuffer() };
    setFile(next);
    setEntries([]);
    entriesRef.current = [];
    setSaved("");
    await read(next);
  }

  async function read(f: { name: string; type: string; bytes: ArrayBuffer }) {
    setReadError("");
    setReading("Reading the file's own text…");
    let pages = f.type === "application/pdf" ? await textLayer(f.bytes) : null;
    let parsed = pages ? parseApcPages(pages.map((text, i) => ({ page: i + 1, text }))) : [];
    if (!parsed.length) {
      // a scan: its pages are read as the box files are (reading only; nothing is translated)
      setReading("This copy is a scan: reading its pages…");
      try {
        const ocr = async (body: Blob | ArrayBuffer, type: string) => {
          const res = await fetch("/api/anuvaad/ocr", { method: "POST", headers: { "Content-Type": type }, body });
          let problem = "";
          let got: string[] | null = null;
          await readStream(res, (event) => {
            if (event.type === "progress") setReading(`This copy is a scan: read ${event.done} of ${event.total} pages…`);
            else if (event.type === "done") got = (event.pages as string[]) ?? [];
            else if (event.type === "error") problem = String(event.error ?? "The pages could not be read.");
          });
          if (!got) throw new Error(problem || "The connection closed before the pages were read.");
          return got as string[];
        };
        if (f.type === "application/pdf") {
          pages = await ocr(f.bytes, f.type);
          parsed = parseApcPages(pages.map((text, i) => ({ page: i + 1, text })));
        } else {
          // Diksha, 24-25 Nov 2026: a phone photo of an APC page gave "No APC entries". The camera stores
          // the photo on its side with a turn flag the reader ignored: the photo is turned upright here,
          // and also read a quarter turn either way; the reading with the most entries is kept.
          setReading("Reading the photo…");
          const photo = new Blob([f.bytes], { type: f.type });
          const tries = await Promise.all(
            [0, 1, 3].map(async (turns) => {
              try {
                const got = await ocr(await toJpeg(photo, turns), "image/jpeg");
                return { got, entries: parseApcPages(got.map((text, i) => ({ page: i + 1, text }))) };
              } catch {
                return null;
              }
            })
          );
          const best = tries.filter((t) => t !== null).sort((a, b) => b!.entries.length - a!.entries.length)[0];
          if (!best) throw new Error("The photo could not be read. Please try again.");
          pages = best.got;
          parsed = best.entries;
        }
      } catch (error) {
        setReading("");
        setReadError(error instanceof Error ? error.message : "The pages could not be read.");
        return;
      }
    }
    setReading("");
    if (!parsed.length) {
      setReadError("No APC entries ((क्र.-1), ग्रंथ :-, Adhyay :-) were found in this file.");
      return;
    }
    const fresh: Entry[] = parsed.map((e, i) => ({ ...e, key: i + 1, include: true }));
    entriesRef.current = fresh;
    setEntries(fresh);
    void findAll(fresh);
  }

  async function findOne(entry: Entry) {
    update(entry.key, { finding: true });
    try {
      const res = await fetch("/api/anuvaad/locate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ passage: entry.text, granth: entry.granth }),
      });
      const json = await res.json().catch(() => ({}));
      const places = res.ok && Array.isArray(json.places) ? (json.places as LocatedPassage[]) : [];
      const found = places.length ? choosePlace(places, { granth: [entry.granth, entry.adhyay].filter(Boolean).join(", "), shastraPath: entry.text }) : { place: null, book: null };
      update(entry.key, { finding: false, ...found });
    } catch {
      update(entry.key, { finding: false, place: null, book: null });
    }
  }

  async function findAll(list: Entry[]) {
    const queue = [...list];
    await Promise.all(
      Array.from({ length: Math.min(PARALLEL_LOOKUPS, queue.length) }, async () => {
        for (let e = queue.shift(); e; e = queue.shift()) await findOne(e);
      })
    );
  }

  /** The library pages to print after an entry: where its text and the book's Gujarati are (±4 when asked), at most 15. */
  function pagesFor(e: Entry): [number, number] | null {
    const place = e.place;
    if (!place?.pdfUrl) return null;
    const own = [place.page, ...(e.book?.pages ?? [])];
    let from = Math.min(...own);
    let to = Math.max(...own);
    if (nearby) {
      from = Math.max(1, from - 4);
      to += 4;
    }
    return [from, Math.min(to, from + 14)];
  }

  async function exportPdf() {
    const chosen = entries.filter((e) => e.include && e.text.trim());
    if (!chosen.length || exporting) return;
    setExporting("Making the entry pages…");
    setExportError("");
    setSaved("");
    try {
      const res = await fetch("/api/anuvaad/pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          numbers: false,
          sections: chosen.map((e) => ({
            caption: e.no ? `(क्र.-${e.no})` : "",
            markup: [apcEntryMarkup(e), e.place && e.book ? bookMarkup(e.place, e.book) : ""].filter(Boolean).join("\n"),
          })),
        }),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error || `The PDF could not be made (${res.status}).`);
      }
      const starts = (res.headers.get("X-Section-Starts") ?? "").split(",").filter(Boolean).map(Number);
      if (starts.length !== chosen.length) throw new Error("The PDF could not be put together. Please try again.");
      const { PDFDocument } = await import("pdf-lib");
      const text = await PDFDocument.load(await res.arrayBuffer());
      const out = await PDFDocument.create();
      out.setTitle(`${pdfName(file?.name ?? "APC")} - APC`);
      let cut = 0;
      for (let k = 0; k < chosen.length; k += 1) {
        const e = chosen[k];
        const own = Array.from({ length: (starts[k + 1] ?? text.getPageCount()) - starts[k] }, (_, j) => starts[k] + j);
        for (const page of await out.copyPages(text, own)) out.addPage(page);
        const range = pagesFor(e);
        if (!range || !e.place) continue;
        setExporting(`Cutting the library pages (${k + 1} of ${chosen.length})…`);
        const pages = await fetch(`/api/anuvaad/located-pdf?granth=${encodeURIComponent(e.place.granthKey)}&from=${range[0]}&to=${range[1]}`);
        if (!pages.ok) continue;
        const src = await PDFDocument.load(await pages.arrayBuffer(), { ignoreEncryption: true });
        for (const page of await out.copyPages(src, src.getPageIndices())) out.addPage(page);
        cut += 1;
      }
      const bytes = await out.save();
      const name = `${pdfName(file?.name ?? "APC")} - granth pages.pdf`;
      saveBlob(new Blob([bytes as BlobPart], { type: "application/pdf" }), name);
      setSaved(`Saved “${name}” to your downloads (${chosen.length} entries, ${cut} with their library pages, ${out.getPageCount()} pages).`);
    } catch (error) {
      setExportError(error instanceof Error ? error.message : "The PDF could not be made.");
    } finally {
      setExporting("");
    }
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    if (e.dataTransfer.files?.length) void take(e.dataTransfer.files);
  }

  const found = entries.filter((e) => e.place).length;
  const withBook = entries.filter((e) => e.book).length;
  const looking = entries.filter((e) => e.finding || e.place === undefined).length;

  return (
    <main className="vyMain">
      <button type="button" className="exGhost anBack" onClick={onBack} disabled={Boolean(reading || exporting)}>
        ‹ Back
      </button>
      {!entries.length ? (
        <section className="vyForm exStep">
          <h1 className="exQuestion">Which APC file?</h1>
          <p className="vyLabel">
            <span>
              A Gitarth Ganga code file (APC). <strong>No AI is used.</strong> For each (क्र.) entry only the granth, the adhyay and the śāstra text are kept; it is found in our books and printed with
              the book&apos;s own pages (and the Shrirang Library Gujarati — the book&apos;s printed translation — where it has one).
            </span>
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
              accept="application/pdf,image/jpeg,image/png"
              hidden
              onChange={(e) => {
                const picked = Array.from(e.target.files ?? []);
                e.target.value = "";
                if (picked.length) void take(picked);
              }}
            />
            <button type="button" className="vyLink" onClick={() => picker.current?.click()} disabled={Boolean(reading)}>
              Choose the APC file
            </button>
            <span className="vyHint">or drop it here · PDF (or a photo of a page)</span>
          </div>
          {reading ? (
            <p className="vyStatus">
              <span className="chTyping" aria-hidden="true">
                <span />
                <span />
                <span />
              </span>
              {reading}
            </p>
          ) : null}
          {readError ? <p className="vyProblem">{readError}</p> : null}
        </section>
      ) : (
        <section className="exStep anRows">
          <div className="vyBar anBar">
            <span className="vyBarCount">
              {entries.filter((e) => e.include).length} of {entries.length} entries · {looking ? `finding ${looking} in our books…` : `${found} found in our books, ${withBook} with the book's own Gujarati`}
            </span>
            <label className="anCheck">
              <input type="checkbox" checked={nearby} onChange={(e) => setNearby(e.target.checked)} />
              With 4 pages before and after
            </label>
            <button type="button" className="vyGo" onClick={() => void exportPdf()} disabled={Boolean(exporting) || looking > 0 || !entries.some((e) => e.include)}>
              {exporting ? "Making the PDF…" : "Download the PDF"}
            </button>
          </div>
          {exporting ? <p className="vyHint" role="status">{exporting}</p> : null}
          {exportError ? <p className="exError" role="alert">{exportError}</p> : null}
          {saved && !exportError ? <p className="anSaved" role="status">{saved}</p> : null}
          <p className="anNote">
            From <strong>{file?.name}</strong>. No AI is used: the text is the file&apos;s own, the Gujarati is the book&apos;s own. Check each entry&apos;s text and untick any to leave out. The PDF gives each entry,
            then the page(s) of our book it is on.
          </p>
          {entries.map((e) => (
            <article key={e.key} className={`vyCard anRow${e.include ? "" : " isOff"}`}>
              <header className="vyCardHead">
                <label className="anPick">
                  <input type="checkbox" checked={e.include} onChange={(ev) => update(e.key, { include: ev.target.checked })} />
                  <span>{e.no ? `(क्र.-${e.no})` : "(क्र. not on the page)"}</span>
                </label>
              </header>
              <label className="anField">
                <span>Granth</span>
                <input className="indic" value={e.granth} onChange={(ev) => update(e.key, { granth: ev.target.value })} />
              </label>
              <label className="anField">
                <span>Adhyay</span>
                <input className="indic" value={e.adhyay} onChange={(ev) => update(e.key, { adhyay: ev.target.value })} />
              </label>
              <label className="anField">
                <span>Śāstra text</span>
                <textarea className="indic" lang="sa" value={e.text} rows={Math.min(12, Math.max(3, Math.ceil(e.text.length / 90)))} onChange={(ev) => update(e.key, { text: ev.target.value })} />
              </label>
              <div className="anPlace">
                {e.finding || e.place === undefined ? (
                  <p className="vyHint">Finding it in our books…</p>
                ) : e.place ? (
                  <>
                    <p>
                      <strong>In our library:</strong> <span className="indic">{e.place.granthName}</span>
                      {e.place.bookNumber ? ` (book ${e.place.bookNumber})` : ""}, {e.place.printedPage ? `page ${e.place.printedPage} (PDF ${e.place.page})` : `PDF page ${e.place.page}`}
                      {e.place.score < 0.6 ? <small> · a partial match, check it</small> : null}
                    </p>
                    <p className="anPlaceLinks">
                      {e.place.pdfUrl ? (
                        <a className="vyLink" href={`/api/anuvaad/located-pdf?granth=${encodeURIComponent(e.place.granthKey)}&from=${e.place.page}&to=${e.place.page}&inline=1`} target="_blank" rel="noreferrer">
                          Open this page
                        </a>
                      ) : (
                        <span className="vyHint">This book has no PDF linked, so its page cannot be printed.</span>
                      )}
                      <button type="button" className="vyLink" onClick={() => void findOne(e)}>
                        Look again
                      </button>
                    </p>
                    {e.book ? (
                      <AnuvaadView markup={bookMarkup(e.place, e.book)} />
                    ) : (
                      <p className="vyHint">This book has no Gujarati of its own here; its page is printed.</p>
                    )}
                  </>
                ) : (
                  <p className="vyHint">
                    Not found in our books.{" "}
                    <button type="button" className="vyLink" onClick={() => void findOne(e)}>
                      Look again
                    </button>
                  </p>
                )}
              </div>
            </article>
          ))}
        </section>
      )}
    </main>
  );
}
