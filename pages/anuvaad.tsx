import Head from "next/head";
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import { AppNav } from "@/components/AppNav";
import { AnuvaadView } from "@/components/AnuvaadView";
import { type BoxRow, parseBoxPages } from "@/lib/anuvaad/box";

// Anuvaad: box files (the printed Sr | V.T | Granth | ShastraPath | … tables,
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
  include: boolean;
  status: "idle" | "waiting" | "running" | "done" | "error";
  error?: string;
  markup?: string;
  literal?: string;
};

const ACCEPT = "application/pdf,image/jpeg,image/png";
const PARALLEL_ROWS = 2;

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

function fileType(file: File) {
  if (file.type) return file.type;
  const ext = file.name.toLowerCase().split(".").pop();
  return ext === "pdf" ? "application/pdf" : ext === "png" ? "image/png" : ext === "jpg" || ext === "jpeg" ? "image/jpeg" : "";
}

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
      const image = upload.type === "image/png" ? await out.embedPng(upload.bytes) : await out.embedJpg(upload.bytes);
      // a photo goes on an A4 page (turned if it is wider than tall), fitted with a small margin
      const landscape = image.width > image.height;
      const [w, h] = landscape ? [841.89, 595.28] : [595.28, 841.89];
      const scale = Math.min((w - 24) / image.width, (h - 24) / image.height);
      const page = out.addPage([w, h]);
      page.drawImage(image, { x: (w - image.width * scale) / 2, y: (h - image.height * scale) / 2, width: image.width * scale, height: image.height * scale });
    }
  }
  const translated = await PDFDocument.load(anuvaad);
  for (const page of await out.copyPages(translated, translated.getPageIndices())) out.addPage(page);
  return out.save();
}

export default function AnuvaadPage() {
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
  const [loose, setLoose] = useState<number[]>([]);
  const nextId = useRef(1);
  const nextBatch = useRef(1);
  const nextRow = useRef(1);
  const picker = useRef<HTMLInputElement | null>(null);

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
    const pages: Array<{ page: number; markdown: string; file: string }> = [];
    let pageNo = uploads.filter((u) => u.status === "done").reduce((n, u) => n + u.pages.length, 0);
    for (const upload of batch) {
      const read = await readOne(upload);
      for (const markdown of read ?? []) pages.push({ page: ++pageNo, markdown, file: upload.name });
    }
    const parsed = parseBoxPages(pages);
    const fileOf = new Map(pages.map((p) => [p.page, p.file]));
    const fresh: Row[] = parsed.rows.map((r) => ({
      ...r,
      key: nextRow.current++,
      vishay: parsed.vishay,
      files: [...new Set(r.pages.map((p) => fileOf.get(p) ?? ""))].filter(Boolean),
      include: r.shastraPath.length > 0,
      status: "idle",
    }));
    setRows((list) => [...list, ...fresh]);
    setLoose((list) => [...list, ...parsed.loose.map((l) => l.page)]);
    setReading(false);
    if (fresh.length) setView("rows");
  }

  async function translateOne(row: Row) {
    updateRow(row.key, { status: "running", error: undefined });
    try {
      const res = await fetch("/api/anuvaad/translate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vishay: row.vishay, granth: row.granth, passage: row.shastraPath }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error || "The anuvaad could not be prepared.");
      updateRow(row.key, { status: "done", markup: String(json.markup ?? ""), literal: String(json.literal ?? "") });
    } catch (error) {
      updateRow(row.key, { status: "error", error: error instanceof Error ? error.message : "The anuvaad could not be prepared." });
    }
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
    // a print tab is opened now, while the click still counts, and filled when the PDF is ready
    const tab = mode === "print" ? window.open("", "_blank") : null;
    try {
      const res = await fetch("/api/anuvaad/pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sections: done.map((r) => ({ caption: [r.sr, r.vishay].filter(Boolean).join(" · "), markup: r.markup })),
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
      if (tab) tab.location.href = URL.createObjectURL(blob);
      else saveBlob(blob, `${vishay ? `${vishay} ` : ""}anuvaad.pdf`);
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
    <>
      <Head>
        <title>Anuvaad</title>
      </Head>
      <div className="vyShell anShell">
        <header className="chTop">
          <strong className="chBrand">Anuvaad</strong>
          <AppNav current="anuvaad" />
        </header>

        <main className="vyMain">
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
                  ગુજરાતી અનુવાદ ({translated.length}){working ? ` · ${working} working` : ""}
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
                <span>The printed box tables (Sr · V.T · Granth · ShastraPath), as PDF scans or photos of the pages. Several files are read as one box, in the order added.</span>
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
              <p className="vyHint">
                Check each passage against its page before translating: a line the reading missed is missing from the anuvaad too. Untick what should not be translated.
              </p>
              {loose.length ? (
                <p className="vyHint">
                  {loose.length === 1 ? "Page" : "Pages"} {loose.join(", ")} had no box table and {loose.length === 1 ? "is" : "are"} left out of the passages (still in the PDF).
                </p>
              ) : null}
              {rows.map((row) => (
                <article key={row.key} className={`vyCard anRow${row.include ? "" : " isOff"}`}>
                  <header className="vyCardHead">
                    <label className="anPick">
                      <input type="checkbox" checked={row.include} onChange={(e) => updateRow(row.key, { include: e.target.checked })} />
                      <span>{row.sr ? `Sr. ${row.sr}` : "Sr. —"}</span>
                      {row.vt ? <span className="vyTag indic">{row.vt}</span> : null}
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
                      {row.inRem ? <>In.Rem {row.inRem} · </> : null}
                      {row.files.join(", ")} · page{row.pages.length > 1 ? "s" : ""} {row.pages.join(", ")}
                    </span>
                    {row.files[0] ? (
                      <button type="button" className="vyLink" onClick={() => viewFile(row.files[0], row.pages[0])}>
                        View page
                      </button>
                    ) : null}
                  </div>
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
                <label className="anCheck">
                  <input type="checkbox" checked={withPages} onChange={(e) => setWithPages(e.target.checked)} />
                  Box pages first
                </label>
                <button type="button" className="exGhost" onClick={() => makePdf("print")} disabled={Boolean(exporting) || !translated.some((r) => r.include)}>
                  {exporting === "print" ? "Making…" : "Open to print"}
                </button>
                <button type="button" className="vyGo" onClick={() => makePdf("download")} disabled={Boolean(exporting) || !translated.some((r) => r.include)}>
                  {exporting === "download" ? "Making the PDF…" : "Export PDF"}
                </button>
              </div>
              {exportError ? <p className="exError" role="alert">{exportError}</p> : null}

              {rows
                .filter((r) => r.include && r.status !== "idle")
                .map((row) => (
                  <article key={row.key} className={`vyCard anResult is-${row.status}`}>
                    <header className="vyCardHead">
                      <h2>
                        {row.sr ? <span className="vyNumber">{row.sr}</span> : null}
                        <span className="indic anGranth">{row.granth || "—"}</span>
                      </h2>
                    </header>
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
                          <button type="button" className="exGhost" onClick={() => translate([row])} disabled={translating}>
                            Translate again
                          </button>
                        </div>
                      </>
                    ) : null}
                  </article>
                ))}
            </section>
          ) : null}
        </main>
      </div>
    </>
  );
}

export const getServerSideProps: GetServerSideProps = async (ctx) => {
  const session = await getServerSession(ctx.req, ctx.res, authOptions);
  if (!session?.user || session.user.status !== "approved") {
    return { redirect: { destination: `/auth/signin?callbackUrl=${encodeURIComponent("/anuvaad")}`, permanent: false } };
  }
  return { props: {} };
};
