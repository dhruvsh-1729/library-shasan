import { useMemo, useRef, useState, type DragEvent } from "react";
import { toJpeg } from "@/components/PhotoText";
import { fileType, readStream, saveBlob } from "@/lib/anuvaad/browser";
import { type BoxRow, parseBoxPages } from "@/lib/anuvaad/box";
import { BOX_TEXT_WIDTHS, boxTextFileName, boxTextRows } from "@/lib/anuvaad/box-text";
import { docxTable } from "@/lib/docx-lite";

// Anuvaad > "Box text only" (Maharaj Saheb, 25 Nov 2026): a box file in, its
// Sr. no., Granth and ShastraPath out — as a PDF with no header, footer or
// column names, or as a Word or Excel file to open and edit. No AI. A box PDF
// printed from the box software is read from its own text in a second; a
// scan or a photo is read page by page.

const ACCEPT = "application/pdf,image/jpeg,image/png";

type Row = BoxRow & { key: number; include: boolean };
type Upload = { name: string; type: string; bytes: ArrayBuffer; state: "waiting" | "reading" | "done" | "error"; note: string };

export function AnuvaadBoxText({ onBack }: { onBack: () => void }) {
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [rows, setRows] = useState<Row[]>([]);
  const [reading, setReading] = useState(false);
  const [error, setError] = useState("");
  const [bands, setBands] = useState(true);
  const [busy, setBusy] = useState<"" | "pdf" | "word" | "excel">("");
  const [saved, setSaved] = useState("");
  const [dragging, setDragging] = useState(false);
  const picker = useRef<HTMLInputElement | null>(null);
  const nextKey = useRef(1);

  const chosen = rows.filter((r) => r.include);
  const vishays = useMemo(() => {
    const out = new Map<string, Row[]>();
    for (const r of rows) out.set(r.vishay || "", [...(out.get(r.vishay || "") ?? []), r]);
    return [...out.entries()];
  }, [rows]);

  async function addFiles(files: FileList | File[]) {
    const fresh: Upload[] = [];
    for (const file of Array.from(files)) {
      const type = fileType(file);
      if (!ACCEPT.split(",").includes(type)) continue;
      const upright = type === "application/pdf" ? null : await toJpeg(file).catch(() => null);
      fresh.push({ name: file.name, type: upright ? "image/jpeg" : type, bytes: await (upright ?? file).arrayBuffer(), state: "waiting", note: "" });
    }
    if (fresh.length) setUploads((list) => [...list, ...fresh]);
  }

  async function read() {
    if (reading) return;
    setReading(true);
    setError("");
    const pages: Array<{ page: number; markdown: string }> = [];
    let n = 0;
    const list = uploads.filter((u) => u.state !== "done");
    for (const upload of list) {
      const set = (change: Partial<Upload>) => setUploads((all) => all.map((u) => (u === upload || u.name === upload.name ? { ...u, ...change } : u)));
      set({ state: "reading", note: "Reading" });
      try {
        const res = await fetch("/api/anuvaad/ocr?box=1", { method: "POST", headers: { "Content-Type": upload.type }, body: upload.bytes });
        let got: string[] | null = null;
        let fast = false;
        await readStream(res, (event) => {
          if (event.type === "progress") set({ note: `Read ${event.done} of ${event.total} pages` });
          else if (event.type === "done") {
            got = (event.pages as string[]) ?? [];
            fast = Boolean(event.textLayer);
          } else if (event.type === "error") set({ state: "error", note: String(event.error ?? "The pages could not be read.") });
        });
        if (got) {
          const read: string[] = got;
          read.forEach((markdown) => pages.push({ page: ++n, markdown }));
          set({ state: "done", note: `${read.length} pages read${fast ? " (from the PDF's own text)" : ""}` });
        }
      } catch {
        set({ state: "error", note: "Could not reach the server. Check the connection and try again." });
      }
    }
    const parsed = parseBoxPages(pages);
    const fresh = parsed.rows.map((r) => ({ ...r, vishay: r.vishay || parsed.vishay, key: nextKey.current++, include: true }));
    setRows((all) => [...all, ...fresh]);
    if (!fresh.length && pages.length) setError("No box table (Sr. | Granth | ShastraPath) was found in these pages.");
    setReading(false);
  }

  const exportRows = () => boxTextRows(chosen, bands);

  async function download(kind: "pdf" | "word" | "excel") {
    if (!chosen.length || busy) return;
    setBusy(kind);
    setSaved("");
    setError("");
    try {
      if (kind === "pdf") {
        const res = await fetch("/api/anuvaad/box-text-pdf", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: boxTextFileName(chosen, "pdf").replace(/\.pdf$/, ""), rows: exportRows() }),
        });
        if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || "The PDF could not be made.");
        const name = boxTextFileName(chosen, "pdf");
        saveBlob(await res.blob(), name);
        setSaved(`Saved “${name}”.`);
      } else if (kind === "word") {
        const bytes = docxTable(exportRows(), BOX_TEXT_WIDTHS);
        const name = boxTextFileName(chosen, "docx");
        saveBlob(new Blob([bytes as BlobPart], { type: "application/vnd.openxmlformats-officedocument.wordprocessingml.document" }), name);
        setSaved(`Saved “${name}”.`);
      } else {
        const XLSX = await import("xlsx");
        // a sheet has no merged band: the vishay goes in a column of its own instead
        const sheet = XLSX.utils.aoa_to_sheet(chosen.map((r) => [r.sr, r.granth, r.shastraPath, ...(bands ? [r.vishay ?? ""] : [])]));
        sheet["!cols"] = [{ wch: 6 }, { wch: 40 }, { wch: 120 }, { wch: 28 }];
        const book = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(book, sheet, "Box text");
        const out = XLSX.write(book, { type: "array", bookType: "xlsx" }) as ArrayBuffer;
        const name = boxTextFileName(chosen, "xlsx");
        saveBlob(new Blob([out], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" }), name);
        setSaved(`Saved “${name}”.`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "The file could not be made.");
    } finally {
      setBusy("");
    }
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    if (e.dataTransfer.files?.length) void addFiles(e.dataTransfer.files);
  }

  const waiting = uploads.filter((u) => u.state === "waiting" || u.state === "error");

  return (
    <main className="vyMain">
      <button type="button" className="exGhost anBack" onClick={onBack}>
        ‹ Back
      </button>
      <section className="vyForm exStep">
        <h1 className="exQuestion">Box text only</h1>
        <p className="vyLabel">
          <span>
            From the box file, only <strong>Sr. no., Granth and ShastraPath</strong> — as a PDF with no header, footer or column names, or as a Word / Excel file to open and edit. <strong>No AI is used.</strong> A box PDF printed from the box software is read in a few seconds.
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
            accept={ACCEPT}
            multiple
            hidden
            onChange={(e) => {
              if (e.target.files?.length) void addFiles(e.target.files);
              e.target.value = "";
            }}
          />
          <button type="button" className="vyLink" onClick={() => picker.current?.click()} disabled={reading}>
            Choose the box files
          </button>
          <span className="vyHint">or drop them here · PDF, JPG, PNG</span>
        </div>
        {uploads.length ? (
          <ul className="anFiles">
            {uploads.map((u, i) => (
              <li key={`${u.name}-${i}`} className={`is-${u.state}`}>
                <span className="anFileName">{u.name}</span>
                <span className="anFileState">{u.state === "error" ? <span className="vyProblem">{u.note}</span> : u.note || "Ready to read"}</span>
              </li>
            ))}
          </ul>
        ) : null}
        <button type="button" className="vyGo exWide" onClick={read} disabled={reading || !waiting.length}>
          {reading ? "Reading…" : waiting.length ? `Read ${waiting.length} file${waiting.length === 1 ? "" : "s"}` : "Read the files"}
        </button>
        {error ? (
          <p className="exError" role="alert">
            {error}
          </p>
        ) : null}
      </section>

      {rows.length ? (
        <section className="exStep anRows">
          {vishays.length > 1 ? (
            <fieldset className="anSource anVishays">
              <legend>Which vishays?</legend>
              {vishays.map(([vishay, list]) => {
                const on = list.filter((r) => r.include).length;
                return (
                  <label key={vishay || "-"}>
                    <input
                      type="checkbox"
                      checked={on > 0}
                      ref={(el) => {
                        if (el) el.indeterminate = on > 0 && on < list.length;
                      }}
                      onChange={(e) => {
                        const keys = new Set(list.map((r) => r.key));
                        setRows((all) => all.map((r) => (keys.has(r.key) ? { ...r, include: e.target.checked } : r)));
                      }}
                    />{" "}
                    <span className="indic">{vishay || "(no vishay line)"}</span> <small>({list.length})</small>
                  </label>
                );
              })}
            </fieldset>
          ) : null}
          <label className="anCheck">
            <input type="checkbox" checked={bands} onChange={(e) => setBands(e.target.checked)} /> Put the vishay name (e.g. <span className="indic">10.8.3 - अनुबंधअहिंसा</span>) over its passages
          </label>
          <div className="vyBar anBar">
            <span className="vyBarCount">{chosen.length} passages</span>
            <button type="button" className="vyGo" onClick={() => download("pdf")} disabled={!chosen.length || Boolean(busy)}>
              {busy === "pdf" ? "Making…" : "PDF"}
            </button>
            <button type="button" className="exSecondary" onClick={() => download("word")} disabled={!chosen.length || Boolean(busy)}>
              Word
            </button>
            <button type="button" className="exSecondary" onClick={() => download("excel")} disabled={!chosen.length || Boolean(busy)}>
              {busy === "excel" ? "Making…" : "Excel"}
            </button>
          </div>
          {saved ? (
            <p className="anSaved" role="status">
              {saved}
            </p>
          ) : null}
          <div className="anBoxTable" role="table">
            {chosen.map((r) => (
              <div key={r.key} role="row" className="anBoxTableRow">
                <span role="cell">{r.sr}</span>
                <span role="cell" className="indic">
                  {r.granth}
                </span>
                <span role="cell" className="indic">
                  {r.shastraPath}
                </span>
              </div>
            ))}
          </div>
        </section>
      ) : null}
    </main>
  );
}
