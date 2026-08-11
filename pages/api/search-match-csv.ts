import type { NextApiRequest, NextApiResponse } from "next";
import { createReadStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setNoStore } from "@/lib/api-cache";
import { DownloadEmailError, getDownloadRecipientClientKey, sendDownloadEmail } from "@/lib/download-email";
import { parseOCRSearchMode } from "@/lib/ocr-search";
import { CsvFileWriter } from "@/lib/search-csv";
import {
  MAX_CSV_GRANTHS,
  MAX_CSV_ROWS,
  SearchMatchError,
  loadSearchMatchLines,
  resolveSearchPdfSources,
  validateSearchDownloadQueries,
} from "@/lib/search-match-pages";

export const config = {
  api: {
    responseLimit: false,
  },
};

type GranthSelection = {
  customId: string;
  sourceRelPath: string;
  granthName: string;
  pages: number[] | null;
};

type CsvBody = {
  customId?: string | null;
  sourceRelPath?: string | null;
  granthName?: string | null;
  q?: string | null;
  queryVariants?: unknown;
  matchMode?: string | null;
  pages?: unknown;
  delivery?: string | null;
  email?: string | null;
  granths?: unknown;
};

function safeFileName(value: string, fallback = "search_matches") {
  const cleaned = String(value || "")
    .replace(/\.(pdf|csv|xlsx)$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return cleaned || fallback;
}

function contentDisposition(filename: string) {
  const ascii = filename.replace(/[^\x20-\x7e]+/g, "_").replace(/["\\]/g, "_") || "download.csv";
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}

function parseQueryVariants(value: unknown) {
  if (Array.isArray(value)) return value.map((item) => String(item || ""));
  if (value == null) return [];
  return [String(value || "")];
}

function parsePages(value: unknown) {
  if (!Array.isArray(value)) return null;
  const pages = [...new Set(
    value
      .map((page) => Math.floor(Number(page)))
      .filter((page) => Number.isFinite(page) && page > 0)
  )].sort((a, b) => a - b);
  return pages.length > 0 ? pages : null;
}

function parseGranthSelections(value: unknown): GranthSelection[] {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const selections: GranthSelection[] = [];

  for (const entry of value) {
    if (!entry || typeof entry !== "object") continue;
    const candidate = entry as Record<string, unknown>;
    const sourceRelPath = String(candidate.sourceRelPath || "").trim();
    if (!sourceRelPath) continue;
    if (seen.has(sourceRelPath)) continue;
    seen.add(sourceRelPath);
    selections.push({
      customId: String(candidate.customId || "").trim(),
      sourceRelPath,
      granthName: String(candidate.granthName || "").trim(),
      pages: parsePages(candidate.pages),
    });
  }

  return selections;
}

function streamFile(res: NextApiResponse, filePath: string, filename: string, cleanupDir: string) {
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", contentDisposition(filename));
  res.setHeader("Cache-Control", "no-store");

  const cleanup = () => {
    void rm(cleanupDir, { recursive: true, force: true });
  };

  res.on("finish", cleanup);
  res.on("close", cleanup);
  createReadStream(filePath).pipe(res);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  let workDir: string | null = null;

  try {
    const body = (req.body || {}) as CsvBody;
    const delivery = body.delivery === "email" ? "email" : "download";
    const matchMode = parseOCRSearchMode(body.matchMode);
    const queries = validateSearchDownloadQueries(String(body.q || "").trim(), parseQueryVariants(body.queryVariants), matchMode);

    const multiSelections = parseGranthSelections(body.granths);
    const selections: GranthSelection[] = multiSelections.length
      ? multiSelections
      : [
          {
            customId: String(body.customId || "").trim(),
            sourceRelPath: String(body.sourceRelPath || "").trim(),
            granthName: String(body.granthName || "").trim(),
            pages: parsePages(body.pages),
          },
        ];

    if (selections.length > MAX_CSV_GRANTHS) {
      throw new SearchMatchError(
        413,
        `Select up to ${MAX_CSV_GRANTHS} granths for one CSV export. ${selections.length} were selected.`
      );
    }

    // A CSV only needs OCR page and line data, so granths without an uploaded
    // PDF are still exported; the PDF lookup only supplies nicer file names.
    const sources = await resolveSearchPdfSources(
      selections.map((selection) => ({ customId: selection.customId, sourceRelPath: selection.sourceRelPath }))
    );
    const resolvedRelPaths = selections.map((selection, index) => {
      const source = sources[index];
      const relPath = selection.sourceRelPath || (source.ok ? source.source.sourceRelPath : "");
      return {
        relPath,
        granthName: selection.granthName || (source.ok ? source.source.pdfName : selection.customId) || relPath,
        pdfName: source.ok ? source.source.pdfName : "",
        pages: selection.pages,
      };
    });

    if (resolvedRelPaths.every((entry) => !entry.relPath)) {
      throw new SearchMatchError(404, "These results are not linked to searchable granth page metadata.");
    }

    workDir = await mkdtemp(path.join(tmpdir(), "ndms-search-csv-"));
    const csvPath = path.join(workDir, "search-matches.csv");
    const writer = new CsvFileWriter(csvPath);
    let truncated = false;
    let granthsWithRows = 0;

    for (const entry of resolvedRelPaths) {
      if (!entry.relPath) continue;
      if (writer.rowCount >= MAX_CSV_ROWS) {
        truncated = true;
        break;
      }

      const { lines, truncated: granthTruncated } = await loadSearchMatchLines(entry.relPath, queries, matchMode, {
        pages: entry.pages,
        maxRows: MAX_CSV_ROWS - writer.rowCount,
      });
      if (granthTruncated) truncated = true;
      if (lines.length > 0) granthsWithRows += 1;

      for (const line of lines) {
        await writer.writeRow([
          entry.granthName,
          entry.pdfName,
          line.page_number,
          line.line_number,
          line.occurrence_count,
          line.matched_words.join(" | "),
          line.line_text,
        ]);
      }
    }

    const rowCount = await writer.close();
    if (rowCount === 0) {
      throw new SearchMatchError(404, "No matching page lines were found for this search.");
    }

    const queryLabel = safeFileName(queries.join("_"), "search");
    const scopeLabel =
      resolvedRelPaths.length === 1
        ? safeFileName(resolvedRelPaths[0].granthName, "granth")
        : `${granthsWithRows || resolvedRelPaths.length}_granths`;
    const filename = `granth_search_${queryLabel}_${scopeLabel}_page_lines.csv`;

    if (delivery === "email") {
      const recipientClientKey = getDownloadRecipientClientKey(req, res);
      const sent = await sendDownloadEmail({
        to: String(body.email || ""),
        filePath: csvPath,
        filename,
        contentType: "text/csv",
        title: `${queries.join(", ")} page and line list`,
        recipientClientKey,
      });
      await rm(workDir, { recursive: true, force: true });
      workDir = null;
      return res.status(200).json({
        emailed: true,
        email: sent.email,
        size_bytes: sent.sizeBytes,
        file_name: filename,
        row_count: rowCount,
        granth_count: granthsWithRows,
        truncated,
      });
    }

    res.setHeader("X-Library-Csv-Rows", String(rowCount));
    res.setHeader("X-Library-Csv-Granths", String(granthsWithRows));
    if (truncated) res.setHeader("X-Library-Csv-Truncated", "1");
    const cleanupDir = workDir;
    workDir = null;
    streamFile(res, csvPath, filename, cleanupDir);
    return undefined;
  } catch (error) {
    if (workDir) await rm(workDir, { recursive: true, force: true });
    setNoStore(res);
    if (error instanceof DownloadEmailError) {
      return res.status(error.status).json({ error: error.message });
    }
    if (error instanceof SearchMatchError) {
      return res.status(error.status).json({ error: error.message });
    }
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}
