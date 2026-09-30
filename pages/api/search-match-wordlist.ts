import type { NextApiRequest, NextApiResponse } from "next";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setNoStore } from "@/lib/api-cache";
import { DownloadEmailError, getDownloadRecipientClientKey, sendDownloadEmail } from "@/lib/download-email";
import { parseOCRSearchMode, parseOCRSearchScripts } from "@/lib/ocr-search";
import {
  MAX_CSV_GRANTHS,
  MAX_CSV_ROWS,
  SearchMatchError,
  loadSearchMatchLines,
  loadSearchMatchOccurrences,
  resolveSearchPdfSources,
  validateSearchDownloadQueries,
} from "@/lib/search-match-pages";
import { type LineListSection, type WordListSection, buildLineListPdf, buildWordListPdf } from "@/lib/word-list-pdf";

/** The line list keeps whole lines (the CSV cuts them at 400 characters). */
const LINE_LIST_MAX_LINE_CHARS = 2000;
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";

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

type WordListBody = {
  customId?: string | null;
  sourceRelPath?: string | null;
  granthName?: string | null;
  q?: string | null;
  queryVariants?: unknown;
  matchMode?: string | null;
  scripts?: unknown;
  pages?: unknown;
  delivery?: string | null;
  email?: string | null;
  granths?: unknown;
};

function safeFileName(value: string, fallback = "word_list") {
  const cleaned = String(value || "")
    .replace(/\.(pdf|csv|xlsx)$/i, "")
    .replace(/[^a-z0-9._\-\u0900-\u097f\u0a80-\u0aff]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return cleaned || fallback;
}

function contentDisposition(filename: string) {
  const ascii = filename.replace(/[^\x20-\x7e]+/g, "_").replace(/["\\]/g, "_") || "download.pdf";
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
    if (!sourceRelPath || seen.has(sourceRelPath)) continue;
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

/** The word shown in the PDF heading: the first query written in an Indian script, else the query itself. */
function headingWord(queries: string[]) {
  return queries.find((query) => /[\u0900-\u097F\u0A80-\u0AFF]/.test(query)) ?? queries[0];
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed" });
  }

  let workDir: string | null = null;

  try {
    // ?layout=lines: one row per matched line (granth page, line number, the
    // line with the words marked, the words); otherwise one row per word.
    const lineLayout = String(req.query.layout || "") === "lines";
    const body = (req.body || {}) as WordListBody;
    const delivery = body.delivery === "email" ? "email" : "download";
    const matchMode = parseOCRSearchMode(body.matchMode);
    const scripts = parseOCRSearchScripts(body.scripts);
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
        `Select up to ${MAX_CSV_GRANTHS} granths for one word list. ${selections.length} were selected.`
      );
    }

    // Like the CSV, the word list needs only OCR page data, so granths without
    // an uploaded PDF are included; the PDF lookup only supplies names.
    const sources = await resolveSearchPdfSources(
      selections.map((selection) => ({ customId: selection.customId, sourceRelPath: selection.sourceRelPath }))
    );
    const entries = selections.map((selection, index) => {
      const source = sources[index];
      const relPath = selection.sourceRelPath || (source.ok ? source.source.sourceRelPath : "");
      return {
        relPath,
        granthName: selection.granthName || (source.ok ? source.source.pdfName : selection.customId) || relPath,
        pages: selection.pages,
      };
    });

    if (entries.every((entry) => !entry.relPath)) {
      throw new SearchMatchError(404, "These results are not linked to searchable granth page metadata.");
    }

    const sections: WordListSection[] = [];
    const lineSections: LineListSection[] = [];
    let rowCount = 0;
    let truncated = false;

    for (const entry of entries) {
      if (!entry.relPath) continue;
      if (rowCount >= MAX_CSV_ROWS) {
        truncated = true;
        break;
      }
      if (lineLayout) {
        const { lines, truncated: granthTruncated } = await loadSearchMatchLines(entry.relPath, queries, matchMode, {
          pages: entry.pages,
          maxRows: MAX_CSV_ROWS - rowCount,
          scripts,
          maxLineChars: LINE_LIST_MAX_LINE_CHARS,
        });
        if (granthTruncated) truncated = true;
        if (lines.length === 0) continue;
        rowCount += lines.length;
        lineSections.push({
          granthName: entry.granthName,
          rows: lines.map((line) => ({
            printedPage: line.printed_page,
            pdfPage: line.page_number,
            lineNumber: line.line_number,
            lineText: line.line_text,
            words: line.matched_words,
          })),
        });
        continue;
      }
      const { occurrences, truncated: granthTruncated } = await loadSearchMatchOccurrences(entry.relPath, queries, matchMode, {
        pages: entry.pages,
        maxRows: MAX_CSV_ROWS - rowCount,
        scripts,
      });
      if (granthTruncated) truncated = true;
      if (occurrences.length === 0) continue;
      rowCount += occurrences.length;
      sections.push({
        granthName: entry.granthName,
        rows: occurrences.map((occurrence) => ({
          word: occurrence.word,
          printedPage: occurrence.printed_page,
          pdfPage: occurrence.page_number,
        })),
      });
    }

    if (rowCount === 0) {
      throw new SearchMatchError(404, "No matching words were found for this search.");
    }

    const word = headingWord(queries);
    const bytes = lineLayout ? await buildLineListPdf({ word, sections: lineSections }) : await buildWordListPdf({ word, sections });

    const named = lineLayout ? lineSections : sections;
    const queryLabel = safeFileName(word, "search");
    const scopeLabel = named.length === 1 ? safeFileName(named[0].granthName, "granth") : `${named.length}_granths`;
    const filename = `granth_search_${queryLabel}_${scopeLabel}_${lineLayout ? "line" : "word"}_list.pdf`;

    if (delivery === "email") {
      workDir = await mkdtemp(path.join(tmpdir(), "ndms-word-list-"));
      const pdfPath = path.join(workDir, "word-list.pdf");
      await writeFile(pdfPath, bytes);
      const recipientClientKey = getDownloadRecipientClientKey(req, res);
      const sent = await sendDownloadEmail({
        to: String(body.email || ""),
        filePath: pdfPath,
        filename,
        contentType: "application/pdf",
        title: `${word} ${lineLayout ? "line" : "word"} list`,
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
        granth_count: named.length,
        truncated,
      });
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", contentDisposition(filename));
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("X-Library-Wordlist-Rows", String(rowCount));
    res.setHeader("X-Library-Wordlist-Granths", String(named.length));
    if (truncated) res.setHeader("X-Library-Wordlist-Truncated", "1");
    return res.status(200).send(Buffer.from(bytes));
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

export default protectApi(handler, PERMISSIONS.pdfBuild);
