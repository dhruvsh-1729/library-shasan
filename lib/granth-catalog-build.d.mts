export const TEXT_ONLY_PATH_SEGMENT: string;
export const TEXT_ONLY_ID_PREFIX: string;

export type GranthCatalogEntry = {
  granth_key: string;
  source_rel_path: string;
  custom_id: string;
  kind: "pdf" | "text";
  document_custom_id: string | null;
  document_rel_path: string | null;
  pdf_link: "path" | "file-name" | "text-only" | "none" | "override";
  book_number: string | null;
  library_code: string | null;
  archive_ids: string[];
  title: string;
  native_title: string;
  part: string | null;
  series: string | null;
  author: string | null;
  page_count: number | null;
  duplicate_of: string | null;
  source_name: string;
  tags: string[];
  /** Kept out of search (a duplicate or an unusable scan), from the overrides. */
  hidden: boolean;
  text_only: boolean;
};

export type GranthCatalogIssue = { granth_key: string; problem: string; candidates: string[] };

export type GranthCatalogSources = {
  turso: Array<{
    granth_key: string;
    book_number?: string | null;
    library_code?: string | null;
    granth_name: string;
    source_rel_path: string;
    page_count?: number | null;
  }>;
  documents: Array<{
    custom_id: string | null;
    original_relative_path: string | null;
    pdf_name: string | null;
    pdf_url?: string | null;
    status?: string | null;
  }>;
  libraryFiles: Array<{ custom_id: string | null; book_id: number | null }>;
  books: Array<{
    id: number;
    title_english: string | null;
    title_display: string | null;
    author_text: string | null;
    book_codes?: string[];
  }>;
  overrides?: {
    granths?: Record<string, Record<string, unknown>>;
    books?: Record<string, Record<string, unknown>>;
  };
};

export function buildGranthCatalog(sources: GranthCatalogSources): {
  entries: GranthCatalogEntry[];
  issues: GranthCatalogIssue[];
};
export function granthDisplayName(entry: Pick<GranthCatalogEntry, "title" | "native_title" | "source_name" | "granth_key" | "part">): string;
