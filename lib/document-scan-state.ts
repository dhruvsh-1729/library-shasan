export const READY_DOCUMENT_STATUSES = ["processed", "processed_existing_turso"] as const;

export const REVIEW_DOCUMENT_STATUSES = [
  "processed_with_google_errors",
  "processed_with_review_pages",
  "partial_searchable_budget_exhausted",
] as const;

export const SEARCHABLE_DOCUMENT_STATUSES = [
  ...READY_DOCUMENT_STATUSES,
  ...REVIEW_DOCUMENT_STATUSES,
] as const;

export type DocumentScanState = "ready" | "review" | "remaining";

const READY_STATUS_SET = new Set<string>(READY_DOCUMENT_STATUSES);
const REVIEW_STATUS_SET = new Set<string>(REVIEW_DOCUMENT_STATUSES);

export function normalizeDocumentStatus(status: string | null | undefined) {
  return String(status ?? "").trim().toLowerCase();
}

export function getDocumentScanState(status: string | null | undefined): DocumentScanState {
  const normalized = normalizeDocumentStatus(status);
  if (READY_STATUS_SET.has(normalized)) return "ready";
  if (REVIEW_STATUS_SET.has(normalized)) return "review";
  return "remaining";
}

export function getDocumentScanLabel(state: DocumentScanState) {
  if (state === "ready") return "Ready";
  if (state === "review") return "Review";
  return "Needs scan";
}

export function getDocumentStatusLabel(status: string | null | undefined, state?: DocumentScanState) {
  const normalized = normalizeDocumentStatus(status);
  if (normalized) {
    return normalized
      .split("_")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }
  return getDocumentScanLabel(state ?? getDocumentScanState(status));
}

export function buildStatusInFilter(statuses: readonly string[]) {
  return `status.in.(${statuses.join(",")})`;
}

export function buildRemainingStatusFilter() {
  return `status.is.null,status.not.in.(${SEARCHABLE_DOCUMENT_STATUSES.join(",")})`;
}
