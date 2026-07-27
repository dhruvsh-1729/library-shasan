import type { FormEvent } from "react";
import { useEffect, useId, useState } from "react";

type PageJumpPagerProps = {
  currentPage: number;
  totalPages: number;
  loading?: boolean;
  ariaLabel: string;
  onPageChange: (page: number) => void;
};

function clampPage(value: number, totalPages: number) {
  const max = Math.max(1, Math.floor(totalPages || 1));
  if (!Number.isFinite(value)) return 1;
  return Math.min(max, Math.max(1, Math.floor(value)));
}

export function PageJumpPager({ currentPage, totalPages, loading = false, ariaLabel, onPageChange }: PageJumpPagerProps) {
  const inputId = useId();
  const safeTotalPages = Math.max(1, Math.floor(totalPages || 1));
  const safeCurrentPage = clampPage(currentPage, safeTotalPages);
  const [pageEntry, setPageEntry] = useState(String(safeCurrentPage));

  useEffect(() => {
    setPageEntry(String(safeCurrentPage));
  }, [safeCurrentPage]);

  function submitPage(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const parsed = Number(pageEntry);
    const nextPage = clampPage(parsed, safeTotalPages);
    setPageEntry(String(nextPage));
    if (nextPage !== safeCurrentPage) onPageChange(nextPage);
  }

  return (
    <form className="pageJumpPager" aria-label={ariaLabel} onSubmit={submitPage}>
      <button type="button" onClick={() => onPageChange(1)} disabled={loading || safeCurrentPage <= 1}>
        First
      </button>
      <button type="button" onClick={() => onPageChange(safeCurrentPage - 1)} disabled={loading || safeCurrentPage <= 1}>
        Previous
      </button>

      <div className="pageJumpInputGroup">
        <label htmlFor={inputId}>Page</label>
        <input
          id={inputId}
          value={pageEntry}
          onChange={(event) => setPageEntry(event.target.value)}
          type="number"
          inputMode="numeric"
          min={1}
          max={safeTotalPages}
          disabled={loading}
        />
        <span>of {safeTotalPages}</span>
      </div>

      <button type="submit" disabled={loading}>
        Go
      </button>
      <button
        type="button"
        onClick={() => onPageChange(safeCurrentPage + 1)}
        disabled={loading || safeCurrentPage >= safeTotalPages}
      >
        Next
      </button>
      <button
        type="button"
        onClick={() => onPageChange(safeTotalPages)}
        disabled={loading || safeCurrentPage >= safeTotalPages}
      >
        Last
      </button>
    </form>
  );
}
