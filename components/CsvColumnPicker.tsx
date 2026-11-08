import { useState } from "react";
import { DEFAULT_SEARCH_CSV_COLUMNS, SEARCH_CSV_COLUMNS, type SearchCsvColumn } from "@/lib/search-csv-columns";

const STORAGE_KEY = "library.csvColumns";

function readSaved(): SearchCsvColumn[] {
  try {
    const raw = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "null");
    const known = new Set<string>(SEARCH_CSV_COLUMNS.map((c) => c.id));
    const list = Array.isArray(raw) ? raw.filter((id): id is SearchCsvColumn => known.has(String(id))) : [];
    return list.length ? list : DEFAULT_SEARCH_CSV_COLUMNS;
  } catch {
    return DEFAULT_SEARCH_CSV_COLUMNS;
  }
}

/** The spreadsheet columns the reader last chose, remembered on this device. */
export function useCsvColumns() {
  const [columns, setColumns] = useState<SearchCsvColumn[]>(() => (typeof window === "undefined" ? DEFAULT_SEARCH_CSV_COLUMNS : readSaved()));
  const update = (next: SearchCsvColumn[]) => {
    setColumns(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // private mode: the choice lasts until the page closes
    }
  };
  return [columns, update] as const;
}

/** Tick the columns the spreadsheet should have; at least one stays ticked. */
export function CsvColumnPicker({ columns, onChange }: { columns: SearchCsvColumn[]; onChange: (next: SearchCsvColumn[]) => void }) {
  const chosen = new Set(columns);
  const toggle = (id: SearchCsvColumn) => {
    const next = SEARCH_CSV_COLUMNS.map((c) => c.id).filter((c) => (c === id ? !chosen.has(c) : chosen.has(c)));
    if (next.length) onChange(next);
  };
  return (
    <fieldset className="sheetColumns">
      <legend>Columns in the spreadsheet</legend>
      <div className="sheetColumnsList">
        {SEARCH_CSV_COLUMNS.map((c) => (
          <label key={c.id} className={`sheetColumn${chosen.has(c.id) ? " isOn" : ""}`}>
            <input type="checkbox" checked={chosen.has(c.id)} onChange={() => toggle(c.id)} />
            {c.label}
          </label>
        ))}
      </div>
      <button type="button" className="sheetColumnsReset" onClick={() => onChange(DEFAULT_SEARCH_CSV_COLUMNS)}>
        Usual columns
      </button>
    </fieldset>
  );
}
