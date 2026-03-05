import { useEffect, useMemo, useState } from "react";

type GranthOption = {
  granth_key: string;
  book_number: string;
  library_code: string | null;
  granth_name: string;
  display_name: string;
  page_count: number;
  xlsx_url: string | null;
};

type ReplaceMatch = {
  match_id: string;
  granth_key: string;
  page_number: number;
  start: number;
  end: number;
  old_word: string;
  context: string;
  granth_title: string;
};

type ReplaceHistoryItem = {
  id: number;
  change_group_id: string | null;
  granth_key: string;
  granth_title: string;
  page_number: number;
  match_start: number;
  match_end: number;
  old_word: string;
  new_word: string;
  changed_at: string;
  reverted_from_change_id: number | null;
};

type ReplaceScope = "page" | "current_granth" | "selected_granths" | "all_granths";

type PageTarget = {
  granthKey: string;
  pageNumber: number;
  title: string;
};

type GranthTarget = {
  granthKey: string;
  title: string;
};

type Props = {
  availableGranths?: GranthOption[];
  currentPageTarget?: PageTarget | null;
  currentGranthTarget?: GranthTarget | null;
  initialSelectedGranthKeys?: string[];
  initialWord?: string;
  title?: string;
  onApplied?: () => void | Promise<void>;
};

function buildDefaultScope(props: Props): ReplaceScope {
  if (props.currentPageTarget) return "page";
  if (props.currentGranthTarget) return "current_granth";
  if ((props.initialSelectedGranthKeys ?? []).length > 0) return "selected_granths";
  return "all_granths";
}

function escapeRegExp(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export default function OcrReplacePanel(props: Props) {
  const [granthOptions, setGranthOptions] = useState<GranthOption[]>(props.availableGranths ?? []);
  const [loadingGranths, setLoadingGranths] = useState(false);
  const [granthFilter, setGranthFilter] = useState("");

  const [replaceScope, setReplaceScope] = useState<ReplaceScope>(buildDefaultScope(props));
  const [replaceWord, setReplaceWord] = useState(props.initialWord ?? "");
  const [replaceWith, setReplaceWith] = useState("");
  const [selectedGranthKeys, setSelectedGranthKeys] = useState<string[]>(props.initialSelectedGranthKeys ?? []);
  const [previewMatches, setPreviewMatches] = useState<ReplaceMatch[]>([]);
  const [selectedMatchIds, setSelectedMatchIds] = useState<Record<string, boolean>>({});
  const [previewLoading, setPreviewLoading] = useState(false);
  const [applyingChanges, setApplyingChanges] = useState(false);
  const [replaceError, setReplaceError] = useState<string | null>(null);
  const [replaceStatus, setReplaceStatus] = useState<string | null>(null);
  const [history, setHistory] = useState<ReplaceHistoryItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  useEffect(() => {
    if (props.availableGranths) {
      setGranthOptions(props.availableGranths);
      return;
    }

    let active = true;
    async function loadGranths() {
      setLoadingGranths(true);
      try {
        const res = await fetch("/api/ocr-granths?limit=10000");
        const json = (await res.json()) as { items?: GranthOption[]; error?: string };
        if (!res.ok) {
          throw new Error(json.error || `Failed to load OCR granths (${res.status})`);
        }
        if (!active) return;
        setGranthOptions(json.items ?? []);
      } catch (error) {
        if (!active) return;
        setReplaceError(error instanceof Error ? error.message : String(error));
      } finally {
        if (active) setLoadingGranths(false);
      }
    }

    void loadGranths();
    return () => {
      active = false;
    };
  }, [props.availableGranths]);

  useEffect(() => {
    if (!replaceWord && props.initialWord) {
      setReplaceWord(props.initialWord);
    }
  }, [props.initialWord, replaceWord]);

  useEffect(() => {
    if (!props.initialSelectedGranthKeys) return;
    setSelectedGranthKeys(props.initialSelectedGranthKeys);
  }, [props.initialSelectedGranthKeys?.join("|")]);

  const filteredGranths = useMemo(() => {
    const keyword = granthFilter.trim().toLowerCase();
    if (!keyword) return granthOptions;
    return granthOptions.filter((row) => row.display_name.toLowerCase().includes(keyword));
  }, [granthFilter, granthOptions]);

  const selectedMatchCount = useMemo(() => {
    return previewMatches.reduce((acc, match) => (selectedMatchIds[match.match_id] ? acc + 1 : acc), 0);
  }, [previewMatches, selectedMatchIds]);

  const selectedGranthCount = useMemo(() => selectedGranthKeys.length, [selectedGranthKeys.length]);

  function renderHighlightedContext(text: string) {
    if (!text || !replaceWord.trim()) return text;
    const pattern = new RegExp(`(${escapeRegExp(replaceWord.trim())})`, "gi");
    const parts = text.split(pattern);
    const normalized = replaceWord.trim().toLowerCase();

    return parts.map((part, idx) => {
      if (!part) return null;
      if (part.toLowerCase() !== normalized) return <span key={idx}>{part}</span>;
      return (
        <mark
          key={idx}
          style={{
            background: "#fff100",
            color: "#111",
            padding: "0 2px",
            borderRadius: 3,
            fontWeight: 700,
          }}
        >
          {part}
        </mark>
      );
    });
  }

  function toggleGranthSelection(granthKey: string) {
    setSelectedGranthKeys((prev) =>
      prev.includes(granthKey) ? prev.filter((value) => value !== granthKey) : [...prev, granthKey]
    );
  }

  async function loadReplaceHistory(wordOverride?: string) {
    setHistoryLoading(true);
    try {
      const params = new URLSearchParams();
      params.set("limit", "180");
      const word = (wordOverride ?? replaceWord).trim();
      if (word) params.set("word", word);

      if (replaceScope === "page" || replaceScope === "current_granth") {
        if (props.currentGranthTarget?.granthKey) {
          params.set("granthKey", props.currentGranthTarget.granthKey);
        }
      } else if (replaceScope === "selected_granths" && selectedGranthKeys.length === 1) {
        params.set("granthKey", selectedGranthKeys[0]);
      }

      const res = await fetch(`/api/ocr-replacements/history?${params.toString()}`);
      const json = (await res.json()) as { items?: ReplaceHistoryItem[]; error?: string };
      if (!res.ok) {
        throw new Error(json.error || `Failed to load history (${res.status})`);
      }
      setHistory(json.items ?? []);
    } catch (error) {
      setReplaceError(error instanceof Error ? error.message : String(error));
    } finally {
      setHistoryLoading(false);
    }
  }

  async function previewReplaceMatches() {
    setReplaceError(null);
    setReplaceStatus(null);

    const word = replaceWord.trim();
    if (!word) {
      setReplaceError("Enter the exact word to find.");
      return;
    }

    if (replaceScope === "page" && !props.currentPageTarget) {
      setReplaceError("Current page target is not available.");
      return;
    }

    if (replaceScope === "current_granth" && !props.currentGranthTarget) {
      setReplaceError("Current granth target is not available.");
      return;
    }

    if (replaceScope === "selected_granths" && selectedGranthKeys.length === 0) {
      setReplaceError("Select at least one granth.");
      return;
    }

    setPreviewLoading(true);
    try {
      const body = {
        word,
        scope: replaceScope,
        granthKey: replaceScope === "current_granth" ? props.currentGranthTarget?.granthKey : undefined,
        granthKeys: replaceScope === "selected_granths" ? selectedGranthKeys : undefined,
        singleTarget:
          replaceScope === "page" && props.currentPageTarget
            ? {
                granth_key: props.currentPageTarget.granthKey,
                page_number: props.currentPageTarget.pageNumber,
              }
            : undefined,
      };

      const res = await fetch("/api/ocr-replacements/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const json = (await res.json()) as {
        matches?: ReplaceMatch[];
        total_matches?: number;
        truncated?: boolean;
        error?: string;
      };
      if (!res.ok) {
        throw new Error(json.error || `Preview failed (${res.status})`);
      }

      const matches = json.matches ?? [];
      setPreviewMatches(matches);
      const nextSelected: Record<string, boolean> = {};
      for (const match of matches) nextSelected[match.match_id] = true;
      setSelectedMatchIds(nextSelected);
      setReplaceStatus(
        `Preview ready: ${json.total_matches ?? matches.length} exact match(es)${
          json.truncated ? " (truncated)" : ""
        }.`
      );
      await loadReplaceHistory(word);
    } catch (error) {
      setReplaceError(error instanceof Error ? error.message : String(error));
    } finally {
      setPreviewLoading(false);
    }
  }

  function setAllMatchSelection(value: boolean) {
    const nextSelected: Record<string, boolean> = {};
    for (const match of previewMatches) {
      nextSelected[match.match_id] = value;
    }
    setSelectedMatchIds(nextSelected);
  }

  function toggleMatchSelection(matchId: string) {
    setSelectedMatchIds((prev) => ({ ...prev, [matchId]: !prev[matchId] }));
  }

  async function applySelectedChanges() {
    setReplaceError(null);
    setReplaceStatus(null);

    const word = replaceWord.trim();
    const newWord = replaceWith.trim();
    if (!word || !newWord) {
      setReplaceError("Enter both the exact word and the replacement.");
      return;
    }

    const chosen = previewMatches.filter((match) => selectedMatchIds[match.match_id]);
    if (chosen.length === 0) {
      setReplaceError("Select at least one exact match.");
      return;
    }

    const sorted = [...chosen].sort((a, b) => {
      if (a.granth_key !== b.granth_key) return a.granth_key.localeCompare(b.granth_key, "en");
      if (a.page_number !== b.page_number) return a.page_number - b.page_number;
      return b.start - a.start;
    });

    const changeGroupId = `group_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    let okCount = 0;
    let failCount = 0;

    setApplyingChanges(true);
    try {
      for (const match of sorted) {
        const res = await fetch("/api/ocr-replacements/apply-one", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            granth_key: match.granth_key,
            page_number: match.page_number,
            start: match.start,
            end: match.end,
            old_word: match.old_word,
            new_word: newWord,
            change_group_id: changeGroupId,
          }),
        });

        if (res.ok) {
          okCount += 1;
        } else {
          failCount += 1;
        }
      }

      setReplaceStatus(`Applied ${okCount} change(s). Failed ${failCount}.`);
      await loadReplaceHistory(word);
      setPreviewMatches([]);
      setSelectedMatchIds({});
      if (props.onApplied) {
        await props.onApplied();
      }
    } catch (error) {
      setReplaceError(error instanceof Error ? error.message : String(error));
    } finally {
      setApplyingChanges(false);
    }
  }

  async function revertHistoryChange(changeId: number) {
    setReplaceError(null);
    setReplaceStatus(null);
    try {
      const res = await fetch("/api/ocr-replacements/revert", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ change_id: changeId }),
      });
      const json = (await res.json()) as { error?: string };
      if (!res.ok) {
        throw new Error(json.error || `Revert failed (${res.status})`);
      }

      setReplaceStatus(`Reverted change #${changeId}.`);
      await loadReplaceHistory();
      if (props.onApplied) {
        await props.onApplied();
      }
    } catch (error) {
      setReplaceError(error instanceof Error ? error.message : String(error));
    }
  }

  return (
    <section
      style={{
        border: "1px solid #d7d3c8",
        borderRadius: 18,
        background: "#fffefb",
        boxShadow: "0 12px 28px rgba(36, 36, 31, 0.08)",
        padding: 18,
      }}
    >
      <h2 style={{ margin: 0, fontSize: 28, lineHeight: 1.2 }}>{props.title ?? "Exact Word Replace"}</h2>
      <p style={{ margin: "8px 0 0", fontSize: 15, lineHeight: 1.55, color: "#475467" }}>
        Preview exact whole-word matches, tick the exact places to update, apply them one-by-one with logging, and
        revert any older change from the history below.
      </p>
      <div style={{ marginTop: 10, display: "grid", gap: 12 }}>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <input
            value={replaceWord}
            onChange={(e) => setReplaceWord(e.target.value)}
            placeholder="Find exact word"
            style={{
              flex: 1,
              minWidth: 200,
              padding: "11px 13px",
              borderRadius: 8,
              border: "1px solid #c7cfd9",
              background: "#fff",
                fontSize: 17,
              }}
            />
          <input
            value={replaceWith}
            onChange={(e) => setReplaceWith(e.target.value)}
            placeholder="Replace with"
            style={{
              flex: 1,
              minWidth: 200,
              padding: "11px 13px",
              borderRadius: 8,
              border: "1px solid #c7cfd9",
              background: "#fff",
              fontSize: 17,
            }}
          />
        </div>

        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
          <strong style={{ fontSize: 16 }}>Scope:</strong>
          {props.currentPageTarget ? (
            <button
              type="button"
              onClick={() => setReplaceScope("page")}
              style={{
                padding: "8px 12px",
                borderRadius: 999,
                border: "1px solid #bcc4ce",
                background: replaceScope === "page" ? "#1f2120" : "#fff",
                color: replaceScope === "page" ? "#fff" : "#222",
                fontSize: 15,
                cursor: "pointer",
              }}
            >
              This page
            </button>
          ) : null}
          {props.currentGranthTarget ? (
            <button
              type="button"
              onClick={() => setReplaceScope("current_granth")}
              style={{
                padding: "8px 12px",
                borderRadius: 999,
                border: "1px solid #bcc4ce",
                background: replaceScope === "current_granth" ? "#1f2120" : "#fff",
                color: replaceScope === "current_granth" ? "#fff" : "#222",
                fontSize: 15,
                cursor: "pointer",
              }}
            >
              This granth
            </button>
          ) : null}
          <button
            type="button"
            onClick={() => setReplaceScope("selected_granths")}
            style={{
              padding: "8px 12px",
              borderRadius: 999,
              border: "1px solid #bcc4ce",
              background: replaceScope === "selected_granths" ? "#1f2120" : "#fff",
              color: replaceScope === "selected_granths" ? "#fff" : "#222",
              fontSize: 15,
              cursor: "pointer",
            }}
          >
            Selected granths
          </button>
          <button
            type="button"
            onClick={() => setReplaceScope("all_granths")}
            style={{
              padding: "8px 12px",
              borderRadius: 999,
              border: "1px solid #bcc4ce",
              background: replaceScope === "all_granths" ? "#1f2120" : "#fff",
              color: replaceScope === "all_granths" ? "#fff" : "#222",
              fontSize: 15,
              cursor: "pointer",
            }}
          >
            All granths
          </button>
        </div>

        {replaceScope === "page" && props.currentPageTarget ? (
          <div style={{ fontSize: 15, opacity: 0.85, lineHeight: 1.55 }}>
            Preview exact matches only on: <strong>{props.currentPageTarget.title}</strong>, page{" "}
            <strong>{props.currentPageTarget.pageNumber}</strong>. Tick one for a single-place update, or multiple on the same page.
          </div>
        ) : null}

        {replaceScope === "current_granth" && props.currentGranthTarget ? (
          <div style={{ fontSize: 15, opacity: 0.85, lineHeight: 1.55 }}>
            Preview exact matches across this granth: <strong>{props.currentGranthTarget.title}</strong>.
          </div>
        ) : null}

        {replaceScope === "selected_granths" ? (
          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ fontSize: 15, opacity: 0.85, lineHeight: 1.55 }}>
              Pick any granths, preview exact matches across them, then tick specific places or keep all for a full batch update.
            </div>
            <div style={{ fontSize: 14, fontWeight: 700, color: "#344054" }}>
              Selected: {selectedGranthCount} granth file{selectedGranthCount === 1 ? "" : "s"}
            </div>
            <input
              value={granthFilter}
              onChange={(e) => setGranthFilter(e.target.value)}
              placeholder="Filter granths..."
              style={{
                padding: "10px 12px",
                borderRadius: 8,
                border: "1px solid #c7cfd9",
                background: "#fff",
                fontSize: 16,
              }}
            />
            <div
              style={{
                maxHeight: 200,
                overflow: "auto",
                display: "grid",
                gap: 6,
                border: "1px solid #d5dae2",
                borderRadius: 10,
                background: "#fafbfc",
                padding: 8,
              }}
            >
              {loadingGranths ? (
                <div style={{ opacity: 0.75 }}>Loading granths...</div>
              ) : filteredGranths.length === 0 ? (
                <div style={{ opacity: 0.75 }}>No matching granths.</div>
              ) : (
                filteredGranths.map((granth) => (
                  <label
                    key={granth.granth_key}
                    style={{
                      display: "flex",
                      gap: 8,
                      alignItems: "center",
                      padding: "6px 8px",
                      borderRadius: 8,
                      background: selectedGranthKeys.includes(granth.granth_key) ? "#edf0f5" : "#fff",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={selectedGranthKeys.includes(granth.granth_key)}
                      onChange={() => toggleGranthSelection(granth.granth_key)}
                    />
                    <span style={{ fontSize: 15 }}>{granth.display_name}</span>
                  </label>
                ))
              )}
            </div>
          </div>
        ) : null}

        {replaceScope === "all_granths" ? (
          <div style={{ fontSize: 15, opacity: 0.85, lineHeight: 1.55 }}>
            Preview exact matches across the entire OCR corpus, then tick specific places or leave all selected to update every exact match everywhere.
          </div>
        ) : null}

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button
            type="button"
            onClick={() => void previewReplaceMatches()}
            disabled={previewLoading}
            style={{ padding: "10px 13px", borderRadius: 10, border: "1px solid #c7cfd9", background: "#fff", fontSize: 15, cursor: "pointer" }}
          >
            {previewLoading ? "Finding matches..." : "Preview matches"}
          </button>
          <button
            type="button"
            onClick={() => void loadReplaceHistory()}
            disabled={historyLoading}
            style={{ padding: "10px 13px", borderRadius: 10, border: "1px solid #c7cfd9", background: "#fff", fontSize: 15, cursor: "pointer" }}
          >
            {historyLoading ? "Loading history..." : "Load history"}
          </button>
          <button
            type="button"
            onClick={() => setAllMatchSelection(true)}
            disabled={previewMatches.length === 0}
            style={{ padding: "10px 13px", borderRadius: 10, border: "1px solid #c7cfd9", background: "#fff", fontSize: 15, cursor: "pointer" }}
          >
            Tick all previewed
          </button>
          <button
            type="button"
            onClick={() => setAllMatchSelection(false)}
            disabled={previewMatches.length === 0}
            style={{ padding: "10px 13px", borderRadius: 10, border: "1px solid #c7cfd9", background: "#fff", fontSize: 15, cursor: "pointer" }}
          >
            Untick all
          </button>
          <button
            type="button"
            onClick={() => void applySelectedChanges()}
            disabled={applyingChanges || selectedMatchCount === 0}
            style={{
              padding: "10px 13px",
              borderRadius: 10,
              border: "1px solid #1f2120",
              background: "#1f2120",
              color: "#fff",
              fontSize: 15,
              cursor: "pointer",
            }}
          >
            {applyingChanges ? "Applying..." : `Apply selected (${selectedMatchCount})`}
          </button>
        </div>

        {replaceStatus ? <div style={{ color: "#0f6b2f", fontWeight: 600 }}>{replaceStatus}</div> : null}
        {replaceError ? <div style={{ color: "#9f1f1f", fontWeight: 600 }}>{replaceError}</div> : null}

        {previewMatches.length > 0 ? (
          <div
            style={{
              border: "1px solid #d5dae2",
              borderRadius: 12,
              background: "#fafbfc",
              padding: 10,
              maxHeight: 300,
              overflow: "auto",
              display: "grid",
              gap: 8,
            }}
          >
            {previewMatches.map((match) => {
              const viewerHref = `/ocr-text-viewer?granthKey=${encodeURIComponent(match.granth_key)}&page=${encodeURIComponent(
                String(match.page_number)
              )}&q=${encodeURIComponent(replaceWord)}`;
              return (
                <label
                  key={match.match_id}
                  style={{
                    display: "grid",
                    gap: 5,
                    padding: "9px 10px",
                    borderRadius: 8,
                    border: "1px solid #d8dde6",
                    background: "#fff",
                  }}
                >
                  <span style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                    <input
                      type="checkbox"
                      checked={Boolean(selectedMatchIds[match.match_id])}
                      onChange={() => toggleMatchSelection(match.match_id)}
                    />
                    <strong>{match.granth_title}</strong>
                    <span style={{ opacity: 0.8 }}>page {match.page_number}</span>
                    <a href={viewerHref} target="_blank" rel="noreferrer">
                      Open page
                    </a>
                  </span>
                  <span style={{ fontSize: 15, lineHeight: 1.6, opacity: 0.92, whiteSpace: "pre-wrap" }}>
                    {renderHighlightedContext(match.context)}
                  </span>
                </label>
              );
            })}
          </div>
        ) : null}

        {history.length > 0 ? (
          <div
            style={{
              border: "1px solid #d5dae2",
              borderRadius: 12,
              background: "#fafbfc",
              padding: 10,
              maxHeight: 300,
              overflow: "auto",
              display: "grid",
              gap: 8,
            }}
          >
            {history.map((item) => (
              <div
                key={item.id}
                style={{
                  padding: "8px 10px",
                  borderRadius: 8,
                  border: "1px solid #d8dde6",
                  background: "#fff",
                  display: "grid",
                  gap: 4,
                }}
              >
                <div style={{ fontSize: 15, fontWeight: 700 }}>
                  #{item.id} {item.granth_title} page {item.page_number}
                </div>
                <div style={{ fontSize: 15, lineHeight: 1.55 }}>
                  <strong>{item.old_word}</strong> → <strong>{item.new_word}</strong> at {item.changed_at}
                </div>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <button
                    type="button"
                    onClick={() => void revertHistoryChange(item.id)}
                    style={{ padding: "8px 11px", borderRadius: 8, border: "1px solid #c7cfd9", background: "#fff", fontSize: 15, cursor: "pointer" }}
                  >
                    Revert this change
                  </button>
                </div>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    </section>
  );
}
