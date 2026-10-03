import Head from "next/head";
import Link from "next/link";
import { useRouter } from "next/router";
import { PdfPageDialog, type PdfDialogTarget } from "@/components/PdfPageDialog";
import { AskSourcePages } from "@/components/AskSourcePages";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import { LANGUAGES } from "@/lib/ai-context";
import { type OCRSearchMode } from "@/lib/ocr-search";
import { prepareRow, rankRows } from "@/lib/granth-name-search";
import { type QueryFormsWord, composeQueries, defaultForms, isRomanQuery } from "@/lib/search-query";
import { toDevanagari } from "@/lib/to-devanagari";
import { AppNav } from "@/components/AppNav";

type GranthOption = { granth_key: string; granth_name: string; page_count: number };
type ScopeKind = "gatha" | "pages" | "search";

type Chapter = { adhikar: number; gathaFrom: number; gathaTo: number; pageStart: number; pageEnd: number };

type ScopePreview = {
  pages: number[];
  verses: Array<{ adhikar: number | null; gatha: number; pageStart: number; pageEnd: number }>;
  needsAdhikar: Chapter[] | null;
  summaryLine: string;
};

type SourcePassage = {
  index: number;
  granthKey: string;
  granthName: string;
  pageNumber: number;
  label: string;
  preview: string;
  /** Where the page opens: its PDF (null for a granth with only OCR text). */
  pdfUrl?: string | null;
};

type Turn = {
  role: "user" | "assistant";
  content: string;
  scopeLine?: string;
  sources?: SourcePassage[];
  truncated?: boolean;
  droppedGathas?: number[];
  needsAdhikar?: Chapter[] | null;
  error?: boolean;
  /** The model's chain of thought (Think harder), shown folded above the answer. */
  thinking?: string;
  /** Still arriving: what the model is doing right now ("Reading part 2 of 3"). */
  streaming?: boolean;
  status?: string;
};

type ChatSummary = { id: string; title: string; updated_at: string; message_count: number };
type StoredMessage = { role: "user" | "assistant"; content: string; meta: Record<string, unknown> };

/** Sources as printed: one line per granth, its pages as ranges ("Sources: Adhyatma Upnishad · pages 2–20"). */
function printedSources(sources: SourcePassage[]) {
  const pages = new Map<string, number[]>();
  for (const s of sources) pages.set(s.granthName, [...(pages.get(s.granthName) ?? []), s.pageNumber]);
  return [...pages].map(([name, list]) => {
    const sorted = [...new Set(list)].sort((a, b) => a - b);
    const ranges: string[] = [];
    for (let i = 0; i < sorted.length; ) {
      let j = i;
      while (j + 1 < sorted.length && sorted[j + 1] === sorted[j] + 1) j += 1;
      ranges.push(i === j ? String(sorted[i]) : `${sorted[i]}–${sorted[j]}`);
      i = j + 1;
    }
    return `${name} · ${sorted.length === 1 ? "page" : "pages"} ${ranges.join(", ")}`;
  });
}

function PrintIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" aria-hidden="true">
      <path d="M5.5 7.5V3h9v4.5M5.5 14H3.5v-5.5a1 1 0 0 1 1-1h11a1 1 0 0 1 1 1V14h-2" />
      <path d="M5.5 11.5h9V17h-9z" />
    </svg>
  );
}

/** "Today", "Yesterday", or a short date, for the history list. */
function chatDay(value: string) {
  const date = new Date(`${value.replace(" ", "T")}Z`);
  if (Number.isNaN(date.getTime())) return "";
  const days = Math.floor((Date.now() - date.getTime()) / 86_400_000);
  if (days <= 0) return "Today";
  if (days === 1) return "Yesterday";
  if (days < 7) return `${days} days ago`;
  return date.toLocaleDateString(undefined, { day: "numeric", month: "short" });
}

/** A saved message as the page shows it. */
function toTurn(message: StoredMessage): Turn {
  const meta = message.meta ?? {};
  return {
    role: message.role,
    content: message.content,
    scopeLine: typeof meta.scopeLine === "string" ? meta.scopeLine : undefined,
    sources: Array.isArray(meta.sources) ? (meta.sources as SourcePassage[]) : undefined,
    truncated: Boolean(meta.truncated),
    droppedGathas: Array.isArray(meta.droppedGathas) ? (meta.droppedGathas as number[]) : [],
    needsAdhikar: Array.isArray(meta.needsAdhikar) ? (meta.needsAdhikar as Chapter[]) : null,
    error: Boolean(meta.error),
    thinking: typeof meta.thinking === "string" ? meta.thinking : undefined,
  };
}

/**
 * A "**" the model opened and never closed (or has not closed yet, while the
 * answer streams) would show as bare asterisks; the unmatched one is dropped.
 */
function tidyMarkdown(text: string) {
  return text
    .split("\n")
    .map((line) => {
      const marks = line.split("**").length - 1;
      if (marks % 2 === 0) return line;
      const at = line.lastIndexOf("**");
      return line.slice(0, at) + line.slice(at + 2);
    })
    .join("\n");
}

/** One page per PDF page, in reading order: two passages from one page show it once. */
function sourcePages(sources: SourcePassage[]) {
  const seen = new Set<string>();
  const pages = [];
  for (const s of sources) {
    const key = `${s.pdfUrl ?? s.granthKey}#${s.pageNumber}`;
    if (seen.has(key)) continue;
    seen.add(key);
    pages.push({ key, granthName: s.granthName, pageNumber: s.pageNumber, label: `p. ${s.pageNumber}`, pdfUrl: s.pdfUrl, source: s });
  }
  return pages;
}

/** Reads like "57–68" but keeps a gap visible, e.g. "57–62, 64–68". */
function describePages(pages: number[]) {
  if (!pages.length) return "no pages";
  const runs: Array<[number, number]> = [];
  for (const page of pages) {
    const last = runs[runs.length - 1];
    if (last && page === last[1] + 1) last[1] = page;
    else runs.push([page, page]);
  }
  return runs.map(([a, b]) => (a === b ? `${a}` : `${a}–${b}`)).join(", ");
}

export default function AskPage() {
  const [granths, setGranths] = useState<GranthOption[]>([]);
  const [granthFilter, setGranthFilter] = useState("");
  const [scopeKind, setScopeKind] = useState<ScopeKind>("gatha");
  const [granthKey, setGranthKey] = useState("");
  const [adhikar, setAdhikar] = useState("");
  const [gathaFrom, setGathaFrom] = useState("");
  const [gathaTo, setGathaTo] = useState("");
  const [pageFrom, setPageFrom] = useState("");
  const [pageTo, setPageTo] = useState("");
  const [query, setQuery] = useState("");
  const [matchMode, setMatchMode] = useState<OCRSearchMode>("sanskrit_forms");
  const [chapters, setChapters] = useState<Chapter[]>([]);
  const [preview, setPreview] = useState<ScopePreview | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [language, setLanguage] = useState<string>("gujarati");
  const [deepMode, setDeepMode] = useState(false);

  const [question, setQuestion] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  // Saved conversations (Turso, per user): the open one, and the list.
  const router = useRouter();
  const [chatId, setChatId] = useState<string | null>(null);
  const [chats, setChats] = useState<ChatSummary[]>([]);
  const [chatsLoaded, setChatsLoaded] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [loadingChat, setLoadingChat] = useState(false);
  const [chatQuery, setChatQuery] = useState("");
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameDraft, setRenameDraft] = useState("");
  const [pdfTarget, setPdfTarget] = useState<PdfDialogTarget | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const endRef = useRef<HTMLDivElement | null>(null);
  const composerRef = useRef<HTMLTextAreaElement | null>(null);
  const [scopeOpen, setScopeOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch("/api/ocr-granths?limit=1000&searchable=1");
        const json = await res.json();
        if (!cancelled && Array.isArray(json.items)) setGranths(json.items);
      } catch {
        /* the picker is optional; the form still works by typing a key */
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Follows a streaming answer down until the reader scrolls up to read;
  // coming back to the bottom starts following again.
  const following = useRef(true);
  useEffect(() => {
    const distance = () => {
      const doc = document.scrollingElement ?? document.documentElement;
      return doc.scrollHeight - doc.scrollTop - window.innerHeight;
    };
    const onUserScroll = () => {
      window.requestAnimationFrame(() => { following.current = distance() < 120; });
    };
    window.addEventListener("wheel", onUserScroll, { passive: true });
    window.addEventListener("touchmove", onUserScroll, { passive: true });
    window.addEventListener("keydown", onUserScroll);
    return () => {
      window.removeEventListener("wheel", onUserScroll);
      window.removeEventListener("touchmove", onUserScroll);
      window.removeEventListener("keydown", onUserScroll);
    };
  }, []);
  useEffect(() => {
    if (following.current) endRef.current?.scrollIntoView({ block: "end" });
  }, [turns, loading]);

  async function refreshChats(query = chatQuery) {
    try {
      const res = await fetch(`/api/ai/chats${query.trim() ? `?q=${encodeURIComponent(query.trim())}` : ""}`);
      const json = await res.json();
      if (res.ok && Array.isArray(json.chats)) setChats(json.chats);
    } catch {
      /* the list is a convenience; asking still works */
    } finally {
      setChatsLoaded(true);
    }
  }
  // The list follows the search box, a moment after typing stops.
  useEffect(() => {
    const timer = window.setTimeout(() => void refreshChats(chatQuery), chatQuery ? 250 : 0);
    return () => window.clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chatQuery]);

  async function saveRename(id: string) {
    const title = renameDraft.trim();
    setRenamingId(null);
    if (!title) return;
    setChats((prev) => prev.map((c) => (c.id === id ? { ...c, title } : c)));
    await fetch(`/api/ai/chats/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title }),
    }).catch(() => undefined);
  }

  /** Opens a cited page: in the PDF viewer, or as OCR text when the granth has no PDF. */
  /**
   * Prints one answer with the question it answers, or the whole chat (index
   * null): Maharaj Saheb reads answers on paper. Everything else on the page
   * is hidden by the print styles in styles/ask.css.
   */
  function printTurns(index: number | null) {
    const root = document.documentElement;
    const marked: Element[] = [];
    const clear = () => {
      root.removeAttribute("data-print");
      for (const el of marked) el.removeAttribute("data-print-target");
      window.removeEventListener("afterprint", clear);
    };
    root.setAttribute("data-print", index === null ? "all" : "one");
    if (index !== null) {
      // The answer and the question just above it.
      const question = turns.slice(0, index).map((t, i) => (t.role === "user" ? i : -1)).filter((i) => i >= 0).pop();
      for (const i of [question, index]) {
        const el = i == null ? null : document.querySelector(`[data-turn="${i}"]`);
        if (el) {
          el.setAttribute("data-print-target", "");
          marked.push(el);
        }
      }
    }
    window.addEventListener("afterprint", clear);
    window.print();
    // Some phone browsers never send afterprint; the dialog has the page by now.
    window.setTimeout(clear, 60_000);
  }

  function openSource(source: SourcePassage) {
    if (source.pdfUrl) {
      setPdfTarget({ pdfUrl: source.pdfUrl, page: source.pageNumber, title: source.granthName });
    } else {
      void router.push(`/ocr-text-viewer?granthKey=${encodeURIComponent(source.granthKey)}&page=${source.pageNumber}`);
    }
  }

  // ?chat=<id> opens a saved conversation, restoring what it was reading.
  const chatParam = typeof router.query.chat === "string" ? router.query.chat : "";
  // Only the chat asked for last may fill the page; an earlier, slower load
  // (or an effect React ran twice) is ignored instead of leaving it spinning.
  const openingChat = useRef("");
  useEffect(() => {
    if (!router.isReady) return;
    if (!chatParam) {
      if (chatId) startNewChat(false);
      return;
    }
    if (chatParam === chatId) return;
    openingChat.current = chatParam;
    const stale = () => openingChat.current !== chatParam;
    setLoadingChat(true);
    (async () => {
      try {
        const res = await fetch(`/api/ai/chats/${encodeURIComponent(chatParam)}`);
        const json = await res.json();
        if (stale()) return;
        if (!res.ok) throw new Error(json.error || "Could not open that chat.");
        const chat = json.chat as { id: string; scope: Record<string, unknown> | null; language: string | null; deep: boolean; messages: StoredMessage[] };
        setChatId(chat.id);
        setTurns(chat.messages.map(toTurn));
        const sc = chat.scope ?? {};
        const kind = sc.kind === "pages" || sc.kind === "search" ? sc.kind : "gatha";
        setScopeKind(kind);
        const keys = Array.isArray(sc.granthKeys) ? (sc.granthKeys as string[]) : [];
        setGranthKey(String(sc.granthKey ?? keys[0] ?? ""));
        setAdhikar(String(sc.adhikar ?? ""));
        setGathaFrom(String(sc.gathaFrom ?? ""));
        setGathaTo(String(sc.gathaTo ?? ""));
        setPageFrom(String(sc.pageFrom ?? ""));
        setPageTo(String(sc.pageTo ?? ""));
        setQuery(String(sc.query ?? ""));
        if (chat.language) setLanguage(chat.language);
        setDeepMode(chat.deep);
        setError(null);
      } catch (e) {
        if (!stale()) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!stale()) setLoadingChat(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [router.isReady, chatParam]);

  function startNewChat(updateUrl = true) {
    setChatId(null);
    setTurns([]);
    setError(null);
    setHistoryOpen(false);
    if (updateUrl && router.query.chat) void router.push("/ask", undefined, { shallow: true });
  }

  function openChat(id: string) {
    setHistoryOpen(false);
    if (id !== chatId) void router.push(`/ask?chat=${encodeURIComponent(id)}`, undefined, { shallow: true });
  }

  async function removeChat(id: string) {
    const res = await fetch(`/api/ai/chats/${encodeURIComponent(id)}`, { method: "DELETE" });
    if (!res.ok) return;
    setChats((prev) => prev.filter((c) => c.id !== id));
    if (id === chatId) startNewChat();
  }

  // Resolve the scope as the form is filled in, so a wrong chapter is visible
  // before the question is asked rather than after the answer comes back.
  useEffect(() => {
    if (scopeKind === "search" || !granthKey) { setPreview(null); setChapters([]); return; }
    let cancelled = false;
    setPreviewing(true);
    const timer = setTimeout(async () => {
      try {
        const res = await fetch("/api/ai/scope", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ granthKey: scopeKind === "gatha" ? granthKey : "", scope: buildScope() }),
        });
        const json = await res.json();
        if (cancelled) return;
        setChapters(Array.isArray(json.chapters) ? json.chapters : []);
        setPreview(json.resolved ?? null);
      } catch {
        if (!cancelled) setPreview(null);
      } finally {
        if (!cancelled) setPreviewing(false);
      }
    }, 250);
    return () => { cancelled = true; clearTimeout(timer); setPreviewing(false); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scopeKind, granthKey, adhikar, gathaFrom, gathaTo, pageFrom, pageTo]);

  // Book names match across scripts and spellings, as on the library page.
  const preparedGranths = useMemo(
    () => granths.map((g) => prepareRow({ source: `${g.granth_key} ${g.granth_name}`, extra: g.granth_name })),
    [granths]
  );
  const filteredGranths = useMemo(() => {
    const q = granthFilter.trim();
    if (!q) return granths.slice(0, 60);
    return rankRows(preparedGranths, q).slice(0, 60).map((i) => granths[i]);
  }, [granths, granthFilter, preparedGranths]);

  const selectedGranth = granths.find((g) => g.granth_key === granthKey);

  const scopeSummary = (() => {
    const name = selectedGranth ? `${selectedGranth.granth_name}` : granthKey || "—";
    if (scopeKind === "gatha") {
      const r = gathaTo && gathaTo !== gathaFrom ? `${gathaFrom}–${gathaTo}` : gathaFrom || "?";
      return `${name} · gatha ${r}${adhikar ? ` · adhikar ${adhikar}` : ""}`;
    }
    if (scopeKind === "pages") {
      const r = pageTo && pageTo !== pageFrom ? `${pageFrom}–${pageTo}` : pageFrom || "?";
      return `${name} · pages ${r}`;
    }
    return query ? `“${query}” · ${granthKey ? name : "whole library"}` : "—";
  })();

  function buildScope(searchQuery = query) {
    if (scopeKind === "gatha") return { kind: "gatha", granthKey, adhikar, gathaFrom, gathaTo };
    if (scopeKind === "pages") return { kind: "pages", granthKey, pageFrom, pageTo };
    return { kind: "search", query: searchQuery, matchMode, granthKeys: granthKey ? [granthKey] : [] };
  }

  /**
   * A word typed in English letters ("hinsa") is looked for as its Devanagari
   * spelling with the most pages in the library (as /search offers it);
   * Devanagari and Gujarati go as typed.
   */
  async function searchQueryInScript() {
    const text = query.trim();
    if (scopeKind !== "search" || !isRomanQuery(text)) return text;
    try {
      const res = await fetch(`/api/query-forms?q=${encodeURIComponent(text)}`);
      const json = (await res.json()) as { words?: QueryFormsWord[] };
      const spelled = res.ok ? composeQueries(defaultForms(json.words ?? []))[0] : "";
      if (spelled) return spelled;
    } catch {
      // the local reading below
    }
    return toDevanagari(text);
  }

  async function ask(e: FormEvent) {
    e.preventDefault();
    const q = question.trim();
    if (!q || loading) return;
    setError(null);
    setLoading(true);
    const history = turns.filter((t) => !t.error).slice(-6).map((t) => ({ role: t.role, content: t.content }));
    following.current = true;
    setTurns((prev) => [...prev, { role: "user", content: q, scopeLine: scopeSummary }]);
    setQuestion("");

    // The answer arrives as a stream of events (see pages/api/ai/ask.ts) into
    // one assistant turn that fills in as they come.
    setTurns((prev) => [...prev, { role: "assistant", content: "", streaming: true, status: deepMode ? "Thinking" : "Reading the pages" }]);
    const patch = (fn: (t: Turn) => Turn) =>
      setTurns((prev) => {
        const next = prev.slice();
        const i = next.length - 1;
        if (i >= 0 && next[i].role === "assistant") next[i] = fn(next[i]);
        return next;
      });

    const adoptChat = (id: unknown) => {
      if (typeof id !== "string" || !id) return;
      if (id !== chatId) {
        setChatId(id);
        void router.replace(`/ask?chat=${encodeURIComponent(id)}`, undefined, { shallow: true });
      }
      void refreshChats();
    };

    let finished = false;
    const handle = (ev: Record<string, unknown>) => {
      switch (ev.type) {
        case "context": {
          const ctx = ev.context as { summaryLine?: string; passages?: SourcePassage[]; truncated?: boolean; droppedGathas?: number[] };
          patch((t) => ({
            ...t,
            scopeLine: ctx.summaryLine,
            sources: ctx.passages ?? [],
            truncated: Boolean(ctx.truncated),
            droppedGathas: Array.isArray(ctx.droppedGathas) ? ctx.droppedGathas : [],
          }));
          break;
        }
        case "status":
          patch((t) => ({ ...t, status: String(ev.text ?? "") }));
          break;
        case "thinking":
          patch((t) => ({ ...t, thinking: (t.thinking ?? "") + String(ev.delta ?? ""), status: t.status || "Thinking" }));
          break;
        case "answer":
          patch((t) => ({ ...t, content: t.content + String(ev.delta ?? ""), status: "Writing" }));
          break;
        case "restart":
          patch((t) => ({ ...t, content: "", status: "Answering directly" }));
          break;
        case "done":
          finished = true;
          adoptChat(ev.chatId);
          patch((t) => ({ ...t, content: String(ev.answer ?? t.content), streaming: false, status: undefined }));
          break;
        case "error":
          finished = true;
          adoptChat(ev.chatId);
          patch((t) => ({
            ...t,
            content: String(ev.error || "Something went wrong."),
            needsAdhikar: Array.isArray(ev.needsAdhikar) ? (ev.needsAdhikar as Chapter[]) : null,
            error: true,
            streaming: false,
            status: undefined,
          }));
          break;
      }
    };

    try {
      const searchQuery = await searchQueryInScript();
      const res = await fetch("/api/ai/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          scope: buildScope(searchQuery),
          question: q,
          language,
          model: deepMode ? "reasoning" : "fast",
          history,
          chatId,
          scopeLine: scopeSummary,
          stream: true,
        }),
      });

      // A request refused before the answer starts comes back as plain JSON.
      if (!(res.headers.get("content-type") ?? "").includes("ndjson") || !res.body) {
        const json = await res.json().catch(() => ({}));
        handle({ type: "error", error: json.error || `Request failed (${res.status}).` });
      } else {
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        const take = (line: string) => {
          if (!line.trim()) return;
          try { handle(JSON.parse(line)); } catch { /* a torn line; the next ones still count */ }
        };
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let nl: number;
          while ((nl = buffer.indexOf("\n")) >= 0) {
            take(buffer.slice(0, nl));
            buffer = buffer.slice(nl + 1);
          }
        }
        take(buffer);
        if (!finished) handle({ type: "error", error: "The connection dropped before the answer finished. Please ask again." });
      }
    } catch (err) {
      handle({ type: "error", error: err instanceof Error ? err.message : "Request failed." });
    } finally {
      setLoading(false);
    }
  }

  const scopeReady = scopeKind === "search" || Boolean(granthKey);
  const languageLabel = LANGUAGES.find((l) => l.id === language)?.label ?? language;
  const scopePill = (() => {
    const name = selectedGranth?.granth_name ?? "";
    if (scopeKind === "search") return query ? `“${query}”${name ? ` in ${name}` : ""}` : "Choose what to read";
    if (!name) return "Choose what to read";
    if (scopeKind === "gatha") {
      const r = gathaTo && gathaTo !== gathaFrom ? `${gathaFrom}–${gathaTo}` : gathaFrom;
      return r ? `${name} · gatha ${r}` : name;
    }
    const r = pageTo && pageTo !== pageFrom ? `${pageFrom}–${pageTo}` : pageFrom;
    return r ? `${name} · pages ${r}` : name;
  })();

  function pickSuggestion(text: string) {
    setQuestion(text);
    if (!scopeReady) setScopeOpen(true);
    composerRef.current?.focus();
  }

  const suggestions = [
    "આ ગાથાનો અર્થ અને સંદર્ભ સમજાવો",
    "Summarise these pages in English",
    "इस गाथा का अर्थ सरल भाषा में समझाइए",
  ];

  return (
    <>
      <Head><title>Ask · Shasan Library</title></Head>
      <div className={`ch${historyOpen ? " isHistoryOpen" : ""}`}>
        <aside className="chHistory" aria-label="Your chats">
          <button type="button" className="chNewChat" onClick={() => startNewChat()}>
            <svg width="16" height="16" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true"><path d="M10 4v12M4 10h12" /></svg>
            New chat
          </button>
          <input
            className="chHistorySearch"
            value={chatQuery}
            onChange={(e) => setChatQuery(e.target.value)}
            placeholder="Search chats"
            aria-label="Search chats"
          />
          <nav className="chHistoryList">
            {!chatsLoaded ? null : chats.length === 0 ? (
              <p className="chHistoryEmpty">{chatQuery ? "No chat matches." : "Your chats will appear here."}</p>
            ) : (
              chats.map((c, i) => {
                const day = chatDay(c.updated_at);
                const showDay = i === 0 || chatDay(chats[i - 1].updated_at) !== day;
                return (
                  <div key={c.id}>
                    {showDay ? <p className="chHistoryDay">{day}</p> : null}
                    <div className={`chHistoryItem${c.id === chatId ? " isOn" : ""}`}>
                      {renamingId === c.id ? (
                        <input
                          className="chRename"
                          autoFocus
                          value={renameDraft}
                          onChange={(e) => setRenameDraft(e.target.value)}
                          onBlur={() => void saveRename(c.id)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter" && !e.nativeEvent.isComposing) void saveRename(c.id);
                            if (e.key === "Escape") setRenamingId(null);
                          }}
                          aria-label="Chat name"
                        />
                      ) : (
                        <button type="button" onClick={() => openChat(c.id)} title={c.title}>
                          {c.title}
                        </button>
                      )}
                      <button type="button" className="chHistoryDelete isRename" aria-label={`Rename “${c.title}”`}
                        onClick={() => { setRenamingId(c.id); setRenameDraft(c.title); }}>
                        <svg width="15" height="15" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M4 16l1-4 8.5-8.5a2 2 0 0 1 3 3L8 15ZM12 5l3 3" /></svg>
                      </button>
                      <button type="button" className="chHistoryDelete" aria-label={`Delete “${c.title}”`}
                        onClick={() => { if (window.confirm("Delete this chat?")) void removeChat(c.id); }}>
                        <svg width="15" height="15" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M4 6h12M8 6V4h4v2M6 6l1 10h6l1-10" /></svg>
                      </button>
                    </div>
                  </div>
                );
              })
            )}
          </nav>
        </aside>
        {historyOpen ? <button type="button" className="chScrim" aria-label="Close chats" onClick={() => setHistoryOpen(false)} /> : null}

        <div className="chBody">
        <header className="chTop">
          <div className="chTopLeft">
            <button type="button" className="chHistoryToggle" aria-label="Your chats" aria-expanded={historyOpen} onClick={() => setHistoryOpen((o) => !o)}>
              <svg width="18" height="18" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" aria-hidden="true"><path d="M3.5 5.5h13M3.5 10h13M3.5 14.5h13" /></svg>
            </button>
            <strong className="chBrand">Ask the library</strong>
            {turns.some((t) => t.role === "assistant" && !t.streaming && !t.error) ? (
              <button type="button" className="chPrintAll" onClick={() => printTurns(null)} aria-label="Print the whole chat" title="Print the whole chat">
                <PrintIcon />
              </button>
            ) : null}
          </div>
          <AppNav current="ask" />
        </header>

        <main className="chMain">
          {loadingChat ? (
            <section className="chWelcome"><span className="chTyping" aria-label="Opening chat"><span /><span /><span /></span></section>
          ) : turns.length === 0 && !loading ? (
            <section className="chWelcome">
              <h1>What would you like to understand?</h1>
              <div className="chSuggestions">
                {suggestions.map((s) => (
                  <button key={s} type="button" className="chSuggestion" onClick={() => pickSuggestion(s)}>
                    {s}
                  </button>
                ))}
              </div>
            </section>
          ) : (
            <section className="chThread" aria-live="polite">
              {turns.map((t, i) => (
                <article key={i} data-turn={i} className={`chTurn ${t.role === "user" ? "isUser" : "isAssistant"}${t.error ? " isError" : ""}`}>
                  {t.role === "assistant" ? <span className="chAvatar" aria-hidden="true">ग्र</span> : null}
                  <div className="chBubble">
                    {t.role === "user" && t.scopeLine ? <div className="chTurnScope">{t.scopeLine}</div> : null}
                    {t.thinking ? (
                      <details className="chThinking" open={t.streaming && !t.content ? true : undefined}>
                        <summary>{t.streaming && !t.content ? (t.status || "Thinking") + "…" : "Thought process"}</summary>
                        <div className="chThinkingText">{t.thinking}</div>
                      </details>
                    ) : null}
                    {t.streaming && !t.content ? (
                      <div className="chStatus"><span className="chTyping" aria-hidden="true"><span /><span /><span /></span>{t.thinking ? null : <span>{t.status}…</span>}</div>
                    ) : (
                      t.role === "assistant" && !t.error ? (
                        // Answers come as Markdown: headings, bold, lists and tables are drawn,
                        // not shown as asterisks. Raw HTML in an answer is never rendered.
                        <div className={`chText chMd${t.streaming ? " isStreaming" : ""}`}>
                          <ReactMarkdown remarkPlugins={[remarkGfm]} components={{
                            a: ({ node: _node, ...props }) => <a {...props} target="_blank" rel="noopener noreferrer" />,
                          }}>{tidyMarkdown(t.content)}</ReactMarkdown>
                        </div>
                      ) : (
                        <div className="chText">{t.content}</div>
                      )
                    )}
                    {t.needsAdhikar?.length ? (
                      <div className="chChapters">
                        <span>Which chapter?</span>
                        {t.needsAdhikar.map((c) => (
                          <button key={c.adhikar} type="button" onClick={() => setAdhikar(String(c.adhikar))}
                            title={`gathas ${c.gathaFrom}–${c.gathaTo}, pages ${c.pageStart}–${c.pageEnd}`}>
                            {c.adhikar}
                          </button>
                        ))}
                      </div>
                    ) : null}
                    {t.droppedGathas?.length ? (
                      <p className="chNote">Gatha {[...new Set(t.droppedGathas)].join(", ")} was not read. Ask about it separately.</p>
                    ) : t.truncated ? (
                      <p className="chNote">Only part of this was read. Choose fewer gathas or pages for a fuller answer.</p>
                    ) : null}
                    {t.role === "assistant" && !t.streaming && !t.error && t.content ? (
                      <button type="button" className="chPrint" onClick={() => printTurns(i)}>
                        <PrintIcon />
                        Print this answer
                      </button>
                    ) : null}
                    {t.role === "assistant" && t.sources?.length ? (
                      // On paper the sources are a plain list (the page thumbnails are not printed).
                      <ol className="chPrintSources">
                        {printedSources(t.sources).map((line) => (
                          <li key={line}>{line}</li>
                        ))}
                      </ol>
                    ) : null}
                    {t.role === "assistant" && t.sources?.length ? (
                      <AskSourcePages
                        pages={sourcePages(t.sources)}
                        onOpen={(p) => openSource((p as ReturnType<typeof sourcePages>[number]).source)}
                      />
                    ) : null}
                    {t.sources?.length ? (
                      <details className="chSources">
                        <summary>
                          {t.sources.slice(0, 4).map((s) => (
                            <button
                              key={s.index}
                              type="button"
                              className="chSourceChip"
                              title={`Open page ${s.pageNumber} of ${s.granthName}`}
                              onClick={(e) => { e.preventDefault(); openSource(s); }}
                            >
                              p. {s.pageNumber}
                            </button>
                          ))}
                          {t.sources.length > 4 ? <span className="chSourceChip">+{t.sources.length - 4}</span> : null}
                          <span className="chSourcesLabel">Sources</span>
                        </summary>
                        <ol>
                          {t.sources.map((s) => (
                            <li key={s.index}>
                              <button type="button" className="chSourceOpen" onClick={() => openSource(s)}>
                                {s.granthName} · {s.label}
                              </button>
                              <p>{s.preview}</p>
                            </li>
                          ))}
                        </ol>
                      </details>
                    ) : null}
                  </div>
                </article>
              ))}
              <div ref={endRef} />
            </section>
          )}
        </main>

        <footer className="chDock">
          {error ? <p className="chError">{error}</p> : null}

          {scopeOpen ? (
            <div className="chScope" role="dialog" aria-label="What to read">
              <div className="chScopeHead">
                <div className="chSegment" role="tablist">
                  {([["gatha", "Gatha"], ["pages", "Pages"], ["search", "A word"]] as const).map(([k, label]) => (
                    <button key={k} type="button" role="tab" aria-selected={scopeKind === k}
                      className={scopeKind === k ? "isOn" : ""} onClick={() => setScopeKind(k)}>
                      {label}
                    </button>
                  ))}
                </div>
                <button type="button" className="chDone" onClick={() => setScopeOpen(false)}>Done</button>
              </div>

              {scopeKind === "search" ? (
                <input className="chField" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Word: हिंसा, હિંસા or hinsa" autoCorrect="off" autoCapitalize="off" spellCheck={false} />
              ) : null}

              <div className="chBookPicker">
                <input className="chField" placeholder={scopeKind === "search" ? "In one book (optional)" : "Find a book"}
                  value={granthFilter} onChange={(e) => setGranthFilter(e.target.value)} />
                {selectedGranth && !granthFilter ? (
                  <div className="chBookChosen">
                    <span>{selectedGranth.granth_name}</span>
                    <button type="button" onClick={() => { setGranthKey(""); setAdhikar(""); setChapters([]); setPreview(null); }}>Change</button>
                  </div>
                ) : (
                  <ul className="chBookList">
                    {filteredGranths.slice(0, 30).map((g) => (
                      <li key={g.granth_key}>
                        <button type="button" onClick={() => { setGranthKey(g.granth_key); setGranthFilter(""); setAdhikar(""); setChapters([]); setPreview(null); }}>
                          {g.granth_name}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              {scopeKind === "gatha" && granthKey ? (
                <div className="chRow">
                  {chapters.length > 1 ? (
                    <select className="chField" value={adhikar} onChange={(e) => setAdhikar(e.target.value)} aria-label="Chapter">
                      <option value="">Chapter…</option>
                      {chapters.map((c) => (
                        <option key={c.adhikar} value={String(c.adhikar)}>
                          Chapter {c.adhikar} · gathas {c.gathaFrom}–{c.gathaTo}
                        </option>
                      ))}
                    </select>
                  ) : null}
                  <input className="chField" inputMode="numeric" value={gathaFrom} onChange={(e) => setGathaFrom(e.target.value)} placeholder="Gatha" aria-label="Gatha from" />
                  <input className="chField" inputMode="numeric" value={gathaTo} onChange={(e) => setGathaTo(e.target.value)} placeholder="to (optional)" aria-label="Gatha to" />
                </div>
              ) : null}

              {scopeKind === "pages" && granthKey ? (
                <div className="chRow">
                  <input className="chField" inputMode="numeric" value={pageFrom} onChange={(e) => setPageFrom(e.target.value)} placeholder="Page" aria-label="Page from" />
                  <input className="chField" inputMode="numeric" value={pageTo} onChange={(e) => setPageTo(e.target.value)} placeholder="to (optional)" aria-label="Page to" />
                </div>
              ) : null}

              {scopeKind !== "search" && granthKey ? (
                <p className="chScopeStatus">
                  {previewing
                    ? "Finding the pages…"
                    : preview?.needsAdhikar?.length
                      ? "Choose a chapter"
                      : preview?.pages.length
                        ? `Reads ${preview.pages.length} page${preview.pages.length === 1 ? "" : "s"} (${describePages(preview.pages)})`
                        : preview?.summaryLine ?? ""}
                </p>
              ) : null}
            </div>
          ) : null}

          <form className="chComposer" onSubmit={ask}>
            <textarea
              ref={composerRef}
              className="chInput"
              rows={1}
              value={question}
              placeholder="Ask anything about the text…"
              onChange={(e) => {
                setQuestion(e.target.value);
                e.target.style.height = "auto";
                e.target.style.height = `${Math.min(e.target.scrollHeight, 220)}px`;
              }}
              onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) { e.preventDefault(); void ask(e as unknown as FormEvent); } }}
            />
            <div className="chTools">
              <button type="button" className={`chPill${scopeReady ? " isSet" : ""}`} onClick={() => setScopeOpen((o) => !o)} aria-expanded={scopeOpen}>
                <svg width="16" height="16" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M3 4.5h5a2 2 0 0 1 2 2V16a1.5 1.5 0 0 0-1.5-1.5H3ZM17 4.5h-5a2 2 0 0 0-2 2V16a1.5 1.5 0 0 1 1.5-1.5H17Z" /></svg>
                <span>{scopePill}</span>
              </button>
              <label className="chPill chSelectPill">
                <select value={language} onChange={(e) => setLanguage(e.target.value)} aria-label="Answer language">
                  {LANGUAGES.map((l) => <option key={l.id} value={l.id}>{l.label}</option>)}
                </select>
                <span aria-hidden="true">{languageLabel}</span>
              </label>
              <button type="button" className={`chPill${deepMode ? " isSet" : ""}`} aria-pressed={deepMode} onClick={() => setDeepMode((d) => !d)}
                title="Slower, more careful answers">
                Think harder
              </button>
              <button type="submit" className="chSend" disabled={loading || !question.trim() || !scopeReady}
                aria-label="Send" title={scopeReady ? "Send" : "Choose what to read first"}>
                <svg width="18" height="18" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M10 16V4M5 9l5-5 5 5" /></svg>
              </button>
            </div>
          </form>
        </footer>
        </div>
        <PdfPageDialog target={pdfTarget} onClose={() => setPdfTarget(null)} />
      </div>
    </>
  );
}

export const getServerSideProps: GetServerSideProps = async (ctx) => {
  const session = await getServerSession(ctx.req, ctx.res, authOptions);
  if (!session?.user || session.user.status !== "approved") {
    return { redirect: { destination: `/auth/signin?callbackUrl=${encodeURIComponent("/ask")}`, permanent: false } };
  }
  return { props: {} };
};
