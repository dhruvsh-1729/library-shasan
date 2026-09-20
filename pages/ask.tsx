import Head from "next/head";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type { GetServerSideProps } from "next";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/auth-options";
import { LANGUAGES } from "@/lib/ai-context";
import { OCR_SEARCH_MODE_OPTIONS, type OCRSearchMode } from "@/lib/ocr-search";

type GranthOption = { granth_key: string; granth_name: string; page_count: number };
type ScopeKind = "gatha" | "pages" | "search";

type SourcePassage = {
  index: number;
  granthKey: string;
  granthName: string;
  pageNumber: number;
  label: string;
  preview: string;
};

type Turn = {
  role: "user" | "assistant";
  content: string;
  scopeLine?: string;
  sources?: SourcePassage[];
  truncated?: boolean;
  error?: boolean;
};

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
  const [matchMode, setMatchMode] = useState<OCRSearchMode>("exact_word");
  const [language, setLanguage] = useState<string>("gujarati");
  const [deepMode, setDeepMode] = useState(false);

  const [question, setQuestion] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const endRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch("/api/ocr-granths?limit=1000");
        const json = await res.json();
        if (!cancelled && Array.isArray(json.items)) setGranths(json.items);
      } catch {
        /* the picker is optional; the form still works by typing a key */
      }
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [turns, loading]);

  const filteredGranths = useMemo(() => {
    const q = granthFilter.trim().toLowerCase();
    const list = q
      ? granths.filter((g) => `${g.granth_key} ${g.granth_name}`.toLowerCase().includes(q))
      : granths;
    return list.slice(0, 60);
  }, [granths, granthFilter]);

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

  function buildScope() {
    if (scopeKind === "gatha") return { kind: "gatha", granthKey, adhikar, gathaFrom, gathaTo };
    if (scopeKind === "pages") return { kind: "pages", granthKey, pageFrom, pageTo };
    return { kind: "search", query, matchMode, granthKeys: granthKey ? [granthKey] : [] };
  }

  async function ask(e: FormEvent) {
    e.preventDefault();
    const q = question.trim();
    if (!q || loading) return;
    setError(null);
    setLoading(true);
    const history = turns.filter((t) => !t.error).slice(-6).map((t) => ({ role: t.role, content: t.content }));
    setTurns((prev) => [...prev, { role: "user", content: q, scopeLine: scopeSummary }]);
    setQuestion("");

    try {
      const res = await fetch("/api/ai/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scope: buildScope(), question: q, language, model: deepMode ? "reasoning" : "fast", history }),
      });
      const json = await res.json();
      if (!res.ok || json.error) {
        setTurns((prev) => [...prev, { role: "assistant", content: json.error || "Something went wrong.", error: true }]);
      } else {
        setTurns((prev) => [...prev, {
          role: "assistant",
          content: json.answer,
          scopeLine: json.context?.summaryLine,
          sources: json.context?.passages ?? [],
          truncated: Boolean(json.context?.truncated),
        }]);
      }
    } catch (err) {
      setTurns((prev) => [...prev, { role: "assistant", content: err instanceof Error ? err.message : "Request failed.", error: true }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <Head><title>Ask the library · Shasan Library</title></Head>
      <div className="askShell">
        <header className="askHeader">
          <div>
            <h1 className="askTitle">Ask the library</h1>
            <p className="askSubtitle">
              Answers are written only from the OCR text of the pages you choose, and every answer lists the pages it used.
            </p>
          </div>
          <nav className="askNav">
            <Link href="/">Library</Link>
            <Link href="/search">Search</Link>
          </nav>
        </header>

        <div className="askLayout">
          <aside className="askPanel">
            <div className="askField">
              <span className="askLabel">What should I read?</span>
              <div className="askSegmented" role="tablist">
                {([["gatha", "Gatha"], ["pages", "Pages"], ["search", "Word"]] as const).map(([k, label]) => (
                  <button key={k} type="button" role="tab" aria-selected={scopeKind === k}
                    className={scopeKind === k ? "isActive" : ""} onClick={() => setScopeKind(k)}>
                    {label}
                  </button>
                ))}
              </div>
            </div>

            {scopeKind !== "search" || granthKey ? null : (
              <p className="askHint">Leave the granth empty to search the whole library.</p>
            )}

            <div className="askField">
              <span className="askLabel">Granth {scopeKind === "search" ? <em>(optional)</em> : null}</span>
              <input className="askInput" placeholder="Filter by name or number…"
                value={granthFilter} onChange={(e) => setGranthFilter(e.target.value)} />
              <select className="askSelect" value={granthKey} onChange={(e) => setGranthKey(e.target.value)}>
                <option value="">{scopeKind === "search" ? "Whole library" : "Choose a granth…"}</option>
                {filteredGranths.map((g) => (
                  <option key={g.granth_key} value={g.granth_key}>
                    {g.granth_key} — {g.granth_name} ({g.page_count}p)
                  </option>
                ))}
              </select>
            </div>

            {scopeKind === "gatha" ? (
              <>
                <div className="askRow">
                  <label className="askField"><span className="askLabel">Gatha from</span>
                    <input className="askInput" inputMode="numeric" value={gathaFrom} onChange={(e) => setGathaFrom(e.target.value)} placeholder="1" /></label>
                  <label className="askField"><span className="askLabel">to <em>(optional)</em></span>
                    <input className="askInput" inputMode="numeric" value={gathaTo} onChange={(e) => setGathaTo(e.target.value)} placeholder="8" /></label>
                </div>
                <label className="askField"><span className="askLabel">Adhikar <em>(optional)</em></span>
                  <input className="askInput" inputMode="numeric" value={adhikar} onChange={(e) => setAdhikar(e.target.value)} placeholder="1" /></label>
              </>
            ) : null}

            {scopeKind === "pages" ? (
              <div className="askRow">
                <label className="askField"><span className="askLabel">Page from</span>
                  <input className="askInput" inputMode="numeric" value={pageFrom} onChange={(e) => setPageFrom(e.target.value)} placeholder="34" /></label>
                <label className="askField"><span className="askLabel">to <em>(optional)</em></span>
                  <input className="askInput" inputMode="numeric" value={pageTo} onChange={(e) => setPageTo(e.target.value)} placeholder="38" /></label>
              </div>
            ) : null}

            {scopeKind === "search" ? (
              <>
                <label className="askField"><span className="askLabel">Word or phrase</span>
                  <input className="askInput" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="હિંસા" /></label>
                <label className="askField"><span className="askLabel">Match</span>
                  <select className="askSelect" value={matchMode} onChange={(e) => setMatchMode(e.target.value as OCRSearchMode)}>
                    {OCR_SEARCH_MODE_OPTIONS.map((o) => <option key={o.mode} value={o.mode}>{o.label}</option>)}
                  </select></label>
              </>
            ) : null}

            <label className="askField"><span className="askLabel">Answer in</span>
              <select className="askSelect" value={language} onChange={(e) => setLanguage(e.target.value)}>
                {LANGUAGES.map((l) => <option key={l.id} value={l.id}>{l.label}</option>)}
              </select></label>

            <label className="askCheck">
              <input type="checkbox" checked={deepMode} onChange={(e) => setDeepMode(e.target.checked)} />
              <span>Think harder <em>(slower, better on subtle questions)</em></span>
            </label>

            <div className="askScopeBox">
              <span className="askLabel">Reading</span>
              <strong>{scopeSummary}</strong>
            </div>
          </aside>

          <section className="askChat">
            <div className="askThread">
              {turns.length === 0 ? (
                <div className="askEmpty">
                  <p><strong>Pick a scope on the left, then ask.</strong></p>
                  <ul>
                    <li>“આ ગાથાનો અર્થ અને સંદર્ભ સમજાવો” — explain this gatha</li>
                    <li>“Summarise these pages in English”</li>
                    <li>“આ શબ્દ ક્યાં ક્યાં વપરાયો છે અને શું અર્થ થાય છે?”</li>
                  </ul>
                </div>
              ) : null}

              {turns.map((t, i) => (
                <article key={i} className={`askTurn ${t.role === "user" ? "isUser" : "isAssistant"} ${t.error ? "isError" : ""}`}>
                  {t.role === "user" && t.scopeLine ? <div className="askTurnScope">{t.scopeLine}</div> : null}
                  <div className="askTurnBody">{t.content}</div>
                  {t.truncated ? <div className="askTruncated">Only part of that scope fitted — narrow it for a fuller answer.</div> : null}
                  {t.sources?.length ? (
                    <details className="askSources">
                      <summary>{t.sources.length} source page{t.sources.length === 1 ? "" : "s"}</summary>
                      <ol>
                        {t.sources.map((s) => (
                          <li key={s.index}>
                            <strong>{s.granthName}</strong> — {s.label}
                            <div className="askSourcePreview">{s.preview}</div>
                          </li>
                        ))}
                      </ol>
                    </details>
                  ) : null}
                </article>
              ))}

              {loading ? <div className="askTurn isAssistant askLoading">Reading the pages…</div> : null}
              <div ref={endRef} />
            </div>

            {error ? <div className="askError">{error}</div> : null}

            <form className="askComposer" onSubmit={ask}>
              <textarea className="askTextarea" rows={2} value={question} placeholder="Ask about the text you selected…"
                onChange={(e) => setQuestion(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void ask(e as unknown as FormEvent); } }} />
              <button type="submit" className="askSend" disabled={loading || !question.trim()}>
                {loading ? "Asking…" : "Ask"}
              </button>
            </form>
          </section>
        </div>
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
