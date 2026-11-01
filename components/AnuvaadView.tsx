import { useEffect, useMemo, useRef, useState } from "react";
import { parseAnuvaad } from "@/lib/anuvaad/markup";

// A Gujarati anuvaad as it reads on the page (the same headings, વિષય line,
// bullets and bold labels as the PDF), with an "Edit" switch that shows the
// markup itself for corrections. The PDF is made from the edited markup.

export function AnuvaadView({
  markup,
  onChange,
  literal,
}: {
  markup: string;
  onChange?: (markup: string) => void;
  /** The sentence-by-sentence translation it was written from, shown for checking. */
  literal?: string;
}) {
  const [editing, setEditing] = useState(false);
  const blocks = useMemo(() => parseAnuvaad(markup), [markup]);
  const area = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    const el = area.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${el.scrollHeight + 2}px`;
  }, [editing, markup]);

  return (
    <div className="anView">
      {editing ? (
        <textarea
          ref={area}
          className="anEdit indic"
          value={markup}
          lang="gu"
          spellCheck={false}
          aria-label="Edit the Gujarati anuvaad"
          onChange={(e) => onChange?.(e.target.value)}
        />
      ) : (
        <article className="anDoc indic" lang="gu">
          {blocks.map((b, i) => {
            switch (b.kind) {
              case "title":
                return <h3 key={i} className="anTitle">{b.text}</h3>;
              case "vishay":
                return (
                  <p key={i} className="anVishay">
                    <strong>વિષય:</strong> {b.text}
                  </p>
                );
              case "section":
                return <h4 key={i} className="anSection">{b.text}</h4>;
              case "point":
                return (
                  <p key={i} className={`anPoint is-${b.level}`}>
                    {b.label ? <strong>{b.label}: </strong> : null}
                    {b.text}
                  </p>
                );
              default:
                return <p key={i} className="anPara">{b.text}</p>;
            }
          })}
        </article>
      )}
      <div className="anViewFoot">
        {onChange ? (
          <button type="button" className="vyLink" onClick={() => setEditing((e) => !e)}>
            {editing ? "Done editing" : "Edit"}
          </button>
        ) : null}
        {editing ? <span className="vyHint"># heading · વિષય: … · ## part · - **label:** text · two spaces before “-” for a sub-point</span> : null}
      </div>
      {literal ? (
        <details className="vyDetails anLiteral">
          <summary>Word-for-word translation (for checking)</summary>
          <p className="indic" lang="gu">{literal}</p>
        </details>
      ) : null}
    </div>
  );
}
