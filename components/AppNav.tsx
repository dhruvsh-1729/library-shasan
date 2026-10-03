import Link from "next/link";
import type { ReactNode } from "react";

export type AppPage = "search" | "ask" | "extract" | "vyutpatti" | "library";

const PAGES: Array<{ key: AppPage; href: string; label: string }> = [
  { key: "search", href: "/", label: "Search" },
  { key: "ask", href: "/ask", label: "Ask" },
  { key: "extract", href: "/granth-extractor", label: "Gatha" },
  { key: "vyutpatti", href: "/vyutpatti", label: "Vyutpatti" },
  { key: "library", href: "/library", label: "Library" },
];

/**
 * The same five places on every page, the current one marked, big enough to
 * tap; on a phone they make one even row that is never cut off. `extra` holds
 * a page's own links (the original PDF, a download…), shown after them.
 */
export function AppNav({ current, extra }: { current?: AppPage; extra?: ReactNode }) {
  return (
    <nav className="appNav" aria-label="Pages">
      <div className="appNavMain">
        {PAGES.map((page) => (
          <Link key={page.key} href={page.href} aria-current={current === page.key ? "page" : undefined}>
            {page.label}
          </Link>
        ))}
      </div>
      {extra ? <div className="appNavExtra">{extra}</div> : null}
    </nav>
  );
}
