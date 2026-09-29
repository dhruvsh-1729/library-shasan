// Line icons for the search light table: one 1.6px stroke, round joins, drawn
// on a 20px grid so they sit on the same optical line as Archivo's caps.

type IconName = "search" | "page" | "pages" | "table" | "close" | "check" | "loupe" | "text" | "chevron" | "export";

const PATHS: Record<IconName, string> = {
  search: "M8.8 15.1a6.3 6.3 0 1 1 0-12.6 6.3 6.3 0 0 1 0 12.6ZM13.3 13.3 17.5 17.5",
  loupe: "M8.5 14.2a5.7 5.7 0 1 1 0-11.4 5.7 5.7 0 0 1 0 11.4ZM12.6 12.6l4.9 4.9M6 6.4a3.2 3.2 0 0 1 2.6-1.3",
  page: "M5 2.5h6.7L15 5.8V17.5H5ZM11.5 2.7V6H15M7.5 9.5h5M7.5 12.5h5",
  pages: "M6.5 5V2.5h6.3L16 5.7V15h-2.5M4 5h7.3L14 7.7V17.5H4ZM6.5 11h5M6.5 14h5",
  table: "M3 4h14v12H3ZM3 8h14M3 12h14M8 4v12",
  text: "M4 4.5h12M4 8h12M4 11.5h12M4 15h7",
  close: "M5 5l10 10M15 5 5 15",
  check: "M4.5 10.5 8.2 14 15.5 6",
  chevron: "M7.5 5l5 5-5 5",
  export: "M10 3v9M6.3 8.5 10 12.2l3.7-3.7M4 14v3h12v-3",
};

export function LightTableIcon({ name, size = 18, title }: { name: IconName; size?: number; title?: string }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 20 20"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.6}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden={title ? undefined : true}
      role={title ? "img" : undefined}
      className="ltIcon"
    >
      {title ? <title>{title}</title> : null}
      <path d={PATHS[name]} />
    </svg>
  );
}
