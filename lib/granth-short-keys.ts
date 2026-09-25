// Short, readable ids for granths in /search URLs ("?in=215,296"), in place of the
// long document custom ids. A granth's key is its leading book number ("215",
// "440-441"); where two granths share a number the first two name parts are used
// ("429_C000391"), and a granth with no number falls back to its id's hash.
// Keys are assigned over the whole catalog, so the same granth always gets the
// same key whatever page or filter it is listed under.

export type ShortKeyInput = {
  id: string;
  /** File or path the granth's number is read from. */
  source: string;
};

const NUMBER_TOKEN = /^\d{1,4}(?:-\d{1,4})?$/;

function baseName(source: string) {
  const parts = String(source || "").split(/[\\/]/).filter(Boolean);
  return parts[parts.length - 1] ?? "";
}

function hashKey(id: string) {
  const tail = id.replace(/[^A-Za-z0-9]/g, "").slice(-12).toLowerCase();
  return `g${tail || "0"}`;
}

export function assignShortKeys(rows: ShortKeyInput[]): Map<string, string> {
  const tokens = rows.map((row) => baseName(row.source).split(/[_\s]+/).filter(Boolean));
  const first = tokens.map((t) => (t[0] && NUMBER_TOKEN.test(t[0]) ? t[0] : null));
  const count = (keys: (string | null)[]) => {
    const m = new Map<string, number>();
    for (const k of keys) if (k) m.set(k, (m.get(k) ?? 0) + 1);
    return m;
  };
  const firstCount = count(first);
  const second = first.map((k, i) => (k && (firstCount.get(k) ?? 0) > 1 && tokens[i][1] ? `${k}_${tokens[i][1]}` : null));
  const secondCount = count(second);

  const out = new Map<string, string>();
  const used = new Set<string>();
  rows.forEach((row, i) => {
    let key = hashKey(row.id);
    if (first[i] && firstCount.get(first[i]!) === 1) key = first[i]!;
    else if (second[i] && secondCount.get(second[i]!) === 1) key = second[i]!;
    // A hash could in principle repeat; never hand out the same key twice.
    while (used.has(key)) key = `${key}x`;
    used.add(key);
    out.set(row.id, key);
  });
  return out;
}
