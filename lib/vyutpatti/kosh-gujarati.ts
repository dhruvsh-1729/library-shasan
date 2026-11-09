// The Gujarati a kosh itself prints for an entry, as the Gujarati anuvaad of
// that entry (Maharaj Saheb, 9 Nov 2026: the anuvaad is to come from the kosh).
//
// Shabda Ratna Mahodadhi and Abhidhāna Vyutpatti Prakriyā Kosh print their
// meanings in Gujarati. The library's old page text often read that Gujarati
// as Devanagari ("डिंसानो अभाव … न ६२वी" for "હિંસાનો અભાવ … ન કરવી"), so the
// page is read again, as Gujarati, and kept: each kosh page is read once.
// The entry is found on the page by its headword (and, among same-headword
// entries, by its derivation), and the Gujarati printed for it is taken as is.

import { getTursoClient } from "@/lib/turso";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { headwordKeys, headwordLines, vyutpattiKosh } from "@/lib/kosh-headwords.mjs";
import { buildKoshPagesPdf } from "@/lib/vyutpatti/kosh-pages";
import { ocrPdf } from "@/lib/anuvaad/ocr";

/** Koshes that print their meanings in Gujarati. */
export function printsGujarati(granthKey: string | null | undefined) {
  const family = vyutpattiKosh(String(granthKey ?? ""))?.family;
  return family === "srm" || family === "avpk";
}

let ready: Promise<unknown> | null = null;
function ensureTable() {
  // a read first: creating is a write, which waits behind any long write elsewhere
  ready ??= (async () => {
    const client = getTursoClient();
    const found = await client.execute(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'kosh_page_gujarati'`);
    if (found.rows.length) return;
    await client.execute(
      `CREATE TABLE IF NOT EXISTS kosh_page_gujarati (
        granth_key TEXT NOT NULL,
        page_number INTEGER NOT NULL,
        content TEXT NOT NULL,
        read_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (granth_key, page_number)
      )`
    );
  })().catch((error) => {
    ready = null;
    throw error;
  });
  return ready;
}

const inFlight = new Map<string, Promise<string>>();

/** The pages' text read as Gujarati: from the store, the rest read now (all in one go) and stored. */
export async function koshPagesGujarati(pages: Array<{ granthKey: string; page: number }>): Promise<Map<string, string>> {
  await ensureTable();
  const client = getTursoClient();
  const id = (p: { granthKey: string; page: number }) => `${p.granthKey}:${p.page}`;
  const wanted = [...new Map(pages.map((p) => [id(p), p])).values()];
  const out = new Map<string, string>();
  if (!wanted.length) return out;
  const stored = await client.execute({
    sql: `SELECT granth_key, page_number, content FROM kosh_page_gujarati WHERE (granth_key, page_number) IN (${wanted.map(() => "(?, ?)").join(",")})`,
    args: wanted.flatMap((p) => [p.granthKey, p.page]),
  });
  for (const row of stored.rows) out.set(`${row.granth_key}:${row.page_number}`, String(row.content ?? ""));

  const missing = wanted.filter((p) => !out.has(id(p)) && !inFlight.has(id(p)));
  if (missing.length) {
    const job = (async () => {
      const pdf = await buildKoshPagesPdf(missing);
      if (!pdf) return [] as string[];
      // Gujarati reading keeps the Sanskrit headwords and derivations in Devanagari
      return ocrPdf(pdf.bytes, "gu-IN");
    })();
    missing.forEach((p, i) =>
      inFlight.set(
        id(p),
        job.then(async (texts) => {
          const text = String(texts[i] ?? "").normalize("NFC");
          if (text.trim()) {
            await client.execute({
              sql: `INSERT INTO kosh_page_gujarati (granth_key, page_number, content) VALUES (?, ?, ?)
                    ON CONFLICT(granth_key, page_number) DO UPDATE SET content = excluded.content, read_at = CURRENT_TIMESTAMP`,
              args: [p.granthKey, p.page, text],
            });
          }
          return text;
        }).finally(() => inFlight.delete(id(p)))
      )
    );
  }
  await Promise.all(
    wanted
      .filter((p) => !out.has(id(p)))
      .map(async (p) => {
        const text = await inFlight.get(id(p))?.catch(() => "");
        if (text) out.set(id(p), text);
      })
  );
  return out;
}

const GUJ = /[઀-૿]/;
const DEV_RUN = /[ऀ-ॣ०-ॿ]/;

/** Letter pairs shared by two strings (folded), for telling same-headword entries apart by their derivation. */
function overlap(a: string, b: string) {
  const x = foldSanskrit(a).replace(/\s+/g, "");
  const y = foldSanskrit(b).replace(/\s+/g, "");
  if (x.length < 2 || y.length < 2) return 0;
  const pairs = new Set<string>();
  for (let i = 0; i + 1 < y.length; i += 1) pairs.add(y.slice(i, i + 2));
  let hit = 0;
  for (let i = 0; i + 1 < x.length; i += 1) if (pairs.has(x.slice(i, i + 2))) hit += 1;
  return hit / (x.length - 1);
}

/**
 * The Gujarati printed for one entry on a kosh page read as Gujarati: the
 * entry's text after its headword, label and derivation, keeping the Gujarati
 * (the kosh's own numbering "૧." "૨." included) and leaving out the Sanskrit
 * quotations, sūtras and synonym lists printed between. "" when the entry is
 * not on the page or prints no Gujarati.
 */
export function gujaratiOfEntry(pageText: string, granthKey: string, head: string, hint = "") {
  const kosh = vyutpattiKosh(granthKey);
  if (!kosh) return "";
  const lines = String(pageText ?? "").normalize("NFC").replace(/\*\*/g, "").split("\n");
  const starts = headwordLines(kosh.format, lines.join("\n")) as Array<{ line: number; head: string }>;
  const want = new Set(headwordKeys(head));
  const found = starts
    .map((s, i) => ({ ...s, end: i + 1 < starts.length ? starts[i + 1].line : lines.length }))
    .filter((s) => headwordKeys(s.head).some((k: string) => want.has(k)))
    .map((s) => ({ ...s, text: lines.slice(s.line, s.end).join(" ").replace(/\s+/g, " ").trim() }));
  if (!found.length) return "";
  // the headword printed exactly as asked comes before a look-alike sharing its stem (हिंसा, not हिंस्रा);
  // among those, the entry whose derivation is the one the vyutpatti read
  const exact = found.filter((s) => foldSanskrit(s.head) === foldSanskrit(head));
  const pool = exact.length ? exact : found;
  const best = pool.length === 1 || !hint ? pool[0] : pool.reduce((a, b) => (overlap(hint, b.text) > overlap(hint, a.text) ? b : a));
  return printedGujarati(best.text, kosh.family);
}

/** The Gujarati of an entry's text (see gujaratiOfEntry). */
export function printedGujarati(entryText: string, family: string) {
  // the scanner's footer, printed across some pages
  let text = entryText
    .replace(/Jain\s+Educat\w*\s+Inte\w*|For\s+Private\s*(?:&|and)\s*Personal\s+Use\s+Only|www\.jainelibrary\.org/gi, " ")
    .replace(/\s{2,}/g, " ");
  if (family === "avpk") {
    // चोर-पुं-३८१-ચોરી કરનાર, ચોર. ☐ synonyms… * derivation…
    // चोर-पुं-३८१-…, जीरण-पुं-४२२-शे० (१०३)-…, अहिंसा-સ્ત્રી-૮૨ …, जीर्ण-पुं न०-१४४૮-…
    text = text.replace(
      /^[^-–]+[-–]\s*[^\s\-–]{1,8}(?:\s*[-–]?\s*(?:न|ન|पुं|પું)[.०૦]?)?\s*[-–]\s*[०-९૦-૯0-9]{1,5}(?:\s*[-–]?\s*\(?\s*(?:शे|શે)[०૦.]?\s*\(?[०-९૦-૯0-9]+\)?)?\s*[-–]?\s*/u,
      ""
    );
    // a reference to the Śeṣa list left at the start: ", (शे. १५३)-"
    text = text.replace(/^[\s,]*\(?\s*(?:शे|શે)[०૦.]?\s*[०-९૦-૯0-9]+\s*\)?\s*[-–]?\s*/u, "");
    // the meaning ends where the synonyms (☐), the derivation (*) or a cross-reference (द्र०) begin
    text = text.split(/[☐□*]|(?:द्र|દ્ર)[०૦.]/u)[0];
    // a sūtra number is not meaning: "(उणा-३३८)"
    text = text.replace(/\s*\(\s*(?:उणा|ઉણા)[^)]*\)/gu, "");
    // AVPK prints this part in Gujarati: letters the reading took for Devanagari are put back
    return text
      .replace(/[\u0900-\u0963\u0966-\u097f]/g, (c) => String.fromCharCode(c.charCodeAt(0) + 0x180))
      .replace(/\s+([.,;])/g, "$1")
      .replace(/\s{2,}/g, " ")
      .trim();
  } else {
    // अहिंसा स्त्री. (न हिंसा) ૧. હિંસાનો અભાવ …: after the first bracket (the derivation), or after the label
    const bracket = text.match(/^[^()]{0,80}\([^()]*\)/u);
    text = bracket ? text.slice(bracket[0].length) : text.replace(/^\S+(?:,\s*\S+)*\s+\S{1,6}\.\s*/u, "");
  }
  // keep the Gujarati: drop Sanskrit quotations ("–अहिंसा परमो धर्मः ।") and other Devanagari runs between
  const pieces = text
    .split(/(?<=[.,;।])\s+|\s+[–—-]\s*(?=[ऀ-ॿ])/u)
    .map((piece) => piece.trim())
    .filter((piece) => GUJ.test(piece) && (piece.match(/[઀-૿]/g)?.length ?? 0) >= (piece.match(new RegExp(DEV_RUN, "g"))?.length ?? 0));
  return pieces
    .join(" ")
    .replace(/\s*[–—-]\s*$/u, "")
    .replace(/\s+([.,;])/g, "$1")
    .replace(/\s{2,}/g, " ")
    .trim();
}

/** For each entry from a Gujarati-printing kosh, its printed Gujarati (index → text). */
export async function koshGujaratiFor(entries: Array<{ head: string; body: string; granthKey?: string; page?: number }>) {
  const want = entries
    .map((e, index) => ({ ...e, index }))
    .filter((e) => e.head && e.granthKey && Number.isFinite(e.page) && printsGujarati(e.granthKey));
  const out = new Map<number, string>();
  if (!want.length) return out;
  const texts = await koshPagesGujarati(want.map((e) => ({ granthKey: e.granthKey!, page: e.page! })));
  for (const e of want) {
    const gu = gujaratiOfEntry(texts.get(`${e.granthKey}:${e.page}`) ?? "", e.granthKey!, e.head, e.body);
    if (gu) out.set(e.index, gu);
  }
  return out;
}
