import { readFileSync } from "node:fs";
import path from "node:path";
import type { NextApiRequest, NextApiResponse } from "next";
import { buildCacheKey, getCachedJson, setNoStore, setPublicCacheHeaders } from "@/lib/api-cache";
import { protectApi } from "@/lib/auth-guard";
import { PERMISSIONS } from "@/lib/auth-permissions";
import { koshFamilyLabels } from "@/lib/koshes.mjs";
import { SOURCE, compoundParts, createLexicon, searchableParts } from "@/lib/sanskrit-compound.mjs";
import type { CompoundPart, CompoundWord } from "@/lib/search-query";
import { foldSanskrit } from "@/lib/sanskrit-fold.mjs";
import { getTursoClient } from "@/lib/turso";

// The words a Devanagari compound is made of, so the search page can offer
// "अकर्कश + प्रशस्त + वचन + …" and search the parts in the koshes, where the
// whole vishay is rarely printed. The word list (data/compound-lexicon.json)
// comes from the kosh headwords only; see
// scripts/build_compound_lexicon.mjs.

export type CompoundPartsResponse = { query: string; words: CompoundWord[] };

let lexicon: ReturnType<typeof createLexicon> | null = null;
function getLexicon() {
  lexicon ??= createLexicon(JSON.parse(readFileSync(path.join(process.cwd(), "data", "compound-lexicon.json"), "utf8")));
  return lexicon;
}

const VOCAB_TABLE = "ocr_pages_folded_vocab";
const DEVANAGARI_WORD = /^[ऀ-ॿ‌‍]+$/;
const MAX_WORDS = 4;

async function pageCounts(terms: string[]) {
  const counts = new Map<string, number>();
  const keys = [...new Set(terms.map((t) => foldSanskrit(t)).filter(Boolean))];
  if (!keys.length) return counts;
  // The vocab table exists once /api/query-forms has run; a missing table only loses the counts.
  const result = await getTursoClient()
    .execute({ sql: `SELECT term, doc FROM ${VOCAB_TABLE} WHERE term IN (${keys.map(() => "?").join(",")})`, args: keys })
    .catch(() => null);
  for (const row of result?.rows ?? []) counts.set(String(row.term), Number(row.doc ?? 0));
  return counts;
}

async function partsFor(words: string[]): Promise<CompoundPartsResponse["words"]> {
  const lex = getLexicon();
  const out: CompoundPartsResponse["words"] = [];
  for (const word of words) {
    const split = compoundParts(word, lex);
    if (!split || searchableParts(split.parts).length < 2) continue;
    out.push({
      word: split.word,
      inKosh: split.whole && split.whole.source !== SOURCE.agamic ? koshFamilyLabels(split.whole.koshes) : [],
      parts: split.parts.map((part): CompoundPart => {
        if (part.kind === "prefix") return { text: part.text, kind: "prefix", term: part.label };
        if (part.kind === "ending" || part.kind === "unknown") return { text: part.text, kind: part.kind, term: part.text };
        // केवली in one kosh is केवलिन् in the others: search the -in stem too.
        const inStem = part.term.endsWith("ी") ? lex.get(`${part.term.slice(0, -1)}िन्`) : null;
        const alias =
          foldSanskrit(part.stem) !== foldSanskrit(part.term) ? part.stem : inStem?.source === SOURCE.head ? inStem.head : undefined;
        return {
          text: part.text,
          kind: "word",
          term: part.term,
          alias,
          koshes: koshFamilyLabels(part.koshes),
          agamicOnly: part.source === SOURCE.agamic,
        };
      }),
    });
  }
  const searched = out.flatMap((w) => w.parts.filter((p) => p.kind === "word" || p.kind === "unknown"));
  const counts = await pageCounts(searched.flatMap((p) => [p.term ?? "", p.alias ?? ""]));
  for (const part of searched) {
    part.pages = Math.max(counts.get(foldSanskrit(part.term ?? "")) ?? 0, counts.get(foldSanskrit(part.alias ?? "")) ?? 0);
  }
  return out;
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }
  const query = String(req.query.q ?? "").normalize("NFC").replace(/\s+/g, " ").trim().slice(0, 200);
  const words = query
    .split(/[\s{}()[\],।॥.\-–—/]+/)
    .filter((w) => DEVANAGARI_WORD.test(w) && Array.from(w).length >= 4)
    .slice(0, MAX_WORDS);
  if (!words.length) {
    setPublicCacheHeaders(res, { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 3600 });
    return res.status(200).json({ query, words: [] } satisfies CompoundPartsResponse);
  }
  try {
    const { value, status } = await getCachedJson(buildCacheKey(req, "compound-parts"), 600, async () => ({
      query,
      words: await partsFor(words),
    }));
    setPublicCacheHeaders(res, { maxAgeSeconds: 600, staleWhileRevalidateSeconds: 3600 }, status);
    return res.status(200).json(value);
  } catch (error) {
    setNoStore(res);
    return res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
  }
}

export default protectApi(handler, PERMISSIONS.libraryRead);
