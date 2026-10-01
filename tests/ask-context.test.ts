// The Ask prompt: what the model is shown and how its citations line up with
// the sources the reader sees. No network, no database.
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildMergePrompt, buildPrompt, chunkPassages, limitToChunks, MAX_CHUNKS, MAX_CONTEXT_CHARS, type Passage, type ResolvedContext } from "@/lib/ai-context";

const page = (n: number, chars: number): Passage => ({
  granthKey: "g", granthName: "Granth", pageNumber: n, label: `page ${n}`, text: "क".repeat(chars),
});

function contextOf(passages: Passage[], verses: ResolvedContext["verses"] = []): ResolvedContext {
  return {
    passages, verses, droppedVerses: [], needsAdhikar: null, truncated: false,
    totalChars: passages.reduce((s, p) => s + p.text.length, 0), summaryLine: "Granth — pages",
  };
}

test("a part's passages keep their number in the whole scope", () => {
  const passages = [1, 2, 3, 4, 5, 6].map((n) => page(n, MAX_CONTEXT_CHARS / 2 - 10));
  const context = contextOf(passages);
  const { chunks } = chunkPassages(passages);
  assert.equal(chunks.length, 3);
  const { user } = buildPrompt({ question: "q", language: "english", context, part: { index: 2, total: 3 }, passages: chunks[1] });
  assert.match(user, /\[3\] Granth — page 3/);
  assert.match(user, /\[4\] Granth — page 4/);
  assert.doesNotMatch(user, /\[1\] /);
});

test("the merge keeps the parts' citations as written", () => {
  const { system } = buildMergePrompt({ question: "q", language: "english", context: contextOf([page(1, 10)]), parts: ["a [1]", "b [3]"] });
  assert.match(system, /never renumber/);
});

test("pages beyond the last call are reported, not silently dropped", () => {
  // One verse per page, each page more than half a call: every page needs its own call.
  const passages = Array.from({ length: MAX_CHUNKS + 2 }, (_, i) => page(i + 1, MAX_CONTEXT_CHARS / 2 + 10));
  const verses = passages.map((p, i) => ({ adhikar: 1, gatha: i + 1, pageStart: p.pageNumber, pageEnd: p.pageNumber }));
  const { chunks, dropped } = chunkPassages(passages, verses);
  assert.equal(chunks.length, MAX_CHUNKS);
  assert.equal(dropped.length, passages.length - chunks.flat().length);
  assert.ok(dropped.length > 0);
  const limited = limitToChunks(contextOf(passages, verses), chunks.flat(), dropped);
  assert.equal(limited.truncated, true);
  assert.deepEqual(limited.droppedVerses.map((v) => v.gatha), dropped.map((p) => p.pageNumber));
  assert.equal(limited.passages.length, chunks.flat().length);
});

test("a scope that fits is passed through unchanged", () => {
  const passages = [page(1, 100), page(2, 100)];
  const context = contextOf(passages);
  const { chunks, dropped } = chunkPassages(passages);
  assert.equal(dropped.length, 0);
  assert.equal(limitToChunks(context, chunks.flat(), dropped), context);
});
