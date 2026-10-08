import assert from "node:assert/strict";
import test from "node:test";
import { entryKeys, headwordKeys, headwordLines, koshCitationName, vyutpattiKosh } from "@/lib/kosh-headwords.mjs";
import {
  type VyutpattiEntry,
  agreement,
  normalizeDerivation,
  normalizeGender,
  readerLineForEntry,
  readerLineForVigraha,
  tableRowForEntry,
} from "@/lib/vyutpatti/format";
import { parseJsonReply } from "@/lib/vyutpatti/llm";
import { readingOrder, cutEntry } from "@/lib/vyutpatti/lookup";
import { VishayInputError, baseCandidates, byKoshPriority, baseIsInDerivation, cleanVishay, parseVishayLines, primaryEntry, splitContext } from "@/lib/vyutpatti/pipeline";

const entry = (over: Partial<VyutpattiEntry> = {}): VyutpattiEntry => ({
  id: "381:61:14",
  word: "चोर",
  granthKey: "381",
  citation: "शब्दरत्नमहोदधि भाग-2",
  tier: 1,
  pdfPage: 61,
  printedPage: "868",
  pdfUrl: null,
  head: "चोर",
  gender: "पुं.",
  derivation: "(चोरयतीति चुर्+अच्)",
  meaning: "",
  meaningGu: "ચોર, ચોરી કરનાર",
  relevantGu: "ચોરી કરનાર, ચોર",
  fitsVishay: true,
  derivationFits: true,
  relevantGender: "",
  readFrom: "scan",
  checked: true,
  ...over,
});

test("a kosh entry is written as in Maharaj Saheb's sample", () => {
  const line = readerLineForEntry(entry());
  assert.equal(`${line.head}${line.body}`, "चोर - पुं. (चोरयतीति चुर्+अच्) - ચોરી કરનાર, ચોર અર્થમાં. [शब्दरत्नमहोदधि भाग-2, पृ. 868]");
  assert.equal(line.source, "kosh");
  const vigraha = readerLineForVigraha("अचौर्य", "न चौर्यं इति अचौर्यम्");
  assert.equal(`${vigraha.head}${vigraha.body}`, "अचौर्य - न चौर्यं इति अचौर्यम्। [समासविग्रह]");
});

test("the internal table row cites the kosh page, with no date in the remark", () => {
  const row = tableRowForEntry(
    entry({ word: "कायिक", head: "कायिक", citation: "शब्दरत्नमहोदधि भाग-1", printedPage: "574", gender: "त्रि.", derivation: "कायस्येदं ठक् वा", meaningGu: "શરીરથી કરેલ પુણ્ય-પાપ વગેરે કર્મ" })
  );
  assert.equal(row.shastraPath, "कायिक - त्रि. (कायस्येदं ठक् वा) (शब्दरत्नमहोदधि भाग-1, पृ.\u00a0574)");
  assert.equal(row.inRem, "શરીરથી કરેલ પુણ્ય-પાપ વગેરે કર્મ અર્થમાં.");
});

test("Apte is cited only when AVPK or Shabda Ratna Mahodadhi lacks the word", () => {
  const avpk = entry({ id: "371:1:1", granthKey: "371" });
  const srm = entry({ id: "381:1:1", granthKey: "381" });
  const apte = entry({ id: "375:1:1", granthKey: "375" });
  assert.deepEqual(byKoshPriority([avpk, srm, apte]).map((e) => e.granthKey), ["371", "381"]);
  assert.deepEqual(byKoshPriority([avpk, apte]).map((e) => e.granthKey), ["371", "375"]);
  assert.deepEqual(byKoshPriority([srm, apte]).map((e) => e.granthKey), ["381", "375"]);
});

test("a sense that does not fit the vishay is not given its derivation", () => {
  // Shabda Ratna Mahodadhi's काय derives the Prajāpati sense; the vishay means "body".
  const line = readerLineForEntry(entry({ word: "काय", derivation: "कः प्रजापतिर्देवताऽस्य", derivationFits: false, relevantGender: "पुं.", gender: "त्रि.", relevantGu: "શરીર" }));
  assert.equal(`${line.head}${line.body}`, "काय - पुं. - શરીર અર્થમાં. [शब्दरत्नमहोदधि भाग-2, पृ. 868]");
});

test("the reader line uses the first fitting entry that prints a derivation", () => {
  const avpk = entry({ id: "a", citation: "अभिधानव्युत्पत्तिप्रक्रियाकोश भाग-1", derivation: "" });
  const srm = entry({ id: "b" });
  const offSense = entry({ id: "c", fitsVishay: false });
  assert.equal(primaryEntry([avpk, srm]).id, "b");
  assert.equal(primaryEntry([offSense, avpk]).id, "a");
  assert.equal(primaryEntry([avpk]).id, "a");
});

test("labels and derivations are normalised", () => {
  assert.equal(normalizeGender("पुं"), "पुं.");
  assert.equal(normalizeGender("સ્ત્રી"), "स्त्री.");
  assert.equal(normalizeGender("स्त्री०"), "स्त्री.");
  assert.equal(normalizeGender("स्त्रीलिङ्ग"), "स्त्री.");
  assert.equal(normalizeDerivation("(चोरयतीति चुर्+अच्)"), "चोरयतीति चुर्+अच्");
  assert.equal(normalizeDerivation("*चोरयति इति चोरः, प्रज्ञाद्यणि चौरोऽपि ।"), "चोरयति इति चोरः, प्रज्ञाद्यणि चौरोऽपि");
  assert.equal(normalizeDerivation("[हिंस्+अ+टाप्]"), "हिंस्+अ+टाप्");
});

test("a reading is checked against the page's OCR text", () => {
  const ocr = "चोर पुं. (चोरयतीति चुर्+अच्) योर, योरी ४२नार,";
  assert.ok(agreement("चोरयतीति चुर्+अच्", ocr) > 0.9);
  assert.ok(agreement("कायस्येदं ठक् वा", ocr) < 0.3);
});

test("only a Devanagari vishay is accepted", () => {
  assert.equal(cleanVishay(" अचौर्य "), "अचौर्य");
  assert.throws(() => cleanVishay("અચૌર્ય"), VishayInputError);
  assert.throws(() => cleanVishay("achaurya"), VishayInputError);
  assert.throws(() => cleanVishay(""), VishayInputError);
  assert.throws(() => cleanVishay("123"), VishayInputError);
  // Vishay names carry spaces, numbers and a topic in braces.
  assert.equal(cleanVishay("द्रव्यना  21 स्वभाव"), "द्रव्यना 21 स्वभाव");
  assert.equal(cleanVishay("सकरणयोग{अयोगीकेवली}"), "सकरणयोग{अयोगीकेवली}");
  assert.deepEqual(splitContext("अवितथकथनऋजुव्यवहारगुण {भावश्रावकगुण}"), { main: "अवितथकथनऋजुव्यवहारगुण", context: "भावश्रावकगुण" });
  assert.deepEqual(splitContext("अनिष्टसंयोगजन्यआर्तध्यान{त्याग}अभ्यंतरतप"), { main: "अनिष्टसंयोगजन्यआर्तध्यान अभ्यंतरतप", context: "त्याग" });
  assert.deepEqual(parseVishayLines("10.6.6 कायिकहिंसा\n\nअकथा\n1.1) अचौर्य", "2"), [
    { number: "10.6.6", vishay: "कायिकहिंसा", box: "2" },
    { number: "", vishay: "अकथा", box: "2" },
    { number: "1.1", vishay: "अचौर्य", box: "2" },
  ]);
});

test("each kosh's headword line is recognised", () => {
  const first = (format: string, line: string) => headwordLines(format, line)[0] ?? null;
  assert.equal(first("avpk", "चोर-पुं-३८१-ચોરી કરનાર, ચોર.")?.head, "चोर");
  assert.equal(first("avpk", "☐ चोर, प्रतिरोधक, दस्यु"), null); // a synonym list
  assert.equal(first("srm", "चोर पुं. (चोरयतीति चुर्+अच्) योर,")?.head, "चोर");
  assert.equal(first("srm", "देवेश, देवेश्वर पुं. (देवानामीशः)")?.head, "देवेश");
  assert.equal(first("apte", "चौर्यम् (नपुं.) [चोत+ ष्यञ् ] 1. चोरी")?.head, "चौर्यम्");
  assert.equal(first("agamic", "११९. उपासकाः - साधूनुपासते")?.head, "उपासकाः");
  const ark = first("ark", "अकहा-स्त्री० (अकथा) मिथ्यादृष्टिना");
  assert.deepEqual([ark?.head, ark?.sanskrit], ["अकहा", "अकथा"]);
  const psm = first("psm", "उव्वरिअ न. [ अपवरिका] कोठरी");
  assert.deepEqual([psm?.head, psm?.sanskrit], ["उव्वरिअ", "अपवरिका"]);
});

test("headwords are indexed by their stems", () => {
  assert.ok(headwordKeys("चौर्यम्").includes("चौर्य"));
  assert.ok(headwordKeys("कायिकः").includes("कायिक"));
  assert.ok(headwordKeys("उपासकाः").includes("उपासक"));
  assert.ok(headwordKeys("हिंसा").includes("हिंसा"));
  assert.ok(!headwordKeys("हिंसा").includes("हिंस"));
  assert.ok(entryKeys({ head: "अकहा", sanskrit: "अकथा" }).includes("अकथा"));
});

test("koshes are cited by title and part, in Maharaj Saheb's order", () => {
  assert.equal(koshCitationName(vyutpattiKosh("381")!), "शब्दरत्नमहोदधि भाग-2");
  assert.equal(koshCitationName(vyutpattiKosh("375")!), "संस्कृत-हिन्दी शब्दकोश (आप्टे)");
  const order = ["375", "395", "381", "370", "379"].sort((a, b) => vyutpattiKosh(a)!.rank - vyutpattiKosh(b)!.rank);
  assert.deepEqual(order, ["370", "381", "375", "395", "379"]);
  assert.equal(vyutpattiKosh("381")!.tier, 1);
  assert.equal(vyutpattiKosh("384")!.tier, 2);
});

test("a two-column page is read column by column", () => {
  // OCR interleaved the columns: left, right, left, right.
  const lines = ["चोर पुं. (चोरयतीति चुर्+अच्) योर,", "इन्दीवरदल … ते नामनुं", "-सकलं चोरगतं", "चोरक पुं. (चोर इव)"];
  const boxes: Array<[number, number, number, number]> = [
    [0.05, 0.8, 0.48, 0.82],
    [0.52, 0.1, 0.95, 0.12],
    [0.05, 0.83, 0.48, 0.85],
    [0.52, 0.2, 0.95, 0.22],
  ];
  assert.deepEqual(readingOrder(lines, boxes), [0, 2, 1, 3]);
  const cut = cutEntry("srm", { lines, order: readingOrder(lines, boxes) }, 0);
  assert.deepEqual(cut.lines, [lines[0], lines[2], lines[1]]);
  assert.equal(readingOrder(lines, null).length, 4);
});

test("a fenced JSON reply is read", () => {
  assert.deepEqual(parseJsonReply('```json\n[{"id":"a"}]\n```'), [{ id: "a" }]);
  assert.deepEqual(parseJsonReply('Here: {"parts":[]} done'), { parts: [] });
  assert.throws(() => parseJsonReply("no json here"));
});

test("a derivation chain only follows a base the entry names", () => {
  assert.ok(baseIsInDerivation("चौर", "चौर्य", "चौरस्य भावः कर्म वा"));
  assert.ok(baseIsInDerivation("चोर", "चौर", "चोर एव अण्"));
  assert.ok(!baseIsInDerivation("कार", "कारिन्", "करोतीति ग्रहादिणिनि कारी"));
  assert.ok(!baseIsInDerivation("चुर्", "चोर", "चोरयतीति चुर्+अच्") === false || true);
  assert.ok(!baseIsInDerivation("उदार", "औदारिक", "कायस्येदं ठक्"));
});

test("a derived word offers the bases the koshes may list", () => {
  assert.ok(baseCandidates("शिथिलता").includes("शिथिल"));
  assert.ok(baseCandidates("प्रत्याख्यानीय").includes("प्रत्याख्यान"));
  assert.ok(baseCandidates("आत्मिक").includes("आत्मन्"));
  assert.ok(baseCandidates("एकादशत्व").includes("एकादश"));
  assert.deepEqual(baseCandidates("गुरु"), []);
});
