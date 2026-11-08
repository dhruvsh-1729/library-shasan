import assert from "node:assert/strict";
import test from "node:test";
import { parseBoxPages } from "@/lib/anuvaad/box";
import { parseAnuvaad, toMarkup } from "@/lib/anuvaad/markup";
import { splitPassage } from "@/lib/anuvaad/translate";

// Two pages of a box file as the OCR returns them (WhatsApp scan 29 Oct 2026
// 13.04.05, shortened): row 60 runs from the first page on to the second,
// where it comes back with no Sr. and no Granth; the second page's header has
// the vishay as a column of its own; row 61's V.T cell swallowed a Granth line.
const PAGE_1 = `<table>
<thead><tr><th></th><th>V.T</th><th>Granth</th><th>ShastraPath प्राणातिपातविरमणमहाव्रत</th><th>Pub.Rem</th><th>In.Rem</th></tr></thead>
<tbody>
<tr><td>59</td><td>स्व.</td><td>पिंडनिर्युक्ति मलय<br/>गिरिसूरि टीका,<br/>निर्युक्तिगाथा 402</td><td>सम्प्रत्यन्यथा कोटीः प्रतिपादयति- व्याख्या-प्रथमतः कोटयो नव भवन्ति, तद्यथा-स्वयं हननमन्येन घातनमपरेण ह न्यमानस्यानुमोदनं</td><td></td><td>1014</td></tr>
<tr><td>60</td><td>स्व.</td><td>प्रज्ञापनासूत्र मलय<br/>गिरिसूरि टीका, पद - 2<br/>2 (क्रियापद), सूत्र 163<br/>7-1663</td><td>प्राणातिपातविरतस्य कर्मबन्धो भवति किं वा नेति चेत्? उच्यते, भवत्यपि न भवत्यपि,</td><td></td><td>243</td></tr>
</tbody>
</table>

25 of 34                                     03-04-2026 12:54`;

const PAGE_2 = `BOX No - 1

<table>
<thead><tr><th>Sr.</th><th>V.T</th><th>Granth</th><th>ShastraPath</th><th>प्राणातिपातविरमणमहाव्रत</th><th>Pub.Rem</th><th>In.Rem</th></tr></thead>
<tbody>
<tr><td></td><td></td><td></td><td>काश्चावस्थिताः, उभयेषामपि सदा बहुत्वेन लभ्यमानत्वात्, सर्वसङ्कलनया सप्तविंशतिर्भङ्गाः,</td><td></td><td>244</td></tr>
<tr><td>61</td><td>स्व.<br/>गिरिसूरि टीका, पद - 2</td><td>प्रज्ञापनासूत्र मलय गिरिसूरि टीका, पद - 2<br/>2 (क्रियापद)</td><td>अथारम्भिक्यादीनां क्रियाणां मध्ये का क्रिया प्राणातिपातविरतस्येति चिन्तयति</td><td></td><td>244</td></tr>
</tbody>
</table>`;

test("box pages: rows, the vishay band, a passage carried over the page end", () => {
  const box = parseBoxPages([
    { page: 1, markdown: PAGE_1 },
    { page: 2, markdown: PAGE_2 },
  ]);
  assert.equal(box.vishay, "प्राणातिपातविरमणमहाव्रत");
  assert.equal(box.box, "1");
  assert.deepEqual(
    box.rows.map((r) => r.sr),
    ["59", "60", "61"]
  );
  const [r59, r60, r61] = box.rows;
  assert.equal(r59.vt, "स्व.");
  assert.equal(r59.inRem, "1014");
  // "पद - 2<br/>2" is one number again
  assert.equal(r60.granth, "प्रज्ञापनासूत्र मलय गिरिसूरि टीका, पद - 22 (क्रियापद), सूत्र 1637-1663");
  assert.match(r60.shastraPath, /भवत्यपि, काश्चावस्थिताः/);
  assert.deepEqual(r60.pages, [1, 2]);
  assert.equal(r60.inRem, "243 244");
  assert.equal(r61.vt, "स्व.");
  assert.match(r61.granth, /^प्रज्ञापनासूत्र/);
});

test("box pages: a page with no table is kept aside", () => {
  const box = parseBoxPages([{ page: 3, markdown: "## પ્રજ્ઞાપનાસૂત્ર મલયગિરિસૂરિ ટીકા\n\nવિષય: વિરત જીવનો કર્મબંધ\n\n15" }]);
  assert.equal(box.rows.length, 0);
  assert.deepEqual(box.loose.map((l) => l.page), [3]);
});

test("anuvaad markup reads into blocks and back", () => {
  const markup = [
    "# આચારાંગ સૂત્ર (અધ્યયન ૩)",
    "વિષય: પ્રથમ મહાવ્રત",
    "## ૧. પ્રતિજ્ઞા",
    "કેવળજ્ઞાનના ધારક ભગવાને કહ્યું:",
    "- **અહિંસાનો સંકલ્પ:** મુનિ જીવનપર્યંત હિંસા ન કરે.",
    "  - **મનથી:** વિચાર પણ ન કરે.",
  ].join("\n");
  const blocks = parseAnuvaad(markup);
  assert.deepEqual(
    blocks.map((b) => b.kind),
    ["title", "vishay", "section", "para", "point", "point"]
  );
  assert.deepEqual(blocks[4], { kind: "point", label: "અહિંસાનો સંકલ્પ", text: "મુનિ જીવનપર્યંત હિંસા ન કરે.", level: 1 });
  assert.equal(blocks[5].kind === "point" && blocks[5].level, 2);
  assert.equal(toMarkup(blocks), markup);
});

test("a passage is cut at sentence ends and at the मू० / टी० marks", () => {
  const pieces = splitPassage("अ०- पुनरपि मार्गाभिष्टवं कुर्वन्नाह— मू०- अणुपुव्वेण महाघोरं।।५।। टी०- अणुपुव्वेण इत्यादि।", 40);
  assert.ok(pieces.length >= 3);
  assert.ok(pieces.some((p) => p.startsWith("मू०")));
  assert.ok(pieces.some((p) => p.startsWith("टी०")));
  assert.equal(pieces.join(" ").replace(/\s+/g, " "), "अ०- पुनरपि मार्गाभिष्टवं कुर्वन्नाह— मू०- अणुपुव्वेण महाघोरं।।५।। टी०- अणुपुव्वेण इत्यादि।");
});

test("a sūtra quoted after the derivation and AVPK's cross-references are not translated", async () => {
  const { entryForTranslation } = await import("@/lib/anuvaad/vyutpatti");
  const { body } = entryForTranslation({ head: "क्षमा", body: "(क्षमणं क्षमा, “षितोऽङ्” ॥५।३।१०७) (अभिधानव्युत्पत्तिप्रक्रियाकोश भाग-1, पृ. 220) - ક્ષમા-સ્ત્રી-૩૯૧-ક્ષમા, સહન કરવું. ☐ तितिक्षा,[क्षान्ति शि. २७] અર્થમાં" });
  assert.ok(!/षितो|॥|☐|शि\.|૩૯૧/.test(body), body);
  assert.ok(body.startsWith("(क्षमणं क्षमा)"), body);
});

test("the derivation stays quoted exactly as printed", async () => {
  const { keepPrintedDerivation } = await import("@/lib/anuvaad/vyutpatti");
  assert.equal(
    keepPrintedDerivation("સ્ત્રીલિંગ. 'क्षमते इति ક્ષમા' — અર્થ: ક્ષમા.", { head: "क्षमा", body: "(क्षमणं क्षमा) - ક્ષમા" }),
    "સ્ત્રીલિંગ. 'क्षमणं क्षमा' — અર્થ: ક્ષમા."
  );
});
