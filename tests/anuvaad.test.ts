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

test("a citation in brackets is never put in place of the derivation", async () => {
  const { keepPrintedDerivation } = await import("@/lib/anuvaad/vyutpatti");
  const text = "પુંલિંગ. 'कायस्य उत्सर्गः' — અર્થ: શરીરનો ત્યાગ.";
  assert.equal(keepPrintedDerivation(text, { head: "काउस्सग्ग", body: "(उत्त. २९) - કાઉસગ્ગ" }), text);
  assert.equal(keepPrintedDerivation(text, { head: "परीसह", body: "(आचा; औप; उव.) - x" }), text);
});

test("the Prakrit koshes' citations are not sent for translation", async () => {
  const { entryForTranslation } = await import("@/lib/anuvaad/vyutpatti");
  assert.equal(entryForTranslation({ head: "काउस्सग्ग", body: "पुं. (कायोत्सर्ग) (उत्त. २९) (आचा; औप; उव.) - શરીરનો ત્યાગ" }).body, "पुं. (कायोत्सर्ग) - શરીરનો ત્યાગ");
});

test("Gujarati letters inside the quoted derivation are written back in Devanagari", async () => {
  const { devanagariQuotes } = await import("@/lib/anuvaad/vyutpatti");
  assert.equal(devanagariQuotes("સ્ત્રીલિંગ. 'મિત્રો દેવતાઽસ્યા मैत्री' — અર્થ"), "સ્ત્રીલિંગ. 'मित्रो देवताऽस्या मैत्री' — અર્થ");
  assert.equal(devanagariQuotes("'उप+अधि+इड्+ ઘઃ'"), "'उप+अधि+इड्+ घः'");
});

test("Gujarati between two quoted pieces is left Gujarati", async () => {
  const { devanagariQuotes } = await import("@/lib/anuvaad/vyutpatti");
  const text = "'चौरस्य भावः' — ચોરનો ભાવ તે 'चौर्य'";
  assert.equal(devanagariQuotes(text), text);
});

test("words ending in द्र. are not taken for a cross-reference", async () => {
  const { entryForTranslation } = await import("@/lib/anuvaad/vyutpatti");
  assert.ok(entryForTranslation({ head: "इन्द्र", body: "पुं. (इन्दतीति इन्द्रः) - इन्द्र. देवराज" }).body.includes("इन्द्र. देवराज"));
});

test("the check takes out a glossary line and the style example's list (13 Nov 2026 page)", async () => {
  const { dropBorrowed } = await import("../lib/anuvaad/translate");
  const blocks = parseAnuvaad(
    [
      "તેથી આ પ્રમાણે હિંસાનું લક્ષણ કહ્યું છે, અને તે પ્રમત્તયોગથી થતો પ્રાણનો નાશ છે.",
      "## શંકા / સમાધાન",
      "નનુ = (શંકા નીચે મુજબ છે) શંકા; ઉચ્યતે = (જવાબ નીચે મુજબ છે) સમાધાન",
      "(૧) હાથી, (૨) વૃષભ, (૩) કેશરી સિંહ, (૪) લક્ષ્મીદેવી, (૫) ફૂલોની માળા, (૬) સૂર્ય, (૭) ચંદ્ર, (૮) ધજા-પતાકા, (૯) કળશ, (૧૦) પદ્મ સરોવર, (૧૧) સમુદ્ર, (૧૨) દેવ વિમાન, (૧૩) રત્નરાશિ, (૧૪) અગ્નિશિખા.",
    ].join("\n")
  );
  const kept = dropBorrowed(blocks, "તેથી આ પ્રમાણે હિંસાનું લક્ષણ કહ્યું છે, પ્રમત્તયોગથી પ્રાણનો નાશ");
  assert.equal(kept.length, 1);
  assert.equal(kept[0].kind, "para");
});

test("the check keeps a page that really lists the fourteen dreams", async () => {
  const { dropBorrowed } = await import("../lib/anuvaad/translate");
  const list = "(૧) હાથી, (૨) વૃષભ, (૩) કેશરી સિંહ, (૪) લક્ષ્મીદેવી, (૫) ફૂલોની માળા, (૬) સૂર્ય, (૭) ચંદ્ર";
  const kept = dropBorrowed(parseAnuvaad(`માતાએ ચૌદ સ્વપ્નો જોયાં — ${list}.`), `માતાએ ચૌદ સ્વપ્નો જોયાં: ${list}`);
  assert.equal(kept.length, 1);
});

test("box passage lookup: words, Gujarati pages and quoted words", async () => {
  const { passageWords, isGujaratiPage, quotedShare, toGujaratiScript } = await import("../lib/anuvaad/locate");
  const words = passageWords("हेतुतः, स्वरूपतोऽनुबन्धतश्च अहिंसा निखिलकुशललोकाभिनन्दनीया वर्त्तते। च इति");
  assert.equal(words[0], "निखिलकुशललोकाभिनंदनीया");
  assert.ok(!words.includes("च") && !words.includes("इति"));
  assert.equal(toGujaratiScript("अहिंसा"), "અહિંસા");
  const gujarati = "હેતુથી, સ્વરૂપથી અને અનુબંધથી અહિંસા સર્વ કુશળ લોકોને માન્ય છે. ".repeat(10);
  assert.ok(isGujaratiPage(gujarati));
  assert.ok(!isGujaratiPage("अहिंसा परमो धर्मः ".repeat(30)));
  assert.ok(quotedShare(["अहिंसा", "हेतुतः"], gujarati) >= 0.5);
});

// ---------------------------------------------------------------- the book's own Gujarati (20 Nov 2026)
// Maharaj Saheb: the anuvaad from our book started 3-4 lines late, came with
// pages around it, and carried the ભાવાર્થ of Pravinbhai Mota's books.
import { bookGujaratiText, gathaMarks, gathaRange } from "@/lib/anuvaad/book-text";
import { granthHints } from "@/lib/anuvaad/locate";

test("the Granth column's gatha numbers", () => {
  assert.deepEqual(gathaRange("पंचाशक मूल, 04 - पूजाविधिप्रकरण पंचाश क, गाथा 42 ; पंचाशक अभयदेवसूरि टीका, गाथा 42"), [42, 42]);
  assert.deepEqual(gathaRange("दशवैकालिक हरिभद्रसूरि सूत्र टीका, अध्ययन - 04 (षड्जीवनिकाय), गाथा 1-6"), [1, 6]);
  // the chapter number after a comma is not a gatha
  assert.deepEqual(gathaRange("संबोधप्रकरणम्, 01 – देवस्वरूप अधिकार, गाथा 290-291, 01 – देवस्वरूप अधिकार, गाथा 298-303"), [290, 303]);
  assert.deepEqual(gathaRange("द्वात्रिंशद् द्वात्रिंशिका, 08 - वाद बत्रीसी, श्लोक 28"), [28, 28]);
  assert.equal(gathaRange("अभिधानव्युत्पत्तिप्रक्रियाकोश भाग-2"), null);
});

test("gatha marks: a verse's end, a Gujarati part's end, a numbered opening", () => {
  assert.deepEqual(gathaMarks("तहवि तई परिसुद्धा, गिहीण कूवाहरणजोगा ॥४२॥").map((k) => [k.from, k.style]), [[42, "close"]]);
  assert.deepEqual(gathaMarks("સંબંધથી પૂજા નિર્દોષ છે. (૪૧-૪૨)").map((k) => [k.from, k.to, k.style]), [[41, 42, "close"]]);
  assert.deepEqual(gathaMarks("(૮) ગાથાર્થ: યતનાથી ચરે, યતનાથી ઊભો રહે").map((k) => [k.from, k.style]), [[8, "open"]]);
  // a number alone on its line ends the part before it
  assert.deepEqual(gathaMarks("(૧૯૯).").map((k) => [k.from, k.style]), [[199, "close"]]);
  assert.deepEqual(gathaMarks("નિર્દોષ એવી પૂજા કરવી જોઈએ એમ (૪૦મી ગાથામાં) કહ્યું છે."), []);
});

const page = (n: number, text: string) => ({ page: n, printedPage: String(n - 15), text, gujarati: true });

test("book Gujarati: starts right after the verse, ends before the next gatha, the passage in bold", () => {
  const pages = [
    page(195, [
      "૪. પૂજાવિધિ પંચાશક",
      "ગાથા-૪૦-૪૨",
      "ટીકાર્થ-અહીં મનુષ્યો વિશેષથી ઉપદેશને યોગ્ય છે, અને ઉપદેશ પ્રમાણે કરનારા",
      "હોવાથી મનુષ્યભવને પામીને એમ કહ્યું છે. (૪૦)",
      "पूजाए कायवहो, पडिकुट्ठो सो य णेव पुज्जाणं ।",
      "उवगारिणित्ति तो सा, परिसुद्धा कह णु होइत्ति ? ॥४१॥",
      "भण्णइ जिणपूयाए, कायवहो जतिवि होइ उ कहिंचि ।",
      "तहवि तई परिसुद्धा, गिहीण कूवाहरणजोगा ॥४२॥",
      "નિર્દોષ એવી પૂજા કરવી જોઈએ એમ (૪૦મી ગાથામાં) કહ્યું છે. તેમાં પૂજાની નિર્દોષતાને",
      "નહિ માનનાર બીજાના મતને બતાવતા ગ્રંથકાર કહે છે–",
    ].join("\n")),
    page(196, [
      "ગાથા-૪૩",
      "૪. પૂજાવિધિ પંચાશક",
      "ઉત્તર–જો કે પૂજામાં કથંચિત્ જીવહિંસા થાય છે, તો પણ ગૃહસ્થોને કૂપઉદાહરણ",
      "સંબંધથી પૂજા નિર્દોષ છે. (૪૧-૪૨)",
      "ગુણોથી યુક્ત છે. જિનપૂજા કર્મક્ષય વગેરે ગુણોને પમાડે છે. (૪૨)",
      "असदारंभपवत्ता, जं च गिही तेण तेसि विन्नेया ।",
      "तन्निव्वित्तिफलच्चिय, एसा परिभावणीयमिणं ॥४३॥",
      "બીજા સમાધાનને કહે છે–",
      "ગાથાર્થ-ગૃહસ્થો આરંભમાં પ્રવર્તેલા છે. તેથી તેમને જિનપૂજા અશુભ આરંભની",
    ].join("\n")),
  ];
  const book = bookGujaratiText(pages, 195, "पंचाशक मूल, 04 - पूजाविधिप्रकरण, गाथा 42", []);
  assert.ok(book);
  assert.equal(book.by, "gatha");
  const main = book.markup.split("\n").filter((l) => l.startsWith("> ")).join(" ");
  assert.match(main, /^> નિર્દોષ એવી પૂજા/);
  assert.match(main, /પમાડે છે\. \(૪૨\)$/);
  // the gatha before and the one after are there, not bold; running heads are not
  assert.match(book.markup, /^ટીકાર્થ-અહીં મનુષ્યો/m);
  assert.doesNotMatch(main, /ગૃહસ્થો આરંભમાં/);
  assert.doesNotMatch(book.markup, /પૂજાવિધિ પંચાશક|ગાથા-૪૩/);
});

test("book Gujarati: numbered Gujarati after a Sanskrit block starts at its own number", () => {
  const pages = [
    page(311, ["पापकर्मबन्धो न भवतीत्यर्थः ॥९॥", "तस्य पापकर्मबन्धो न भवति ॥८॥", "હવે ઉપદેશ નામનો પાંચમો અર્થાધિકાર કહેવાય છે.", "(૧) ગાથાર્થ: અયત ચરતો પ્રાણભૂતોને હિંસે છે. પાપકર્મ બાંધે છે."].join("\n")),
    page(312, ["(૭) ગાથાર્થ: કેવી રીતે ચાલવું, કેવી રીતે ઊભા રહેવું, કેવી રીતે બેસવું, કેવી રીતે", "ઊંઘવું, કેવી રીતે ખાતો અને બોલતો સાધુ પાપ કર્મ ન બાંધે?", "આચાર્ય કહે છે કે", "(૮) ગાથાર્થ: યતનાથી ચરે, યતનાથી ઊભો રહે, યતનાથી બેસે, યતનાથી ઊંઘે,", "યતનાથી વાપરતો અને બોલતો પાપકર્મ ન બાંધે.", "(૯) ગાથાર્થ: સર્વભૂતાત્મભૂત, સમ્યક્ રીતે ભૂતોને જોતાં, પિહિતાશ્રવવાળા, દાન્ત", "જીવને પાપકર્મ ન બંધાય.", "(૧૦) ગાથાર્થ: પહેલા જ્ઞાન અને પછી દયા છે."].join("\n")),
  ];
  const book = bookGujaratiText(pages, 311, "दशवैकालिक सूत्र मूल, अध्ययन - 04, गाथा 8-9", []);
  assert.ok(book);
  const main = book.markup.split("\n").filter((l) => l.startsWith("> "));
  assert.equal(main[0], "> આચાર્ય કહે છે કે");
  assert.match(main.join(" "), /\(૮\) ગાથાર્થ: યતનાથી ચરે/);
  assert.match(main[main.length - 1], /જીવને પાપકર્મ ન બંધાય\.$/);
  assert.doesNotMatch(main.join(" "), /\(૭\)|\(૧૦\)/);
});

test("book Gujarati: the ભાવાર્થ of Pravinbhai Mota's books is left out", () => {
  const pages = [
    page(69, [
      "हिंसकत्वं न तेनेदं वैद्यस्य स्याद्रिपोरिव ।।२८।।",
      "શ્લોકાર્થ :-",
      "જે કારણથી હિંસ્ય પ્રાણીનો કર્મનો વિપાક હોતે છતે દુષ્ટાશયની નિમિત્તતા હિંસકપણું છે.",
      "ટીકાર્થ :-",
      "હિંસકનું વૈયાવચ્ચ કરનારપણાનો વ્યપદેશ નથી. એ પ્રમાણે જાણવું. ।।૨૮।।",
      "ભાવાર્થ :-",
      "પૂર્વપક્ષી કહે છે કે હિંસ્ય જીવના કર્મથી જ તેની હિંસા થાય છે, માટે હિંસકને દોષ નથી.",
      "અવતરણિકા :-",
      "હવે બીજી રીતે હિંસાના અસંભવને કહે છે ।।૨૯।।",
    ].join("\n")),
  ];
  const book = bookGujaratiText(pages, 69, "द्वात्रिंशद् द्वात्रिंशिका, 08 - वाद बत्रीसी, श्लोक 28", []);
  assert.ok(book);
  assert.match(book.markup, /^> .*શ્લોકાર્થ/m);
  assert.doesNotMatch(book.markup, /ભાવાર્થ|પૂર્વપક્ષી કહે છે/);
});

test("the Granth column's book names, as consonants", () => {
  assert.ok(granthHints("पंचाशक अभयदेवसूरि टीका, 07 – जिनभवन विधिप्रकरण, गाथा 29- 30").includes("pncsk"));
  assert.ok(granthHints("संबोधप्रकरणम्, 01 – देवस्वरूप अधिकार, गाथा 200-201").includes("snbdprkrn"));
  assert.ok(granthHints("द्वात्रिंशद् द्वात्रिंशिका प्रकरण, 08 - वाद बत्रीसी, श्लोक 28").includes("vdbtrs"));
});

test("a sideways photo: the reading made of kosh words is kept (18 Nov 2026)", async () => {
  const { bestReading } = await import("../lib/photo-reading");
  // the photo as taken, turned clockwise, turned back: what the OCR gave for each
  const readings = ["अकात्तरअहिंसा लौकिकअहिंसा अकोत्तरहिंसा लौकिकहिंसा", "लोकोत्तरहिंसा लोकोत्तरहिंसा लोकोत्तरहिंसा", "लोकोत्तरअहिंसा लौकिकअहिंसा लोकोत्तरहिंसा लौकिकहिंसा"];
  assert.equal(bestReading(readings), 2);
});

test("the vishay band across a box table is not a passage", () => {
  const box = parseBoxPages([
    {
      page: 1,
      markdown: `<table><thead><tr><th>Sr.</th><th>V.T</th><th>Granth</th><th>ShastraPath</th><th>Pub.Rem</th><th>In.Rem</th></tr></thead><tbody>
<tr><td colspan="6">10.8.1 - हेतुअहिंसा</td></tr>
<tr><td>2</td><td>व्या.</td><td>उपदेशरहस्य स्वोपज्ञ<br/>टीका, गाथा 4</td><td>एतद्वैलक्षण्यं चाऽहिंसायां योजनीयं, यतनाया हेतुत्वात्</td><td></td><td></td></tr>
</tbody></table>`,
    },
  ]);
  assert.deepEqual(box.rows.map((r) => r.sr), ["2"]);
  assert.equal(box.vishay, "10.8.1 - हेतुअहिंसा");
});

test("a page's footnotes are not read into its text", async () => {
  const { footnoteLines } = await import("@/lib/anuvaad/book-text");
  // Panchashak p.195: the footnote came into the middle of gatha 41-42's bold sentence
  const raw = ["ગાથાર્થ–પ્રશ્ન–પૂજામાં જીવહિંસા થાય છે. (તીર્થંકરોએ) જીવહિંસાનો નિષેધ કર્યો", "૧.લલિતવિસ્તરા લોગસ્સસૂત્ર."];
  assert.deepEqual([...footnoteLines(raw)], [1]);
  // a closing gatha mark alone at the foot, or a numbered heading mid-page, is not one
  assert.equal(footnoteLines(["ટીકાર્થ– … કહ્યું છે.", "(૮)."]).size, 0);
  assert.equal(footnoteLines(["૫. યતિના દ્વાર સ્વાશયવૃદ્ધિદ્વાર કહ્યું.", "ગાથાર્થ–જિનમંદિરનિર્માણમાં સર્વકાર્યોમાં", "આદરથી યતના કરવી જોઈએ.", "ટીકાર્થ–જિનભવનનિર્માણમાં", "કાષ્ઠાદિ દળ લાવવું,", "ભૂમિ ખોદવી, ભીંત ચણવી"]).size, 0);
});

test("an APC page keeps each entry's granth, adhyay and text, and nothing else", async () => {
  const { parseApcPages } = await import("@/lib/anuvaad/apc");
  // the 17 Nov 2026 sample, as read (its margin note in Gujarati and Hindi, and the "See Code Anuvaad" part)
  const page = `गीतार्थ गंगा
APC - 1

SPK>M017>17.2D>Apc file (कथा-अकथा)

12. Dummy extra
Vishay Search By Code - With Dummy extra= धर्मकथा, आक्षेपणीकथा आदि चार

(क्र.-1)

ग्रंथ :- अनुयोगद्वारसूत्र जिनदासगणिमहत्तर चूर्णि भाग-1 (प्रकाशक:-महावीर जैन विद्यालय,

LibNo:-B010073)

Adhyay :- ग्रंथ प्रारंभ, सूत्र-14

Pointers :- 12049 to 12343

तमेवं आवस्सए त्ति सुतपदं अधिज्जित्ता से णं 'सः' अधीतश्रुतः
वा वायणादी करेज्ज। णो अणुप्पेहाए त्ति दव्वावस्सयं ण लब्भति,

See Code Anuvaad :-
1. Pointers :- 12049 to 12343
सूत्रनी वाचना, पृच्छना, परावर्तना, धर्मकथा उपयोग विना संभव ।
UQ (परिणाम, धर्मकथा NDMS 12-2-26)

(क्र.-2)

ग्रंथ :- अनुयोगद्वारसूत्र हेमचंद्रसूरि - मलधारी टीका भाग-1 (प्रकाशक:-महावीर जैन विद्यालय,
LibNo:-B010073)
Adhyay :- ग्रंथ प्रारंभ,सूत्र-14
Pointers :- 35638 to 35968
तदेवं यस्य जन्तोरावश्यकशास्त्रं शिक्षितादिगुणोपेतं भवति स जन्तुस्तत्रावश्यकशास्त्रे वाचनया

5 प्रकार के स्वाध्याय या
અને સંમજિયા 5 4 મા ઉપયોગ

Dt.17/2/26                                                               Page 1 of 254`;
  const next = `प्रच्छनयाऽनवगताथदिर्गुरुं प्रति प्रश्नलक्षणया

See Code Anuvaad :-`;
  const entries = parseApcPages([
    { page: 1, text: page },
    { page: 2, text: next },
  ]);
  assert.deepEqual(entries.map((e) => e.no), ["1", "2"]);
  assert.equal(entries[0].granth, "अनुयोगद्वारसूत्र जिनदासगणिमहत्तर चूर्णि भाग-1");
  assert.equal(entries[0].adhyay, "ग्रंथ प्रारंभ, सूत्र-14");
  assert.equal(entries[0].text, "तमेवं आवस्सए त्ति सुतपदं अधिज्जित्ता से णं 'सः' अधीतश्रुतः वा वायणादी करेज्ज। णो अणुप्पेहाए त्ति दव्वावस्सयं ण लब्भति,");
  assert.equal(entries[1].adhyay, "ग्रंथ प्रारंभ, सूत्र-14");
  // carried on to the next page; the margin note left out
  assert.equal(entries[1].text, "तदेवं यस्य जन्तोरावश्यकशास्त्रं शिक्षितादिगुणोपेतं भवति स जन्तुस्तत्रावश्यकशास्त्रे वाचनया प्रच्छनयाऽनवगताथदिर्गुरुं प्रति प्रश्नलक्षणया");
  assert.deepEqual(entries[1].pages, [1, 2]);
});

test("markup: '| ' frames a line, '> ' makes it bold", () => {
  const blocks = parseAnuvaad("| तमेवं आवस्सए त्ति\n> ગાથાર્થ\nબીજું");
  assert.deepEqual(blocks.map((b) => b.kind), ["boxed", "main", "para"]);
  assert.equal(toMarkup(blocks), "| तमेवं आवस्सए त्ति\n> ગાથાર્થ\nબીજું");
});
