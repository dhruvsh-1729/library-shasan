import assert from "node:assert/strict";
import test from "node:test";
import { boxFromWords, type TextWord } from "@/lib/anuvaad/box-text-layer";
import { parseBoxPages } from "@/lib/anuvaad/box";
import { boxTextRows } from "@/lib/anuvaad/box-text";
import { dropUnsourced, sourceLinesOnly } from "@/lib/anuvaad/translate";
import { parseAnuvaad, toMarkup } from "@/lib/anuvaad/markup";
import { parseApcPages } from "@/lib/anuvaad/apc";
import { docxTable, zipStore } from "@/lib/docx-lite";

// Words placed as pdftotext gives them for "10.8.1 - 10.8.6.pdf" (25 Nov 2026), page 1, shortened.
const w = (x0: number, y0: number, text: string, width = text.length * 7): TextWord => ({ x0, y0, x1: x0 + width, y1: y0 + 17, text });
const PAGE: TextWord[] = [
  w(380, 23, "10.8.1"),
  w(0, 87, "BOX"),
  w(4, 115, "Sr."),
  w(55, 115, "V.T"),
  w(105, 115, "Granth"),
  w(223, 115, "ShastraPath"),
  w(711, 115, "Pub.Re"),
  w(770, 115, "In.Rem"),
  w(711, 139, "m"),
  w(372, 170, "10.8.1"),
  w(413, 170, "-"),
  w(424, 170, "हेतुअहिंसा"),
  // row 1: the Granth cut inside a word at the end of a full line
  w(4, 203, "1"),
  w(55, 203, "व्यु."),
  w(105, 203, "अभिधानव्युत्पत्तिप्र", 108),
  w(105, 226, "क्रियाकोश"),
  w(160, 226, "भाग-2"),
  w(223, 203, "हेतु"),
  w(240, 203, "पुं."),
  w(400, 203, "(अभिधानव्युत्पत्तिप्रक्रियाकोश", 290),
  w(770, 203, "19-04-202"),
  w(770, 226, "3"),
  // row 2: an ordinary word wrap
  w(4, 305, "2"),
  w(55, 305, "व्या."),
  w(105, 305, "उपदेशरहस्य", 75),
  w(185, 305, "स्वोपज्ञ", 26),
  w(105, 328, "टीका,"),
  w(145, 328, "गाथा"),
  w(180, 328, "4"),
  w(223, 305, "एतद्वैलक्षण्यं"),
  w(330, 305, "चाहिंसायां"),
  w(223, 328, "योजनीयं।"),
  w(4, 560, "सा.नि."),
  w(380, 560, "1"),
  w(390, 560, "of"),
  w(405, 560, "12"),
];

test("a box PDF's own text becomes the box table, the vishay band on each row", () => {
  const box = boxFromWords([PAGE]);
  assert.ok(box);
  assert.equal(box.rows, 2);
  const parsed = parseBoxPages(box.pages.map((markdown, i) => ({ page: i + 1, markdown })));
  assert.equal(parsed.rows.length, 2);
  assert.equal(parsed.rows[0].vishay, "10.8.1 - हेतुअहिंसा");
  assert.equal(parsed.rows[0].granth, "अभिधानव्युत्पत्तिप्रक्रियाकोश भाग-2");
  assert.equal(parsed.rows[0].inRem, "19-04-2023");
  assert.equal(parsed.rows[1].granth, "उपदेशरहस्य स्वोपज्ञ टीका, गाथा 4");
  assert.equal(parsed.rows[1].shastraPath, "एतद्वैलक्षण्यं चाहिंसायां योजनीयं।");
  assert.equal(parsed.rows[0].shastraPath, "हेतु पुं. (अभिधानव्युत्पत्तिप्रक्रियाकोश");
});

test("a page without the box header is not a box", () => {
  assert.equal(boxFromWords([[w(10, 10, "हेतु"), w(60, 10, "अहिंसा")]]), null);
});

test("box text rows: the vishay once over its passages, then Sr | Granth | ShastraPath", () => {
  const rows = boxTextRows([
    { sr: "1", granth: "क", shastraPath: "ख", vishay: "10.8.3 - अनुबंधअहिंसा" },
    { sr: "2", granth: "ग", shastraPath: "घ", vishay: "10.8.3 - अनुबंधअहिंसा" },
    { sr: "1", granth: "च", shastraPath: "छ", vishay: "10.8.6 - अनुबंधहिंसा" },
  ]);
  assert.deepEqual(rows, [{ band: "10.8.3 - अनुबंधअहिंसा" }, ["1", "क", "ख"], ["2", "ग", "घ"], { band: "10.8.6 - अनुबंधहिंसा" }, ["1", "च", "छ"]]);
});

test("a Word file is a zip with the document part", () => {
  const bytes = docxTable([{ band: "10.8.3" }, ["1", "क", "ख"]], [6, 24, 70]);
  assert.equal(String.fromCharCode(bytes[0], bytes[1]), "PK");
  const text = new TextDecoder().decode(bytes);
  assert.match(text, /word\/document\.xml/);
  assert.match(text, /<w:gridSpan w:val="3"\/>/);
  assert.ok(zipStore([{ name: "a.txt", data: new Uint8Array([65]) }]).length > 22);
});

test("the AI's anuvaad loses a quoted verse the source does not have (10.8.1 Sr. 3, 25 Nov 2026)", () => {
  const source = "हेतुअहिंसा जयणारूपे,";
  const reply = [
    "મૂ —",
    "हेतुअहिंसा जयणारूपे,",
    "अणुव्रत-महव्वय-रूवेण । सव्वपाण-भूय-जीव-सत्ते, वोसिरिओ तिविहं तिविहेण ॥८॥",
    "ટી. — 'હેતુઅહિંસા' — અહિંસાનું કારણ જે પચ્ચક્ખાણ તે.",
  ].join("\n");
  const out = toMarkup(dropUnsourced(parseAnuvaad(reply), source));
  assert.doesNotMatch(out, /अणुव्रत/);
  assert.match(out, /हेतुअहिंसा जयणारूपे/);
  assert.match(out, /અહિંસાનું કારણ/);
});

test("a Gujarati edition's page goes to the AI without its Gujarati lines", () => {
  const page = ["सम्मद्दिट्ठीण जईण-मकसाईणं भवे अहिंसतियं ।", "ગાથાર્થ-સમ્યગ્દૃષ્ટિ જીવોમાં, સાધુઓમાં અને વીતરાગ ભગવંતોમાં ત્રણ પ્રકારની અહિંસા સંભવે છે. યતનામાં ઉદ્યમવાળા અને યતનામાં ઉદ્યમથી રહિત જીવોને આશ્રયીને"].join("\n");
  assert.equal(sourceLinesOnly(page), "सम्मद्दिट्ठीण जईण-मकसाईणं भवे अहिंसतियं ।");
});

test("an APC page photographed from its granth line (no (क्र.-N) on it) still gives its entry", () => {
  const text = [
    "गीतार्थ गंगा",
    "ग्रंथ :- आचारांगसूत्र प्रथम श्रुतस्कंध शीलांकाचार्य टीका (प्रकाशक:-सिद्धचक्र साहित्य प्रचारक",
    "समिति, LibNo:-C009853)",
    "Adhyay :- 08 - विमोक्षअध्ययन, उद्देशक - 2, सूत्र-200",
    "Pointers :- 659134 to 659726",
    "तं गृहपतिमनासेवनया यथाऽनेन विधानेनोपकल्पितमाहारादिकं नाहं भुञ्जे एवं तस्य",
  ].join("\n");
  const entries = parseApcPages([{ page: 1, text }]);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].granth, "आचारांगसूत्र प्रथम श्रुतस्कंध शीलांकाचार्य टीका");
});
