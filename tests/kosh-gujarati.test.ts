// The Gujarati anuvaad from the kosh itself, and Maharaj Saheb's saved words.
import assert from "node:assert/strict";
import { test } from "node:test";
import { gujaratiOfEntry, printedGujarati, printsGujarati } from "@/lib/vyutpatti/kosh-gujarati";
import { isAnuvaadPage, isExplanation, wordInText, wordKey } from "@/lib/anuvaad/saved-words";
import { rowEntry } from "@/lib/vyutpatti/format";

// Shabda Ratna Mahodadhi part 1, page read as Gujarati (9 Nov 2026)
const SRM_PAGE = `अहिंसा स्त्री. (न हिंसा) ૧. હિંસાનો અભાવ, મન
વચન કે કાયાથી કોઈપણ પ્રાણીને પીડા ન કરવી તે,
પ્રાણ પીડાથી દૂર રહેવું તે. –अहिंसा परमो धर्मः ।
अहिंसान त्रि. (न+हिंस् शीलार्थे+आनश्) હિંસાશીલ
નહિ તે.
अहिंस्र त्रि. (न हिंस्रः) અહિંસક, હિંસાશીલ નહિ તે.
अहिंस्र पु ते નામનું એક વૃક્ષ, કુલિક વૃક્ષ (કુલે
ખાડા).`;

test("the Gujarati Shabda Ratna Mahodadhi prints for an entry is taken as printed", () => {
  assert.equal(printsGujarati("380"), true);
  assert.equal(printsGujarati("371"), true);
  assert.equal(printsGujarati("375"), false); // Apte prints Hindi
  assert.equal(
    gujaratiOfEntry(SRM_PAGE, "380", "अहिंसा"),
    "૧. હિંસાનો અભાવ, મન વચન કે કાયાથી કોઈપણ પ્રાણીને પીડા ન કરવી તે, પ્રાણ પીડાથી દૂર રહેવું તે."
  );
  // the Sanskrit quotation after it is not Gujarati and is left out
  assert.ok(!gujaratiOfEntry(SRM_PAGE, "380", "अहिंसा").includes("परमो"));
  assert.equal(gujaratiOfEntry(SRM_PAGE, "380", "अहिंसान"), "હિંસાશીલ નહિ તે.");
  assert.equal(gujaratiOfEntry(SRM_PAGE, "380", "निर्जरा"), "");
});

test("AVPK's meaning is the Gujarati after the number, before the synonyms and derivation", () => {
  assert.equal(printedGujarati("चोर-पुं-३८१-ચોરી કરનાર, ચોર. ☐ तस्कर, स्तेन * चोरयति इति चोरः", "avpk"), "ચોરી કરનાર, ચોર.");
  assert.equal(printedGujarati("जीरण-पुं-४२२-शे० (१०३)-જીરું. द्र० कणाशब्दः ।", "avpk"), "જીરું.");
  assert.equal(printedGujarati("अहिंसा-સ્ત્રી-૮૨ કોઈ જીવને ન મારવું તે પ્રથમ યમ.", "avpk"), "કોઈ જીવને ન મારવું તે પ્રથમ યમ.");
  // Gujarati the old reading took for Devanagari is put back in Gujarati letters
  assert.equal(printedGujarati("क्षमा-स्त्री-९४१-पृथ्वी.", "avpk"), "પૃથ્વી.");
  // the scanner's footer is not part of the meaning
  assert.equal(printedGujarati("आत्मन्-पुं-१२-આત્મા જીવ. Jain Education International ☐ क्षेत्रज्ञ", "avpk"), "આત્મા જીવ.");
});

test("a vyutpatti row carries its kosh page for the kosh's Gujarati", () => {
  const e = rowEntry({ shastraPath: "अहिंसा - स्त्री. (न हिंसा) (शब्दरत्नमहोदधि भाग-1, पृ. 182)", inRem: "x", granthKey: "380", pdfPage: 305 });
  assert.equal(e.granthKey, "380");
  assert.equal(e.page, 305);
  assert.equal(rowEntry({ shastraPath: "a - b", inRem: "" }).granthKey, undefined);
});

test("saved words are found by their stem in either script, and in the box's forms", () => {
  assert.equal(wordKey("अहिंसा"), wordKey("અહિંસા"));
  assert.equal(wordKey("देवः"), wordKey("देव"));
  assert.ok(wordInText("प्रत्याख्यान", "से नूणं भंते सव्वपाणेहिं ... प्रत्याख्यानं कृतवान्"));
  assert.ok(wordInText("विरत", "स विरतः संयतः"));
  assert.ok(!wordInText("निर्जरा", "स विरतः संयतः"));
});

test("only real explanations are kept from an anuvaad page", () => {
  assert.equal(isExplanation("परिच्छिन्न", "મર્યાદિત"), true);
  assert.equal(isExplanation("विरत", "વિરત"), false); // the word itself again
  assert.equal(isExplanation("तथा", "તથા ત્રીજું ગુણવ્રત"), false); // an ordinary word
  assert.equal(isExplanation("आयुष्य", "અને આયુષ્ય સિવાય છ કર્મો બાંધે છે."), false); // a fragment
  assert.equal(isExplanation("अपूर्वकरण", "(૮નું)"), false);
  assert.equal(isAnuvaadPage("વિષય: વિરત જીવનો કર્મબંધ ".repeat(20)), true);
  assert.equal(isAnuvaadPage("स्थावराः पृथिव्यादयः किं बहुनोक्तेन ".repeat(20)), false);
});

test("the entry printed with the asked headword wins over a look-alike", () => {
  const page = `हिंसा स्त्री. (हिंस्+अ+टाप्) વધ, મારી નાંખવું તે,
ચોરી વગેરે કામ.
हिंस्रा स्त्री. (हिंस्र+टाप्) જટામાંસી વનસ્પતિ, કાકાદની વનસ્પતિ.`;
  assert.equal(gujaratiOfEntry(page, "382", "हिंसा", "स्त्री. - y"), "વધ, મારી નાંખવું તે, ચોરી વગેરે કામ.");
});
