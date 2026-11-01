// The Gujarati anuvaad of a box passage, in the form of the anuvaad pages
// Maharaj Saheb sent with the box files (WhatsApp scans, 29 Oct 2026):
//
//   પ્રજ્ઞાપનાસૂત્ર મલયગિરિસૂરિ ટીકા (પદ ૨૨ — ક્રિયાપદ, સૂત્ર ૧૬૩૭-૧૬૬૩)
//   વિષય: વિરત જીવનો કર્મબંધ, ગુણસ્થાનક વ્યવસ્થા અને ૨૭ ભંગોની વિચારણા
//   ૧. મૂળ જિજ્ઞાસા અને ગુણસ્થાનક અનુસાર બંધક સ્વરૂપ
//     • શંકા: …
//     • સમાધાન: …
//        ◦ ૭ પ્રકૃતિના બંધક: …
//
// His pen marks on those pages set the rules in the prompt below: what is
// not about the box's vishay is left out (the eight limbs of yoga struck from
// the Pātañjala page), no sūtra numbers inside the text, no stacked synonyms
// in brackets ("(મૂર્ખ/અજ્ઞાની)" struck), and no invented headings and labels
// where the text runs on as one explanation ("દુષ્પરિણામ:", "શુભ પરિણામ:"
// struck; "કારણ કે", "પરંતુ જે સાધકને … હોય અર્થાત્" written in).
//
// The reader never sees which service writes the anuvaad.

import { CHAT_MODELS, SarvamChatError, chat } from "@/lib/sarvam-chat";
import { type AnuvaadBlock, parseAnuvaad, toMarkup } from "@/lib/anuvaad/markup";

export { type AnuvaadBlock, parseAnuvaad, toMarkup };

export type Anuvaad = {
  blocks: AnuvaadBlock[];
  markup: string;
  /** The sentence-by-sentence translation the anuvaad was written from, for checking. */
  literal: string;
};

export class AnuvaadError extends Error {
  constructor(message: string, readonly status = 502) {
    super(message);
  }
}

const RULES = `You write the Gujarati anuvaad (bhāvānuvād) of Jain śāstra passages for a Jain ācārya, who prints it and reads it next to the original. The passage is in Sanskrit or Prakrit (Ardhamāgadhī) — never read it as Hindi.

ACCURACY COMES FIRST
- Say what the passage says, all of it that bears on the vishay, and nothing it does not say. Never add facts, examples, numbers, names or conclusions of your own. If a word or line cannot be read with confidence, render it closely rather than guessing a meaning.
- Keep every distinction the text draws (kinds, counts, conditions, exceptions, who asks and who answers). Numbers and counts must match the text exactly.
- The passage came from OCR of a printed page: a word broken at a line end appears split by a space ("ह न्यमानस्य" is "हन्यमानस्य"), and a letter may be misread. Read it as printed text would be read; never mention the OCR.
- "मू०" marks the mūla (sūtra or gāthā) and "टी०" the ṭīkā (commentary); "अ०" introduces the context. Translate the mūla and explain it through the ṭīkā, as one account.
- The box is about one vishay (given below). Give full weight to what the passage says about it. A part that is not about the vishay (a list the passage passes through, the preamble of another topic) is left out or said in one short line.

LANGUAGE
- Pure, plain Gujarati as Jain sādhus write it, with the established Jain terms in their Gujarati forms: પ્રાણાતિપાત-વિરમણ, મહાવ્રત, પચ્ચક્ખાણ, ગુણસ્થાનક, કર્મબંધ, ત્રસ-સ્થાવર, સૂક્ષ્મ-બાદર, પર્યાપ્તા-અપર્યાપ્તા, એકેન્દ્રિય, જીવનિકાય, સમિતિ, ભાવના, આધાકર્મ, અનુમોદન, ત્રિવિધ-ત્રિવિધેન, યાવજ્જીવ, ઇત્યાદિ.
- Respectful forms: ભગવંત, વીર પ્રભુ, ગૌતમ સ્વામી, સુધર્મસ્વામી, ઉપાધ્યાય શ્રી યશોવિજયજી.
- Gujarati digits (૧, ૨, ૨૭) in the Gujarati text.
- A short phrase of the original may be quoted in single quotes in Gujarati script ('મહાઘોર', 'એકાન્તબાળ') where the text dwells on that word.
- Brackets only to give the technical term or one needed clarification — never two synonyms in a bracket, never a sūtra or gāthā number inside the text.
- Prefer one clear, connected explanation with connectives (કારણ કે, તેથી, પરંતુ, અર્થાત્, જેમ … તેમ) over chopping it into labelled fragments.

FORM (exactly this markup, nothing before or after it)
# <the granth, in Gujarati script, as a heading: name, then the place in brackets, e.g. પ્રજ્ઞાપનાસૂત્ર મલયગિરિસૂરિ ટીકા (પદ ૨૨ — ક્રિયાપદ, સૂત્ર ૧૬૩૭-૧૬૬૩)>
વિષય: <one line: what this passage teaches about the vishay>
## ૧. <a short heading for the first part>
<a paragraph of plain text, when the part is one explanation>
- **<label>:** <text>     (a point, when the text itself gives distinct items: શંકા / સમાધાન, a list of kinds, the five bhāvanās, examples)
  - **<label>:** <text>   (a sub-point under the point above)
## ૨. <next part>
…
Use one to four numbered parts, as the passage needs; a short passage may need one part and no points. Every point has a short bold label followed by a colon.`;

// One of Maharaj Saheb's own pages (Ācārāṅga, bhāvanā cūlikā), with his pen
// corrections applied, as the model of tone and form.
const EXAMPLE_PASSAGE = `Granth: आचारांगसूत्र द्वितीय श्रुत स्कंध मूल-निर्युक्ति, चूलिका - 3 (भावनाचूलिका अपरनाम भावनाअध्ययन - 3), सूत्र 179
Passage: तओ णं समणे भगवं महावीरे उप्पन्ननाणदंसणधरे गोयमाईणं समणाणं पंच महव्वयाइं सभावणाइं छज्जीवनिकाया आतिक्खति भासइ परूवेइ, तंजहा-पुढविकाए जाव तसकाए, पढमं भंते! महव्वयं पच्चक्खामि सव्वं पाणाइवायं से सुहुमं वा बायरं वा तसं वा थावरं वा नेव सयं पाणाइवायं करिज्जा 3 जावज्जीवाए तिविहं तिविहेणं मणसा वयसा कायसा तस्स भंते! पडिक्कमामि निंदामि गरिहामि अप्पाणं वोसिरामि, तस्सिमाओ पंच भावणाओ भवंति, तत्थिमा पढमा भावणा-इरियासमिए से निग्गंथे नो अणइरियासमिएत्ति, केवली बूया०-अणइरियासमिए से निग्गंथे पाणाइं भूयाइं जीवाइं सत्ताइं अभिहणिज्ज वा वत्तिज्ज वा परियाविज्ज वा लेसिज्ज वा उद्दविज्ज वा, इरियासमिए से निग्गंथे नो इरियाअसमिइत्ति पढमा भावणा 1। अहावरा दुच्चा भावणा-मणं परियाणइ से निग्गंथे, जे य मणे पावए सावज्जे सकिरिए अण्हयकरे छेयकरे भेयकरे अहिगरणिए पाउसिए पारियाविए पाणाइवाइए भूओवघाइए, तहप्पगारं मणं नो पधारिज्जा गमणाए, मणं परिजाणइ से निग्गंथे, जे य मणे अपावएत्ति दुच्चा भावणा 2। अहावरा तच्चा भावणा-वइं परिजाणइ से निग्गंथे, जा य वई पाविया सावज्जा सकिरिया जाव भूओवघाइया तहप्पगारं वइं नो उच्चारिज्जा, जे वइं परिजाणइ से निग्गंथे, जाव वइं अपावियत्ति तच्चा भावणा 3। अहावरा चउत्था भावणा-आयाणभंडमत्तनिक्खेवणासमिए से निग्गंथे, नो अणायाणभंडमत्तनिक्खेवणासमिए, केवली बूया०-आयाणभंडमत्तनिक्खेवणाअसमिए से निग्गंथे पाणाइं भूयाइं जीवाइं सत्ताइं अभिहणिज्जा वा जाव उद्दविज्ज वा, तम्हा आयाणभंडमत्तनिक्खेवणासमिए से निग्गंथे, नो आयाणभंडनिक्खेवणाअसमिएत्ति चउत्था भावणा 4। अहावरा पंचमा भावणा-आलोइयपाणभोयणभोई से निग्गंथे नो अणालोइयपाणभोयणभोई, केवली बूया०-अणालोईयपाणभोयणभोई से निग्गंथे पाणाणि वा 4 अभिहणिज्ज वा जाव उद्दविज्ज वा, तम्हा आलोइयपाणभोयणभोई से निग्गंथे नो अणालोईयपाणभोयणभोईत्ति पंचमा भावणा 5। एयावता महव्वए सम्मं काएण फासिए पालिए तीरिए किट्टिए अवट्ठिए आणाए आराहिए यावि भवइ, पढमे भंते! महव्वए पाणाइवायाओ वेरमणं।।`;

const EXAMPLE_ANUVAAD = `# આચારાંગ સૂત્ર (દ્વિતીય શ્રુતસ્કંધ, ભાવના ચૂલિકા, અધ્યયન ૩, સૂત્ર ૧)
વિષય: પ્રથમ પ્રાણાતિપાત-વિરમણ મહાવ્રત અને તેની ૫ ભાવનાઓ
## ૧. મહાવ્રતની સ્વીકૃતિ અને પ્રતિજ્ઞા
કેવળજ્ઞાન અને કેવળદર્શનના ધારક ભગવાન મહાવીરે ગૌતમ સ્વામી આદિ મુનિઓને છ જીવનિકાય (પૃથ્વીકાયથી ત્રસકાય સુધી)ની રક્ષા કાજે પ્રથમ મહાવ્રતની પ્રરૂપણા કરી છે:
- **અહિંસાનો સંકલ્પ:** મુનિ જીવનપર્યંત (યાવજ્જીવ) સૂક્ષ્મ કે બાદર, ત્રસ કે સ્થાવર — કોઈપણ જીવની હિંસા ન કરવાની દૃઢ પ્રતિજ્ઞા લે છે.
- **ત્રિવિધ-ત્રિવિધેન નિયમ:** મન, વચન કે કાયાથી — પોતે હિંસા ન કરવી, અન્ય પાસે ન કરાવવી અને હિંસા કરનારનું અનુમોદન પણ ન કરવું.
- **દોષ-શુદ્ધિ:** ભૂતકાળમાં અજાણતાં થયેલા દોષોનું પ્રતિક્રમણ કરવું, આત્મ-નિંદા કરવી, ગુરુસમક્ષ ગર્હા (દોષોનું પ્રકાશન) કરવી અને પાપ પ્રવૃત્તિઓનો ત્યાગ (વ્યુત્સર્ગ) કરવો.
## ૨. વ્રતની રક્ષા કાજે ૫ ભાવનાઓ
પ્રથમ મહાવ્રત દોષરહિત અને અખંડ રહે તે માટે મુનિ પાંચ બાબતોનું સદા પાલન કરે છે, તેને પાંચ ભાવનાઓ કહેવાય છે તે આ પ્રમાણે:
- **૧. ઈર્યા સમિતિ (જોઈને ચાલવું):** ચાલતી વખતે માર્ગમાં રહેલા જીવો દબાઈ કે ઘવાઈ ન જાય તે માટે સદાય જોઈ-તપાસીને જ ગમન કરવું; અસાવધાન થઈને ન ચાલવું.
- **૨. મન પરિજ્ઞાન (મનની શુદ્ધિ):** પાપકારી, સાવદ્ય, ક્લેશ-વૈર વધારનારા કે કોઈપણ જીવને પરિતાપ અને ઘાત કરાવનારા અશુભ વિચારોથી મનને રોકી રાખવું અને સદા નિર્દોષ મન રાખવું.
- **૩. વચન પરિજ્ઞાન (વાણીની શુદ્ધિ):** કઠોર, સાવદ્ય કે અન્ય જીવોને પીડા અને ઉપઘાત પહોંચાડનારી અશુભ ભાષા ન બોલવી; માત્ર નિર્દોષ અને હિતકારી વાણી જ ઉચ્ચારવી.
- **૪. આદાન-ભાંડ-નિક્ષેપણા સમિતિ (સાધનોની સાચવણી):** પાત્ર કે વસ્ત્રો જેવાં સાધનો લેવા-મૂકવામાં જીવોની હિંસા ન થાય તે માટે હંમેશા પૂરતી કાળજીપૂર્વક જોઈ-તપાસી અને પૂંજીને જ સાધન મૂકવું કે ઉપાડવું.
- **૫. આલોકિત પાન-ભોજન (જોઈને આહાર-પાણી):** અંધારામાં કે જોયા વિના આહાર-પાણી વાપરવાથી થતી જીવહિંસા ટાળવા સૂર્યપ્રકાશ કે યોગ્ય અજવાળામાં બરાબર જોઈ-ચકાસીને જ ભોજન-પાણી ગ્રહણ કરવું.
## ૩. પરિપૂર્ણ આરાધના (ફળશ્રુતિ)
જ્યારે મુનિ આ પાંચ ભાવનાઓ સાથે આ મહાવ્રતનું કાયાથી આચરણ કરે છે, પાલન કરે છે, અંત સુધી નિભાવે છે, તેની પ્રભાવના કરે છે અને તેમાં દૃઢ રહે છે — ત્યારે જ જિનાજ્ઞા મુજબ પ્રથમ પ્રાણાતિપાત-વિરમણ મહાવ્રતની સાચી આરાધના સિદ્ધ થાય છે.`;

export type PassageInput = {
  /** The box's vishay ("प्राणातिपातविरमणमहाव्रत"). */
  vishay: string;
  granth: string;
  passage: string;
  /** In.Rem / Pub.Rem notes, which may say what to keep. */
  note?: string;
};

function userPrompt(input: PassageInput) {
  return [
    `Vishay of the box: ${input.vishay || "(not given)"}`,
    `Granth: ${input.granth || "(not given)"}`,
    `Passage: ${input.passage}`,
    "",
    "Write its Gujarati anuvaad in the form given.",
  ].join("\n");
}

const GUJARATI = /[઀-૿]/g;

/** The reply must be Gujarati: a refusal or an English answer is not an anuvaad. */
function looksGujarati(text: string) {
  const letters = text.replace(/[\s\d\p{P}\p{S}]/gu, "").length;
  return letters > 0 && (text.match(GUJARATI)?.length ?? 0) / letters > 0.6;
}

const MAX_PASSAGE = 14_000;

// Sarvam's models read Sanskrit but not Ardhamāgadhī: on their own they give
// पच्चक्खाय as "प्रत्यक्ष" and अकंत as "अकण्टक" (tested 1 Nov 2026). The words
// that recur in the box passages are given to them here; the rest of a mūla is
// read through the ṭīkā that comes with it.
const GLOSSARY = `Glossary — source word = its Gujarati (use these meanings; never copy a "=" line into the answer):
पाणाइवाय, प्राणातिपात = પ્રાણાતિપાત (જીવહિંસા)
वेरमण, विरमण = વિરમણ (અટકવું)
पच्चक्खाय, पच्चक्खाण, प्रत्याख्यान = પચ્ચક્ખાણ (ત્યાગની પ્રતિજ્ઞા) — never "પ્રત્યક્ષ"
सुपच्चक्खाय = સુપ્રત્યાખ્યાન (સાચું પચ્ચક્ખાણ)
दुपच्चक्खाय = દુષ્પ્રત્યાખ્યાન (ખોટું પચ્ચક્ખાણ)
पाण, भूय, जीव, सत्त (प्राण, भूत, जीव, सत्त्व) = પ્રાણ, ભૂત, જીવ, સત્ત્વ — keep these four words; सत्त्व is never "પશુ"
भंते = હે ભગવંત (Gautama, the disciple, is asking)
गोयमा = હે ગૌતમ (Bhagavān is answering)
से केणट्ठेणं = એવું શા કારણે
सिय, स्यात् = કદાચ
जाव, यावत् = "થી લઈને … સુધી", as in "સર્વ પ્રાણથી લઈને સર્વ સત્ત્વો સુધી"
णो एवं अभिसमन्नागयं भवति = એને આવું સમ્યક્ જ્ઞાન નથી હોતું
अभिसमन्नागय, अभिसमन्वागत = સમ્યક્ જાણેલું
तस, त्रस = ત્રસ જીવ
थावर, स्थावर = સ્થાવર જીવ
सुहुम, सूक्ष्म = સૂક્ષ્મ
बायर, बादर = બાદર
तिविहं तिविहेणं, त्रिविधं त्रिविधेन = ત્રિવિધ-ત્રિવિધે (કરવું, કરાવવું, અનુમોદવું — મન, વચન, કાયાથી)
संजय, संयत = સંયત (હિંસાદિ ટાળવામાં પ્રયત્નશીલ)
विरय, विरत = વિરત (હિંસાદિથી અટકેલો)
पडिहय, प्रतिहत = ભૂતકાળનાં પાપ નિંદાથી હણનાર
पच्चक्खाय-पावकम्म, प्रत्याख्यात-पापकर्म = ભવિષ્યનાં પાપ પચ્ચક્ખાણથી ત્યાગનાર
असंजय-विरय-पडिहय-पच्चक्खाय-पावकम्म = અસંયત, અવિરત, પાપકર્મોને ન હણનાર કે ન ત્યાગનાર
सकिरिय, सक्रिय = સક્રિય (કાયિકી આદિ ક્રિયાવાળો)
अकिरिय, अक्रिय = અક્રિય
असंवुड, असंवृत = અસંવૃત (આશ્રવદ્વાર ખુલ્લાં)
संवुड, संवृत = સંવૃત (આશ્રવદ્વાર બંધ)
एगंतदंड, एकान्तदण्ड = એકાન્તદંડ (બીજાને સર્વથા દંડ-પીડા આપનાર)
एगंतबाल, एकान्तबाल = એકાન્તબાળ (સર્વથા અજ્ઞાની)
एगंतपंडिय, एकान्तपण्डित = એકાન્તપંડિત
मुसावाई = મૃષાવાદી; सच्चवाई = સત્યવાદી
मोसं भासं = અસત્ય ભાષા; सच्चं भासं = સત્ય ભાષા
अकंत, अकान्त = અપ્રિય (अकंतदुक्ख = જેને દુઃખ અપ્રિય છે) — never "કાંટા"
महव्वय, महाव्रत = મહાવ્રત; अणुव्रत = અણુવ્રત
भावणा, भावना = ભાવના
इरियासमिय, ईर्यासमित = ઈર્યાસમિતિવાળો (જોઈને ચાલનાર)
निग्गंथ, निर्ग्रन्थ = નિર્ગ્રંથ મુનિ
केवली बूया = કેવળી ભગવંત કહે છે
जावज्जीवाए, यावज्जीवम् = જીવનપર્યંત
पडिक्कमामि निंदामि गरिहामि अप्पाणं वोसिरामि = પ્રતિક્રમણ કરું છું, નિંદા કરું છું, ગર્હા કરું છું અને (પાપયુક્ત) આત્માનો ત્યાગ કરું છું
आहाकम्म, आधाकर्म = આધાકર્મ; पूइकम्म, पूतिकर्म = પૂતિકર્મ; एसणा, एषणा = એષણા
छज्जीवनिकाय, षड्जीवनिकाय = છ જીવનિકાય
कृत-कारित-अनुमति = કરવું, કરાવવું, અનુમોદવું
बन्धक = બંધક (કર્મ બાંધનાર); विरह = વિરહકાળ; भङ्ग = ભંગ
अवच्छिन्न = મર્યાદિત (જાતિ, દેશ, કાળ કે સમયની મર્યાદાવાળું); अनवच्छिन्न = કોઈ મર્યાદા વિનાનું
समय (in जाति-देश-काल-समय) = પ્રયોજન
व्यभिचार = દોષ (ભંગ)
सार्वभौम = સાર્વભૌમ (સર્વ ભૂમિકામાં લાગુ)
Guṇasthānakas, by name and number: मिथ्यादृष्टि ૧લું, सासादन ૨જું, मिश्र ૩જું, अविरतसम्यग्दृष्टि ૪થું, देशविरत ૫મું, प्रमत्तसंयत ૬ઠ્ઠું, अप्रमत्तसंयत ૭મું, अपूर्वकरण ૮મું, अनिवृत्तिबादर(सम्पराय) ૯મું, सूक्ष्मसम्पराय ૧૦મું, उपशान्तमोह ૧૧મું, क्षीणमोह ૧૨મું, सयोगिकेवली ૧૩મું, अयोगिकेवली ૧૪મું ગુણસ્થાનક. A compound like प्रमत्ताप्रमत्तापूर्वकरणानिवृत्तबादरसम्परायाः is a list of these.
सप्तविध/अष्टविध/षड्विध/एकविध-बन्धक = ૭/૮/૬/૧ કર્મપ્રકૃતિ બાંધનાર; अबन्धक = અબંધક
उत्कर्षतः = ઉત્કૃષ્ટથી (વધુમાં વધુ); जघन्यपदे = જઘન્યથી (ઓછામાં ઓછા); षण्मासविरह = છ માસનો વિરહકાળ (એટલો સમય ન હોય)
ननु = (a doubt follows) શંકા; उच्यते = (the answer follows) સમાધાન
कषाय = કષાય; योग = યોગ (મન-વચન-કાયાની પ્રવૃત્તિ); प्रकृति = (કર્મ)પ્રકૃતિ; संज्वलन = સંજ્વલન
इत्यादि = ઇત્યાદિ; इति भावः = એવો ભાવ છે; इत्यर्थः = એવો અર્થ છે; तथाहि = તે આ પ્રમાણે`;

const LITERAL_RULES = `You translate Jain śāstra into Gujarati for a Jain ācārya. The text is Sanskrit (ṭīkā, commentary) or Prakrit (Ardhamāgadhī mūla, marked मू०); never read it as Hindi.
Translate the text the user gives, completely and literally, clause by clause, in order: nothing left out, nothing added, nothing explained beyond the words. Translate the meaning into plain Gujarati sentences — do not just write the Sanskrit or Prakrit words in Gujarati letters. Where the text comments on a word ('सकिरिए'त्ति …), quote that word in Gujarati script and give the meaning the text gives it. Keep numbers exact.
The text came from OCR: a word split by a space at a line end is one word ("ह न्यमानस्य" = "हन्यमानस्य").
Output only the Gujarati translation — no heading, no note, no repetition of the source.

${GLOSSARY}`;

const STRUCTURE_RULES = `${RULES}

You are given a literal Gujarati translation of the passage, made sentence by sentence; it is your only source for the meaning. Keep its sentences and their meaning: group them under the parts, turn its distinct items into points, join and smooth the Gujarati, and leave out what is not about the vishay. Do not add any fact, example, reason or speaker that is not in it, and keep who asks and who answers exactly as it has them. Use the glossary for the Gujarati of Jain terms.

${GLOSSARY}

A page in the right form (another passage):
${"{EXAMPLE}"}`;

/** The passage in pieces of a few sentences, cut at sentence ends and at the मू० / टी० / अ० marks. */
export function splitPassage(passage: string, max = 550): string[] {
  const marked = passage.replace(/\s*((?:अ|मू|टी)[०.]\s*-)/g, "\n$1").split("\n").map((p) => p.trim()).filter(Boolean);
  const pieces: string[] = [];
  for (const part of marked) {
    const sentences = part.match(/[^।॥?!]+(?:[।॥?!]+[\s।॥0-9०-९().,]*|$)/g) ?? [part];
    let current = "";
    for (const raw of sentences) {
      const sentence = raw.trim();
      if (!sentence) continue;
      // a very long sentence (a ṭīkā running on with commas) is cut at a comma
      const bits = sentence.length > max ? sentence.match(new RegExp(`[^,]{1,${max}}(?:,|$)`, "g")) ?? [sentence] : [sentence];
      for (const bit of bits) {
        if (current && current.length + bit.length + 1 > max) {
          pieces.push(current);
          current = bit.trim();
        } else current = `${current} ${bit}`.trim();
      }
    }
    if (current) pieces.push(current);
  }
  return pieces;
}

// Gujarati that is a translation has these little words; Sanskrit or Prakrit
// spelt in Gujarati letters has almost none of them.
const GUJARATI_WORDS = /(^|\s)(છે|છીએ|થાય|કરે|અને|તે|એટલે|તેથી|જે|કે|પણ|નથી|હોય|માટે|કહે|એમ|એવું|તો)(?=[\s,.;:।?!]|$)/g;

function readsAsGujarati(text: string) {
  const words = text.split(/\s+/).filter(Boolean).length;
  if (!words) return false;
  const gujarati = (text.match(GUJARATI_WORDS)?.length ?? 0) / words;
  // visarga and Sanskrit case endings left as they were
  const sanskrit = (text.match(/ઃ|્(?=[\s,.;।]|$)|(^|\s)(ઇતિ|ભવતિ|એવ|અપિ|ચ|વા|તુ|ઇત્યર્થઃ)(?=[\s,.;।]|$)/g)?.length ?? 0) / words;
  return gujarati >= 0.1 && sanskrit < 0.12;
}

/** Cuts a reply that started repeating itself: at the first sentence (or, with `lines`, line) it has already said. */
function stopRepeats(text: string, lines = false) {
  const parts = lines ? text.split("\n") : text.split(/(?<=[.।?!])\s+/);
  const seen = new Set<string>();
  const out: string[] = [];
  for (const part of parts) {
    const key = part.trim();
    if (key.length > 25 && seen.has(key)) break;
    seen.add(key);
    out.push(part);
  }
  return out.join(lines ? "\n" : " ").trim();
}

async function literal(piece: string, depth = 0): Promise<string> {
  let best = "";
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const { content } = await chat({
      model: CHAT_MODELS.fast,
      temperature: attempt ? 0.3 : 0,
      maxTokens: Math.min(4000, 400 + piece.length * 4),
      messages: [
        { role: "system", content: LITERAL_RULES },
        {
          role: "user",
          content: attempt ? `Translate the meaning of this into Gujarati sentences (not a transliteration):\n${piece}` : piece,
        },
      ],
    });
    const text = stopRepeats(content.trim());
    // A reply far longer than the piece has run on into text of its own.
    const sane = text.length < piece.length * 4 ? text : text.slice(0, piece.length * 3);
    if (readsAsGujarati(sane)) return sane;
    best ||= sane;
  }
  // Still spelt out rather than translated: smaller pieces read better.
  if (depth < 2 && piece.length > 160) {
    const mid = piece.length / 2;
    const cuts = [...piece.matchAll(/[,।;]\s/g)].map((m) => (m.index ?? 0) + 1);
    const cut = cuts.sort((a, b) => Math.abs(a - mid) - Math.abs(b - mid))[0] ?? piece.lastIndexOf(" ", mid);
    if (cut > 0 && cut < piece.length - 1) {
      const halves = await Promise.all([literal(piece.slice(0, cut).trim(), depth + 1), literal(piece.slice(cut).trim(), depth + 1)]);
      return halves.join(" ");
    }
  }
  return best;
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T, index: number) => Promise<R>) {
  const out = new Array<R>(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i], i);
      }
    })
  );
  return out;
}

/** Tokens a prompt of this many characters of Indic text takes (measured: about 1.6 characters a token). */
const tokensFor = (chars: number) => Math.ceil(chars / 1.6);

export async function translatePassage(input: PassageInput): Promise<Anuvaad> {
  const passage = input.passage.replace(/\s+/g, " ").trim();
  if (!passage) throw new AnuvaadError("This row has no passage to translate.", 400);
  if (passage.length > MAX_PASSAGE) throw new AnuvaadError("This passage is too long to translate at once. Split it into two rows.", 400);

  let lastError: unknown = null;
  let literals: string[] | null = null;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      literals ??= await mapLimit(splitPassage(passage), 4, (piece) => literal(piece));
      const literalText = literals.join("\n");
      const system = STRUCTURE_RULES.replace("{EXAMPLE}", EXAMPLE_ANUVAAD);
      const user = [
        `Vishay of the box: ${input.vishay || "(not given)"}`,
        `Granth: ${input.granth || "(not given)"}`,
        "",
        "Literal Gujarati translation of the passage:",
        literalText,
        "",
        "Write the Gujarati anuvaad in the form given.",
      ].join("\n");
      const room = 31_000 - tokensFor(system.length + user.length);
      const { content } = await chat({
        model: CHAT_MODELS.fast,
        temperature: 0,
        maxTokens: Math.max(1500, Math.min(8000, room)),
        messages: [
          { role: "system", content: system },
          { role: "user", content: user },
        ],
      });
      const markup = stopRepeats(content.replace(/<think>[\s\S]*?<\/think>/g, "").trim(), true);
      const blocks = parseAnuvaad(markup);
      if (blocks.length && looksGujarati(markup)) return { blocks, markup: toMarkup(blocks), literal: literalText };
      lastError = new AnuvaadError("The anuvaad came back incomplete.");
    } catch (error) {
      lastError = error;
      if (error instanceof SarvamChatError && error.status >= 400 && error.status < 500 && error.status !== 429) break;
    }
  }
  console.error("anuvaad failed:", lastError instanceof Error ? lastError.message : lastError);
  throw new AnuvaadError("The anuvaad could not be prepared just now. Please try again.");
}
