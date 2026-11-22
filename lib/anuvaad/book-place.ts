// Where a passage's book Gujarati comes from, said in Gujarati, and which of
// the places found to take it from. No server code: the browser pages use it
// (box files on /anuvaad, and "Pages of a PDF" in components/AnuvaadPages).

import type { LocatedPassage } from "@/lib/anuvaad/locate";
import { type BookText, bookGujaratiText, pageGujaratiText, passageWords } from "@/lib/anuvaad/book-text";
import { gujaratiNumber } from "@/lib/anuvaad/pages";

/** "પાનું ૧૮૦–૧૮૧" from the printed page numbers (PDF numbers when a page has none). */
export function gujaratiPages(place: LocatedPassage, pdfPages: number[]) {
  const printed = pdfPages.map((n) => Number(place.pages.find((p) => p.page === n)?.printedPage) || n);
  const a = printed[0];
  const b = printed[printed.length - 1];
  return a === b ? `પાનું ${gujaratiNumber(a)}` : `પાનાં ${gujaratiNumber(a)}–${gujaratiNumber(b)}`;
}

/** Book name, number and pages, as the anuvaad's first line: every printed passage says where it is from. */
export function whereFrom(place: LocatedPassage, pdfPages: number[]) {
  return `${place.granthName}${place.bookNumber ? ` (ગ્રંથ નં. ${place.bookNumber})` : ""}, ${gujaratiPages(place, pdfPages)}`;
}

/** The book's own Gujarati, the passage's part in bold, with where it is from. */
export function bookMarkup(place: LocatedPassage, book: BookText) {
  return [`(પુસ્તકમાંથી: ${whereFrom(place, book.pages)})`, book.markup].join("\n");
}

/**
 * Of the places found, the one to use: the first whose pages hold the book's
 * own Gujarati for the passage, as long as the box does not name another
 * book found; else the first. A Granth column naming several books gives each
 * book the gatha numbers of its own part (LocatedPassage.granthPart).
 */
export function choosePlace(places: LocatedPassage[], row: { granth: string; shastraPath: string }) {
  const words = passageWords(row.shastraPath);
  const withBook = places.map((place) => ({ place, book: bookGujaratiText(place.pages, place.page, place.granthPart ?? row.granth, words) }));
  const named = places.some((p) => p.named);
  const pick = withBook.find((x) => x.book && (x.place.named || !named)) ?? withBook[0];
  return pick ? { place: pick.place, book: pick.book } : { place: null, book: null };
}

/**
 * For a whole page found in our library: the Gujarati printed on that page,
 * else (a page of Sanskrit whose Gujarati follows) the book's Gujarati for
 * the page's text, as for a passage.
 */
export function choosePagePlace(places: LocatedPassage[], text: string) {
  for (const place of places) {
    const book = pageGujaratiText(place.pages, place.page);
    if (book) return { place, book };
  }
  return choosePlace(places, { granth: "", shastraPath: text });
}
