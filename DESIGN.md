---
name: Granth Search
description: The Light Table. Every hit is a page slip on backlit glass, the word ringed in grease pencil, ready to be checked.
colors:
  glass: "#e6edf1"
  glass-lit: "#f7fafc"
  glass-edge: "#cad7df"
  slip: "#ffffff"
  ink: "#14212a"
  ink-2: "#43535f"
  ink-3: "#526270"
  rule: "#d5dfe5"
  tick: "#9fb3bf"
  pencil: "#c62f1c"
  pencil-wash: "rgba(198, 47, 28, 0.1)"
  cobalt: "#1f5390"
  cobalt-deep: "#173e6c"
  cobalt-wash: "#e4edf7"
  amber: "#815300"
  amber-wash: "#fbf0d9"
typography:
  display:
    fontFamily: "Archivo, Segoe UI, system-ui, sans-serif"
    fontSize: "clamp(38px, 5vw, 60px)"
    fontWeight: 680
    lineHeight: 0.95
    letterSpacing: "-0.03em"
    fontVariation: "'wdth' 92"
  headline:
    fontFamily: "Archivo, Segoe UI, system-ui, sans-serif"
    fontSize: "clamp(28px, 3.2vw, 38px)"
    fontWeight: 720
    lineHeight: 1
    letterSpacing: "-0.02em"
    fontVariation: "'wdth' 112"
  title:
    fontFamily: "Archivo, Segoe UI, system-ui, sans-serif"
    fontSize: "17px"
    fontWeight: 650
    lineHeight: 1.3
  body:
    fontFamily: "Archivo, Segoe UI, system-ui, sans-serif"
    fontSize: "15px"
    fontWeight: 400
    lineHeight: 1.45
    fontFeature: "tnum"
  label:
    fontFamily: "Archivo, Segoe UI, system-ui, sans-serif"
    fontSize: "12.5px"
    fontWeight: 600
    letterSpacing: "0.02em"
    fontVariation: "'wdth' 88"
  folio:
    fontFamily: "Archivo, Segoe UI, system-ui, sans-serif"
    fontSize: "34px"
    fontWeight: 650
    lineHeight: 1
    letterSpacing: "-0.02em"
    fontVariation: "'wdth' 80"
  indic-line:
    fontFamily: "Tiro Devanagari Sanskrit, Noto Serif Gujarati, Noto Serif Devanagari, serif"
    fontSize: "21px"
    fontWeight: 400
    lineHeight: 1.8
  indic-query:
    fontFamily: "Tiro Devanagari Sanskrit, Noto Serif Gujarati, Noto Serif Devanagari, serif"
    fontSize: "clamp(22px, 2.4vw, 28px)"
    fontWeight: 400
    lineHeight: 1.5
  indic-chip:
    fontFamily: "Tiro Devanagari Sanskrit, Noto Serif Gujarati, Noto Serif Devanagari, serif"
    fontSize: "20px"
    fontWeight: 400
    lineHeight: 1.4
rounded:
  slip: "3px"
  control: "6px"
  action: "7px"
  field: "8px"
  bar: "12px"
  glass: "16px"
  pill: "999px"
spacing:
  xs: "6px"
  sm: "8px"
  md: "12px"
  lg: "18px"
  xl: "30px"
components:
  button-search:
    backgroundColor: "{colors.cobalt}"
    textColor: "{colors.slip}"
    rounded: "{rounded.field}"
    padding: "0 22px"
    height: "48px"
  button-search-hover:
    backgroundColor: "{colors.cobalt-deep}"
  button-action:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.ink}"
    rounded: "{rounded.action}"
    padding: "0 13px"
    height: "38px"
  button-action-primary:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.cobalt}"
    rounded: "{rounded.action}"
    padding: "0 13px"
    height: "38px"
  button-action-hover:
    backgroundColor: "{colors.cobalt-wash}"
  segment-option:
    textColor: "{colors.ink-2}"
    rounded: "{rounded.control}"
    padding: "0 12px"
    height: "36px"
  segment-option-on:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.slip}"
  spelling-chip:
    textColor: "{colors.ink}"
    rounded: "{rounded.pill}"
    padding: "4px 12px 5px"
    typography: "{typography.indic-chip}"
  spelling-chip-on:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.pencil}"
  query-bar:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.ink}"
    rounded: "{rounded.bar}"
    padding: "8px 8px 8px 18px"
    typography: "{typography.indic-query}"
  granth-chip:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.ink}"
    rounded: "{rounded.action}"
    padding: "4px 4px 4px 10px"
  page-slip:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.ink}"
    rounded: "{rounded.slip}"
    padding: "20px 24px 16px"
  loupe:
    backgroundColor: "{colors.slip}"
    textColor: "{colors.ink-2}"
    rounded: "{rounded.pill}"
    size: "148px"
  tag-text-only:
    backgroundColor: "{colors.amber-wash}"
    textColor: "{colors.amber}"
    rounded: "{rounded.pill}"
    padding: "2px 8px"
---

# Design System: Granth Search

> **Scope.** The Light Table governs the whole app. Search (`/`, `pages/index.tsx`, `styles/search.css`) is its fullest form; `styles/theme.css` carries it to every other shell: the library (`/library`), Ask (`/ask`, with `styles/ask.css`), the OCR text viewer (`tv*`), the PDF viewer (`pv*`), the spreadsheet viewer, the extractor, admin and auth. Tokens live under `.lt`; fonts resolve on the `.ltFonts` wrapper in `pages/_app.tsx`. `styles/globals.css` still holds structural rules (the pdf.js text layer, legacy shells) and is overridden, not extended.

## Overview

**Creative North Star: "The Light Table"**

A researcher's backlit glass table. The ground is cool, faintly blue glass with ruler ticks along its top and left edges; each search hit is a white page slip laid on it, mounted with crop marks at its corners; the matched word on the slip is ringed by hand in red grease pencil, and a loupe in the slip's right margin shows that word at twice the size. Actions are written in cobalt, like a china marker. The page exists to let someone check a hit on its page, so the text itself is the largest, highest-contrast thing on screen.

Density is working density, not dashboard density: one reading column of slips in page order, generous Indic line height (1.8), and counts set as big, condensed tabular numerals that read as a tally, not a KPI. Light arrives as state: the glass brightens once when results land and a sheen sweeps it while searching. Nothing else moves.

The world refuses the category's grid of same-size result cards with a yellow highlighter. Hits are never highlighted with a fill; they are ringed.

**Key Characteristics:**
- Cool backlit glass ground with 8px/40px ruler ticks; white slips with crop-mark corners.
- One hand-drawn red ring per matched word, the only red on a healthy screen.
- Cobalt for every action and focus; ink-black for the selected segment.
- Archivo (variable width) for the interface; Tiro Devanagari Sanskrit and Noto Serif Gujarati for the text, always larger than the UI around it.
- Tabular numerals everywhere; the tally is the headline.
- Amber means "partial or not verified", never decoration.

## Colors

A cool, low-chroma glass-and-ink neutral field carrying three purposeful inks: grease-pencil red for the hit, cobalt for action, amber for doubt.

### Primary
- **China-Marker Cobalt** (`cobalt`): every action and interactive affordance: the Search button fill, primary slip action outline and text, text buttons and links, focus outlines (2px, 2px offset), checkbox accent, the caret, the current step of the progress list. **Deep Cobalt** (`cobalt-deep`) is its hover. **Cobalt Wash** (`cobalt-wash`) is the hover ground of outline actions, the selected row in the granth picker, and the 4px focus halo around the query bar.

### Secondary
- **Grease-Pencil Red** (`pencil`): the ring around a matched word (drawn as an SVG stroke, never a fill), the border and count of a chosen spelling chip, the remove-granth hover. **Pencil Wash** (`pencil-wash`) is text selection and the remove-button hover ground.

### Tertiary
- **Lamp Amber** (`amber`) with **Amber Wash** (`amber-wash`): partial counts (the scan tape and its progress track), warnings, the count line of an unverified slip, and the "text only" tag. It marks what is not fully verified.

### Neutral
- **Backlit Glass** (`glass`): the page ground, under a white 280px top fade.
- **Lit Glass** (`glass-lit`): the lighter tier inside panels: picker toolbar, picker row hover.
- **Glass Edge** (`glass-edge`): 1px borders of the query bar, glass panel, segments, chips, form tallies; the divider between word groups.
- **Slip White** (`slip`): slips, the query bar, picker, dialogs, outline actions.
- **Ink** (`ink`): primary text and the selected segment fill; the loupe rim.
- **Ink 2** (`ink-2`): secondary text: notes, native titles, unselected segment text, tally units.
- **Ink 3** (`ink-3`): tertiary text: labels, counts on chips, metadata, the search icon.
- **Rule** (`rule`): hairlines inside slips (head/foot rules, dashed rule between lines), outline-action borders, the kbd hint.
- **Tick** (`tick`): ruler ticks, crop marks, the dashed outline of an unverified slip, scrollbar thumb.

### Named Rules
**The Ring, Not the Highlighter Rule.** A matched word is marked only by the grease-pencil ring, an open, uneven red stroke around the word with a transparent background. Never mark a hit with a background fill, yellow or otherwise.

**The Three Inks Rule.** Red marks the hit, cobalt marks what you can do, amber marks what is not verified. No ink is borrowed for another job; a red button or a cobalt highlight breaks the table.

## Typography

**Display / UI Font:** Archivo, variable width axis (with Segoe UI, system-ui, sans-serif)
**Text Fonts:** Tiro Devanagari Sanskrit for Devanagari, Noto Serif Gujarati for Gujarati (with Noto Serif Devanagari / Noto Serif Gujarati, serif), applied with the `indic` class

**Character:** A sturdy, slightly technical grotesque whose width axis does the hierarchy work (wide for the masthead, condensed for folios, labels and book numbers), set against calm book serifs for the scripture text. The UI is always the smaller voice.

### Hierarchy
- **Display** (680, clamp 38 to 60px, 0.95, width 92%, -0.03em): the occurrence count in the tally. The page count beside it uses the headline size in Ink 2.
- **Headline** (720, clamp 28 to 38px, 1, width 112%, -0.02em): the masthead title only.
- **Folio** (650, 34px, width 80%): the page number at the top right of every slip, under an 11.5px condensed "page" label.
- **Title** (650, 17px, 1.3, balanced wrap): the granth title on a slip.
- **Body** (400, 15px, 1.45, tabular numerals): the base UI text. Notes 13 to 14px; welcome and empty text 16px at 64ch max.
- **Label** (600, 12.5px, width 88%, +0.02em, sentence case): field legends ("Match", "Count hits in", "In", "Searched as").
- **Indic line** (400, 21px, 1.8; 20px under 720px): the passages on a slip. The query input is 22 to 28px; spelling chips 20px; form tallies 18px; picker native names 17px; the loupe 40px (27px on phones).

### Named Rules
**The Text Is Larger Rule.** Indic text is never set smaller than the Latin UI beside it; any Devanagari or Gujarati run on this surface is at least 16px and usually 18 to 22px, at line height 1.4 or more.

**The Width Axis Rule.** Hierarchy among Latin labels is carried by Archivo's width axis and weight, not by uppercase or tracking. Folios, book numbers and labels condense (80 to 88%); only the masthead widens.

**The Tabular Tally Rule.** All numerals are tabular (`font-variant-numeric: tabular-nums` on the root) and formatted en-IN (12,082). An inexact count carries a trailing "+", never a rounded or approximate word.

## Layout

A single centered frame (max 1320px, 28px side padding, 64px bottom, 18px vertical gap) stacks: masthead (title and note left, text nav right, bottom-aligned), the console (query bar, spellings, match settings, chosen granths, picker), then the glass panel holding the tally and the slips.

Slips form **one reading column in page order** (max 1040px, 30px apart), never a grid. Each slip is a two-column grid: lines (flexible) and a 148px loupe column, 32px gutter, with head and foot rows spanning both. Controls wrap as a flex row of fieldsets (12px by 22px gaps). Spacing sits on a loose 6/8/12/18/30 rhythm, with slip and glass interiors at 20 to 30px.

**Under 720px:** frame padding drops to 16px (the gutter), the nav stays on one line, match settings collapse behind a one-line summary button ("Sanskrit forms · Both scripts · All granths" with a chevron that rotates 90 degrees when open), segments stretch to full width with 40px buttons, the slip reflows to head / lines / loupe-and-foot with a 96px loupe, slip actions fill the row, and the pager hides First and Last.

## Elevation & Depth

Depth is physical and lit from below. The glass is an inset panel (a white radial bloom from the top over a cool vertical gradient, inner top highlight); objects laid on it cast short, soft shadows. There are no hard or offset shadows.

### Shadow Vocabulary
- **Slip on glass** (`0 1px 0 rgba(20,33,42,.06), 0 2px 4px rgba(20,33,42,.05), 0 22px 40px -26px rgba(20,33,42,.42)`): page slips only.
- **Console lift** (`0 1px 1px rgba(20,33,42,.05), 0 12px 28px -14px rgba(20,33,42,.3)`): the query bar and the granth picker.
- **Loupe** (`0 14px 26px -12px rgba(20,33,42,.5)`): the lens, which also rises 3px when its slip is hovered or focused.
- **Dialog** (`0 30px 70px -20px rgba(20,33,42,.5)`) over a 38% ink scrim with 3px blur.

### Named Rules
**The Lamp Below Rule.** Light comes from under the glass: shadows are short and soft, highlights sit on top edges, and the one bloom (the lamp coming on as results land, 900ms) rises from beneath. Nothing is lit from a direction the table doesn't have.

**The Unverified Slip Rule.** A hit the page text does not confirm loses its mount: no shadow, no crop marks, 72% white, a 1px dashed Tick outline, and an amber count line reading "Index hit, not confirmed in the page text". Doubt is shown by the object, not by a badge.

## Shapes

Radius grows with the size and softness of the object: slips are nearly square (3px) because they are paper; controls 6 to 8px; the query bar and picker 12px; the glass 16px (12px on phones). Spelling chips, tags, the "+N forms" pill and the loupe are fully round.

Recurring geometry belongs to the table: **crop marks** (12px L-shaped hairlines in Tick, 9px outside each slip corner), **ruler ticks** (a 1px tick every 8px at 6px long, every 40px at 12px long, top and left edges of the glass), and the **ring** (an open elliptical stroke, heavier on the downstroke, overshooting where the pencil started; even-numbered rings are mirrored and tilted 1.5 degrees so no two look stamped). Rules inside slips are 1px solid between head, body and foot, 1px dashed between passages.

Icons are one family (LightTableIcon): 20px grid, 1.6px round-capped stroke, `currentColor`, sized 14 to 22px.

## Components

### Buttons
Confident but quiet; one filled button per surface.
- **Search (primary):** Cobalt fill, white 16px/650 text, 8px radius, 48px tall (46 on phones), ends the query bar. Hover Deep Cobalt, press moves down 1px, busy shows a 15px ring spinner and "Searching".
- **Outline action:** white, 1px Rule border, 7px radius, 38px tall, 14px/580 Ink text with a 16px leading icon. Hover: Cobalt border on Cobalt Wash. The **primary** variant ("Check on page") has a Cobalt border and Cobalt text at rest. Disabled at 50% opacity.
- **Text button:** Cobalt 13.5px/600, underlined at 0.2em offset, 1px thickness ("Show all 5", "Clear all", "Search again").

### Segmented controls
- **Style:** a fieldset with a condensed label legend above a 3px-padded tray (Glass Edge border, 8px radius, half-white).
- **State:** options are 36px, 6px radius, Ink 2 text; hover white; **selected is solid Ink with white text**. Script options add a 16px outlined check square.

### Spelling chips
- **Style:** pill, Glass Edge border, 55% white, a 20px Indic form followed by its page count in 12px Ink 3.
- **State:** chosen chips go white with a doubled Grease-Pencil Red border (1px border plus 1px inset) and a red count; forms with zero pages get a dashed border. A word always keeps one spelling.

### Inputs / Fields
- **Query bar:** white, 1px Glass Edge, 12px radius, console lift shadow, 22px search icon, Indic input at 22 to 28px, a "/" keyboard hint (hidden on focus and on phones). **Focus:** border turns Cobalt with a 4px Cobalt Wash halo.
- **Picker filter:** 42px, 7px radius, Rule border, 17px Indic-capable text.
- **Errors:** inline red-on-pink block (8px radius); warnings are amber text only.

### Navigation
Plain text links in Ink 2 at 14px/560, 8px by 12px padding, 6px radius; hover to Ink on 75% white. No active pill; the page title carries location.

### Page Slip (signature)
White, 3px radius, slip shadow, crop marks, laid down with a 520ms settle (from 60% opacity, 8px lower, 1.5px blur) staggered 35ms per slip up to 12. Head: title, native title in Indic 16px, condensed "Part" and "No." metadata; folio at right. Body: up to three ringed passages (Indic 21px, 1.8), dashed rules between. Foot: occurrence count in Ink 3, show-all, then actions right-aligned.

### Loupe (signature)
A 148px circle (96px on phones): 3px Ink rim with a 2px white outer ring, white lens, the hit centred at 40px Indic with 24 characters of context either side clipped by the lens. A small Ink pill below reads "+N forms" when the page holds more than one form.

### Viewers
The text, PDF and spreadsheet viewers reuse the slip. A viewer page is a `tvPage` slip (white, 4px radius, slip shadow) with a condensed folio at the left and the text at Indic 20px, 1.8; the current page gets a 2px Cobalt ring. The OCR viewer adds a sticky 280px hits column of `tvHit` cards (the ringed excerpt, page and count), which becomes a horizontal strip under 860px. The PDF viewer has one sticky glass toolbar (`pvBar`): text nav left, then grouped page and zoom trays and a single "Select text" toggle that goes solid Ink when on; the page sits centred on the glass as a slip. Opening a viewer jumps straight to the hit, instantly the first time (a background tab never runs a smooth scroll).

### Tally
The occurrence count at display size and the page count at headline size, side by side, then the searched forms in Indic 22px with a one-line scope ("Sanskrit forms · Devanagari + Gujarati · in all granths"). Below: a row of form tallies (6px radius, 60% white, Indic 18px plus a right-aligned count), the amber scan tape when counts are partial, then the pager and "Export all matches". When the settings no longer match the results, the figures dim to 55% and a "Search again" line appears.

## Do's and Don'ts

### Do:
- **Do** scope every new Light Table rule under `.lt` and use the `--lt-*` tokens; the shared dialogs restyle only inside it.
- **Do** mark hits with the grease-pencil ring (`mark.ltRing`) and nothing else, both on slips and in matched-page lists.
- **Do** keep every action and focus in Cobalt, with a 2px outline at 2px offset for `:focus-visible`.
- **Do** set Indic text at 16px or more (usually 18 to 22px) and line height 1.4 to 1.8, larger than the Latin UI beside it.
- **Do** show unverified or partial states with the object itself (dashed unmounted slip, amber tape, trailing "+"), in amber.
- **Do** lay results as one column of slips in page order, each with its folio and loupe.
- **Do** honor `prefers-reduced-motion`: all Light Table animation and transition turns off.

### Don't:
- **Don't** highlight a match with a background fill or yellow highlighter.
- **Don't** arrange hits as a grid of same-size result cards.
- **Don't** use Grease-Pencil Red for buttons, errors or decoration; it belongs to the ring and to chosen spellings.
- **Don't** use hard or offset shadows; depth is soft and lit from below.
- **Don't** set Latin labels in uppercase with wide tracking; condense with Archivo's width axis instead.
- **Don't** add explanatory copy. A label names the thing ("Select text", "Fix OCR text"); a status says only what changed ("page 10 not found"). No intros, helper paragraphs or badges for the normal case: show a state only when something is not ready.
- **Don't** add inline `style` to pages; give the element a class in `theme.css` under its surface prefix.
