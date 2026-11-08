// Line numbers as a reader counts them: no blank lines, no header.
import assert from "node:assert/strict";
import { test } from "node:test";
import { bookLineNumbers, headSignature } from "@/lib/page-lines";

test("blank lines, the page number and the running head are not counted", () => {
  // Book 300, page 350: जीव is on the 27th text line, the OCR's line 39.
  const page = ["३५०", "", "षड्दर्शन समुच्चय भाग - १, परिशिष्ट - ५, षड्दर्शन समुच्चयभूमिका", "", "यद्वा शेषदर्शनाना", "अत एव समुदिताः", "", "नयास्तव"].join("\n");
  const heads = new Set([headSignature("षड्दर्शन समुच्चय भाग - १, परिशिष्ट - ५, षड्दर्शन समुच्चयभूमिका")]);
  const line = bookLineNumbers(page, heads, "350");
  assert.equal(line(5), 1);
  assert.equal(line(6), 2);
  assert.equal(line(8), 3);
});

test("a running head above a framed page number is header too", () => {
  const page = ["स्वोपज्ञ- वृत्ति- विभूषितं योगशास्त्रम्", "॥ १०१० ॥", "एक-द्वि-त्रि- चतुः", "शोध्यानि ।"].join("\n");
  const line = bookLineNumbers(page, new Set(), "1010");
  assert.equal(line(3), 1);
  assert.equal(line(4), 2);
});

test("lines without letters are not lines of text", () => {
  const line = bookLineNumbers(["૧૪૬", "-", "પહેલી લીટી", "**********", "બીજી લીટી"].join("\n"), new Set(), "146");
  assert.equal(line(3), 1);
  assert.equal(line(5), 2);
});
