// Every shastriya word in tests/fixtures/shastriya-words.ts must be among the
// readings of each way it is typed in Roman letters (no database: the
// library's page counts then pick among these readings, see lib/roman-forms).
import assert from "node:assert/strict";
import { test } from "node:test";
import { romanWordReadings } from "@/lib/roman-sanskrit";
import { foldSanskrit, sanskritQueryForms } from "@/lib/sanskrit-fold.mjs";
import { SHASTRIYA_WORDS, toGujarati } from "./fixtures/shastriya-words";

test("each Roman typing of a shastriya word can be read as that word", () => {
  const missed: string[] = [];
  for (const { word, roman } of SHASTRIYA_WORDS) {
    const target = foldSanskrit(word);
    for (const typed of roman) {
      if (!romanWordReadings(typed, 600).some((reading) => foldSanskrit(reading.text) === target)) missed.push(`${typed} → ${word}`);
    }
  }
  assert.deepEqual(missed, []);
});

test("a shastriya word written in Gujarati letters folds to the same search term", () => {
  for (const { word } of SHASTRIYA_WORDS) assert.equal(foldSanskrit(toGujarati(word)), foldSanskrit(word), word);
});

test("the word's own form is always among its Sanskrit forms", () => {
  for (const { word } of SHASTRIYA_WORDS) assert.ok(sanskritQueryForms(word).includes(foldSanskrit(word)), word);
});

test("s-, vat- and ū-stems are declined as Sanskrit declines them", () => {
  const has = (word: string, ...forms: string[]) => {
    const all = sanskritQueryForms(word);
    for (const form of forms) assert.ok(all.includes(foldSanskrit(form)), `${word} → ${form}`);
  };
  has("तपस्", "तपः", "तपसा", "तपोभिः", "तपांसि", "तपःसु");
  has("तपः", "तपसा", "तपोभिः");
  has("मनस्", "मनः", "मनसा", "मनोभिः");
  has("ज्योतिस्", "ज्योतिः", "ज्योतिषा", "ज्योतिर्भिः");
  has("आयुस्", "आयुः", "आयुषा", "आयुर्भिः");
  has("भगवत्", "भगवान्", "भगवन्तम्", "भगवता", "भगवद्भिः");
  has("भगवान्", "भगवता", "भगवन्तः");
  has("वधू", "वधूः", "वध्वा", "वधूनाम्");
  has("कर्म", "कर्मणा", "कर्माणि");
  assert.ok(!sanskritQueryForms("तपस्").includes(foldSanskrit("तपस्भिः")));
});
