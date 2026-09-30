#!/usr/bin/env python3
"""Where each printed word sits on a line, from the page image.

Input (JSON on stdin): {"pdf": path, "pages": {"<page>": [[x0, y0, x1, y1], ...]}}
with line boxes as page fractions (top-left origin). Output (JSON on stdout):
{"<page>": [[x0, x1, x0, x1, ...] per line]}: each line's ink runs, joined
across letter-sized gaps, in 1/2000ths of the page width. The runs are split
into words at ring time, at the widest gaps, as many as the line's text has
words (lib/line-box-rings.ts), so no fixed word-gap guess is needed here.

Used by scripts/import_word_segments.mjs; a ring then goes around exactly the
printed word instead of an estimate along the line (lib/line-box-rings.ts).
"""
import json
import os
import subprocess
import sys
import tempfile

from PIL import Image

DPI = 100
CHUNK = 40


def segments(img, box):
    w, h = img.size
    x0, y0, x1, y1 = box
    lh = max(1.0, (y1 - y0) * h)
    left = max(0, int((x0 - 0.01) * w))
    right = min(w, int((x1 + 0.01) * w) + 1)
    top = max(0, int(y0 * h + lh * 0.08))
    bottom = min(h, int(y1 * h - lh * 0.08) + 1)
    if right - left < 2 or bottom - top < 2:
        return []
    crop = img.crop((left, top, right, bottom))
    cw, ch = crop.size
    px = crop.load()
    dark = [[px[x, y] < 140 for x in range(cw)] for y in range(ch)]
    # a rule or underline across the line is not letters
    rows = [sum(r) < cw * 0.55 for r in dark]
    cols = [sum(1 for y in range(ch) if rows[y] and dark[y][x]) for x in range(cw)]
    runs = []
    x = 0
    while x < cw:
        if not cols[x]:
            x += 1
            continue
        start = x
        while x < cw and cols[x]:
            x += 1
        runs.append([start, x])
    if not runs:
        return []
    # join the strokes of one letter and close letters; word gaps are wider
    join = max(1, lh * 0.1)
    merged = [runs[0]]
    for r in runs[1:]:
        if r[0] - merged[-1][1] <= join:
            merged[-1][1] = r[1]
        else:
            merged.append(r)
    out = []
    for a, b in merged:
        mass = sum(cols[a:b])
        if b - a < max(2, lh * 0.06) and mass < max(4, lh * 0.25):
            continue  # a speck of noise
        out += [round((left + a) / w * 2000), round((left + b) / w * 2000)]
    return out


def main():
    job = json.load(sys.stdin)
    pages = sorted(int(p) for p in job["pages"])
    result = {}
    with tempfile.TemporaryDirectory() as tmp:
        for i in range(0, len(pages), CHUNK):
            chunk = pages[i:i + CHUNK]
            first, last = chunk[0], chunk[-1]
            subprocess.run(
                ["pdftoppm", "-r", str(DPI), "-gray", "-png", "-f", str(first), "-l", str(last), job["pdf"], os.path.join(tmp, "p")],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            files = {int(f.rsplit("-", 1)[1].split(".")[0]): os.path.join(tmp, f) for f in os.listdir(tmp) if f.endswith(".png")}
            for p in chunk:
                f = files.get(p)
                if not f:
                    continue
                img = Image.open(f).convert("L")
                result[str(p)] = [segments(img, box) for box in job["pages"][str(p)]]
            for f in files.values():
                os.remove(f)
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
