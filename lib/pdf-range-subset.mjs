// Builds a small PDF holding only some pages of a remote PDF, fetching just
// the bytes those pages use with HTTP Range requests.
//
// Exports used to download the whole source file first: 265 MB and ~30 s for
// the largest granth, while copying its pages then took 30 ms. A scanned
// page needs only its own objects (page dictionary, content stream, page
// image, fonts), so this reads the cross-reference data at the end of the
// file, walks the page tree to the wanted pages, collects every object they
// reference (following references, never /Parent), fetches those byte ranges
// in parallel, and writes them into a new PDF with its own page tree.
//
// Object numbers are kept as they are, so no reference inside an object needs
// rewriting; only the page dictionaries are rebuilt (new /Parent, and the
// attributes they inherited from the old page tree). Anything this does not
// understand (encryption, a broken cross-reference) throws, and the caller
// falls back to the full download.
//
// Plain JS so scripts can use it as well as the app.

import { inflateSync } from "node:zlib";

const CHUNK = 256 * 1024;
/** Fetched with the first request: the xref and trailer sit at the end of the file. */
const TAIL_BYTES = 4 * CHUNK;
/** Read after each wanted page object: a scanned page's content and image usually follow it. */
const PAGE_WINDOW = 3 * CHUNK;
/** Parsed cross-reference data of recently used PDFs, so repeat exports skip it. */
const PDF_CACHE = new Map();
const PDF_CACHE_SIZE = 12;
const MAX_PARALLEL = 8;
const WS = new Set([0x00, 0x09, 0x0a, 0x0c, 0x0d, 0x20]);
const DELIM = new Set([0x28, 0x29, 0x3c, 0x3e, 0x5b, 0x5d, 0x7b, 0x7d, 0x2f, 0x25]);
const latin1 = new TextDecoder("latin1");

export class RangeSubsetError extends Error {}

// ------------------------------------------------------------------ bytes

/** Cached, chunk-aligned reads of a remote file. */
class RangeFile {
  constructor(url, size, fetchImpl) {
    this.url = url;
    this.size = size;
    this.fetch = fetchImpl;
    this.chunks = new Map();
    this.bytesFetched = 0;
    this.requests = 0;
  }

  /** Opens the file with one request that also brings its tail (where the xref lives). */
  static async open(url, fetchImpl = fetch) {
    const res = await fetchImpl(url, { headers: { Range: `bytes=-${TAIL_BYTES}` } });
    const range = res.headers.get("content-range") || "";
    const match = range.match(/bytes (\d+)-(\d+)\/(\d+)/);
    if (res.status !== 206 || !match) throw new RangeSubsetError("The PDF host does not serve byte ranges.");
    const [start, , total] = [Number(match[1]), Number(match[2]), Number(match[3])];
    const file = new RangeFile(url, total, fetchImpl);
    const bytes = new Uint8Array(await res.arrayBuffer());
    file.bytesFetched += bytes.length;
    file.requests += 1;
    // Keep whole chunks only; the partial first chunk is fetched again if needed.
    const firstChunk = Math.ceil(start / CHUNK);
    for (let c = firstChunk; c * CHUNK < total; c += 1) {
      const from = c * CHUNK - start;
      file.chunks.set(c, bytes.subarray(from, Math.min(bytes.length, from + CHUNK)));
    }
    if (start === 0) file.chunks.set(0, bytes.subarray(0, Math.min(bytes.length, CHUNK)));
    return file;
  }

  async fetchRange(start, end) {
    for (let attempt = 0; ; attempt += 1) {
      try {
        const res = await this.fetch(this.url, { headers: { Range: `bytes=${start}-${end - 1}` } });
        if (res.status !== 206) throw new RangeSubsetError(`Range request answered ${res.status}.`);
        const bytes = new Uint8Array(await res.arrayBuffer());
        if (bytes.length !== end - start) throw new RangeSubsetError("Short range response.");
        this.bytesFetched += bytes.length;
        this.requests += 1;
        return bytes;
      } catch (error) {
        if (attempt >= 2) throw error;
      }
    }
  }

  /** Makes sure [start, end) is cached, fetching missing chunks in a few merged requests. */
  async load(ranges) {
    const missing = new Set();
    for (const [start, end] of ranges) {
      for (let c = Math.floor(start / CHUNK); c * CHUNK < Math.min(end, this.size); c += 1) {
        if (!this.chunks.has(c)) missing.add(c);
      }
    }
    const sorted = [...missing].sort((a, b) => a - b);
    const runs = [];
    for (const c of sorted) {
      const last = runs[runs.length - 1];
      if (last && c === last[1] + 1 && last[1] - last[0] < 15) last[1] = c;
      else runs.push([c, c]);
    }
    let next = 0;
    const worker = async () => {
      while (next < runs.length) {
        const [a, b] = runs[next++];
        const start = a * CHUNK;
        const end = Math.min(this.size, (b + 1) * CHUNK);
        const bytes = await this.fetchRange(start, end);
        for (let c = a; c <= b; c += 1) {
          const from = c * CHUNK - start;
          this.chunks.set(c, bytes.subarray(from, Math.min(bytes.length, from + CHUNK)));
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(MAX_PARALLEL, runs.length) }, worker));
  }

  async read(start, end) {
    end = Math.min(end, this.size);
    await this.load([[start, end]]);
    return this.slice(start, end);
  }

  slice(start, end) {
    end = Math.min(end, this.size);
    const out = new Uint8Array(Math.max(0, end - start));
    let pos = start;
    while (pos < end) {
      const c = Math.floor(pos / CHUNK);
      const chunk = this.chunks.get(c);
      if (!chunk) throw new RangeSubsetError("Chunk not loaded.");
      const from = pos - c * CHUNK;
      const take = Math.min(chunk.length - from, end - pos);
      out.set(chunk.subarray(from, from + take), pos - start);
      pos += take;
    }
    return out;
  }
}

// ------------------------------------------------------------------ parsing

/** A small PDF object parser: enough for dictionaries, arrays and references. */
class Lexer {
  constructor(bytes, pos = 0) {
    this.b = bytes;
    this.p = pos;
  }

  skipWs() {
    const b = this.b;
    for (;;) {
      while (this.p < b.length && WS.has(b[this.p])) this.p += 1;
      if (b[this.p] === 0x25) {
        while (this.p < b.length && b[this.p] !== 0x0a && b[this.p] !== 0x0d) this.p += 1;
      } else return;
    }
  }

  regular() {
    const start = this.p;
    while (this.p < this.b.length && !WS.has(this.b[this.p]) && !DELIM.has(this.b[this.p])) this.p += 1;
    return latin1.decode(this.b.subarray(start, this.p));
  }

  /** Parses one value. References come back as { ref: [num, gen] }. */
  value() {
    this.skipWs();
    const b = this.b;
    const c = b[this.p];
    if (c === 0x2f) {
      this.p += 1;
      return { name: this.regular() };
    }
    if (c === 0x3c && b[this.p + 1] === 0x3c) {
      this.p += 2;
      const dict = new Map();
      for (;;) {
        this.skipWs();
        if (b[this.p] === 0x3e && b[this.p + 1] === 0x3e) {
          this.p += 2;
          return { dict };
        }
        const key = this.value();
        if (!key || key.name == null) throw new RangeSubsetError("Bad dictionary key.");
        dict.set(key.name, this.value());
      }
    }
    if (c === 0x5b) {
      this.p += 1;
      const arr = [];
      for (;;) {
        this.skipWs();
        if (b[this.p] === 0x5d) {
          this.p += 1;
          return { arr };
        }
        arr.push(this.value());
      }
    }
    if (c === 0x28) {
      const start = this.p;
      let depth = 0;
      for (; this.p < b.length; this.p += 1) {
        if (b[this.p] === 0x5c) this.p += 1;
        else if (b[this.p] === 0x28) depth += 1;
        else if (b[this.p] === 0x29 && --depth === 0) {
          this.p += 1;
          break;
        }
      }
      return { raw: latin1.decode(b.subarray(start, this.p)) };
    }
    if (c === 0x3c) {
      const start = this.p;
      while (this.p < b.length && b[this.p] !== 0x3e) this.p += 1;
      this.p += 1;
      return { raw: latin1.decode(b.subarray(start, this.p)) };
    }
    const word = this.regular();
    if (word === "") throw new RangeSubsetError(`Unexpected byte ${c} at ${this.p}.`);
    if (/^[+-]?\d+$/.test(word)) {
      // "12 0 R" is a reference; look ahead without consuming otherwise.
      const save = this.p;
      this.skipWs();
      const gen = this.regular();
      if (/^\d+$/.test(gen)) {
        this.skipWs();
        if (this.b[this.p] === 0x52 && (this.p + 1 >= this.b.length || WS.has(this.b[this.p + 1]) || DELIM.has(this.b[this.p + 1]))) {
          this.p += 1;
          return { ref: [Number(word), Number(gen)] };
        }
      }
      this.p = save;
      return { num: Number(word) };
    }
    if (/^[+-]?[\d.]+$/.test(word)) return { num: Number(word) };
    return { word };
  }
}

function serialize(v) {
  if (v.name != null) return `/${v.name}`;
  if (v.ref) return `${v.ref[0]} ${v.ref[1]} R`;
  if (v.num != null) return String(v.num);
  if (v.raw != null) return v.raw;
  if (v.word != null) return v.word;
  if (v.arr) return `[${v.arr.map(serialize).join(" ")}]`;
  if (v.dict) return `<<${[...v.dict].map(([k, x]) => `/${k} ${serialize(x)}`).join(" ")}>>`;
  throw new RangeSubsetError("Cannot serialize value.");
}

function collectRefs(v, out) {
  if (!v) return;
  if (v.ref) out.push(v.ref[0]);
  else if (v.arr) for (const x of v.arr) collectRefs(x, out);
  else if (v.dict) for (const [k, x] of v.dict) if (k !== "Parent") collectRefs(x, out);
}

function indexOf(bytes, text, from = 0) {
  const pat = [...text].map((ch) => ch.charCodeAt(0));
  outer: for (let i = from; i <= bytes.length - pat.length; i += 1) {
    for (let k = 0; k < pat.length; k += 1) if (bytes[i + k] !== pat[k]) continue outer;
    return i;
  }
  return -1;
}

function lastIndexOf(bytes, text) {
  const pat = [...text].map((ch) => ch.charCodeAt(0));
  outer: for (let i = bytes.length - pat.length; i >= 0; i -= 1) {
    for (let k = 0; k < pat.length; k += 1) if (bytes[i + k] !== pat[k]) continue outer;
    return i;
  }
  return -1;
}

/** Decodes a FlateDecode stream, applying a PNG predictor when one is set. */
function decodeStream(dict, data) {
  const filter = dict.get("Filter");
  const names = filter?.name ? [filter.name] : filter?.arr ? filter.arr.map((f) => f.name) : [];
  if (names.length === 0) return data;
  if (names.length !== 1 || names[0] !== "FlateDecode") throw new RangeSubsetError(`Unsupported filter ${names.join(",")}.`);
  let out = inflateSync(data);
  const parms = dict.get("DecodeParms")?.dict;
  const predictor = parms?.get("Predictor")?.num ?? 1;
  if (predictor >= 10) {
    const columns = parms.get("Columns")?.num ?? 1;
    const row = columns + 1;
    const rows = Math.floor(out.length / row);
    const result = new Uint8Array(rows * columns);
    const prev = new Uint8Array(columns);
    for (let r = 0; r < rows; r += 1) {
      const type = out[r * row];
      for (let i = 0; i < columns; i += 1) {
        const raw = out[r * row + 1 + i];
        const left = i > 0 ? result[r * columns + i - 1] : 0;
        const up = prev[i];
        let value;
        if (type === 0) value = raw;
        else if (type === 1) value = raw + left;
        else if (type === 2) value = raw + up;
        else if (type === 3) value = raw + ((left + up) >> 1);
        else {
          const upLeft = i > 0 ? prev[i - 1] : 0;
          const p = left + up - upLeft;
          const pa = Math.abs(p - left);
          const pb = Math.abs(p - up);
          const pc = Math.abs(p - upLeft);
          value = raw + (pa <= pb && pa <= pc ? left : pb <= pc ? up : upLeft);
        }
        result[r * columns + i] = value & 0xff;
      }
      prev.set(result.subarray(r * columns, (r + 1) * columns));
    }
    out = result;
  } else if (predictor !== 1) {
    throw new RangeSubsetError(`Unsupported predictor ${predictor}.`);
  }
  return out;
}

// ------------------------------------------------------------------ the subsetter

class RemotePdf {
  constructor(file) {
    this.file = file;
    /** objNum -> { type: 1, offset } | { type: 2, stream, index } */
    this.xref = new Map();
    this.trailer = null;
    this.objStmCache = new Map();
    this.sortedOffsets = [];
  }

  async init() {
    const tailStart = Math.max(0, this.file.size - 64 * 1024);
    const tail = await this.file.read(tailStart, this.file.size);
    const sx = lastIndexOf(tail, "startxref");
    if (sx < 0) throw new RangeSubsetError("No startxref.");
    const lex = new Lexer(tail, sx + 9);
    let offset = lex.value().num;
    const seen = new Set();
    while (offset != null && !seen.has(offset)) {
      seen.add(offset);
      const trailer = await this.readXrefAt(offset);
      this.trailer ??= trailer;
      const stm = trailer.get("XRefStm")?.num;
      if (stm != null && !seen.has(stm)) {
        seen.add(stm);
        await this.readXrefAt(stm);
      }
      offset = trailer.get("Prev")?.num;
    }
    if (!this.trailer) throw new RangeSubsetError("No trailer.");
    if (this.trailer.has("Encrypt")) throw new RangeSubsetError("Encrypted PDF.");
    this.sortedOffsets = [...new Set([...this.xref.values()].filter((e) => e.type === 1).map((e) => e.offset))].sort((a, b) => a - b);
  }

  /** Reads one xref section (table or stream); earlier-read entries win (newest first). */
  async readXrefAt(offset) {
    let bytes = await this.file.read(offset, offset + 64 * 1024);
    const lex = new Lexer(bytes);
    lex.skipWs();
    if (latin1.decode(bytes.subarray(lex.p, lex.p + 4)) === "xref") {
      // A classic table can be long: read until "trailer" is in the buffer.
      let tIdx = indexOf(bytes, "trailer");
      let span = 64 * 1024;
      while (tIdx < 0 && offset + span < this.file.size) {
        span *= 4;
        bytes = await this.file.read(offset, offset + span);
        tIdx = indexOf(bytes, "trailer");
      }
      if (tIdx < 0) throw new RangeSubsetError("xref table without trailer.");
      const tl = new Lexer(bytes, lex.p + 4);
      for (;;) {
        tl.skipWs();
        if (tl.p >= tIdx) break;
        const first = tl.value().num;
        const count = tl.value().num;
        for (let i = 0; i < count; i += 1) {
          const off = tl.value().num;
          tl.value();
          const kind = tl.value().word;
          const num = first + i;
          if (kind === "n" && !this.xref.has(num) && off > 0) this.xref.set(num, { type: 1, offset: off });
          else if (!this.xref.has(num)) this.xref.set(num, { type: 0 });
        }
      }
      const trailer = new Lexer(bytes, tIdx + 7).value().dict;
      return trailer;
    }
    // Cross-reference stream: "n g obj <<...>> stream ... endstream".
    const { dict, data } = await this.readStreamObjectAt(offset);
    const w = dict.get("W").arr.map((x) => x.num);
    const size = dict.get("Size").num;
    const index = dict.get("Index")?.arr.map((x) => x.num) ?? [0, size];
    const raw = decodeStream(dict, data);
    const rowLen = w[0] + w[1] + w[2];
    const field = (row, from, len, fallback) => {
      if (len === 0) return fallback;
      let v = 0;
      for (let k = 0; k < len; k += 1) v = v * 256 + raw[row + from + k];
      return v;
    };
    let row = 0;
    for (let s = 0; s < index.length; s += 2) {
      for (let i = 0; i < index[s + 1]; i += 1, row += rowLen) {
        const num = index[s] + i;
        if (this.xref.has(num)) continue;
        const type = field(row, 0, w[0], 1);
        const a = field(row, w[0], w[1], 0);
        const b = field(row, w[0] + w[1], w[2], 0);
        if (type === 1) this.xref.set(num, { type: 1, offset: a });
        else if (type === 2) this.xref.set(num, { type: 2, stream: a, index: b });
        else this.xref.set(num, { type: 0 });
      }
    }
    return dict;
  }

  /** The stream object starting at a byte offset, with its raw (still encoded) data. */
  async readStreamObjectAt(offset) {
    let bytes = await this.file.read(offset, offset + 64 * 1024);
    const lex = new Lexer(bytes);
    lex.value();
    lex.value();
    lex.skipWs();
    lex.regular(); // "obj"
    const dict = lex.value().dict;
    lex.skipWs();
    const kw = lex.regular();
    if (kw !== "stream") throw new RangeSubsetError("Expected a stream.");
    if (bytes[lex.p] === 0x0d) lex.p += 1;
    if (bytes[lex.p] === 0x0a) lex.p += 1;
    const lengthValue = dict.get("Length");
    const length = lengthValue?.num ?? (lengthValue?.ref ? await this.resolveNumber(lengthValue.ref[0]) : null);
    if (length == null) throw new RangeSubsetError("Stream without length.");
    const dataStart = offset + lex.p;
    const data = await this.file.read(dataStart, dataStart + length);
    return { dict, data };
  }

  async resolveNumber(num) {
    const obj = await this.objectText(num);
    const v = new Lexer(obj.bytes, obj.bodyStart).value();
    return v.num;
  }

  /** The end of the object at `offset`: the next object's offset, or the file end. */
  nextOffset(offset) {
    const list = this.sortedOffsets;
    let lo = 0;
    let hi = list.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (list[mid] <= offset) lo = mid + 1;
      else hi = mid;
    }
    return lo < list.length ? list[lo] : this.file.size;
  }

  async objStm(num) {
    let pending = this.objStmCache.get(num);
    if (!pending) {
      pending = (async () => {
        const entry = this.xref.get(num);
        if (!entry || entry.type !== 1) throw new RangeSubsetError("Object stream not found.");
        const { dict, data } = await this.readStreamObjectAt(entry.offset);
        const decoded = decodeStream(dict, data);
        const n = dict.get("N").num;
        const first = dict.get("First").num;
        const head = new Lexer(decoded);
        const offsets = [];
        for (let i = 0; i < n; i += 1) {
          const objNum = head.value().num;
          const off = head.value().num;
          offsets.push([objNum, first + off]);
        }
        return { decoded, offsets };
      })();
      this.objStmCache.set(num, pending);
    }
    return pending;
  }

  /**
   * An object's source bytes, where its value starts, and whether it lives in
   * an object stream (then it is written out as a plain object).
   */
  async objectText(num) {
    const entry = this.xref.get(num);
    if (!entry || entry.type === 0) return null;
    if (entry.type === 2) {
      const stm = await this.objStm(entry.stream);
      const [, start] = stm.offsets[entry.index];
      const end = entry.index + 1 < stm.offsets.length ? stm.offsets[entry.index + 1][1] : stm.decoded.length;
      return { bytes: stm.decoded.subarray(start, end), bodyStart: 0, compressed: true };
    }
    const bytes = await this.file.read(entry.offset, this.nextOffset(entry.offset));
    const lex = new Lexer(bytes);
    lex.value();
    lex.value();
    lex.skipWs();
    lex.regular();
    return { bytes, bodyStart: lex.p, compressed: false };
  }

  /** The parsed value of an object (dictionary part only for streams). */
  async objectValue(num) {
    const obj = await this.objectText(num);
    if (!obj) return null;
    return new Lexer(obj.bytes, obj.bodyStart).value();
  }

  async resolve(v) {
    return v?.ref ? this.objectValue(v.ref[0]) : v;
  }

  /** Page dictionaries (with inherited attributes) for 1-based page numbers. */
  async findPages(pageNumbers) {
    const root = await this.resolve(this.trailer.get("Root"));
    const pagesRef = root.dict.get("Pages");
    const wanted = new Set(pageNumbers);
    const found = new Map();
    const INHERIT = ["Resources", "MediaBox", "CropBox", "Rotate"];

    const walk = async (ref, base, inherited) => {
      const node = await this.resolve(ref);
      if (!node?.dict) throw new RangeSubsetError("Bad page tree.");
      const type = node.dict.get("Type")?.name;
      const inh = { ...inherited };
      for (const key of INHERIT) if (node.dict.has(key)) inh[key] = node.dict.get(key);
      if (type === "Page" || !node.dict.has("Kids")) {
        if (wanted.has(base + 1)) found.set(base + 1, { num: ref.ref[0], dict: node.dict, inherited: inh });
        return 1;
      }
      const kids = (await this.resolve(node.dict.get("Kids"))).arr;
      // Load every kid in one parallel batch rather than one request each.
      await this.file.load(
        kids
          .map((k) => this.xref.get(k.ref?.[0]))
          .filter(Boolean)
          .flatMap((e) => {
            if (e.type === 1) return [[e.offset, this.nextOffset(e.offset)]];
            const s = this.xref.get(e.stream);
            return s?.type === 1 ? [[s.offset, this.nextOffset(s.offset)]] : [];
          })
      );
      let offset = base;
      for (const kid of kids) {
        // Skip whole subtrees that hold none of the wanted pages.
        const kidNode = await this.resolve(kid);
        const count = kidNode.dict.get("Type")?.name === "Pages" ? kidNode.dict.get("Count")?.num ?? 0 : 1;
        const hasWanted = [...wanted].some((p) => p > offset && p <= offset + count);
        if (hasWanted) await walk(kid, offset, inh);
        offset += count;
      }
      return offset - base;
    };
    await walk(pagesRef, 0, {});
    return found;
  }
}

/**
 * Fetches the given pages of a remote PDF and returns a standalone PDF with
 * just those pages, in the order given (duplicates dropped).
 * @param {string} url
 * @param {number[]} pageNumbers 1-based
 * @returns {Promise<{ bytes: Uint8Array, pages: number[], bytesFetched: number, requests: number, sourceSize: number }>}
 */
export async function extractPdfPagesByRange(url, pageNumbers, { fetchImpl = fetch } = {}) {
  const order = [...new Set(pageNumbers.map((p) => Math.floor(Number(p))).filter((p) => p > 0))];
  let pdf = PDF_CACHE.get(url);
  if (!pdf) {
    const file = await RangeFile.open(url, fetchImpl);
    pdf = new RemotePdf(file);
    await pdf.init();
    PDF_CACHE.set(url, pdf);
    if (PDF_CACHE.size > PDF_CACHE_SIZE) PDF_CACHE.delete(PDF_CACHE.keys().next().value);
  }
  const file = pdf.file;
  const fetchedBefore = file.bytesFetched;
  const requestsBefore = file.requests;
  const pages = await pdf.findPages(order);
  // Speculatively read what follows each page object in one parallel batch.
  await file.load(
    order
      .map((p) => pages.get(p))
      .filter(Boolean)
      .map((page) => pdf.xref.get(page.num))
      .filter((e) => e?.type === 1)
      .map((e) => [e.offset, e.offset + PAGE_WINDOW])
  );
  const kept = order.filter((p) => pages.has(p));
  if (kept.length === 0) throw new RangeSubsetError("None of the pages are in this PDF.");

  // Every object the kept pages reach, fetched breadth-first; each level's
  // byte ranges are loaded together so the requests run in parallel.
  const needed = new Set();
  let frontier = [];
  for (const p of kept) {
    const page = pages.get(p);
    const refs = [];
    collectRefs({ dict: page.dict }, refs);
    for (const v of Object.values(page.inherited)) collectRefs(v, refs);
    frontier.push(...refs);
  }
  const values = new Map();
  while (frontier.length) {
    const batch = [...new Set(frontier)].filter((n) => !needed.has(n) && pdf.xref.get(n)?.type);
    batch.forEach((n) => needed.add(n));
    const ranges = batch
      .map((n) => pdf.xref.get(n))
      .filter((e) => e.type === 1)
      .map((e) => [e.offset, pdf.nextOffset(e.offset)]);
    const streams = [...new Set(batch.map((n) => pdf.xref.get(n)).filter((e) => e.type === 2).map((e) => e.stream))];
    for (const s of streams) {
      const e = pdf.xref.get(s);
      if (e?.type === 1) ranges.push([e.offset, pdf.nextOffset(e.offset)]);
    }
    await file.load(ranges);
    frontier = [];
    for (const n of batch) {
      const obj = await pdf.objectText(n);
      if (!obj) continue;
      values.set(n, obj);
      const value = new Lexer(obj.bytes, obj.bodyStart).value();
      collectRefs(value, frontier);
    }
  }

  // Write the new file: kept objects as they were, rebuilt page dictionaries,
  // and a fresh page tree and catalog.
  const pageNums = new Set(kept.map((p) => pages.get(p).num));
  let maxNum = 0;
  for (const n of pdf.xref.keys()) maxNum = Math.max(maxNum, n);
  const pagesNum = maxNum + 1;
  const catalogNum = maxNum + 2;

  const chunks = [];
  let length = 0;
  const offsets = new Map();
  const push = (part) => {
    const bytes = typeof part === "string" ? Buffer.from(part, "latin1") : part;
    chunks.push(bytes);
    length += bytes.length;
  };
  push("%PDF-1.7\n%\xE2\xE3\xCF\xD3\n");

  const writeObject = (num, body) => {
    offsets.set(num, length);
    push(`${num} 0 obj\n`);
    push(body);
    push("\nendobj\n");
  };

  for (const p of kept) {
    const page = pages.get(p);
    const dict = new Map(page.dict);
    for (const [key, v] of Object.entries(page.inherited)) if (!dict.has(key)) dict.set(key, v);
    dict.set("Parent", { ref: [pagesNum, 0] });
    dict.delete("Annots");
    writeObject(page.num, serialize({ dict }));
  }
  for (const [num, obj] of values) {
    if (pageNums.has(num)) continue;
    if (obj.compressed) {
      writeObject(num, obj.bytes);
    } else {
      // Copy the object's body through "endobj", keeping stream bytes intact.
      const end = lastIndexOf(obj.bytes, "endobj");
      writeObject(num, obj.bytes.subarray(obj.bodyStart, end > 0 ? end : obj.bytes.length));
    }
  }
  writeObject(pagesNum, `<< /Type /Pages /Count ${kept.length} /Kids [${kept.map((p) => `${pages.get(p).num} 0 R`).join(" ")}] >>`);
  writeObject(catalogNum, `<< /Type /Catalog /Pages ${pagesNum} 0 R >>`);

  const xrefStart = length;
  const size = catalogNum + 1;
  let xref = `xref\n0 ${size}\n0000000000 65535 f \n`;
  for (let n = 1; n < size; n += 1) {
    const off = offsets.get(n);
    xref += off == null ? "0000000000 65535 f \n" : `${String(off).padStart(10, "0")} 00000 n \n`;
  }
  push(xref);
  push(`trailer\n<< /Size ${size} /Root ${catalogNum} 0 R >>\nstartxref\n${xrefStart}\n%%EOF\n`);

  return {
    bytes: new Uint8Array(Buffer.concat(chunks)),
    pages: kept,
    bytesFetched: file.bytesFetched - fetchedBefore,
    requests: file.requests - requestsBefore,
    sourceSize: file.size,
  };
}
