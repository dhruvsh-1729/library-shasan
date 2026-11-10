// Copies chosen pages out of a library PDF without downloading the book.
// A granth PDF is hundreds of MB and the pages asked for are a few MB of it,
// so only the index at the end of the file and the objects those pages use
// are fetched, as byte ranges. Anything this reader does not understand
// throws, and the caller falls back to downloading the whole file.
import { inflateSync } from "node:zlib";
import {
  PDFArray,
  PDFContext,
  PDFDict,
  PDFDocument,
  PDFName,
  PDFNumber,
  PDFObject,
  PDFObjectCopier,
  PDFObjectParser,
  PDFObjectStreamParser,
  PDFPage,
  PDFPageLeaf,
  PDFPageTree,
  PDFRawStream,
  PDFRef,
  PDFStream,
} from "pdf-lib";

/**
 * The end of the file, fetched once and kept. pdf-lib writes the index and every
 * page dictionary there, so after this request only the page images are left.
 */
const TAIL_BYTES = 2 * 1024 * 1024;
/** Each request waits seconds before its first byte, so near ranges go as one. */
const MERGE_GAP_BYTES = 2 * 1024 * 1024;
/** Read this much around each request: the small objects of one page sit side by side. */
const PAD_BYTES = 256 * 1024;
const PARALLEL_REQUESTS = 6;

type Where = { offset: number } | { container: number };

function fail(reason: string): never {
  throw new Error(`Page-range reader: ${reason}`);
}

function latin1(bytes: Uint8Array) {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("latin1");
}

/**
 * The entries of a cross-reference stream. Read here rather than by pdf-lib's
 * parser, which skips the row predictor Acrobat writes these streams with.
 */
function xrefEntries(xref: PDFRawStream) {
  const dict = xref.dict;
  const number = (value: PDFObject | undefined) => (value instanceof PDFNumber ? value.asNumber() : undefined);
  const filter = dict.get(PDFName.of("Filter"));
  let data: Uint8Array = xref.contents;
  if (filter === PDFName.of("FlateDecode")) data = inflateSync(data);
  else if (filter) fail("the index is packed in a way this reader does not know");

  const parms = dict.get(PDFName.of("DecodeParms"));
  const predictor = parms instanceof PDFDict ? number(parms.get(PDFName.of("Predictor"))) ?? 1 : 1;
  const widths = dict.lookup(PDFName.of("W"), PDFArray).asArray().map((w) => number(w) ?? fail("the index has no field widths"));
  if (widths.length !== 3) fail("the index has no field widths");
  const rowBytes = widths[0] + widths[1] + widths[2];
  if (predictor >= 10) {
    const columns = parms instanceof PDFDict ? number(parms.get(PDFName.of("Columns"))) ?? 1 : 1;
    const rows = Math.floor(data.length / (columns + 1));
    const plain = new Uint8Array(rows * columns);
    for (let row = 0; row < rows; row++) {
      const kind = data[row * (columns + 1)];
      for (let col = 0; col < columns; col++) {
        const raw = data[row * (columns + 1) + 1 + col];
        const left = col > 0 ? plain[row * columns + col - 1] : 0;
        const up = row > 0 ? plain[(row - 1) * columns + col] : 0;
        if (kind > 2) fail("the index uses a row predictor this reader does not know");
        plain[row * columns + col] = (raw + (kind === 1 ? left : kind === 2 ? up : 0)) & 255;
      }
    }
    data = plain;
  } else if (predictor !== 1) {
    fail("the index uses a row predictor this reader does not know");
  }

  const index = dict.get(PDFName.of("Index"));
  const sections = index instanceof PDFArray ? index.asArray().map((v) => number(v) ?? 0) : [0, number(dict.get(PDFName.of("Size"))) ?? fail("the index has no size")];
  const out: Array<{ number: number; type: number; value: number }> = [];
  let at = 0;
  const field = (width: number) => {
    let value = 0;
    for (let i = 0; i < width; i++) value = value * 256 + data[at++];
    return value;
  };
  for (let s = 0; s + 1 < sections.length; s += 2) {
    for (let i = 0; i < sections[s + 1]; i++) {
      if (at + rowBytes > data.length) fail("the index is shorter than it says");
      const type = widths[0] ? field(widths[0]) : 1;
      const value = field(widths[1]);
      field(widths[2]);
      out.push({ number: sections[s] + i, type, value });
    }
  }
  return out;
}

class RemotePdf {
  readonly context = PDFContext.create();
  size = 0;
  fetchedBytes = 0;
  requests = 0;
  private where = new Map<number, Where>();
  /** Every place an object starts, ascending; an object ends where the next one starts. */
  private starts: number[] = [];
  private loaded = new Set<number>();
  private root: PDFRef | null = null;
  /** Every block fetched so far; a later read inside one costs no request. */
  private blocks: Array<{ start: number; bytes: Uint8Array }> = [];

  constructor(private url: string) {}

  private async range(start: number, end: number) {
    for (const block of this.blocks) {
      if (start >= block.start && end <= block.start + block.bytes.length) {
        return block.bytes.subarray(start - block.start, end - block.start);
      }
    }
    const from = Math.max(0, start - PAD_BYTES);
    const to = Math.min(this.size, end + PAD_BYTES);
    const { bytes } = await this.request(`bytes=${from}-${to - 1}`);
    if (bytes.length !== to - from) fail("the file host sent a different range than asked");
    this.fetchedBytes += bytes.length;
    this.requests += 1;
    this.blocks.push({ start: from, bytes });
    return bytes.subarray(start - from, end - from);
  }

  /** One range request; a dropped connection is tried again, a refusal is not. */
  private async request(range: string) {
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await fetch(this.url, { headers: { Range: range } });
        if (response.status >= 500) throw new Error(`the file host answered ${response.status}`);
        if (response.status !== 206) fail(`the file host answered ${response.status} to a range request`);
        return { bytes: new Uint8Array(await response.arrayBuffer()) as Uint8Array, contentRange: response.headers.get("content-range") ?? "" };
      } catch (error) {
        if (error instanceof Error && error.message.startsWith("Page-range reader:")) throw error;
        lastError = error;
      }
    }
    const cause = lastError instanceof Error && lastError.cause instanceof Error ? `: ${lastError.cause.message}` : "";
    fail(`the file host dropped the connection (${lastError instanceof Error ? lastError.message : String(lastError)}${cause})`);
  }

  async open() {
    const head = await this.request(`bytes=-${TAIL_BYTES}`);
    const total = Number(/\/(\d+)\s*$/.exec(head.contentRange)?.[1]);
    if (!Number.isFinite(total) || total <= 0) fail("the file size is unknown");
    this.size = total;
    const tail = head.bytes;
    const tailStart = total - tail.length;
    this.fetchedBytes += tail.length;
    this.requests += 1;

    const text = latin1(tail);
    const mark = text.lastIndexOf("startxref");
    const xrefOffset = mark < 0 ? NaN : Number(/^startxref\s+(\d+)/.exec(text.slice(mark))?.[1]);
    if (!Number.isFinite(xrefOffset) || xrefOffset >= total) fail("no startxref at the end of the file");
    this.blocks.push({ start: tailStart, bytes: tail });

    // Newest index first; an older one (a file saved again after an edit) only
    // fills in the objects the newer ones do not mention.
    const starts = new Set<number>([total]);
    const settled = new Set<number>();
    let at: number | null = xrefOffset;
    for (let hops = 0; at != null; hops++) {
      if (hops > 32) fail("the index chain loops");
      starts.add(at);
      // An index has no stated length before it is parsed; 1 MB holds any of ours.
      const bytes = await this.range(at, at >= tailStart ? total : Math.min(total, at + 1024 * 1024));
      const xref = this.parseIndirect(bytes).object;
      if (!(xref instanceof PDFRawStream) || xref.dict.get(PDFName.of("Type")) !== PDFName.of("XRef")) {
        fail("the file has a cross-reference table, not a stream");
      }
      if (xref.dict.has(PDFName.of("Encrypt"))) fail("the file is encrypted");
      if (!this.root) {
        const root = xref.dict.get(PDFName.of("Root"));
        if (!(root instanceof PDFRef)) fail("the file names no catalog");
        this.root = root;
      }
      for (const entry of xrefEntries(xref)) {
        if (entry.type === 1) starts.add(entry.value);
        if (settled.has(entry.number)) continue;
        settled.add(entry.number);
        if (entry.type === 1) this.where.set(entry.number, { offset: entry.value });
        else if (entry.type === 2) this.where.set(entry.number, { container: entry.value });
      }
      const prev = xref.dict.get(PDFName.of("Prev"));
      at = prev instanceof PDFNumber ? prev.asNumber() : null;
      if (at != null && !(at >= 0 && at < total)) fail("the index chain points outside the file");
    }
    this.starts = [...starts].sort((a, b) => a - b);
  }

  /** `N G obj <object>` at the start of the bytes. */
  private parseIndirect(bytes: Uint8Array) {
    const header = /^\s*(\d+)\s+(\d+)\s+obj/.exec(latin1(bytes.subarray(0, 48)));
    if (!header) fail("an object is not where the index says");
    const object = PDFObjectParser.forBytes(bytes.subarray(header[0].length), this.context).parseObject();
    return { number: Number(header[1]), generation: Number(header[2]), object };
  }

  private endOf(offset: number) {
    let low = 0;
    let high = this.starts.length - 1;
    while (low < high) {
      const mid = (low + high) >> 1;
      if (this.starts[mid] > offset) high = mid;
      else low = mid + 1;
    }
    const end = this.starts[low];
    if (!(end > offset)) fail("an object lies past the end of the index");
    return end;
  }

  /** Fetches and parses these objects, a few merged byte ranges at a time. */
  async load(numbers: Iterable<number>): Promise<number[]> {
    const fresh: number[] = [];
    const direct = new Set<number>();
    const asked = new Set<number>();
    const inside = new Set<number>();
    for (const number of numbers) {
      if (this.loaded.has(number)) continue;
      const at = this.where.get(number);
      if (!at) continue; // not in the file: the copier leaves such a reference empty, as pdf-lib does
      if ("container" in at) {
        inside.add(number);
        const holder = this.where.get(at.container);
        if (!holder || "container" in holder) fail("an object stream is missing");
        if (!this.loaded.has(at.container)) direct.add(at.container);
      } else {
        direct.add(number);
        asked.add(number);
      }
    }

    const wanted = [...direct]
      .map((number) => {
        const offset = (this.where.get(number) as { offset: number }).offset;
        return { number, offset, end: this.endOf(offset) };
      })
      .sort((a, b) => a.offset - b.offset);
    const groups: Array<{ start: number; end: number; items: typeof wanted }> = [];
    const held = (item: { offset: number; end: number }) =>
      this.blocks.some((block) => item.offset >= block.start && item.end <= block.start + block.bytes.length);
    for (const item of wanted) {
      if (held(item)) {
        groups.push({ start: item.offset, end: item.end, items: [item] });
        continue;
      }
      const last = groups[groups.length - 1];
      if (last && !held({ offset: last.start, end: last.end }) && item.offset - last.end <= MERGE_GAP_BYTES) {
        last.end = Math.max(last.end, item.end);
        last.items.push(item);
      } else {
        groups.push({ start: item.offset, end: item.end, items: [item] });
      }
    }

    let next = 0;
    const worker = async () => {
      while (next < groups.length) {
        const group = groups[next++];
        const bytes = await this.range(group.start, group.end);
        for (const item of group.items) {
          const parsed = this.parseIndirect(bytes.subarray(item.offset - group.start, item.end - group.start));
          if (parsed.number !== item.number) fail("an object is not where the index says");
          this.context.assign(PDFRef.of(parsed.number, parsed.generation), parsed.object);
          this.loaded.add(item.number);
          if (asked.has(item.number)) fresh.push(item.number);
          if (parsed.object instanceof PDFRawStream && parsed.object.dict.get(PDFName.of("Type")) === PDFName.of("ObjStm")) {
            await PDFObjectStreamParser.forStream(parsed.object).parseIntoContext();
          }
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(PARALLEL_REQUESTS, groups.length) }, worker));

    for (const number of inside) {
      this.loaded.add(number);
      fresh.push(number);
    }
    return fresh;
  }

  async object(ref: PDFRef) {
    await this.load([ref.objectNumber]);
    return this.context.lookup(ref);
  }

  /** The page leaves at these 0-based positions, walking down the page tree. */
  async pageLeaves(indexes: number[]) {
    const catalog = await this.object(this.root as PDFRef);
    if (!(catalog instanceof PDFDict)) fail("the catalog is unreadable");
    const pagesRef = catalog.get(PDFName.of("Pages"));
    if (!(pagesRef instanceof PDFRef)) fail("the catalog names no pages");
    const top = await this.object(pagesRef);
    if (!(top instanceof PDFPageTree)) fail("the page tree is unreadable");
    const total = top.lookup(PDFName.of("Count"), PDFNumber).asNumber();

    const found = new Map<number, PDFPageLeaf>();
    const wanted = [...new Set(indexes)].filter((i) => i >= 0 && i < total).sort((a, b) => a - b);
    const descend = async (node: PDFPageTree, first: number, targets: number[], depth: number): Promise<void> => {
      if (depth > 64) fail("the page tree loops");
      const kids = node.lookup(PDFName.of("Kids"), PDFArray).asArray();
      await this.load(kids.map((kid) => (kid instanceof PDFRef ? kid.objectNumber : fail("a page is not a reference"))));
      let at = first;
      for (const kid of kids) {
        const child = this.context.lookup(kid as PDFRef);
        if (child instanceof PDFPageTree) {
          const count = child.lookup(PDFName.of("Count"), PDFNumber).asNumber();
          const mine = targets.filter((i) => i >= at && i < at + count);
          if (mine.length) await descend(child, at, mine, depth + 1);
          at += count;
        } else if (child instanceof PDFPageLeaf) {
          if (targets.includes(at)) found.set(at, child);
          at += 1;
        } else {
          fail("the page tree holds something that is not a page");
        }
      }
    };
    await descend(top, 0, wanted, 0);
    if (found.size !== wanted.length) fail("some pages were not found in the page tree");
    return { total, found };
  }

  /** Loads everything the pages point to, level by level, as the copier will follow it. */
  async loadEverythingUnder(leaves: PDFPageLeaf[]) {
    const seen = new Set<number>();
    const refsIn = (start: PDFObject) => {
      const out: number[] = [];
      const stack: PDFObject[] = [start];
      while (stack.length) {
        const value = stack.pop() as PDFObject;
        if (value instanceof PDFRef) {
          if (!seen.has(value.objectNumber)) {
            seen.add(value.objectNumber);
            out.push(value.objectNumber);
          }
        } else if (value instanceof PDFArray) {
          stack.push(...value.asArray());
        } else if (value instanceof PDFDict || value instanceof PDFStream) {
          const dict = value instanceof PDFStream ? value.dict : value;
          // A page is copied without its parent, so the rest of the book is never reached.
          const skipParent = dict instanceof PDFPageLeaf;
          for (const [key, entry] of dict.entries()) {
            if (skipParent && key === PDFName.of("Parent")) continue;
            stack.push(entry);
          }
        }
      }
      return out;
    };

    let frontier = leaves.flatMap((leaf) => refsIn(leaf));
    while (frontier.length) {
      const fresh = await this.load(frontier);
      frontier = fresh.flatMap((number) => {
        const value = this.context.lookup(PDFRef.of(number, 0));
        return value ? refsIn(value) : [];
      });
    }
  }
}

/**
 * Appends the chosen pages (1-based, in the order given) to `out`, fetching
 * only what they need. Nothing is added to `out` unless every page was read.
 */
export async function appendRemotePages(out: PDFDocument, pdfUrl: string, pages: number[]) {
  const pdf = new RemotePdf(pdfUrl);
  await pdf.open();
  const { total, found } = await pdf.pageLeaves(pages.map((p) => p - 1));
  const kept = pages.filter((p) => p >= 1 && p <= total);
  const leaves = kept.map((p) => found.get(p - 1) as PDFPageLeaf);
  await pdf.loadEverythingUnder(leaves);

  const copier = PDFObjectCopier.for(pdf.context, out.context);
  for (const leaf of leaves) {
    const copied = copier.copy(leaf);
    out.addPage(PDFPage.of(copied, out.context.register(copied), out));
  }
  return { pages: kept.length, fetchedBytes: pdf.fetchedBytes, requests: pdf.requests, fileBytes: pdf.size };
}
