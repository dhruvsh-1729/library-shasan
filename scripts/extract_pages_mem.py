#!/usr/bin/env python3
import os, sys, json, subprocess, shlex
import fitz  # PyMuPDF

# OCR fallback (optional)
USE_OCR = os.getenv("USE_OCR_FALLBACK", "1") == "1"
REQUESTED_OCR_LANGS = os.getenv("OCR_LANGS", "guj+san+eng")
OCR_DPI = int(os.getenv("OCR_DPI", "300"))
OCR_TESSDATA_DIR = os.getenv("OCR_TESSDATA_DIR", os.path.join(os.getcwd(), ".library_pipeline_state", "tessdata"))
OCR_EXTRA_TESSDATA_DIRS = os.getenv("OCR_EXTRA_TESSDATA_DIRS", "/home/dell/Downloads")
SYSTEM_TESSDATA_DIRS = [
    "/usr/share/tesseract-ocr/5/tessdata",
    "/usr/share/tesseract-ocr/4.00/tessdata",
    "/usr/share/tessdata",
    "/usr/local/share/tessdata",
]
_RESOLVED_OCR_LANGS = None

def parse_path_list(raw: str) -> list[str]:
    out = []
    for chunk in (raw or "").replace(",", os.pathsep).split(os.pathsep):
        value = chunk.strip()
        if value and value not in out:
            out.append(value)
    return out

def tessdata_source_dirs() -> list[str]:
    out = []
    def add(path: str):
        if path and path not in out:
            out.append(path)

    prefix = os.getenv("TESSDATA_PREFIX")
    if prefix:
        add(prefix)
        add(os.path.join(prefix, "tessdata"))
    for path in SYSTEM_TESSDATA_DIRS:
        add(path)
    for path in parse_path_list(OCR_EXTRA_TESSDATA_DIRS):
        add(path)
    return out

def find_tessdata_source(lang: str, source_dirs: list[str]) -> str | None:
    for directory in source_dirs:
        candidate = os.path.join(directory, f"{lang}.traineddata")
        if os.path.exists(candidate):
            return candidate
    return None

def ensure_tessdata_link(source: str, dest: str):
    if os.path.exists(dest):
        return
    try:
        os.symlink(source, dest)
    except Exception:
        import shutil
        shutil.copyfile(source, dest)

def ensure_tessdata_dir(requested: list[str]) -> str:
    os.makedirs(OCR_TESSDATA_DIR, exist_ok=True)
    source_dirs = tessdata_source_dirs()
    for lang in requested:
        source = find_tessdata_source(lang, source_dirs)
        if source:
            ensure_tessdata_link(source, os.path.join(OCR_TESSDATA_DIR, f"{lang}.traineddata"))
    return OCR_TESSDATA_DIR

def resolve_ocr_langs() -> str:
    global _RESOLVED_OCR_LANGS
    if _RESOLVED_OCR_LANGS:
        return _RESOLVED_OCR_LANGS

    requested = [lang.strip() for lang in REQUESTED_OCR_LANGS.split("+") if lang.strip()]
    if not requested:
        raise RuntimeError("OCR_LANGS did not contain any language codes")
    tessdata_dir = ensure_tessdata_dir(requested)

    try:
        result = subprocess.run(
            ["tesseract", "--list-langs", "--tessdata-dir", tessdata_dir],
            check=True,
            capture_output=True,
            text=True,
        )
        available = {
            line.strip()
            for line in (result.stdout + "\n" + result.stderr).splitlines()
            if line.strip() and not line.lower().startswith("list of available languages")
        }
    except Exception:
        _RESOLVED_OCR_LANGS = REQUESTED_OCR_LANGS
        return _RESOLVED_OCR_LANGS

    selected = [lang for lang in requested if lang in available]
    missing = [lang for lang in requested if lang not in available]
    if not selected:
        raise RuntimeError(
            f"None of requested OCR langs are installed. requested={REQUESTED_OCR_LANGS}, "
            f"available={','.join(sorted(available))}"
        )
    if missing:
        print(f"[ocr] Missing tesseract langs skipped: {','.join(missing)}", file=sys.stderr)

    _RESOLVED_OCR_LANGS = "+".join(selected)
    return _RESOLVED_OCR_LANGS

def tesseract_config(psm: int) -> str:
    tessdata_dir = ensure_tessdata_dir([lang.strip() for lang in REQUESTED_OCR_LANGS.split("+") if lang.strip()])
    return f"--tessdata-dir {shlex.quote(tessdata_dir)} --oem 1 --psm {psm}"

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\x00", "")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()

def meaningful(s: str) -> bool:
    return len("".join(s.split())) > 0

def ocr_page(page: fitz.Page) -> str:
    # Render page to pixmap in memory, then OCR in memory
    # Avoids writing images to disk.
    import pytesseract
    from PIL import Image
    import io

    zoom = OCR_DPI / 72.0
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)

    img = Image.open(io.BytesIO(pix.tobytes("png")))
    txt = pytesseract.image_to_string(img, lang=resolve_ocr_langs(), config=tesseract_config(6))
    txt = normalize_text(txt)
    if not meaningful(txt):
        # retry with different psm
        txt = pytesseract.image_to_string(img, lang=resolve_ocr_langs(), config=tesseract_config(3))
        txt = normalize_text(txt)
    return txt

def main():
    pdf_bytes = sys.stdin.buffer.read()
    if not pdf_bytes:
        print(json.dumps({"error": "No PDF bytes received on stdin"}))
        sys.exit(2)

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        print(json.dumps({"error": f"Failed to open PDF: {str(e)}"}))
        sys.exit(2)

    pages_out = []
    stats = {
        "totalPages": doc.page_count,
        "textLayerPages": 0,
        "ocrPages": 0,
        "emptyPages": 0
    }

    for i in range(doc.page_count):
        page = doc.load_page(i)
        # First try text-layer
        text = normalize_text(page.get_text("text") or "")
        source = "text-layer"

        if not meaningful(text) and USE_OCR:
            source = "ocr"
            text = ocr_page(page)

        if meaningful(text):
            if source == "text-layer": stats["textLayerPages"] += 1
            else: stats["ocrPages"] += 1
        else:
            stats["emptyPages"] += 1

        pages_out.append({"page_number": i + 1, "text": text, "source": source, "chars": len(text)})

    print(json.dumps({"pages": pages_out, "stats": stats}, ensure_ascii=False))

if __name__ == "__main__":
    main()
