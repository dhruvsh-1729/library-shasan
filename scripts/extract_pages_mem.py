#!/usr/bin/env python3
import os, sys, json
import fitz  # PyMuPDF

# OCR fallback (optional)
USE_OCR = os.getenv("USE_OCR_FALLBACK", "1") == "1"
OCR_LANGS = os.getenv("OCR_LANGS", "guj+hin")
OCR_DPI = int(os.getenv("OCR_DPI", "300"))

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
    txt = pytesseract.image_to_string(img, lang=OCR_LANGS, config="--oem 1 --psm 6")
    txt = normalize_text(txt)
    if not meaningful(txt):
        # retry with different psm
        txt = pytesseract.image_to_string(img, lang=OCR_LANGS, config="--oem 1 --psm 3")
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
