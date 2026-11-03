"""Readable per-page text from a PDF, falling back to tesseract OCR.

Some publisher PDFs carry a text layer that does not decode: subset fonts
without a ToUnicode map make fitz return glyph ids ("*+?9.?34/") instead of
characters. JAMA's LEAP-Block protocol and SAP (2026-W45) were like that,
and 310k chars of it went into the appraisal prompt as an "appendix". Scanned
PDFs with no text layer at all fail the same way, just silently.

`text_layer_unreadable` spots both; `ocr_page_texts` re-reads the pages with
tesseract through PyMuPDF (`brew install tesseract`). The OCR result is
cached next to the PDF as `<stem>.ocr.json`, since a 120-page SAP takes
~2.5 minutes at 300 dpi.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from functools import lru_cache
from pathlib import Path

import fitz  # PyMuPDF

OCR_DPI = int(os.getenv("JOURNAL_FETCHER_OCR_DPI", "300"))
OCR_LANGUAGE = os.getenv("JOURNAL_FETCHER_OCR_LANGUAGE", "eng")

# Common English function words. Measured on the 41 supplement PDFs in
# output/supplements/ (2026-10/11): readable files score 0.12-0.36 of their
# alphabetic tokens in this set, collaborator rosters (mostly names) 0.04-0.08,
# the two broken-font LEAP-Block files 0.002 and 0.009.
_STOPWORDS = frozenset(
    "the of and to in a for with or is was were be by on at as are from that "
    "this not an will all no".split()
)
_STOPWORD_FLOOR = 0.02
# Below this many alphabetic tokens the ratio means nothing (a one-page data
# sharing statement); such files are judged by characters per page instead.
_MIN_WORDS = 200
# A scanned PDF with no text layer extracts to (almost) nothing per page.
_MIN_CHARS_PER_PAGE = 50
_WORD_RE = re.compile(r"[A-Za-z]{2,}")


class OcrUnavailableError(RuntimeError):
    """tesseract is not installed or has no data for OCR_LANGUAGE."""


def text_layer_unreadable(text: str, pages: int) -> bool:
    """True when a PDF's extracted text is glyph garbage or missing."""
    if pages >= 2 and len(text.strip()) < _MIN_CHARS_PER_PAGE * pages:
        return True
    words = [w.lower() for w in _WORD_RE.findall(text)]
    if len(words) < _MIN_WORDS:
        return False
    hits = sum(w in _STOPWORDS for w in words)
    return hits / len(words) < _STOPWORD_FLOOR


def doc_text_unreadable(doc: fitz.Document) -> bool:
    text = "\n".join(page.get_text() for page in doc)
    return text_layer_unreadable(text, doc.page_count)


@lru_cache(maxsize=1)
def _tessdata() -> str:
    if shutil.which("tesseract") is None:
        raise OcrUnavailableError("tesseract not on PATH (brew install tesseract)")
    try:
        return fitz.get_tessdata()
    except Exception as e:
        raise OcrUnavailableError(f"tesseract language data not found: {e}") from e


def ocr_page(page: fitz.Page) -> str:
    """OCR one page (ignoring its text layer)."""
    textpage = page.get_textpage_ocr(
        language=OCR_LANGUAGE, dpi=OCR_DPI, full=True, tessdata=_tessdata()
    )
    return page.get_text(textpage=textpage)


def ocr_page_texts(pdf_path: Path) -> list[str]:
    """OCR every page of `pdf_path`, cached as `<stem>.ocr.json`."""
    cache = pdf_path.with_suffix(".ocr.json")
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    with fitz.open(pdf_path) as doc:
        texts = [ocr_page(page) for page in doc]
    cache.write_text(json.dumps(texts, ensure_ascii=False), encoding="utf-8")
    return texts
