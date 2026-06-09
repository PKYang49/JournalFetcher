"""Extract figure-bearing pages from journal PDFs for multimodal appraisal."""

from __future__ import annotations

import re
from pathlib import Path

import fitz  # PyMuPDF

DEFAULT_MIN_DIM = 200
DEFAULT_MAX_PAGES = 6
DEFAULT_MIN_DRAWINGS = 40
DEFAULT_ZOOM = 3.0
# Caption-based fallback for vector-drawn figures (e.g. Lancet KM curves,
# CONSORT diagrams): a whole step-curve is a single path, so the drawing-object
# count stays below DEFAULT_MIN_DRAWINGS even though the page is clearly a
# figure. We then require a "Figure N" caption plus a block of vector drawing
# covering most of the page.
DEFAULT_MIN_CAPTION_DRAWINGS = 8
DEFAULT_MIN_CAPTION_COVERAGE = 0.6

_FIGURE_CAPTION_RE = re.compile(r"^\s*(?:Figure|Fig\.?)\s*\d+\b", re.IGNORECASE | re.MULTILINE)


def _drawings_coverage(draws: list[dict], page_rect: fitz.Rect) -> float:
    """Fraction of the page spanned by the union bounding box of all drawings."""
    page_area = page_rect.width * page_rect.height
    if not draws or page_area <= 0:
        return 0.0
    x0 = min(d["rect"].x0 for d in draws)
    y0 = min(d["rect"].y0 for d in draws)
    x1 = max(d["rect"].x1 for d in draws)
    y1 = max(d["rect"].y1 for d in draws)
    return max(0.0, (x1 - x0) * (y1 - y0)) / page_area


def _is_vector_figure_page(
    page: fitz.Page,
    draws: list[dict],
    min_caption_drawings: int,
    min_caption_coverage: float,
) -> bool:
    """True when a page holds a vector-drawn figure that the raw drawing count
    would miss: needs a 'Figure N' caption line and a meaningful block of vector
    drawing covering most of the page."""
    if len(draws) < min_caption_drawings:
        return False
    if not _FIGURE_CAPTION_RE.search(page.get_text()):
        return False
    return _drawings_coverage(draws, page.rect) >= min_caption_coverage


def extract_embedded(
    pdf_path: Path,
    out_dir: Path,
    *,
    min_dim: int = DEFAULT_MIN_DIM,
    max_images: int = DEFAULT_MAX_PAGES,
) -> list[Path]:
    """Save embedded raster images whose shorter side is at least min_dim."""
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    seen: set[int] = set()
    with fitz.open(pdf_path) as doc:
        for page in doc:
            for img in page.get_images(full=True):
                xref = img[0]
                if xref in seen:
                    continue
                seen.add(xref)
                pix = fitz.Pixmap(doc, xref)
                if min(pix.width, pix.height) < min_dim:
                    continue
                if pix.n - pix.alpha >= 4:
                    pix = fitz.Pixmap(fitz.csRGB, pix)
                out = out_dir / f"img_p{page.number + 1:03d}_x{xref}.png"
                pix.save(out)
                saved.append(out)
                if len(saved) >= max_images:
                    return saved
    return saved


def extract_figure_pages(
    pdf_path: Path,
    out_dir: Path,
    *,
    zoom: float = DEFAULT_ZOOM,
    min_drawings: int = DEFAULT_MIN_DRAWINGS,
    min_dim: int = DEFAULT_MIN_DIM,
    max_pages: int = DEFAULT_MAX_PAGES,
    min_caption_drawings: int = DEFAULT_MIN_CAPTION_DRAWINGS,
    min_caption_coverage: float = DEFAULT_MIN_CAPTION_COVERAGE,
) -> list[Path]:
    """Render figure-bearing pages of a PDF to PNG; return saved paths.

    A page qualifies if it contains a meaningful raster image, enough vector
    drawing operations, or a "Figure N" caption alongside a page-spanning block
    of vector drawing (the last case catches vector plots whose object count is
    low because a whole curve is a single path). Whole-page rendering is the
    robust path for journal PDFs because plots are commonly vector drawings
    rather than embedded raster images.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    mat = fitz.Matrix(zoom, zoom)
    with fitz.open(pdf_path) as doc:
        # Pass 1: collect qualifying pages and whether each carries a "Figure N"
        # caption (i.e. holds a labelled figure, not just a table/decoration).
        candidates: list[tuple[int, bool]] = []
        for page in doc:
            has_big_raster = False
            for img in page.get_images(full=True):
                pix = fitz.Pixmap(doc, img[0])
                if min(pix.width, pix.height) >= min_dim:
                    has_big_raster = True
                    break

            draws = page.get_drawings()
            has_caption = bool(_FIGURE_CAPTION_RE.search(page.get_text()))
            is_figure_page = (
                has_big_raster
                or len(draws) >= min_drawings
                or _is_vector_figure_page(
                    page, draws, min_caption_drawings, min_caption_coverage
                )
            )
            if is_figure_page:
                candidates.append((page.number, has_caption))

        # When more pages qualify than the budget allows, keep captioned figure
        # pages first so a key labelled figure late in a long document is not
        # crowded out by earlier table/decoration pages. Within each group,
        # preserve document order.
        candidates.sort(key=lambda c: (not c[1], c[0]))
        selected = sorted(page_no for page_no, _ in candidates[:max_pages])

        # Pass 2: render the chosen pages in document order.
        for page_no in selected:
            page = doc[page_no]
            pix = page.get_pixmap(matrix=mat)
            if pix.n - pix.alpha >= 4:
                pix = fitz.Pixmap(fitz.csRGB, pix)
            out = out_dir / f"page_{page_no + 1:03d}.png"
            pix.save(out)
            saved.append(out)
    return saved
