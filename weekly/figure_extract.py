"""Extract figure-bearing pages from journal PDFs for multimodal appraisal."""

from __future__ import annotations

import re
from collections.abc import Collection
from pathlib import Path

import fitz  # PyMuPDF

DEFAULT_MIN_DIM = 200
DEFAULT_MAX_PAGES = 12
# Separate budget for table pages whose structure was lost in PDF→markdown.
# Kept apart from the figure budget so a paper with many figures can't crowd
# out its baseline-characteristics table, which carries as much appraisal
# weight as any plot.
DEFAULT_MAX_TABLE_PAGES = 4
DEFAULT_MIN_DRAWINGS = 40
# 2.5x (=180 DPI) renders a journal page at ~1440x1935. Calibrated against the
# 4.8-era vision cap (2576px long edge, ~3.6 MP): a 3.0x render (4.0 MP) is
# downscaled server-side before the model ever sees it — the extra pixels cost
# tokens but carry no detail. 2.5x lands just under that cap: ~21% fewer image
# tokens per page for ~12% less effective resolution than 3.0x.
# NOTE: appraisals now run on Opus 5 (2026-07-25); its image cap has not been
# re-verified. If Opus 5 raised the cap, 2.5x is still legible but no longer
# the token-optimal point — re-check the cap and revisit this if image cost
# looks off in the first weekly run.
DEFAULT_ZOOM = 2.5
# Caption-based fallback for vector-drawn figures (e.g. Lancet KM curves,
# CONSORT diagrams): a whole step-curve is a single path, so the drawing-object
# count stays below DEFAULT_MIN_DRAWINGS even though the page is clearly a
# figure. We then require a figure caption plus a block of vector drawing
# covering most of the page.
DEFAULT_MIN_CAPTION_DRAWINGS = 8
DEFAULT_MIN_CAPTION_COVERAGE = 0.6

_FIGURE_CAPTION_RE = re.compile(r"^\s*(?:Figure|Fig\.?)\s*\d+\b", re.IGNORECASE | re.MULTILINE)

# Whole-paper summary panels: JACC's Central Illustration, and the graphical /
# visual abstract several other journals run. They carry no "Figure N" number,
# so _FIGURE_CAPTION_RE misses them, and they often sit near the end of the PDF
# where the page budget would drop them — hence a pattern of their own plus top
# priority in the ordering below. These are the single most informative page in
# a JACC paper, so losing one to truncation is the worst failure mode here.
_KEY_FIGURE_RE = re.compile(
    r"^\s*(?:CENTRAL\s+ILLUSTRATION|GRAPHICAL\s+ABSTRACT|VISUAL\s+ABSTRACT)\b",
    re.IGNORECASE | re.MULTILINE,
)


# Table pages are rendered only when the PDF→markdown pass failed to turn the
# table into a markdown table (see `parsed_table_pages` below). Two ways that
# happens in practice: the journal ships the table as vector art with no ruling
# lines that pymupdf4llm can latch onto (JACC), or the table is embedded as an
# image and comes out as a scrambled "picture text" run (Lancet). Both leave
# the numbers technically present but with the row/column mapping destroyed —
# worse than absent, because the model will still read them.
_TABLE_CAPTION_RE = re.compile(r"^\s*Table\s*\d+\b", re.IGNORECASE | re.MULTILINE)


def _caption_kind(text: str) -> tuple[bool, bool]:
    """Return (is_key_figure, has_any_caption) for a page's text."""
    is_key = bool(_KEY_FIGURE_RE.search(text))
    return is_key, is_key or bool(_FIGURE_CAPTION_RE.search(text))


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
    would miss: needs a figure caption line ("Figure N" or a key-figure panel)
    and a meaningful block of vector drawing covering most of the page."""
    if len(draws) < min_caption_drawings:
        return False
    if not _caption_kind(page.get_text())[1]:
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
    parsed_table_pages: Collection[int] | None = None,
    max_table_pages: int = DEFAULT_MAX_TABLE_PAGES,
) -> list[Path]:
    """Render figure-bearing pages of a PDF to PNG; return saved paths.

    A page qualifies if it contains a meaningful raster image, enough vector
    drawing operations, or a figure caption alongside a page-spanning block
    of vector drawing (the last case catches vector plots whose object count is
    low because a whole curve is a single path). Whole-page rendering is the
    robust path for journal PDFs because plots are commonly vector drawings
    rather than embedded raster images.

    `parsed_table_pages` is the set of 1-indexed page numbers whose tables did
    survive PDF→markdown as markdown tables. Pass it to also render, under
    their own `max_table_pages` budget, any page carrying a "Table N" caption
    that is *not* in that set — those are tables the model would otherwise read
    as scrambled text. Leave it None to skip table rendering entirely.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    mat = fitz.Matrix(zoom, zoom)
    with fitz.open(pdf_path) as doc:
        # Pass 1: collect qualifying pages, noting whether each carries a figure
        # caption (i.e. holds a labelled figure, not just a table/decoration)
        # and whether that caption marks a whole-paper summary panel.
        candidates: list[tuple[int, bool, bool]] = []
        unparsed_tables: list[int] = []
        for page in doc:
            has_big_raster = False
            for img in page.get_images(full=True):
                pix = fitz.Pixmap(doc, img[0])
                if min(pix.width, pix.height) >= min_dim:
                    has_big_raster = True
                    break

            text = page.get_text()
            draws = page.get_drawings()
            is_key, has_caption = _caption_kind(text)
            is_figure_page = (
                has_big_raster
                or len(draws) >= min_drawings
                or _is_vector_figure_page(
                    page, draws, min_caption_drawings, min_caption_coverage
                )
            )
            if is_figure_page:
                candidates.append((page.number, is_key, has_caption))
            if (
                parsed_table_pages is not None
                and page.number + 1 not in parsed_table_pages
                and _TABLE_CAPTION_RE.search(text)
            ):
                unparsed_tables.append(page.number)

        # When more pages qualify than the budget allows, rank key-figure panels
        # first, then captioned figure pages, so neither a Central Illustration
        # nor a key labelled figure late in a long document is crowded out by
        # earlier table/decoration pages. Within each group, preserve document
        # order.
        candidates.sort(key=lambda c: (not c[1], not c[2], c[0]))
        selected = sorted(page_no for page_no, _, _ in candidates[:max_pages])

        # Add lost tables on their own budget. Pages already picked as figure
        # pages are skipped — they're rendered either way.
        if unparsed_tables and max_table_pages > 0:
            chosen = set(selected)
            extra = [n for n in unparsed_tables if n not in chosen][:max_table_pages]
            selected = sorted(chosen.union(extra))

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
