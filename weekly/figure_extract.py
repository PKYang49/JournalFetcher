"""Extract figure-bearing pages from journal PDFs for multimodal appraisal."""

from __future__ import annotations

from pathlib import Path

import fitz  # PyMuPDF

DEFAULT_MIN_DIM = 200
DEFAULT_MAX_PAGES = 6
DEFAULT_MIN_DRAWINGS = 40
DEFAULT_ZOOM = 3.0


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
) -> list[Path]:
    """Render figure-bearing pages of a PDF to PNG; return saved paths.

    A page qualifies if it contains a meaningful raster image or enough vector
    drawing operations to look like a figure page. Whole-page rendering is the
    robust path for journal PDFs because plots are commonly vector drawings
    rather than embedded raster images.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    mat = fitz.Matrix(zoom, zoom)
    with fitz.open(pdf_path) as doc:
        for page in doc:
            has_big_raster = False
            for img in page.get_images(full=True):
                pix = fitz.Pixmap(doc, img[0])
                if min(pix.width, pix.height) >= min_dim:
                    has_big_raster = True
                    break

            is_figure_page = has_big_raster or len(page.get_drawings()) >= min_drawings
            if not is_figure_page:
                continue

            pix = page.get_pixmap(matrix=mat)
            if pix.n - pix.alpha >= 4:
                pix = fitz.Pixmap(fitz.csRGB, pix)
            out = out_dir / f"page_{page.number + 1:03d}.png"
            pix.save(out)
            saved.append(out)
            if len(saved) >= max_pages:
                break
    return saved
