"""Spike: extract journal figures from a PDF and run a multimodal appraisal.

Goal of the spike (not production): find out whether feeding figures as images
to the appraisal backend produces a figure-level appraisal worth the extra
tokens. Both backends can see images: claude -p via the Read tool (renders
PNG/JPG visually), codex exec via its `-i/--image <FILE>...` flag.

It reuses `modules.claude_exec.run_claude_prompt` with `read_dirs`, the same
mechanism the appraisal pipeline already uses for JAMA reference files.

Measured on 42201288_Butler_2026 (Opus 4.6, --mode pages, 4 figure pages):
text-only $0.4035 vs text+figures $0.5106 → +$0.107/article (~+$2-4/month at
5-8 selected appraisals/week). See codex-task-figures.md for the production
integration handoff.

Usage:
    python3 -m weekly.spike_figure_appraisal <pdf> [--mode embedded|pages]
        [--max-images 6] [--min-dim 200] [--zoom 2.0]
        [--keep] [--no-appraise] [--measure]

Examples:
    python3 -m weekly.spike_figure_appraisal <pdf> --mode pages --no-appraise
    python3 -m weekly.spike_figure_appraisal <pdf> --mode pages --measure
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import fitz  # PyMuPDF

from modules.claude_exec import (
    ClaudeError,
    get_claude_appraisal_model,
    run_claude_prompt,
)
from weekly.appraise_selected import _convert_pdf_to_markdown

# Skip logos / icons / rules: only keep embedded rasters at least this many
# pixels on the shorter side.
DEFAULT_MIN_DIM = 200
# Cap how many images we hand to the model — token cost scales with image area.
DEFAULT_MAX_IMAGES = 6
# Page render zoom for `pages` mode (2.0 ≈ 144 dpi, legible axis labels).
DEFAULT_ZOOM = 2.0
# Vector-drawing count above which a page is treated as containing a figure.
# Journal figures (KM curves, forest plots) are usually vector, not raster, so
# get_images() misses them; the path-op count separates figure pages (50-260)
# from text pages (0-11) cleanly in practice.
DEFAULT_MIN_DRAWINGS = 40

APPRAISAL_PROMPT = """\
你是一位心臟科臨床流行病學家,正在對一篇期刊論文的「圖」做方法學評讀。

目錄 {figdir} 下有 {n} 張從 PDF 抽出的圖檔:
{filelist}

請用 Read 工具逐一讀取這些圖檔(它們是 PNG/JPG,Read 會以影像呈現),然後針對
「圖本身傳達的證據」做評讀。針對你看到的圖型,挑相關的講:

- Kaplan-Meier 曲線:兩組曲線何時開始分離?風險表(number at risk)後期樣本是否
  銳減導致尾段不可信?信賴區間是否標示?
- Forest plot:點估計與 CI 的方向、是否跨越無效線、異質性 (I²)、次組分析是否過多。
- CONSORT / flow diagram:納入→隨機→分析的人數是否一致?dropout / 排除理由是否充分?
- Funnel plot:是否對稱(發表偏誤)?
- 其他圖表:座標軸是否從非零起點誇大效果?是否有 cherry-picking 的時間點?

只根據圖中實際可見的內容評讀,看不清楚就明說「圖解析度不足無法判讀」,不要臆測。
最後用繁體中文輸出,每張圖一段,標明檔名。
"""


# --- Approach B: one call carrying markdown + figures -----------------------
# These mirror the production split (text appraisal) plus a figures addendum,
# so the cost DELTA between the two below approximates the real incremental
# cost of folding figures into the existing appraisal call. The shared skill
# system prompt is constant across both, so it cancels out of the delta.

_TEXT_ONLY_PROMPT = """\
你是心臟科臨床流行病學家。請對下方 PDF 轉出的 markdown 做完整批判性評讀
(研究設計、偏誤、效應量與精確度、臨床意義),繁體中文輸出。

PDF converted markdown:
{markdown}
"""

_COMBINED_PROMPT = """\
你是心臟科臨床流行病學家。請對下方文章做完整批判性評讀,繁體中文輸出。
文章本文是 PDF 轉出的 markdown(圖已被移除)。另外目錄 {figdir} 下有 {n} 張
從同一篇 PDF 抽出的圖頁:
{filelist}

請先讀完 markdown,再用 Read 工具逐一讀取這些圖檔(PNG,Read 會以影像呈現),
把「圖傳達但文字沒講清楚的證據」併入評讀:KM 曲線分離時點與 number-at-risk
後期銳減、forest plot 的 CI 是否跨越無效線、座標軸是否從非零起點誇大效果等。
看不清楚就明說,不要臆測。

PDF converted markdown:
{markdown}
"""


def appraise_text_only(markdown: str) -> tuple[str, float]:
    """Baseline: markdown-only appraisal (mirrors current production)."""
    return run_claude_prompt(
        _TEXT_ONLY_PROMPT.format(markdown=markdown),
        model=get_claude_appraisal_model(),
        timeout=1800,
    )


def appraise_combined(
    figdir: Path, files: list[Path], markdown: str
) -> tuple[str, float]:
    """Approach B: one call with markdown + figures via the Read tool."""
    filelist = "\n".join(f"  - {p.name}" for p in files)
    prompt = _COMBINED_PROMPT.format(
        figdir=figdir, n=len(files), filelist=filelist, markdown=markdown
    )
    return run_claude_prompt(
        prompt,
        model=get_claude_appraisal_model(),
        read_dirs=[figdir],
        timeout=1800,
    )


def extract_embedded(doc: fitz.Document, outdir: Path, *, min_dim: int) -> list[Path]:
    """Save embedded raster images whose shorter side >= min_dim.

    Deduped by xref so an image reused across pages is written once. CMYK /
    alpha pixmaps are normalised to RGB so the PNG is readable everywhere.
    """
    saved: list[Path] = []
    seen: set[int] = set()
    for page in doc:
        for img in page.get_images(full=True):
            xref = img[0]
            if xref in seen:
                continue
            seen.add(xref)
            pix = fitz.Pixmap(doc, xref)
            if min(pix.width, pix.height) < min_dim:
                continue
            if pix.n - pix.alpha >= 4:  # CMYK / DeviceN → RGB
                pix = fitz.Pixmap(fitz.csRGB, pix)
            out = outdir / f"img_p{page.number + 1:03d}_x{xref}.png"
            pix.save(out)
            saved.append(out)
    return saved


def render_pages_with_images(
    doc: fitz.Document,
    outdir: Path,
    *,
    min_dim: int,
    zoom: float,
    min_drawings: int,
) -> list[Path]:
    """Render whole pages that look like they contain a figure.

    A page qualifies if it has a meaningful raster image OR enough vector
    drawing ops to be a figure. This captures vector figures (forest plots,
    KM curves, axis labels) that `extract_embedded` misses, at the cost of
    also capturing surrounding text on the page.
    """
    saved: list[Path] = []
    mat = fitz.Matrix(zoom, zoom)
    for page in doc:
        has_big_raster = any(
            min((pix := fitz.Pixmap(doc, img[0])).width, pix.height) >= min_dim
            for img in page.get_images(full=True)
        )
        is_figure_page = has_big_raster or len(page.get_drawings()) >= min_drawings
        if not is_figure_page:
            continue
        out = outdir / f"page_{page.number + 1:03d}.png"
        page.get_pixmap(matrix=mat).save(out)
        saved.append(out)
    return saved


def appraise_figures(figdir: Path, files: list[Path]) -> tuple[str, float]:
    """Hand the extracted figures to claude (Read tool) for appraisal."""
    filelist = "\n".join(f"  - {p.name}" for p in files)
    prompt = APPRAISAL_PROMPT.format(
        figdir=figdir, n=len(files), filelist=filelist
    )
    return run_claude_prompt(
        prompt,
        model=get_claude_appraisal_model(),
        read_dirs=[figdir],
        timeout=1800,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", type=Path, help="path to the journal PDF")
    parser.add_argument(
        "--mode",
        choices=("embedded", "pages"),
        default="embedded",
        help="embedded: extract raster images; pages: render pages with images",
    )
    parser.add_argument("--max-images", type=int, default=DEFAULT_MAX_IMAGES)
    parser.add_argument("--min-dim", type=int, default=DEFAULT_MIN_DIM)
    parser.add_argument("--zoom", type=float, default=DEFAULT_ZOOM)
    parser.add_argument(
        "--min-drawings",
        type=int,
        default=DEFAULT_MIN_DRAWINGS,
        help="pages mode: vector-op count above which a page is a figure page",
    )
    parser.add_argument(
        "--keep", action="store_true", help="keep the extracted PNG dir"
    )
    parser.add_argument(
        "--no-appraise",
        action="store_true",
        help="only extract figures, skip the model call",
    )
    parser.add_argument(
        "--measure",
        action="store_true",
        help="approach B: run text-only then text+figures, report cost delta",
    )
    args = parser.parse_args(argv)

    if not args.pdf.is_file():
        print(f"PDF not found: {args.pdf}", file=sys.stderr)
        return 2

    figdir = Path(tempfile.mkdtemp(prefix="spike_figs_"))
    doc = fitz.open(args.pdf)
    try:
        if args.mode == "embedded":
            files = extract_embedded(doc, figdir, min_dim=args.min_dim)
        else:
            files = render_pages_with_images(
                doc,
                figdir,
                min_dim=args.min_dim,
                zoom=args.zoom,
                min_drawings=args.min_drawings,
            )
    finally:
        doc.close()

    files.sort()
    if len(files) > args.max_images:
        print(
            f"[extract] {len(files)} figures found, capping to "
            f"{args.max_images} (use --max-images to raise)"
        )
        files = files[: args.max_images]

    print(f"[extract] mode={args.mode} -> {len(files)} image(s) in {figdir}")
    for p in files:
        kb = p.stat().st_size / 1024
        print(f"  {p.name}  ({kb:.0f} KB)")

    if not files:
        print("[extract] no figures matched the size filter; nothing to appraise")
        return 1

    if args.no_appraise:
        print("[appraise] skipped (--no-appraise). PNGs left in", figdir)
        return 0

    if args.measure:
        markdown = _convert_pdf_to_markdown(args.pdf)
        print(f"\n[measure] markdown={len(markdown):,} chars, "
              f"{len(files)} figure(s), model={get_claude_appraisal_model()}")
        try:
            print("[measure] (1/2) text-only baseline ...")
            _, cost_text = appraise_text_only(markdown)
            print(f"          text-only cost = ${cost_text:.4f}")
            print("[measure] (2/2) text + figures (approach B) ...")
            combined_text, cost_combined = appraise_combined(figdir, files, markdown)
        except ClaudeError as e:
            print(f"[measure] claude path failed: {e}", file=sys.stderr)
            return 1
        delta = cost_combined - cost_text
        print(f"\n{'=' * 70}")
        print(f"[measure] text-only         ${cost_text:.4f}")
        print(f"[measure] text + figures    ${cost_combined:.4f}")
        print(f"[measure] figure increment  ${delta:+.4f}  "
              f"({delta / cost_text * 100:+.0f}% over baseline)" if cost_text
              else f"[measure] figure increment  ${delta:+.4f}")
        print(f"{'=' * 70}\n[combined appraisal]\n")
        print(combined_text)
        if args.keep:
            print(f"\n[done] PNGs kept in {figdir}")
        return 0

    print(f"\n[appraise] sending {len(files)} image(s) to "
          f"{get_claude_appraisal_model()} via Read tool ...")
    try:
        text, cost = appraise_figures(figdir, files)
    except ClaudeError as e:
        print(f"[appraise] claude path failed: {e}", file=sys.stderr)
        return 1

    print(f"\n{'=' * 70}\n[appraisal]  (cost ${cost:.4f})\n{'=' * 70}\n")
    print(text)
    if args.keep:
        print(f"\n[done] PNGs kept in {figdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
