"""Fetch, classify, measure and package an article's supplementary files.

Every appraised article gets its supplements downloaded, classified
(appendix / protocol / data_sharing / disclosure / collaborators / unknown)
and sized; one line per article goes to SUPPLEMENT_LOG. `build_supplement_
context` then turns them into appraisal input:

- appendix → converted to markdown and inlined in the user prompt, within
  SUPPLEMENT_INLINE_CHAR_CAP and the article backstop; its labelled eFigure
  pages and unparsed eTable pages are rendered under their own image budget.
- protocol / SAP / unknown, and any appendix over the cap → page-marked text
  file the model reads on demand (claude Read, codex rg/sed).
- disclosure / data_sharing / collaborators → dropped.

Files are kept under output/supplements/<key>/ with a manifest.json, so a
re-run of the same appraisal reuses them without hitting the publisher.

    python3 -m weekly.supplements --doi 10.1056/NEJMoa2608397
    python3 -m weekly.supplements --doi <DOI> --pdf output/pdfs/x.pdf --force
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import fitz  # PyMuPDF

from modules.downloader import _get, _is_pdf, classify_supplement
from modules.ego_browser import fetch_supplements_via_ego
from weekly.figure_extract import (
    _SUPP_FIGURE_CAPTION_RE,
    _SUPP_TABLE_CAPTION_RE,
    count_figure_pages,
    extract_supplement_pages,
)

SUPPLEMENT_DIR = Path("output/supplements")
SUPPLEMENT_LOG = Path("output/logs/supplements.jsonl")

# Inline budget for appendix markdown. First two measured appendices were
# 61k (JAMA) and 76k (NEJM) chars, about the size of the article itself, so
# 400k (~100k tokens) leaves room for SR/NMA appendices with long search
# strategies and RoB tables. Over the cap the appendix is offered for Read
# instead — never truncated, for the same reason as ARTICLE_CHAR_BACKSTOP.
SUPPLEMENT_INLINE_CHAR_CAP = int(
    os.getenv("JOURNAL_FETCHER_SUPPLEMENT_INLINE_CHARS", "400000")
)
# Image budget for supplement pages, separate from the article's own figure
# budget so a figure-heavy appendix cannot crowd out the main figures.
SUPPLEMENT_MAX_IMAGE_PAGES = int(
    os.getenv("JOURNAL_FETCHER_SUPPLEMENT_MAX_IMAGES", "8")
)
_DROPPED_KINDS = frozenset({"disclosure", "data_sharing", "collaborators"})
# Guideline supplements are evidence tables running to hundreds of pages;
# offer them for lookup rather than inlining.
_READ_ONLY_APPENDIX_ROUTES = frozenset({"cpg", "consensus"})

_KIND_LABELS = {
    "appendix": "Supplementary Appendix",
    "protocol": "Protocol / Statistical Analysis Plan",
    "unknown": "其他 supplement",
}

SUPPLEMENT_CONTEXT = """\
本文 supplement（程式已從出版社頁面下載並分類；disclosure、data sharing、
研究者名單已排除）：

{sections}

使用規則：
- Supplementary appendix 是評讀證據的一部分：subgroup、sensitivity、
  per-protocol / as-treated、missing data 處理、eTable 的 baseline 與次要結果，
  與正文結論相關時要納入，並標注出處（例如 [Supplement eTable 3]）。
- 正文與 supplement 數字不一致時要明說。
- 標為「按需讀取」的檔案不在 prompt 內，請用 Read 工具（或 shell 的 rg / sed）
  讀取相關段落，不要逐頁讀完。檔內以「=== page N ===」標頁碼。
- 有 Protocol / SAP 時，至少查核：事前登錄的 primary outcome 與正文是否一致
  （outcome switching）、樣本數計算假設、事前指定的分析族群與 subgroup，
  以及正文分析是否偏離 SAP；引用時標注 [Protocol p.N] 或 [SAP p.N]。
- 只根據實際讀到的內容評讀；某檔讀不到就明說，不要臆測。
"""


@dataclass(frozen=True)
class SupplementContext:
    """What one article's supplements contribute to the appraisal call."""

    prompt_block: str
    read_dir: Path | None
    image_paths: list[Path]
    inlined: list[str]
    read_only: list[str]

_ZIP_MAGIC = b"PK\x03\x04"

# Publisher protocol bundles are often stitched PDFs with dangling xrefs;
# MuPDF still reads them but prints one stderr line per broken object
# (thousands for an 800-page bundle), which buries the launchd logs.
fitz.TOOLS.mupdf_display_errors(False)


@dataclass
class SupplementFile:
    url: str
    link_text: str
    kind: str
    format: str
    source: str | None
    bytes: int | None
    pages: int | None
    text_chars: int | None
    figure_pages: int | None
    figure_labels: int | None
    table_labels: int | None
    path: str | None
    error: str | None


def _env_flag(name: str) -> bool:
    raw = os.getenv(name, "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def supplement_logging_enabled() -> bool:
    """Read JOURNAL_FETCHER_SUPPLEMENT_LOG; default on (fetch + log)."""
    return _env_flag("JOURNAL_FETCHER_SUPPLEMENT_LOG")


def supplement_appraisal_enabled() -> bool:
    """Read JOURNAL_FETCHER_APPRAISAL_SUPPLEMENT; default on (feed the model)."""
    return _env_flag("JOURNAL_FETCHER_APPRAISAL_SUPPLEMENT")


def _article_key(article: dict) -> str:
    raw = str(article.get("pmid") or article.get("doi") or "unknown")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", raw)


def _sniff_format(content: bytes, url: str) -> str:
    if _is_pdf(content):
        return "pdf"
    ext = Path(urlparse(url).path).suffix.lower().lstrip(".")
    if content.startswith(_ZIP_MAGIC):
        # docx / xlsx / pptx are zip containers too; trust the URL extension.
        return ext if ext in ("docx", "xlsx", "pptx") else "zip"
    if content.lstrip()[:1] == b"<":
        return "html"
    return ext or "bin"


def _pdf_metrics(path: Path) -> dict[str, int]:
    with fitz.open(path) as doc:
        text = "\n".join(page.get_text() for page in doc)
        pages = doc.page_count
    return {
        "pages": pages,
        "text_chars": len(text),
        "figure_pages": count_figure_pages(path),
        "figure_labels": len({m.lower() for m in _SUPP_FIGURE_CAPTION_RE.findall(text)}),
        "table_labels": len({m.lower() for m in _SUPP_TABLE_CAPTION_RE.findall(text)}),
    }


def _fetch_over_http(url: str) -> tuple[bytes | None, str | None]:
    """Retry a link ego could not read in-page (CDN without CORS headers)."""
    try:
        resp = _get(url, allow_redirects=True)
    except Exception as e:
        return None, f"http_error: {e}"
    if resp.status_code != 200:
        return None, f"http_{resp.status_code}"
    return resp.content, None


def _materialize(index: int, link, out_dir: Path) -> SupplementFile:
    """Turn one scanned link into a classified, measured file on disk."""
    content: bytes | None = None
    source: str | None = None
    error = link.error
    if link.path is not None and link.path.exists():
        content = link.path.read_bytes()
        source = f"ego_{link.via}"
    else:
        content, http_error = _fetch_over_http(link.url)
        if content is not None:
            source, error = "http", None
        else:
            error = f"{error}; {http_error}" if error else http_error

    if content is None:
        return SupplementFile(
            url=link.url, link_text=link.text, kind=classify_supplement(None, link.url),
            format="missing", source=None, bytes=None, pages=None, text_chars=None,
            figure_pages=None, figure_labels=None, table_labels=None, path=None,
            error=error,
        )

    fmt = _sniff_format(content, link.url)
    kind = classify_supplement(content, link.url)
    dest = out_dir / f"supp_{index:02d}_{kind}.{fmt}"
    dest.write_bytes(content)
    if link.path is not None and link.path != dest:
        link.path.unlink(missing_ok=True)

    metrics: dict[str, int | None] = dict.fromkeys(
        ("pages", "text_chars", "figure_pages", "figure_labels", "table_labels")
    )
    if fmt == "pdf":
        try:
            metrics.update(_pdf_metrics(dest))
        except Exception as e:
            error = f"measure_failed: {e}"
    elif fmt == "html":
        # A challenge / login page came back instead of the file.
        error = error or "got_html"

    return SupplementFile(
        url=link.url, link_text=link.text, kind=kind, format=fmt, source=source,
        bytes=len(content), path=str(dest), error=error, **metrics,
    )


def _main_metrics(pdf_path: Path | None, markdown_chars: int | None) -> dict:
    if pdf_path is None or not pdf_path.is_file():
        return {"markdown_chars": markdown_chars}
    try:
        metrics = _pdf_metrics(pdf_path)
    except Exception as e:
        return {"markdown_chars": markdown_chars, "error": f"measure_failed: {e}"}
    return {
        "pages": metrics["pages"],
        "text_chars": metrics["text_chars"],
        "markdown_chars": markdown_chars,
        "figure_pages": metrics["figure_pages"],
    }


def _summarize_by_kind(files: list[SupplementFile]) -> dict[str, dict[str, int]]:
    totals: dict[str, dict[str, int]] = {}
    for f in files:
        t = totals.setdefault(f.kind, {"files": 0, "pages": 0, "text_chars": 0, "figure_pages": 0})
        t["files"] += 1
        t["pages"] += f.pages or 0
        t["text_chars"] += f.text_chars or 0
        t["figure_pages"] += f.figure_pages or 0
    return totals


def record_supplements(
    article: dict,
    *,
    main_pdf: Path | None,
    route: str | None,
    main_markdown_chars: int | None,
) -> dict | None:
    """Fetch + classify + measure supplements for one article and log them.

    Returns the record — freshly logged, or the cached manifest when this
    article was already fetched — or None when the article has no DOI. A
    failed ego run, or one that left any file unfetched, is logged but writes
    no manifest, so the next appraisal of the same article tries again.
    """
    doi = article.get("doi")
    if not doi:
        return None
    out_dir = SUPPLEMENT_DIR / _article_key(article)
    manifest = out_dir / "manifest.json"
    if manifest.exists():
        print(f"  [supplements] reusing {manifest}")
        return json.loads(manifest.read_text(encoding="utf-8"))

    started = time.monotonic()
    scan = fetch_supplements_via_ego(f"https://doi.org/{doi}", out_dir)
    files = (
        [_materialize(i, link, out_dir) for i, link in enumerate(scan.links, 1)]
        if scan is not None
        else []
    )
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pmid": article.get("pmid"),
        "doi": doi,
        "journal": article.get("journal_key") or article.get("journal"),
        "route": route,
        "status": "scan_failed" if scan is None else ("ok" if files else "no_links"),
        "landing_url": scan.landing_url if scan else None,
        "landing_title": scan.landing_title if scan else None,
        "elapsed_s": round(time.monotonic() - started, 1),
        "main": _main_metrics(main_pdf, main_markdown_chars),
        "by_kind": _summarize_by_kind(files),
        "files": [asdict(f) for f in files],
    }

    SUPPLEMENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with SUPPLEMENT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    # Only a complete fetch is cached; any missing file means the next
    # appraisal of this article tries the publisher again.
    if scan is not None and all(f.format != "missing" for f in files):
        manifest.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    kinds = ", ".join(
        f"{k}={v['files']}f/{v['pages']}p" for k, v in record["by_kind"].items()
    ) or "none"
    print(f"  [supplements] {record['status']} ({record['elapsed_s']}s): {kinds}")
    return record


def _page_marked_text(pdf_path: Path) -> Path:
    """Write (once) a plain-text copy with page markers for on-demand reads.

    Plain fitz text rather than pymupdf4llm: protocol bundles run to hundreds
    of pages, and the model only greps them for specific sections.
    """
    out = pdf_path.with_suffix(".txt")
    if not out.exists():
        with fitz.open(pdf_path) as doc:
            parts = [f"=== page {page.number + 1} ===\n{page.get_text()}" for page in doc]
        out.write_text("\n".join(parts), encoding="utf-8")
    return out


def _describe(f: dict) -> str:
    label = _KIND_LABELS.get(f["kind"], f["kind"])
    pages = f"，{f['pages']} 頁" if f.get("pages") else ""
    return f"{label}（{Path(f['path']).name}{pages}）"


def build_supplement_context(
    record: dict,
    *,
    route: str | None,
    budget_chars: int,
    convert: Callable[[Path], tuple[str, frozenset[int]]],
    image_dir: Path | None,
) -> SupplementContext | None:
    """Package a supplement record as prompt text, a Read dir and images.

    `budget_chars` is what the article backstop leaves after the main text;
    the inline cap is the smaller of it and SUPPLEMENT_INLINE_CHAR_CAP.
    `convert` is the appraisal's own PDF→markdown pass (reference stripping,
    table-survival detection), passed in to avoid a circular import.
    Supplement page images are rendered into `image_dir` (the appraisal's
    figure temp dir); None skips them. Returns None when nothing usable
    remains after dropping disclosure / data-sharing / roster files.
    """
    remaining = min(SUPPLEMENT_INLINE_CHAR_CAP, budget_chars)
    sections: list[str] = []
    read_only: list[str] = []
    inlined: list[str] = []
    images: list[Path] = []
    read_dir: Path | None = None

    for f in record.get("files", []):
        if f.get("format") != "pdf" or not f.get("path") or f["kind"] in _DROPPED_KINDS:
            continue
        path = Path(f["path"]).resolve()
        if not path.exists():
            continue

        if f["kind"] == "appendix" and route not in _READ_ONLY_APPENDIX_ROUTES:
            markdown, parsed_tables = convert(path)
            if len(markdown) <= remaining:
                remaining -= len(markdown)
                inlined.append(path.name)
                sections.append(f"### {_describe(f)}\n\n{markdown}")
                if image_dir is not None:
                    images += extract_supplement_pages(
                        path,
                        image_dir,
                        prefix=path.stem,
                        max_pages=SUPPLEMENT_MAX_IMAGE_PAGES - len(images),
                        parsed_table_pages=parsed_tables,
                    )
                continue
            print(
                f"  [supplements] {path.name}: {len(markdown):,} chars over the "
                f"{remaining:,}-char inline budget; offering for Read instead"
            )

        text_path = _page_marked_text(path)
        read_dir = text_path.parent
        read_only.append(text_path.name)
        sections.append(f"### {_describe(f)}——按需讀取\n\n檔案：{text_path}")

    if not sections:
        return None
    if images:
        listing = "\n".join(f"- {p.name}" for p in images)
        sections.append(
            f"### Supplement 圖頁（{len(images)} 張 PNG，位於 {image_dir}）\n\n"
            f"{listing}\n\n這些是 supplement 的 eFigure，以及未能轉成文字表格的 eTable "
            f"頁面。請用 Read 讀取並判讀，與上方 appendix 文字對照；看不清楚就明說。"
        )
    return SupplementContext(
        prompt_block=SUPPLEMENT_CONTEXT.format(sections="\n\n".join(sections)),
        read_dir=read_dir,
        image_paths=images,
        inlined=inlined,
        read_only=read_only,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and log an article's supplements.")
    parser.add_argument("--doi", required=True)
    parser.add_argument("--pmid", help="Storage key; defaults to the DOI")
    parser.add_argument("--pdf", type=Path, help="Main article PDF, for size comparison")
    parser.add_argument("--force", action="store_true", help="Discard any earlier fetch first")
    args = parser.parse_args()

    article = {"doi": args.doi, "pmid": args.pmid}
    if args.force:
        shutil.rmtree(SUPPLEMENT_DIR / _article_key(article), ignore_errors=True)
    record = record_supplements(
        article, main_pdf=args.pdf, route=None, main_markdown_chars=None
    )
    if record is not None:
        print(json.dumps(record, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
