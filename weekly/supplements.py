"""Fetch, classify and measure an article's supplementary files (record-only).

This is the measurement phase for feeding supplements into appraisal: every
appraised article gets its supplements downloaded, classified (appendix /
protocol / data_sharing / disclosure / unknown) and sized, and one line is
appended to SUPPLEMENT_LOG. Nothing here is sent to the model yet — the logged
page / char / figure-page counts are what the inline caps will be set from.

Files are kept under output/supplements/<key>/ with a manifest.json, so a later
phase can reuse them without hitting the publisher again, and a re-run of the
same appraisal skips the fetch.

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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import fitz  # PyMuPDF

from modules.downloader import _get, _is_pdf, classify_supplement
from modules.ego_browser import fetch_supplements_via_ego
from weekly.figure_extract import count_figure_pages

SUPPLEMENT_DIR = Path("output/supplements")
SUPPLEMENT_LOG = Path("output/logs/supplements.jsonl")

# Unique supplement figure / table labels: "eFigure 3", "Figure S3",
# "Supplementary Table 2", "eTable 12". Counted as distinct labels, since a
# label is repeated in the table of contents and in running text.
_SUPP_FIGURE_RE = re.compile(
    r"^\s*((?:e|Supplementa(?:l|ry)\s+)?(?:Figure|Fig\.?)\s*S?\d+)",
    re.IGNORECASE | re.MULTILINE,
)
_SUPP_TABLE_RE = re.compile(
    r"^\s*((?:e|Supplementa(?:l|ry)\s+)?Table\s*S?\d+)",
    re.IGNORECASE | re.MULTILINE,
)

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


def supplement_logging_enabled() -> bool:
    """Read JOURNAL_FETCHER_SUPPLEMENT_LOG; default on (record-only, no model input)."""
    raw = os.getenv("JOURNAL_FETCHER_SUPPLEMENT_LOG", "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


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
        "figure_labels": len({m.lower() for m in _SUPP_FIGURE_RE.findall(text)}),
        "table_labels": len({m.lower() for m in _SUPP_TABLE_RE.findall(text)}),
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

    Returns the logged record, or None when the article has no DOI or was
    already recorded (manifest present). A failed ego run, or one that left
    any file unfetched, is logged but writes no manifest, so the next
    appraisal of the same article tries again.
    """
    doi = article.get("doi")
    if not doi:
        return None
    out_dir = SUPPLEMENT_DIR / _article_key(article)
    manifest = out_dir / "manifest.json"
    if manifest.exists():
        print(f"  [supplements] already recorded: {manifest}")
        return None

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
