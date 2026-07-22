"""Request a single literature appraisal from the CLI.

Two entry points, both reuse the weekly appraisal pipeline (same PDF→markdown
conversion, route classification, JAMA references, prompt cache, claude/codex
dispatch) and both publish the appraisal HTML plus a Discord link:

  # by DOI — fetch metadata, download the PDF, appraise, publish, notify
  python3 -m weekly.request_appraisal --doi 10.1016/j.jacc.2026.01.020

  # by local PDF — skip download, appraise the file you already have
  python3 -m weekly.request_appraisal --pdf ~/Downloads/paper.pdf
  python3 -m weekly.request_appraisal --pdf ~/Downloads/paper.pdf --doi 10.1016/...

Flags:
  --week        label used for the appraisals/<week>/ folder + back-nav
                (default: current ISO week)
  --title       override the title when no DOI/PubMed metadata is available
  --no-push     skip git commit/push of docs/
  --no-discord  skip the Discord completion notice
  --force       re-run even if an appraisal report already exists on disk
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

# Distinct exit codes so a wrapper can tell a claude usage-limit (worth a
# deferred retry) apart from a hard failure.
EXIT_CLAUDE_LIMIT = 10
EXIT_CLAUDE_ERROR = 11

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
ADHOC_DIR = ROOT / "output" / "requests"


def _normalize_doi(value: str) -> str:
    doi = str(value or "").strip()
    doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi, flags=re.I)
    return doi.rstrip(".")


def _local_id_from_text(text: str) -> str:
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
    return f"req-{digest}"


def _crossref_metadata(doi: str) -> dict | None:
    """Minimal Crossref fallback when PubMed has no record for the DOI."""
    try:
        resp = requests.get(
            f"https://api.crossref.org/works/{doi}",
            headers={"User-Agent": "JournalFetcher (mailto:researcher@example.com)"},
            timeout=30,
        )
        resp.raise_for_status()
        item = resp.json().get("message", {})
    except (requests.RequestException, ValueError):
        return None
    if not item:
        return None

    title_list = item.get("title") or []
    title = title_list[0] if title_list else ""
    journal_list = item.get("container-title") or []
    journal = journal_list[0] if journal_list else ""
    authors = []
    for person in item.get("author", []) or []:
        family = person.get("family", "")
        given = person.get("given", "")
        name = f"{family} {given}".strip()
        if name:
            authors.append(name)
    year = ""
    for key in ("published-print", "published-online", "published", "issued"):
        parts = (item.get(key) or {}).get("date-parts") or []
        if parts and parts[0]:
            year = str(parts[0][0])
            break
    return {
        "pmid": "",
        "title": title,
        "abstract": item.get("abstract", ""),
        "doi": doi,
        "journal": journal,
        "authors": authors,
        "year": year,
        "pub_types": item.get("type", ""),
        "pub_type": "",
    }


def _week_article_by_doi(week: str, doi: str) -> dict | None:
    """Return the week's articles.json record matching this DOI, if any.

    Used by --update-weekly so the appraised card reuses the real pmid /
    journal_key / summary already on the weekly page (dedup + enrichment in
    render_weekly key on pmid), instead of a freshly resolved dict.
    """
    path = ROOT / "output" / "weekly" / week / "articles.json"
    if not doi or not path.exists():
        return None
    try:
        articles = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    target = _normalize_doi(doi).lower()
    for article in articles if isinstance(articles, list) else []:
        if _normalize_doi(str(article.get("doi", ""))).lower() == target:
            return article
    return None


def _resolve_metadata(doi: str, title_override: str) -> dict:
    """Build the article dict from a DOI (PubMed → Crossref → minimal)."""
    from modules import pubmed

    article: dict | None = None
    if doi:
        article = pubmed.fetch_article_by_doi(doi)
        if article:
            print(f"[meta] PubMed: {article.get('title', '')[:80]}")
        else:
            article = _crossref_metadata(doi)
            if article:
                print(f"[meta] Crossref: {article.get('title', '')[:80]}")

    if article is None:
        article = {
            "pmid": "",
            "title": title_override or "",
            "doi": doi,
            "journal": "",
            "authors": [],
            "year": "",
            "pub_type": "",
        }
        print("[meta] no PubMed/Crossref record; using minimal metadata")

    if title_override:
        article["title"] = title_override
    article["doi"] = doi or article.get("doi", "")
    return article


def _finalize_article_keys(article: dict, seed: str) -> None:
    """Ensure a string `pmid` key for pipeline dicts; keep the real one as
    `original_pmid` (used in the appraisal user prompt)."""
    real_pmid = str(article.get("pmid", "")).strip()
    article["original_pmid"] = real_pmid
    article["pmid"] = real_pmid or _local_id_from_text(seed)
    article["selected_for_appraisal"] = True
    article.setdefault("selection_reason", "由 CLI 手動請求文獻評讀。")
    article.setdefault("selection_tags", ["cli_request"])


def _get_pdf(article: dict, pdf_arg: str | None) -> Path | None:
    if pdf_arg:
        pdf_path = Path(pdf_arg).expanduser()
        if not pdf_path.is_file():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")
        print(f"[pdf] using local file: {pdf_path}")
        return pdf_path

    from modules.downloader import download_articles

    print(f"[pdf] downloading via DOI {article.get('doi', '')}...")
    results = download_articles([article], out_dir=ADHOC_DIR / "pdfs")
    return results.get(str(article.get("pmid", "")))


def _update_weekly_page(week: str, article: dict) -> None:
    """Fold the finished appraisal into docs/<week>.html's 已評讀 section.

    Delegates to the on-demand worker's shared steps so the manual CLI path and
    the button-driven path produce identical weekly markup and state.
    """
    from weekly import process_appraisal_requests as par

    week_dir = ROOT / "output" / "weekly" / week
    par.update_weekly_html_for_article(week, article)
    par.persist_selected_article(week_dir, article)
    print(f"[weekly] updated docs/{week}.html (已評讀)")


def run(
    *,
    doi: str,
    pdf: str | None,
    week: str,
    title: str,
    no_push: bool,
    no_discord: bool,
    force: bool,
    update_weekly: bool,
    backend: str = "claude",
) -> int:
    load_dotenv(ROOT / ".env")
    from modules import claude_exec
    from weekly import appraise_selected, publish, render
    from weekly.appraise_selected import ArticleTooLargeError

    # Backend selection (default claude). `claude` runs claude-only Opus with no
    # silent codex fallback — a usage limit surfaces as EXIT_CLAUDE_LIMIT instead
    # of a quietly codex-authored appraisal (the W26 surprise). `codex` marks the
    # dispatcher exhausted up front so both classify and appraisal skip claude
    # and go straight to codex.
    if backend == "codex":
        claude_exec.mark_claude_exhausted("--backend codex requested")
    else:
        os.environ["JOURNAL_FETCHER_CLAUDE_ONLY"] = "1"

    doi = _normalize_doi(doi)
    if not doi and not pdf:
        print("[error] need --doi or --pdf", file=sys.stderr)
        return 2

    # When integrating into the weekly page, prefer that week's existing record
    # so pmid/journal_key/summary line up with the rest of the page.
    article = None
    if update_weekly and doi:
        article = _week_article_by_doi(week, doi)
        if article is not None:
            article = {**article}
            print(f"[meta] {week} articles.json: {article.get('title', '')[:80]}")
    if article is None:
        article = _resolve_metadata(doi, title)

    seed = doi or (pdf or "") or (article.get("title", "") or "request")
    _finalize_article_keys(article, seed)

    if update_weekly:
        tags = article.get("selection_tags") or []
        if "manual_request" not in tags:
            article["selection_tags"] = [*tags, "manual_request"]

    if not article.get("title"):
        print("[error] no title resolved; pass --title for a PDF-only request", file=sys.stderr)
        return 2

    pdf_path = _get_pdf(article, pdf)
    if pdf_path is None:
        print(f"[error] PDF download failed for {doi}", file=sys.stderr)
        return 1

    appraisal_dir = ADHOC_DIR / "appraisals"
    report_path: Path | None
    try:
        if force:
            stale = appraisal_dir / appraise_selected._safe_report_name(article)
            if stale.exists():
                stale.unlink()
        report_path = appraise_selected.appraise_pdf(article, pdf_path, appraisal_dir)
    except ArticleTooLargeError as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1
    except claude_exec.ClaudeLimitError as e:
        # claude-only mode: usage limit hit, no codex fallback produced.
        print(f"[claude-limit] {str(e)[:160]}", file=sys.stderr)
        return EXIT_CLAUDE_LIMIT
    except claude_exec.ClaudeError as e:
        print(f"[claude-error] {str(e)[:160]}", file=sys.stderr)
        return EXIT_CLAUDE_ERROR
    if not report_path:
        print("[error] appraisal failed (no report produced)", file=sys.stderr)
        return 1
    article["appraisal_path"] = str(report_path)

    published = render.publish_appraisals([article], week)
    appraisal_url = str(article.get("appraisal_url", ""))
    if not published or not appraisal_url:
        print("[error] appraisal HTML not produced", file=sys.stderr)
        return 1
    print(f"[render] {published[0]}")

    if update_weekly:
        _update_weekly_page(week, article)

    from weekly import process_appraisal_requests as par

    identifier = f"doi:{doi}" if doi else f"pmid:{article['pmid']}"
    par.push_and_notify(
        week, article, appraisal_url,
        identifier=identifier, no_push=no_push, no_discord=no_discord,
    )

    full_url = appraisal_url
    if not full_url.startswith(("http://", "https://")):
        full_url = publish.site_url(appraisal_url)
    print(f"[done] {full_url}")
    return 0


def main() -> int:
    from weekly import render

    parser = argparse.ArgumentParser(description="Request a single literature appraisal from the CLI.")
    parser.add_argument("--doi", default="", help="Article DOI (with or without https://doi.org/ prefix)")
    parser.add_argument("--pdf", default=None, help="Path to a local PDF to appraise (skips download)")
    parser.add_argument("--week", default="", help="Label for appraisals/<week>/ folder (default: current ISO week)")
    parser.add_argument("--title", default="", help="Title override when no DOI/PubMed metadata is available")
    parser.add_argument("--no-push", action="store_true", help="Skip git commit/push of docs/")
    parser.add_argument("--no-discord", action="store_true", help="Skip Discord completion notice")
    parser.add_argument("--force", action="store_true", help="Re-run even if an appraisal report already exists")
    parser.add_argument("--update-weekly", action="store_true", help="Fold the appraisal into docs/<week>.html 已評讀 section")
    parser.add_argument(
        "--backend",
        choices=["claude", "codex"],
        default="claude",
        help=(
            "Which backend to use (default: claude). "
            "claude = claude-only Opus, no codex fallback "
            f"(exit {EXIT_CLAUDE_LIMIT} on usage limit, {EXIT_CLAUDE_ERROR} on other claude error); "
            "codex = go straight to codex, skip claude."
        ),
    )
    parser.add_argument(
        "--claude-only",
        action="store_true",
        help="Deprecated alias for --backend claude (kept for existing scripts).",
    )
    args = parser.parse_args()

    week = args.week.strip() or render.iso_week_label()[0]
    # --claude-only is a legacy alias; --backend wins if both somehow appear.
    backend = "claude" if args.claude_only else args.backend

    try:
        return run(
            doi=args.doi,
            pdf=args.pdf,
            week=week,
            title=args.title.strip(),
            no_push=args.no_push,
            no_discord=args.no_discord,
            force=args.force,
            update_weekly=args.update_weekly,
            backend=backend,
        )
    except Exception as e:  # noqa: BLE001 - standalone CLI should report a clear failure
        print(f"[error] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
