"""Process on-demand appraisal requests from the weekly HTML.

The public GitHub Pages HTML can only write a request to the Apps Script relay.
This local worker pulls those requests with the private sync token, downloads
the PDF, runs the full appraisal, publishes the appraisal HTML, and sends a
Discord link.

Run manually:
  python3 -m weekly.process_appraisal_requests

Recommended launchd schedule:
  run every 10-15 minutes while the Mac is awake.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

from weekly.appraisal_status import RequestStatus, TERMINAL_APPRAISAL_FAILURES

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
STATE_PATH = DATA_DIR / "appraisal_requests_processed.jsonl"
WEEKLY_DIR = ROOT / "output" / "weekly"
DOCS_DIR = ROOT / "docs"

# On-demand requests run claude-only on Opus too (no codex fallback). This
# 15-minute worker does not block in-process, so when it hits the usage limit
# it records a `retry_after` timestamp from Claude's reported reset time and
# skips the request until then. The 5h value is only a parsing fallback.
APPRAISAL_RETRY_WAIT_SECONDS = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_RETRY_WAIT", str(5 * 60 * 60))
)
APPRAISAL_RESET_BUFFER_SECONDS = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_RESET_BUFFER", "120")
)

# Transient appraisal errors (non-limit claude error + codex fallback also
# down, markdown conversion hiccup, etc.) leave no report but are often fixed by
# a re-run. Defer them for a short window instead of marking terminal `failed`,
# capped so a permanently broken article eventually gives up rather than
# deferring forever every 15 minutes. PDF-download / too-large failures stay
# terminal — a re-run won't fix a missing PDF or an oversized article.
APPRAISAL_ERROR_RETRY_WAIT_SECONDS = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_ERROR_RETRY_WAIT", str(30 * 60))
)
APPRAISAL_ERROR_RETRY_MAX = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_ERROR_RETRY_MAX", "3")
)
# appraise_selected statuses that a re-run cannot recover from live in
# weekly.appraisal_status.TERMINAL_APPRAISAL_FAILURES (imported above).


def _normalize_doi(value: str) -> str:
    doi = str(value or "").strip()
    doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi, flags=re.I)
    return doi.rstrip(".").lower()


def _request_identifier(row: dict) -> str:
    pmid = str(row.get("pmid", "")).strip()
    if pmid:
        return f"pmid:{pmid}"
    doi = _normalize_doi(str(row.get("doi", "")))
    if doi:
        return f"doi:{doi}"
    return ""


def _candidate_identifiers(row: dict) -> list[str]:
    identifiers: list[str] = []
    pmid = str(row.get("pmid", "")).strip()
    doi = _normalize_doi(str(row.get("doi", "")))
    if pmid:
        identifiers.append(f"pmid:{pmid}")
    if doi:
        identifiers.append(f"doi:{doi}")
    return identifiers


def _article_identifiers(article: dict) -> list[str]:
    identifiers: list[str] = []
    pmid = str(article.get("pmid", "")).strip()
    doi = _normalize_doi(str(article.get("doi", "")))
    if pmid:
        identifiers.append(f"pmid:{pmid}")
    if doi:
        identifiers.append(f"doi:{doi}")
    return identifiers


def _local_id_from_doi(doi: str) -> str:
    digest = hashlib.sha1(_normalize_doi(doi).encode("utf-8")).hexdigest()[:12]
    return f"doi-{digest}"


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _merge_article_record(base: dict, update: dict) -> dict:
    """Merge appraisal metadata without dropping existing summary fields."""
    merged = {**base, **update}
    for key in ("summary", "commentary", "abstract"):
        if not update.get(key) and base.get(key):
            merged[key] = base[key]
    return merged


def _load_state() -> dict[tuple[str, str], dict]:
    records: dict[tuple[str, str], dict] = {}
    if not STATE_PATH.exists():
        return records
    for line in STATE_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        identifiers = _candidate_identifiers(obj)
        explicit_identifier = str(obj.get("identifier", "")).strip()
        if explicit_identifier:
            identifiers.insert(0, explicit_identifier)
        for identifier in dict.fromkeys(identifiers):
            key = (str(obj.get("week", "")), identifier)
            if key[0] and key[1]:
                records[key] = obj
    return records


def _append_state(record: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _state_blocks(record: dict) -> bool:
    """Whether an existing state record should keep us from (re)processing.

    Terminal records (done / failed) always block. A `deferred` record only
    blocks until its `retry_after` epoch passes — after that the request is
    retried (the claude usage-limit window is assumed reset).
    """
    if str(record.get("status", "")).strip() == RequestStatus.DEFERRED:
        try:
            return float(record.get("retry_after", 0)) > time.time()
        except (TypeError, ValueError):
            return False
    return True


def _sync_requests(timeout: int = 30) -> list[dict]:
    load_dotenv(ROOT / ".env")
    url = os.getenv("FEEDBACK_ENDPOINT_URL", "").strip()
    token = os.getenv("FEEDBACK_SYNC_TOKEN", "").strip()
    if not url:
        print("[appraisal-request] FEEDBACK_ENDPOINT_URL not set; skip")
        return []
    if not token:
        raise RuntimeError("FEEDBACK_SYNC_TOKEN is not set")

    resp = requests.post(
        url,
        json={"action": "sync_appraisal_requests", "token": token},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("ok"):
        error = str(payload.get("error", ""))
        if "bad week" in error or "bad verdict" in error:
            print(
                "[appraisal-request] relay does not support appraisal requests yet; "
                "redeploy scripts/feedback_relay.gs"
            )
            return []
        raise RuntimeError(f"relay rejected request: {error}")
    return list(payload.get("rows", []))


def _known_articles() -> dict[tuple[str, str], dict]:
    """Known weekly articles keyed by (week, pmid/doi identifier)."""
    known: dict[tuple[str, str], dict] = {}
    if not WEEKLY_DIR.exists():
        weekly_paths: list[Path] = []
    else:
        weekly_paths = list(WEEKLY_DIR.glob("*/articles.json"))

    def add_article(week: str, article: dict) -> None:
        for identifier in _article_identifiers(article):
            known.setdefault((week, identifier), article)

    for article_path in weekly_paths:
        week = article_path.parent.name
        articles = _load_json(article_path, [])
        if isinstance(articles, list):
            for article in articles:
                add_article(week, article)

    for html_path in DOCS_DIR.glob("20*-W*.html"):
        week = html_path.stem
        for article in _articles_from_weekly_html(html_path):
            add_article(week, article)
    return known


def _articles_from_weekly_html(path: Path) -> list[dict]:
    """Recover article metadata from an already-published weekly HTML page."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(path.read_text(encoding="utf-8"), "html.parser")
    articles: list[dict] = []
    for node in soup.find_all("article"):
        title_node = node.find("h2")
        title = title_node.get_text(" ", strip=True) if title_node else ""
        doi = ""
        pmid = ""
        for link in node.find_all("a"):
            href = link.get("href", "")
            if href.startswith("https://doi.org/"):
                doi = href.replace("https://doi.org/", "", 1)
            elif "pubmed.ncbi.nlm.nih.gov" in href:
                match = re.search(r"/(\d{6,12})/?$", href)
                if match:
                    pmid = match.group(1)
            elif "view=appraise" in href:
                qs = parse_qs(urlparse(href).query)
                pmid = pmid or (qs.get("pmid", [""])[0])
                doi = doi or (qs.get("doi", [""])[0])

        if not doi:
            continue

        tag = node.find("span", class_="tag")
        journal_key = tag.get_text(strip=True) if tag else ""
        authors_node = node.find("div", class_="authors")
        authors_text = authors_node.get_text(" ", strip=True) if authors_node else ""
        year_match = re.search(r"\b(20\d{2})\b", authors_text)
        year = year_match.group(1) if year_match else ""
        author_part = authors_text.split("·", 1)[0].strip()
        authors = [a.strip() for a in author_part.split(",") if a.strip()]
        articles.append(
            {
                "pmid": pmid,
                "doi": doi,
                "journal_key": journal_key,
                "journal": journal_key,
                "title": title,
                "authors": authors,
                "year": year,
                "source_html": str(path),
            }
        )
    return articles


def _journal_counts(articles: list[dict]) -> list[dict]:
    counts: dict[str, int] = {}
    order: list[str] = []
    for article in articles:
        key = str(article.get("journal_key") or article.get("journal") or "").strip()
        if not key:
            key = "Unknown"
        if key not in counts:
            counts[key] = 0
            order.append(key)
        counts[key] += 1
    return [{"name": key, "count": counts[key]} for key in order]


def _merge_selected(week_dir: Path, article: dict) -> list[dict]:
    selected_path = week_dir / "selected_articles.json"
    selected = _load_json(selected_path, [])
    if not isinstance(selected, list):
        selected = []

    identifiers = set(_article_identifiers(article))
    merged: list[dict] = []
    replaced = False
    for item in selected:
        if identifiers.intersection(_article_identifiers(item)):
            merged.append(_merge_article_record(item, article))
            replaced = True
        else:
            merged.append(item)
    if not replaced:
        articles = _load_json(week_dir / "articles.json", [])
        if isinstance(articles, list):
            for weekly_article in articles:
                if identifiers.intersection(_article_identifiers(weekly_article)):
                    article = _merge_article_record(weekly_article, article)
                    break
        article.setdefault("selected_for_appraisal", True)
        article.setdefault("selection_reason", "由週報摘要區手動請求文獻評讀。")
        article.setdefault("selection_tags", ["manual_request"])
        merged.append(article)

    _write_json(selected_path, merged)
    return merged


def _prepare_article_for_pipeline(article: dict) -> dict:
    prepared = {**article}
    if not str(prepared.get("pmid", "")).strip():
        prepared["original_pmid"] = ""
        prepared["pmid"] = _local_id_from_doi(str(prepared.get("doi", "")))
    return prepared


def _render_weekly_page(week: str, selected: list[dict]) -> Path:
    from weekly import render

    load_dotenv(ROOT / ".env")
    feedback_endpoint = os.getenv("FEEDBACK_ENDPOINT_URL", "").strip()
    week_dir = WEEKLY_DIR / week
    articles = _load_json(week_dir / "articles.json", [])
    if not isinstance(articles, list) or not articles:
        raise FileNotFoundError(f"missing weekly articles: {week_dir / 'articles.json'}")

    html = render.render_weekly(
        articles,
        week_label=week,
        journal_counts=_journal_counts(articles),
        selected_articles=selected,
        feedback_endpoint=feedback_endpoint,
    )
    return render.write_weekly(html, f"{week}.html")


# ── Shared "finalize a single appraisal" steps ───────────────────────────
# These three helpers are the skeleton common to both single-article entry
# points — the on-demand relay worker (_process_one) and the manual CLI
# (weekly.request_appraisal). Each was previously copy-pasted in both files;
# centralizing them keeps the weekly-HTML update, the selected_articles.json
# persistence, and the git-push / Discord notice identical across entry points
# (see AGENTS.md: the two paths must produce the same markup and state).


def update_weekly_html_for_article(week: str, article: dict) -> None:
    """Fold one finished appraisal into docs/<week>.html.

    Full re-render when the week's articles.json exists (system-selected or a
    week we still have metadata for); otherwise patch just the appraisal link
    into the already-published HTML.
    """
    week_dir = WEEKLY_DIR / week
    if (week_dir / "articles.json").exists():
        selected = _merge_selected(week_dir, article)
        _render_weekly_page(week, selected)
    else:
        _update_existing_weekly_html_appraisal_link(week, article)


def persist_selected_article(week_dir: Path, article: dict) -> None:
    """Merge one article's post-appraisal fields into selected_articles.json.

    Persists appraisal_route / appraisal_route_source / appraisal_status /
    appraisal_path (from appraise_selected) and appraisal_url (from
    publish_appraisals) so a later re-run hits the route cache and skips the
    Haiku classify call. No-op when the week has no selected_articles.json.
    """
    selected_json = week_dir / "selected_articles.json"
    if not selected_json.exists():
        return
    existing = json.loads(selected_json.read_text(encoding="utf-8"))
    pmid = str(article.get("pmid", ""))
    persisted: list[dict] = []
    replaced = False
    for item in existing:
        if str(item.get("pmid", "")) == pmid:
            persisted.append(_merge_article_record(item, article))
            replaced = True
        else:
            persisted.append(item)
    if not replaced:
        persisted.append(article)
    selected_json.write_text(
        json.dumps(persisted, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def push_and_notify(
    week: str,
    article: dict,
    appraisal_url: str,
    *,
    identifier: str,
    no_push: bool,
    no_discord: bool,
) -> None:
    """git-commit-and-push docs/<week>.html and send the per-appraisal Discord
    notice. Shared tail of both single-article entry points."""
    from weekly import publish

    if no_push:
        print("[git] --no-push set; skip docs push")
    else:
        publish.git_commit_and_push(f"{week}.html", f"{week} appraisal {identifier}")

    if no_discord:
        print("[discord] --no-discord set; skip appraisal notice")
    else:
        publish.send_appraisal_notice(week, article, appraisal_url)


def _process_one(
    row: dict,
    article: dict,
    force: bool,
    no_push: bool,
    no_discord: bool,
    prev_error_retries: int = 0,
) -> bool:
    from modules import claude_exec
    from modules.downloader import download_articles
    from weekly import appraise_selected, render

    week = str(row.get("week", "")).strip()
    identifier = _request_identifier(row)
    week_dir = WEEKLY_DIR / week
    pdf_dir = week_dir / "pdfs"
    appraisal_dir = week_dir / "appraisals"

    article = _prepare_article_for_pipeline(article)
    article["selected_for_appraisal"] = True
    article.setdefault("selection_reason", "由週報摘要區手動請求文獻評讀。")
    article.setdefault("selection_tags", ["manual_request"])
    result_key = str(article.get("pmid", ""))

    print(f"[appraisal-request] processing {week} {identifier}: {article.get('title', '')[:80]}")
    download_results = download_articles([article], out_dir=pdf_dir)
    try:
        result = appraise_selected.appraise_selected([article], download_results, appraisal_dir)
    except claude_exec.ClaudeLimitError as e:
        wait, reset_at = claude_exec.retry_wait_seconds(
            str(e),
            fallback_seconds=APPRAISAL_RETRY_WAIT_SECONDS,
            buffer_seconds=APPRAISAL_RESET_BUFFER_SECONDS,
        )
        retry_after = time.time() + wait
        _append_state(
            {
                "week": week,
                "identifier": identifier,
                **row,
                "status": RequestStatus.DEFERRED,
                "retry_after": retry_after,
            }
        )
        print(
            f"[appraisal-request] deferred {identifier}: claude usage limit; "
            f"retry after ~{wait / 3600:.1f}h"
            f"{f' ({reset_at:%Y-%m-%d %H:%M %Z})' if reset_at else ''} "
            f"({str(e)[:120]})"
        )
        return False
    report_path = result.get(result_key)
    if not report_path:
        appraisal_status = str(article.get("appraisal_status", "")).strip()
        if appraisal_status in TERMINAL_APPRAISAL_FAILURES:
            _append_state(
                {"week": week, "identifier": identifier, **row,
                 "status": RequestStatus.FAILED, "reason": appraisal_status}
            )
            print(f"[appraisal-request] failed {identifier} ({appraisal_status})")
            return False
        # Transient error (no PDF/too-large status): retry a few times before
        # giving up, so a flaky claude/codex/conversion error self-heals.
        attempts = prev_error_retries + 1
        if attempts > APPRAISAL_ERROR_RETRY_MAX:
            _append_state(
                {"week": week, "identifier": identifier, **row,
                 "status": RequestStatus.FAILED,
                 "reason": "appraisal_error", "error_retries": attempts}
            )
            print(
                f"[appraisal-request] failed {identifier}: appraisal error after "
                f"{APPRAISAL_ERROR_RETRY_MAX} retries"
            )
            return False
        retry_after = time.time() + APPRAISAL_ERROR_RETRY_WAIT_SECONDS
        _append_state(
            {"week": week, "identifier": identifier, **row,
             "status": RequestStatus.DEFERRED,
             "retry_after": retry_after, "error_retries": attempts,
             "reason": "appraisal_error"}
        )
        print(
            f"[appraisal-request] deferred {identifier}: appraisal error "
            f"(attempt {attempts}/{APPRAISAL_ERROR_RETRY_MAX}); "
            f"retry in ~{APPRAISAL_ERROR_RETRY_WAIT_SECONDS / 60:.0f} min"
        )
        return False

    render.publish_appraisals([article], week)
    update_weekly_html_for_article(week, article)
    persist_selected_article(week_dir, article)

    appraisal_url = str(article.get("appraisal_url", ""))
    if not appraisal_url:
        _append_state({"week": week, "identifier": identifier, **row,
                       "status": RequestStatus.FAILED})
        print(f"[appraisal-request] appraisal HTML missing {identifier}")
        return False

    push_and_notify(
        week, article, appraisal_url,
        identifier=identifier, no_push=no_push, no_discord=no_discord,
    )

    _append_state(
        {
            "week": week,
            "identifier": identifier,
            "pmid": str(row.get("pmid", "")).strip(),
            "doi": str(row.get("doi", "")).strip(),
            "status": RequestStatus.DONE,
            "appraisal_url": appraisal_url,
            "force": force,
        }
    )
    print(f"[appraisal-request] done {identifier}: {appraisal_url}")
    return True


def _update_existing_weekly_html_appraisal_link(week: str, article: dict) -> None:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return

    html_path = DOCS_DIR / f"{week}.html"
    if not html_path.exists():
        return
    target_doi = _normalize_doi(str(article.get("doi", "")))
    target_pmid = str(article.get("original_pmid", article.get("pmid", ""))).strip()
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")
    for node in soup.find_all("article"):
        article_doi = ""
        article_pmid = ""
        request_link = None
        for link in node.find_all("a"):
            href = link.get("href", "")
            if href.startswith("https://doi.org/"):
                article_doi = _normalize_doi(href)
            elif link.get_text(strip=True) == "請求評讀":
                request_link = link
                qs = parse_qs(urlparse(href).query)
                article_pmid = qs.get("pmid", [""])[0]
                article_doi = article_doi or _normalize_doi(qs.get("doi", [""])[0])
        if article_doi != target_doi and (not target_pmid or article_pmid != target_pmid):
            continue
        if request_link:
            request_link["href"] = str(article.get("appraisal_url", ""))
            request_link.attrs.pop("target", None)
            request_link.attrs.pop("rel", None)
            request_link.attrs.pop("class", None)
            request_link.string = "評讀結果"
        break
    html_path.write_text(str(soup), encoding="utf-8")


def process_requests(
    *,
    limit: int,
    force: bool,
    dry_run: bool,
    no_push: bool,
    no_discord: bool,
) -> int:
    # On-demand appraisals run claude-only on Opus (no codex fallback); a
    # usage-limit raises ClaudeLimitError, which _process_one defers.
    os.environ["JOURNAL_FETCHER_CLAUDE_ONLY"] = "1"

    rows = _sync_requests()
    known = _known_articles()
    state = _load_state()
    processed = 0

    for row in rows:
        week = str(row.get("week", "")).strip()
        identifiers = _candidate_identifiers(row)
        identifier = identifiers[0] if identifiers else ""
        status = str(row.get("status", RequestStatus.REQUESTED)).strip() or RequestStatus.REQUESTED
        if status != RequestStatus.REQUESTED:
            continue
        if not identifier:
            continue
        if not force and any(
            (rec := state.get((week, candidate))) is not None and _state_blocks(rec)
            for candidate in identifiers
        ):
            continue
        article = None
        matched_identifier = identifier
        for candidate in identifiers:
            article = known.get((week, candidate))
            if article is not None:
                matched_identifier = candidate
                break
        if article is None:
            print(f"[appraisal-request] skip unknown article: {week} {identifier}")
            continue
        article = {**article}
        if row.get("pmid") and not str(article.get("pmid", "")).strip():
            article["pmid"] = str(row.get("pmid", "")).strip()
        if dry_run:
            print(f"[dry-run] would appraise {week} {matched_identifier}: {article.get('title', '')}")
            processed += 1
        else:
            prev_record = next(
                (rec for candidate in identifiers if (rec := state.get((week, candidate)))),
                None,
            )
            try:
                prev_error_retries = int((prev_record or {}).get("error_retries", 0))
            except (TypeError, ValueError):
                prev_error_retries = 0
            if _process_one(
                row, article, force, no_push, no_discord,
                prev_error_retries=prev_error_retries,
            ):
                processed += 1
        if limit and processed >= limit:
            break

    print(f"[appraisal-request] processed {processed} request(s)")
    return processed


def main() -> int:
    parser = argparse.ArgumentParser(description="Process weekly HTML appraisal requests.")
    parser.add_argument("--limit", type=int, default=3, help="Maximum requests per run; 0 means no limit")
    parser.add_argument("--force", action="store_true", help="Reprocess even if local state has this request")
    parser.add_argument("--dry-run", action="store_true", help="List requests without downloading/appraising")
    parser.add_argument("--no-push", action="store_true", help="Skip git commit/push of docs/")
    parser.add_argument("--no-discord", action="store_true", help="Skip Discord completion notice")
    args = parser.parse_args()

    try:
        process_requests(
            limit=args.limit,
            force=args.force,
            dry_run=args.dry_run,
            no_push=args.no_push,
            no_discord=args.no_discord,
        )
    except Exception as e:  # noqa: BLE001 - standalone worker should report a clear failure
        print(f"[appraisal-request] failed: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
