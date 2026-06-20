"""Scan recent weeks for `deferred_claude_limit` appraisals and retry them
once their `appraisal_deferred_until` has passed.

Called from `process_appraisal_requests.main()` at the start of each cron
tick (every 15 min per `scripts/com.pokai.weekly-appraisal-requests.plist`),
so an article deferred for 4.5h gets picked up within ~15 min of its retry
window opening.

State machine (persisted in `output/weekly/<week>/selected_articles.json`):

  done / failed / pdf_failed / too_large           ← initial appraise_selected
       │ ClaudeLimitError in claude-only mode
       ▼
  deferred_claude_limit + appraisal_deferred_until + appraisal_retry_count=0
       │ scan_and_retry runs after appraisal_deferred_until
       ├─ success ───────────── ▶ done                 (publish + re-render HTML)
       ├─ ClaudeLimitError ──── ▶ deferred_claude_limit + retry_count++ +
       │                          new appraisal_deferred_until
       │  (after MAX_RETRIES)    permanently_failed
       └─ other Exception ────── ▶ failed              (no auto-retry; manual)

Discord notifications are intentionally suppressed on retry: the original
launchd run's notification (or its absence, on the weekly path with
`--no-discord`) already set expectations; a delayed Discord ping 4-9 hours
later is more confusing than helpful. The HTML / docs update silently.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
WEEKLY_DIR = ROOT / "output" / "weekly"

# Defaults; overridable via env.
DEFAULT_MAX_RETRIES = int(os.getenv("JOURNAL_FETCHER_APPRAISAL_MAX_RETRIES", "3"))
DEFAULT_LOOKBACK_WEEKS = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_RETRY_LOOKBACK_WEEKS", "4")
)
DEFER_MINUTES = int(os.getenv("JOURNAL_FETCHER_APPRAISAL_DEFER_MINUTES", "270"))
TZ = ZoneInfo("Asia/Taipei")


def _recent_week_dirs(lookback_weeks: int) -> list[Path]:
    """Return the most recent N week directories that have selected_articles.json."""
    if not WEEKLY_DIR.exists():
        return []
    weeks = sorted(
        (
            p
            for p in WEEKLY_DIR.iterdir()
            if p.is_dir() and (p / "selected_articles.json").exists()
        ),
        reverse=True,
    )
    return weeks[:lookback_weeks]


def _ready_to_retry(
    article: dict,
    now: datetime,
    max_retries: int,
    include_failed_statuses: bool = False,
) -> bool:
    status = article.get("appraisal_status")
    # Backfill path: opt-in retry of statuses produced before the deferred
    # retry mechanism existed (e.g. W25's 4 'failed' + 1 'pdf_failed' that
    # were really claude usage limits).
    if include_failed_statuses and status in ("failed", "pdf_failed"):
        if article.get("appraisal_retry_count", 0) >= max_retries:
            return False
        return True
    if status != "deferred_claude_limit":
        return False
    if article.get("appraisal_retry_count", 0) >= max_retries:
        return False
    defer_str = str(article.get("appraisal_deferred_until", "")).strip()
    if not defer_str:
        return False
    try:
        defer_until = datetime.fromisoformat(defer_str)
    except ValueError:
        return False
    if defer_until.tzinfo is None:
        defer_until = defer_until.replace(tzinfo=TZ)
    return now >= defer_until


def _write_back(week_dir: Path, selected: list[dict]) -> None:
    (week_dir / "selected_articles.json").write_text(
        json.dumps(selected, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _ensure_pdf(article: dict, week_dir: Path) -> Path | None:
    """Make sure the PDF exists locally, redownloading if needed."""
    pmid = str(article.get("pmid", ""))
    pdf_path = week_dir / "pdfs" / f"{pmid}.pdf"
    if pdf_path.exists():
        return pdf_path
    try:
        from modules.downloader import download_articles
    except ImportError as e:
        print(f"  [retry] downloader unavailable: {e}", file=sys.stderr)
        return None
    print(f"  [retry] redownloading missing pdf for {pmid}")
    results = download_articles([article], out_dir=week_dir / "pdfs")
    return results.get(pmid)


def _next_defer_iso(now: datetime) -> str:
    return (now + timedelta(minutes=DEFER_MINUTES)).isoformat(timespec="seconds")


def _publish_and_refresh(
    week: str,
    week_dir: Path,
    article: dict,
    selected: list[dict],
    no_push: bool,
) -> None:
    """Publish the appraisal HTML and re-render the weekly page."""
    from weekly import render
    try:
        published = render.publish_appraisals([article], week)
        if published:
            print(f"    [ok] published {published[0]}")
    except Exception as e:  # noqa: BLE001
        print(f"    [warn] publish failed: {e}", file=sys.stderr)
        return

    # Re-render the weekly page so the card moves from `failed` → linked.
    try:
        if (week_dir / "articles.json").exists():
            from weekly.process_appraisal_requests import _render_weekly_page
            _render_weekly_page(week, selected)
        else:
            from weekly.process_appraisal_requests import (
                _update_existing_weekly_html_appraisal_link,
            )
            _update_existing_weekly_html_appraisal_link(week, article)
    except Exception as e:  # noqa: BLE001
        print(f"    [warn] weekly HTML refresh failed: {e}", file=sys.stderr)

    if no_push:
        return
    try:
        from weekly import publish
        publish.git_commit_and_push(f"{week}.html", f"{week} appraisal retry")
    except Exception as e:  # noqa: BLE001
        print(f"    [warn] git push failed: {e}", file=sys.stderr)


def scan_and_retry(
    *,
    now: datetime | None = None,
    lookback_weeks: int | None = None,
    max_retries: int | None = None,
    no_push: bool = False,
    target_week: str | None = None,
    include_failed_statuses: bool = False,
) -> int:
    """Scan recent weeks for deferred appraisals past their retry window.

    Returns the number of articles that reached a terminal state this sweep
    (done / permanently_failed / too_large). Articles re-deferred do not count.

    `target_week` limits the sweep to one specific week directory; `lookback_weeks`
    is ignored in that case. `include_failed_statuses` opt-in also retries
    status='failed' and 'pdf_failed' (immediately, ignoring deferred_until) — use
    for one-shot backfill of pre-mechanism failures (e.g. W25 5/5 failed).
    """
    now = now or datetime.now(TZ)
    lookback_weeks = lookback_weeks or DEFAULT_LOOKBACK_WEEKS
    max_retries = max_retries or DEFAULT_MAX_RETRIES
    finalized = 0

    # Honor claude-only here too: the retry path exists *because* of claude-only.
    # If a caller has explicitly disabled it, defer-retry still works but
    # ClaudeLimitError stops propagating (codex fallback kicks in instead).
    os.environ.setdefault("JOURNAL_FETCHER_CLAUDE_ONLY", "1")

    from modules import claude_exec
    from weekly import appraise_selected

    if target_week:
        wd = WEEKLY_DIR / target_week
        if not (wd / "selected_articles.json").exists():
            print(
                f"[retry] {target_week}: selected_articles.json not found",
                file=sys.stderr,
            )
            return 0
        week_dirs = [wd]
    else:
        week_dirs = _recent_week_dirs(lookback_weeks)

    for week_dir in week_dirs:
        week = week_dir.name
        sj = week_dir / "selected_articles.json"
        try:
            selected = json.loads(sj.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            print(f"[retry] {week}: cannot read {sj.name}: {e}", file=sys.stderr)
            continue

        candidates = [
            a
            for a in selected
            if _ready_to_retry(a, now, max_retries, include_failed_statuses)
        ]
        if not candidates:
            continue

        print(f"\n[retry-deferred] {week}: {len(candidates)} article(s) past retry window")
        for article in candidates:
            pmid = str(article.get("pmid", ""))
            title = (article.get("title") or "")[:70]
            attempt = int(article.get("appraisal_retry_count", 0)) + 1
            print(f"  [retry {attempt}/{max_retries}] {pmid}: {title}")

            pdf_path = _ensure_pdf(article, week_dir)
            if not pdf_path:
                article["appraisal_retry_count"] = attempt
                article["appraisal_last_error"] = "pdf still missing on retry"
                if attempt >= max_retries:
                    article["appraisal_status"] = "permanently_failed"
                    finalized += 1
                    print("    [give-up] pdf still missing; permanently_failed")
                else:
                    article["appraisal_deferred_until"] = _next_defer_iso(now)
                    print(
                        f"    [defer] pdf missing; next attempt after "
                        f"{article['appraisal_deferred_until']}"
                    )
                continue

            article["appraisal_retry_count"] = attempt
            appraisal_dir = week_dir / "appraisals"
            try:
                report_path = appraise_selected.appraise_pdf(
                    article, pdf_path, appraisal_dir
                )
            except claude_exec.ClaudeLimitError as e:
                article["appraisal_last_error"] = (
                    f"claude usage limit (retry {attempt}): {str(e)[:160]}"
                )
                if attempt >= max_retries:
                    article["appraisal_status"] = "permanently_failed"
                    article.pop("appraisal_deferred_until", None)
                    finalized += 1
                    print(
                        f"    [give-up] still limited after {attempt} attempts; "
                        f"permanently_failed"
                    )
                else:
                    article["appraisal_deferred_until"] = _next_defer_iso(now)
                    print(
                        f"    [defer] still limited; next attempt after "
                        f"{article['appraisal_deferred_until']}"
                    )
                continue
            except appraise_selected.ArticleTooLargeError as e:
                article["appraisal_status"] = "too_large"
                article["appraisal_path"] = ""
                article["appraisal_last_error"] = str(e)[:200]
                article.pop("appraisal_deferred_until", None)
                finalized += 1
                print(f"    [too_large] {str(e)[:160]}")
                continue
            except Exception as e:  # noqa: BLE001
                article["appraisal_last_error"] = (
                    f"{type(e).__name__}: {str(e)[:160]}"
                )
                if attempt >= max_retries:
                    article["appraisal_status"] = "permanently_failed"
                    article.pop("appraisal_deferred_until", None)
                    finalized += 1
                    print(
                        f"    [give-up] retry {attempt}: "
                        f"{type(e).__name__}: {str(e)[:120]}"
                    )
                else:
                    # Real error (not claude limit) — likely needs human triage;
                    # mark failed instead of looping every 15 min.
                    article["appraisal_status"] = "failed"
                    article.pop("appraisal_deferred_until", None)
                    print(
                        f"    [error] retry {attempt}: {type(e).__name__}: "
                        f"{str(e)[:120]}; manual triage required"
                    )
                continue

            if report_path:
                article["appraisal_status"] = "done"
                article["appraisal_path"] = str(report_path)
                article.pop("appraisal_deferred_until", None)
                article.pop("appraisal_last_error", None)
                _publish_and_refresh(week, week_dir, article, selected, no_push)
                finalized += 1
            else:
                article["appraisal_status"] = "failed"
                article["appraisal_path"] = ""
                article["appraisal_last_error"] = "no report produced"
                article.pop("appraisal_deferred_until", None)
                finalized += 1

        _write_back(week_dir, selected)

    if finalized:
        print(f"\n[retry-deferred] {finalized} article(s) finalized this sweep")
    return finalized


def main() -> int:
    """Standalone CLI: scan and retry once. Useful for manual triage."""
    import argparse
    parser = argparse.ArgumentParser(
        description="Retry appraisals deferred by claude usage limit."
    )
    parser.add_argument(
        "--lookback-weeks",
        type=int,
        default=DEFAULT_LOOKBACK_WEEKS,
        help=f"How many recent week dirs to scan (default {DEFAULT_LOOKBACK_WEEKS})",
    )
    parser.add_argument(
        "--week",
        default=None,
        help=(
            "Limit to a specific week (e.g. 2026-W25). "
            "Overrides --lookback-weeks when set."
        ),
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Mark permanently_failed after N attempts (default {DEFAULT_MAX_RETRIES})",
    )
    parser.add_argument(
        "--include-failed",
        action="store_true",
        help=(
            "Also retry status='failed' / 'pdf_failed' (treat as immediately "
            "due for retry). Use for one-shot backfill of pre-deferred-mechanism "
            "failures; the normal cron should leave this off."
        ),
    )
    parser.add_argument("--no-push", action="store_true", help="Skip git push")
    args = parser.parse_args()
    finalized = scan_and_retry(
        lookback_weeks=args.lookback_weeks,
        max_retries=args.max_retries,
        no_push=args.no_push,
        target_week=args.week,
        include_failed_statuses=args.include_failed,
    )
    return 0 if finalized >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
