"""Weekly report runner.

Pipeline:
  1. fetch articles for each journal (last `days` days, top `count` per journal)
  2. summarize each abstract via `claude -p`, falling back to `codex exec`
  3. optionally select top articles, download PDFs, and generate full appraisals
  4. render HTML to docs/<YYYY>-Wxx.html and update docs/index.html
  5. (optional) git push
  6. (optional) Discord webhook

Usage:
  python -m weekly.run_weekly                    # generate + push + discord
  python -m weekly.run_weekly --dry-run          # generate only, no git/discord
  python -m weekly.run_weekly --no-push          # skip git push
  python -m weekly.run_weekly --no-discord       # skip Discord
  python -m weekly.run_weekly --journals NEJM Lancet
  python -m weekly.run_weekly --count 5 --days 14
  python -m weekly.run_weekly --select-top 2 --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

# Allow `python weekly/run_weekly.py` execution as well as `-m weekly.run_weekly`
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules import crossref, pubmed  # noqa: E402
from weekly import render, summarize_weekly  # noqa: E402

DEFAULT_JOURNALS = list(pubmed.JOURNAL_QUERIES.keys())
WEEK_LABEL_RE = re.compile(r"^\d{4}-W(?:0[1-9]|[1-4]\d|5[0-3])$")

# Appraisal usage-limit wait-and-resume tuning. The weekly appraisal phase runs
# claude-only on Opus (no codex fallback); when it hits the rolling usage limit
# we parse Claude's reported reset time and resume shortly afterward. The 5h
# value is only a fallback when the CLI does not return a parseable reset time.
APPRAISAL_RETRY_WAIT_SECONDS = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_RETRY_WAIT", str(5 * 60 * 60))
)
APPRAISAL_RESET_BUFFER_SECONDS = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_RESET_BUFFER", "120")
)
APPRAISAL_RETRY_MAX_CYCLES = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_RETRY_MAX_CYCLES", "5")
)


def fetch_all(journals: list[str], days: int | None, count: int) -> list[dict]:
    """Fetch articles from each journal, tagging with `journal_key`.

    When `days` is None, use the per-journal default from
    `pubmed.JOURNAL_DEFAULT_WINDOW` (a historical (min, max) day-range), or
    fall back to a last-7-days window. When the user supplies `--days`
    explicitly, it applies uniformly to all journals.
    """
    all_articles: list[dict] = []
    for key in journals:
        date_range = None
        window_label = ""
        if days is None and key in pubmed.JOURNAL_DEFAULT_WINDOW:
            date_range = pubmed.JOURNAL_DEFAULT_WINDOW[key]
            lo, hi = min(date_range), max(date_range)
            window_label = f"{lo}-{hi}d ago"
            kw = {"date_range": date_range}
        else:
            window = days if days is not None else 7
            window_label = f"last {window}d"
            kw = {"days": window}
        print(f"[{key}] fetching {window_label}, up to {count} articles ...")
        try:
            if key == "BJSM" and date_range is not None:
                articles = crossref.fetch_bjsm_articles(
                    count=count,
                    date_range=date_range,
                )
            else:
                articles = pubmed.fetch_journal_articles(key, count=count, **kw)
        except Exception as e:
            print(f"  [error] {key} fetch failed: {e}")
            continue
        for a in articles:
            a["journal_key"] = key
        print(f"  -> got {len(articles)} articles")
        all_articles.extend(articles)
    return all_articles


def appraise_with_resume(
    selected_articles: list[dict],
    download_results: dict,
    appraisal_dir: Path,
) -> None:
    """Run the full appraisal phase claude-only on Opus, waiting out the
    reported usage-limit reset and resuming until every article is appraised.

    The dispatcher is forced into claude-only mode (no codex fallback) so a
    usage-limit raises ClaudeLimitError instead of silently producing a codex
    appraisal. On that error we sleep until Claude's reported reset time
    (plus a short buffer) and re-run appraise_selected; finished reports are
    skipped cheaply on re-entry.
    """
    import time

    from modules import claude_exec
    from weekly import appraise_selected

    os.environ["JOURNAL_FETCHER_CLAUDE_ONLY"] = "1"
    # Summaries (Haiku) may have tripped the process-local exhausted flag and
    # fallen back to codex; reset it so the appraisal phase gets a fresh claude
    # attempt before deciding to wait.
    claude_exec.reset_claude_exhausted()

    for cycle in range(APPRAISAL_RETRY_MAX_CYCLES + 1):
        try:
            appraise_selected.appraise_selected(
                selected_articles,
                download_results,
                appraisal_dir,
            )
            return
        except claude_exec.ClaudeLimitError as e:
            if cycle >= APPRAISAL_RETRY_MAX_CYCLES:
                print(
                    f"[warn] appraisal hit claude usage limit and exhausted "
                    f"{APPRAISAL_RETRY_MAX_CYCLES} retry cycle(s); leaving "
                    f"remaining articles un-appraised. ({str(e)[:120]})",
                    file=sys.stderr,
                )
                return
            wait, reset_at = claude_exec.retry_wait_seconds(
                str(e),
                fallback_seconds=APPRAISAL_RETRY_WAIT_SECONDS,
                buffer_seconds=APPRAISAL_RESET_BUFFER_SECONDS,
            )
            reset_note = (
                f", parsed reset={reset_at:%Y-%m-%d %H:%M %Z}"
                if reset_at is not None
                else ", reset time unavailable; using fallback"
            )
            print(
                f"[info] appraisal hit claude usage limit ({str(e)[:120]}); "
                f"sleeping {wait}s (~{wait / 3600:.1f}h{reset_note}) then resuming the "
                f"unfinished articles (cycle {cycle + 1}/"
                f"{APPRAISAL_RETRY_MAX_CYCLES}).",
                file=sys.stderr,
            )
            time.sleep(wait)
            claude_exec.reset_claude_exhausted()


def journal_count_summary(articles: list[dict], journals: list[str]) -> list[dict]:
    counts = {k: 0 for k in journals}
    for a in articles:
        k = a.get("journal_key")
        if k in counts:
            counts[k] += 1
    return [{"name": k, "count": counts[k]} for k in journals if counts[k] > 0]


def resolve_week_label(week: str) -> tuple[str, str, str]:
    """Resolve an optional explicit ISO week while retaining today's date."""
    if not week:
        return render.iso_week_label()
    if not WEEK_LABEL_RE.fullmatch(week):
        raise ValueError(f"invalid --week value: {week!r}; expected YYYY-Www")
    return week, f"{week}.html", render.iso_week_label()[2]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate weekly journal report.")
    parser.add_argument(
        "--journals",
        nargs="+",
        default=DEFAULT_JOURNALS,
        help="Subset of journals (keys from pubmed.JOURNAL_QUERIES)",
    )
    parser.add_argument("--count", type=int, default=10, help="Per-journal count")
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=(
            "Lookback window in days (overrides per-journal defaults; "
            "default 7d, BJSM 90-100d ago)"
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Generate HTML only")
    parser.add_argument("--no-push", action="store_true", help="Skip git push")
    parser.add_argument("--no-discord", action="store_true", help="Skip Discord webhook")
    parser.add_argument(
        "--no-summarize",
        action="store_true",
        help="Skip backend summarization (debug HTML layout)",
    )
    parser.add_argument(
        "--select-top",
        type=int,
        default=0,
        help="Select N articles for PDF download and full appraisal",
    )
    parser.add_argument(
        "--no-download-selected",
        action="store_true",
        help="Select top articles but skip PDF download",
    )
    parser.add_argument(
        "--no-appraise-selected",
        action="store_true",
        help="Download selected PDFs but skip full appraisal",
    )
    parser.add_argument(
        "--no-sync-feedback",
        action="store_true",
        help="Skip pulling highlight feedback from the configured relay",
    )
    parser.add_argument(
        "--appraise-allow-codex",
        action="store_true",
        help=(
            "Allow codex fallback for appraisals. Default is claude-only Opus: "
            "on a usage limit, wait ~5h and resume instead of falling back."
        ),
    )
    parser.add_argument(
        "--week",
        default="",
        help="Explicit output ISO week label, for example 2026-W26",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    feedback_endpoint = os.getenv("FEEDBACK_ENDPOINT_URL", "").strip()

    try:
        label, filename, date_str = resolve_week_label(args.week.strip())
    except ValueError as e:
        parser.error(str(e))
    print(f"=== Weekly report {label} ===")
    print(f"Journals: {', '.join(args.journals)}")

    articles = fetch_all(args.journals, days=args.days, count=args.count)
    if not articles:
        print("[abort] no articles fetched.")
        return 1

    if args.no_summarize:
        for a in articles:
            a["summary"] = "[--no-summarize 模式：未生成摘要]"
    else:
        print(f"\nSummarizing {len(articles)} articles via claude/codex backends ...")
        summarize_weekly.summarize_articles(articles)

    # Persist the full article list — sync_feedback uses it as the PMID
    # whitelist so feedback on 本週文章摘要 articles is not dropped as spam.
    weekly_out_dir = ROOT / "output" / "weekly" / label
    weekly_out_dir.mkdir(parents=True, exist_ok=True)
    (weekly_out_dir / "articles.json").write_text(
        json.dumps(articles, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[ok] wrote {weekly_out_dir / 'articles.json'}")

    selected_articles: list[dict] = []
    if args.select_top > 0:
        from weekly import select_articles

        if not args.no_sync_feedback:
            try:
                from weekly import sync_feedback

                print("\nSyncing highlight feedback from relay ...")
                sync_feedback.sync_feedback()
            except Exception as e:
                print(f"[warn] feedback sync failed; selecting without it: {e}")

        pdf_dir = weekly_out_dir / "pdfs"
        appraisal_dir = weekly_out_dir / "appraisals"

        print(f"\nSelecting top {args.select_top} articles for full appraisal ...")
        selected_articles = select_articles.select_top_articles(
            articles,
            limit=args.select_top,
            out_dir=weekly_out_dir,
        )
        selected_meta = select_articles.write_selected_metadata(
            selected_articles,
            weekly_out_dir,
        )
        print(f"[ok] wrote {selected_meta}")

        download_results: dict[str, Path | None] = {}
        if args.no_download_selected:
            print("[skip] selected PDF download disabled")
        else:
            from modules.downloader import download_articles

            print(f"\nDownloading selected PDFs -> {pdf_dir}")
            download_results = download_articles(selected_articles, out_dir=pdf_dir)

        if args.no_appraise_selected:
            print("[skip] selected appraisal disabled")
        elif args.no_download_selected:
            print("[skip] selected appraisal needs downloaded PDFs")
        else:
            from weekly import appraise_selected

            print(f"\nGenerating full appraisals -> {appraisal_dir}")
            if args.appraise_allow_codex:
                appraise_selected.appraise_selected(
                    selected_articles,
                    download_results,
                    appraisal_dir,
                )
            else:
                appraise_with_resume(
                    selected_articles,
                    download_results,
                    appraisal_dir,
                )
            select_articles.write_selected_metadata(selected_articles, weekly_out_dir)
            published = render.publish_appraisals(selected_articles, label)
            if published:
                select_articles.write_selected_metadata(selected_articles, weekly_out_dir)
                print(f"[ok] published {len(published)} appraisal HTML file(s)")

    counts = journal_count_summary(articles, args.journals)
    out_path, index_path = render.render_and_write_weekly(
        articles,
        week_label=label,
        filename=filename,
        date_str=date_str,
        journal_counts=counts,
        selected_articles=selected_articles,
        feedback_endpoint=feedback_endpoint,
    )
    print(f"\n[ok] wrote {out_path}")
    print(f"[ok] updated {index_path}")

    if args.dry_run:
        print("\n[dry-run] skipping git push & discord")
        return 0

    if not args.no_push:
        try:
            from weekly import publish

            publish.git_commit_and_push(filename, label)
        except ImportError:
            print("[skip] weekly/publish.py not implemented yet")
        except Exception as e:
            print(f"[error] git push failed: {e}")
            traceback.print_exc()

    if not args.no_discord:
        try:
            from weekly import publish

            publish.send_discord(label, articles, counts, filename)
        except ImportError:
            print("[skip] weekly/publish.py not implemented yet")
        except Exception as e:
            print(f"[error] discord push failed: {e}")
            traceback.print_exc()

    return 0


if __name__ == "__main__":
    sys.exit(main())
