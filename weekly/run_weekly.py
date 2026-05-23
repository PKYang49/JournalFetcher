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


def journal_count_summary(articles: list[dict], journals: list[str]) -> list[dict]:
    counts = {k: 0 for k in journals}
    for a in articles:
        k = a.get("journal_key")
        if k in counts:
            counts[k] += 1
    return [{"name": k, "count": counts[k]} for k in journals if counts[k] > 0]


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
        help="Skip codex exec summarization (debug HTML layout)",
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
        help="Skip pulling highlight feedback from the Apps Script relay",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    feedback_endpoint = os.getenv("FEEDBACK_ENDPOINT_URL", "").strip()

    label, filename, date_str = render.iso_week_label()
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
        print(f"\nSummarizing {len(articles)} articles via codex exec ...")
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
            appraise_selected.appraise_selected(
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
    html = render.render_weekly(
        articles,
        week_label=label,
        journal_counts=counts,
        selected_articles=selected_articles,
        feedback_endpoint=feedback_endpoint,
    )
    out_path = render.write_weekly(html, filename)
    index_path = render.update_index(filename, label, len(articles), date_str)
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
