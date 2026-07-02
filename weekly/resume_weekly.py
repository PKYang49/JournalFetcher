"""Resume a weekly appraisal phase from persisted articles, selections, and PDFs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules import pubmed  # noqa: E402
from weekly import render, select_articles  # noqa: E402
from weekly.run_weekly import appraise_with_resume, journal_count_summary  # noqa: E402


def _load_json_list(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ValueError(f"expected JSON list: {path}")
    return value


def _existing_downloads(selected: list[dict], pdf_dir: Path) -> dict[str, Path | None]:
    results: dict[str, Path | None] = {}
    for article in selected:
        pmid = str(article.get("pmid", ""))
        matches = sorted(pdf_dir.glob(f"{pmid}_*.pdf")) if pmid else []
        results[pmid] = matches[0] if matches else None
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", required=True, help="ISO week label, e.g. 2026-W26")
    parser.add_argument("--no-push", action="store_true", help="Skip git commit and push")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    week = args.week.strip()
    week_dir = ROOT / "output" / "weekly" / week
    articles = _load_json_list(week_dir / "articles.json")
    selected = _load_json_list(week_dir / "selected_articles.json")
    downloads = _existing_downloads(selected, week_dir / "pdfs")

    found = sum(path is not None for path in downloads.values())
    print(f"=== Resume weekly appraisals {week} ===", flush=True)
    print(f"[resume] selected={len(selected)}, PDFs={found}", flush=True)

    appraise_with_resume(selected, downloads, week_dir / "appraisals")
    select_articles.write_selected_metadata(selected, week_dir)
    published = render.publish_appraisals(selected, week)
    if published:
        select_articles.write_selected_metadata(selected, week_dir)
        print(f"[ok] published {len(published)} appraisal HTML file(s)", flush=True)

    counts = journal_count_summary(articles, list(pubmed.JOURNAL_QUERIES))
    filename = f"{week}.html"
    render.render_and_write_weekly(
        articles,
        week_label=week,
        filename=filename,
        date_str=render.iso_week_label()[2],
        journal_counts=counts,
        selected_articles=selected,
        feedback_endpoint=os.getenv("FEEDBACK_ENDPOINT_URL", "").strip(),
    )
    print(f"[ok] rebuilt docs/{filename}", flush=True)

    if not args.no_push:
        from weekly import publish

        publish.git_commit_and_push(filename, week)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
