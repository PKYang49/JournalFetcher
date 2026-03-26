#!/usr/bin/env python3
"""
JournalFetcher — 主程式入口

流程：
  Phase 1  : 從 PubMed 抓取最新文章列表
  Phase 2a : 用 Claude Code CLI 生成繁體中文摘要
  Phase 2b : Terminal checkbox 勾選文章
  Phase 3  : 下載 PDF（機構內網 IP 授權）
  提示     : 開啟 Claude.ai，手動上傳 PDF + Literature Appraisal Skill 進行評讀

用法：
  python fetch_journals.py
  python fetch_journals.py --journals NEJM JAMA
  python fetch_journals.py --count 10
  python fetch_journals.py --no-download
"""

import argparse
import datetime
import logging
import sys
from pathlib import Path

import modules.downloader as downloader_module
from modules.pubmed import fetch_journal_articles
from modules.summarize import summarize_articles
from modules.selector import select_for_summary, print_summaries, select_for_download, select_for_download_simple
from modules.downloader import download_articles

# ── 設定 ──────────────────────────────────────────────────────────────────────
ALL_JOURNALS = ["NEJM", "Lancet", "JAMA", "JACC", "EHJ", "EuroIntervention", "Circulation"]
JOURNAL_NAME_MAP = {journal.lower(): journal for journal in ALL_JOURNALS}
DEFAULT_COUNT = 20
OUTPUT_ROOT = Path("output")

# ── CLI 參數 ───────────────────────────────────────────────────────────────────
def _parse_journal_name(value: str) -> str:
    journal = JOURNAL_NAME_MAP.get(value.lower())
    if journal is None:
        raise argparse.ArgumentTypeError(
            f"invalid choice: {value!r} (choose from {', '.join(ALL_JOURNALS)})"
        )
    return journal


def parse_args():
    parser = argparse.ArgumentParser(
        description="抓取頂尖醫學期刊最新文章，生成中文摘要，下載 PDF。"
    )
    parser.add_argument(
        "--journals",
        nargs="+",
        type=_parse_journal_name,
        default=ALL_JOURNALS,
        metavar="JOURNAL",
        help=f"要抓取的期刊，預設全部：{ALL_JOURNALS}",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        help=f"每本期刊抓取篇數（預設 {DEFAULT_COUNT}）",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="生成摘要後直接結束，不進入勾選與下載",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="跳過摘要生成，直接列標題讓你勾選下載",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="搜尋最近幾天內的文章（預設 30）",
    )
    return parser.parse_args()


def _setup_logging(run_dir: Path) -> Path:
    log_file = run_dir / "errors.log"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    root_logger.handlers.clear()
    root_logger.addHandler(logging.FileHandler(log_file, encoding="utf-8"))
    root_logger.addHandler(logging.StreamHandler(sys.stderr))
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)
    return log_file


# ── 主流程 ─────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    # 建立本次執行資料夾
    today = datetime.date.today().strftime("%Y-%m-%d")
    run_dir = OUTPUT_ROOT / today
    run_dir.mkdir(parents=True, exist_ok=True)
    log_file = _setup_logging(run_dir)
    downloader_module.FAILURES_LOG = run_dir / "download_failures.log"

    print(f"\n{'='*60}")
    print(f"  JournalFetcher  |  {today}")
    print(f"  期刊：{', '.join(args.journals)}  |  每本 {args.count} 篇")
    print(f"{'='*60}\n")

    # ── Phase 1：抓取文章列表 ────────────────────────────────────────────────
    print("【Phase 1】從 PubMed 抓取文章...\n")
    all_articles: list[dict] = []
    for journal in args.journals:
        print(f"  → {journal}...")
        try:
            articles = fetch_journal_articles(journal, days=args.days, count=args.count)
            print(f"     取得 {len(articles)} 篇")
            all_articles.extend(articles)
        except Exception as e:
            print(f"  [ERROR] {journal} 抓取失敗：{e}")
            logging.error(f"Phase 1 {journal}: {e}", exc_info=True)

    if not all_articles:
        print("\n沒有抓到任何文章，結束。")
        return

    print(f"\n共取得 {len(all_articles)} 篇文章。\n")

    # ── Phase 2a：選擇要產生摘要的文章 ──────────────────────────────────────
    if args.no_summary:
        print("【Phase 2a】略過摘要生成（--no-summary）。\n")
        for a in all_articles:
            a["summary"] = ""
        summarized = all_articles
    else:
        print("\n【Phase 2a】請選擇要產生摘要的文章...\n")
        try:
            to_summarize, skip_all = select_for_summary(all_articles)
        except Exception as e:
            print(f"  [ERROR] 選擇介面失敗：{e}")
            logging.error(f"Phase 2a select: {e}", exc_info=True)
            to_summarize, skip_all = [], False

        if skip_all:
            print("\n結束。")
            return

        summarized = []
        if to_summarize:
            print(f"\n已選擇 {len(to_summarize)} 篇，生成繁體中文摘要...\n")
            try:
                summarized = summarize_articles(to_summarize)
            except Exception as e:
                print(f"  [ERROR] 摘要生成失敗：{e}")
                logging.error(f"Phase 2a summarize: {e}", exc_info=True)
                for a in to_summarize:
                    if "summary" not in a:
                        a["summary"] = a.get("abstract", "")[:150] + "..."
                summarized = to_summarize

    # ── Phase 2b：顯示完整摘要 ──────────────────────────────────────────────
    if summarized:
        print_summaries(summarized)

    # ── Phase 2c：選擇要下載的文章 ──────────────────────────────────────────
    if args.no_download:
        print("\n[--no-download] 略過下載。")
        return

    # 分出已摘要 vs 未摘要的文章
    summarized_pmids = {a["pmid"] for a in summarized}
    others = [a for a in all_articles if a["pmid"] not in summarized_pmids]

    if not summarized:
        # 沒有摘要文章，直接列全部文章問要下載哪些
        print("\n【Phase 2c】請選擇要下載的文章...\n")
        try:
            selected = select_for_download_simple(all_articles)
        except Exception as e:
            print(f"  [ERROR] 選擇介面失敗：{e}")
            logging.error(f"Phase 2c: {e}", exc_info=True)
            selected = []
    else:
        print("\n【Phase 2c】請選擇要下載的文章...\n")
        try:
            selected = select_for_download(summarized, others)
        except Exception as e:
            print(f"  [ERROR] 選擇介面失敗：{e}")
            logging.error(f"Phase 2c: {e}", exc_info=True)
            selected = []

    if not selected:
        print("\n未選擇任何文章，結束。")
        return

    print(f"\n已選擇 {len(selected)} 篇文章。")

    print(f"\n【Phase 3】下載 PDF → {run_dir}\n")
    try:
        results = download_articles(selected, out_dir=run_dir)
    except Exception as e:
        print(f"  [ERROR] 下載流程失敗：{e}")
        logging.error(f"Phase 3: {e}", exc_info=True)
        results = {}

    # 統計
    success = [p for p in results.values() if p is not None]
    failed = [pmid for pmid, p in results.items() if p is None]

    print(f"\n下載完成：{len(success)} 成功 / {len(failed)} 失敗")
    if failed:
        print(f"失敗 PMID：{', '.join(failed)}")
        print(f"詳細記錄：{downloader_module.FAILURES_LOG}")

    # ── 評讀提示 ──────────────────────────────────────────────────────────────
    _print_appraise_hint(selected, run_dir, downloaded=True)


def _print_appraise_hint(articles: list[dict], pdf_dir: Path, downloaded: bool = True):
    """Print instructions for manual appraisal via Claude.ai."""
    print(f"\n{'='*60}")
    print("【評讀提示】")
    print(f"{'='*60}")
    print("1. 開啟 Claude.ai（已在 Project Instructions 載入 literature-appraisal-SKILL.md）")
    print("2. 上傳以下 PDF，直接對話評讀：\n")
    for a in articles:
        title = a.get("title", "")[:70]
        pmid = a.get("pmid", "")
        journal = a.get("journal", "")
        authors = a.get("authors", [])
        first_author = authors[0] if authors else ""
        year = a.get("year", "")
        fname = f"{pmid}_{first_author.split()[0] if first_author else 'unknown'}_{year}.pdf"
        fpath = pdf_dir / fname
        if not downloaded:
            status = "－ (未下載)"
        elif fpath.exists():
            status = "✓"
        else:
            status = "✗ (下載失敗)"
        print(f"   [{journal}] {status} {fname}")
        print(f"            {title}")
    if downloaded:
        print(f"\nPDF 路徑：{pdf_dir.resolve()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
