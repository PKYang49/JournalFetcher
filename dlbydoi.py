#!/usr/bin/env python3
"""
從 DOI 列表下載 PDF（純 HTTP，不開瀏覽器）。

用法：
  python download_by_doi.py dois.txt
  python download_by_doi.py dois.txt --out-dir output/manual
  echo "10.1093/eurheartj/ehaf791" | python download_by_doi.py -
"""

import argparse
import re
import sys
import time
from pathlib import Path

from modules.downloader import (
    _get,
    _is_pdf,
    _is_incomplete_elsevier_pdf,
    _is_usable_pdf_file,
    _try_direct,
    _try_doi_redirect,
    _try_elsevier_api,
    _try_nodriver,
    _try_unpaywall,
    _try_pmc,
    _try_nodriver_url,
    _direct_pdf_urls,
    ELSEVIER_API_KEY,
    UNPAYWALL_EMAIL,
)


def _fetch_metadata(doi: str) -> dict:
    """Fetch article metadata from Crossref for filename."""
    try:
        resp = _get(f"https://api.crossref.org/works/{doi}")
        resp.raise_for_status()
        msg = resp.json()["message"]
        authors = msg.get("author", [])
        first_author = authors[0].get("family", "unknown") if authors else "unknown"
        first_author = re.sub(r"[^\w]", "", first_author)
        year = ""
        for date_field in ("published-print", "published-online", "issued"):
            parts = msg.get(date_field, {}).get("date-parts", [[]])
            if parts and parts[0] and parts[0][0]:
                year = str(parts[0][0])
                break
        journal = msg.get("short-container-title", [""])[0] or msg.get("container-title", [""])[0] or ""
        return {"first_author": first_author, "year": year, "journal": journal}
    except Exception:
        return {"first_author": "unknown", "year": "0000", "journal": ""}


def _detect_journal(doi: str, journal: str) -> str:
    """Map known DOI prefixes / journal names for _try_direct."""
    j = journal.lower()
    if "n engl j med" in j or "nejm" in j:
        return "The New England journal of medicine"
    if "jama" in j:
        return "JAMA"
    if "lancet" in j:
        return "Lancet"
    if "j am coll cardiol" in j or "jacc" in j:
        return "Journal of the American College of Cardiology"
    if "eur heart j" in j:
        return "European heart journal"
    if "circulation" in j:
        return "Circulation"
    if "eurointervention" in j:
        return "EuroIntervention"
    return journal


def download_one(doi: str, out_dir: Path) -> Path | None:
    """Download a single PDF by DOI. Returns path or None."""
    # Fetch metadata for filename
    meta = _fetch_metadata(doi)
    fname = f"{meta['first_author']}_{meta['year']}_{doi.replace('/', '_')}.pdf"
    dest = out_dir / fname

    if dest.exists() and _is_usable_pdf_file(dest, doi):
        print(f"  [skip] {dest.name}")
        return dest

    journal = _detect_journal(doi, meta["journal"])
    is_elsevier = doi.startswith("10.1016/")
    content = None
    step = 1

    # [1] Direct PDF URL
    print(f"  [{step}] Direct PDF URL...")
    content = _try_direct(doi, journal)
    step += 1

    # [2] DOI redirect
    if not content:
        print(f"  [{step}] DOI redirect...")
        content = _try_doi_redirect(doi)
        step += 1

    # [3] Elsevier API
    if not content and is_elsevier and ELSEVIER_API_KEY:
        print(f"  [{step}] Elsevier API...")
        content = _try_elsevier_api(doi)
        if content and _is_incomplete_elsevier_pdf(content, doi):
            print(f"  [incomplete] Elsevier API returned preview only, trying nodriver...")
            content = None
        step += 1

    # [4] ScienceDirect browser session for Elsevier journals
    if not content and is_elsevier:
        print(f"  [{step}] ScienceDirect browser session...")
        content = _try_nodriver(doi)
        step += 1

    # [5] Unpaywall
    if not content:
        print(f"  [{step}] Unpaywall...")
        content = _try_unpaywall(doi)
        step += 1

    # [6] PMC
    if not content:
        print(f"  [{step}] PMC...")
        content = _try_pmc(doi)
        step += 1

    # [7] nodriver (Cloudflare-protected sites: NEJM, JAMA, etc.)
    if not content:
        urls = _direct_pdf_urls(doi, journal)
        if urls:
            print(f"  [{step}] nodriver (browser)...")
            for url in urls:
                content = _try_nodriver_url(url)
                if content:
                    break
            step += 1

    if content:
        dest.write_bytes(content)
        print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
        return dest

    print(f"  [FAIL] 所有方法皆失敗")
    return None


def read_dois(source: str) -> list[str]:
    """Read DOIs from file or stdin."""
    if source == "-":
        lines = sys.stdin.read().splitlines()
    else:
        lines = Path(source).read_text().splitlines()

    dois = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # 支援 https://doi.org/10.xxxx 格式
        line = re.sub(r"^https?://doi\.org/", "", line)
        if not re.match(r"^10\.\d{4,}/\S+", line):
            continue
        dois.append(line)
    return dois


def main():
    parser = argparse.ArgumentParser(description="從 DOI 列表下載 PDF")
    parser.add_argument("input", help="DOI 列表檔案（一行一個），或 - 從 stdin 讀取")
    parser.add_argument("--out-dir", default="output/doi_downloads", help="PDF 輸出資料夾")
    args = parser.parse_args()

    dois = read_dois(args.input)
    if not dois:
        print("沒有讀到任何 DOI。")
        return

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n共 {len(dois)} 篇，下載至 {out_dir}\n")

    success = 0
    failed = []
    for i, doi in enumerate(dois, 1):
        print(f"[{i}/{len(dois)}] {doi}")
        result = download_one(doi, out_dir)
        if result:
            success += 1
        else:
            failed.append(doi)
        time.sleep(1)

    print(f"\n{'='*50}")
    print(f"完成：{success} 成功 / {len(failed)} 失敗")
    if failed:
        print(f"失敗 DOI：")
        for d in failed:
            print(f"  {d}")
    print(f"PDF 路徑：{out_dir.resolve()}")


if __name__ == "__main__":
    main()
