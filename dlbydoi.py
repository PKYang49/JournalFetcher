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
from urllib.parse import unquote

from modules.downloader import (
    _get,
    _is_pdf,
    _is_incomplete_elsevier_pdf,
    _is_lww_paywall_pdf,
    _is_usable_pdf_file,
    _try_direct,
    _try_doi_redirect,
    _try_elsevier_api,
    _try_jamanetwork_playwright,
    _try_lww_direct,
    _try_nodriver,
    _try_springer_playwright,
    _try_proquest_playwright,
    _try_unpaywall,
    _try_pmc,
    _try_nodriver_url,
    _direct_pdf_urls,
    playwright_nejm_batch_download,
    playwright_oup_batch_download,
    ELSEVIER_API_KEY,
    UNPAYWALL_EMAIL,
)


def _normalize_doi(value: str) -> str:
    """Normalize DOI text copied from URLs, Markdown, or Discord autolinks."""
    doi = unquote(value.strip().strip("<>`"))
    doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi, flags=re.I)
    doi = doi.rstrip(".,);]>'\"/")
    return doi


def _fetch_metadata(doi: str) -> dict:
    """Fetch article metadata from Crossref (filename + Ovid search hints)."""
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
        title = (msg.get("title") or [""])[0]
        return {
            "first_author": first_author,
            "year": year,
            "journal": journal,
            "title": title,
            "volume": msg.get("volume", "") or "",
            "issue": msg.get("issue", "") or "",
            "pages": msg.get("page", "") or "",
        }
    except Exception:
        return {
            "first_author": "unknown",
            "year": "0000",
            "journal": "",
            "title": "",
            "volume": "",
            "issue": "",
            "pages": "",
        }


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
    if "med sci sports exerc" in j or doi.lower().startswith("10.1249/mss."):
        return "Medicine and science in sports and exercise"
    if "br j sports med" in j or doi.lower().startswith("10.1136/bjsports-"):
        return "British Journal of Sports Medicine"
    if "heart" in j or doi.lower().startswith("10.1136/heartjnl-"):
        return "Heart"
    return journal


def download_one(doi: str, out_dir: Path) -> Path | None:
    """Download a single PDF by DOI. Returns path or None."""
    doi = _normalize_doi(doi)

    # Fetch metadata for filename
    meta = _fetch_metadata(doi)
    fname = f"{meta['first_author']}_{meta['year']}_{doi.replace('/', '_')}.pdf"
    dest = out_dir / fname

    if dest.exists() and _is_usable_pdf_file(dest, doi):
        print(f"  [skip] {dest.name}")
        return dest

    journal = _detect_journal(doi, meta["journal"])
    is_elsevier = doi.startswith("10.1016/")
    is_msse = doi.lower().startswith("10.1249/mss.") or "med sci sports exerc" in journal.lower()
    is_sports_medicine = doi.lower().startswith("10.1007/s40279-")
    is_jamanetwork = doi.lower().startswith("10.1001/")
    is_heart = doi.lower().startswith("10.1136/heartjnl-")
    is_bjsm = doi.lower().startswith("10.1136/bjsports-")
    content = None
    step = 1

    # JAMA Network (JAMA + JAMA Cardio + sub-journals): Cloudflare + JS-rendered
    # citation_pdf_url. Only Playwright + article-page warmup works.
    if is_jamanetwork:
        print(f"  [{step}] JAMA Network Playwright...")
        content = _try_jamanetwork_playwright(doi)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        print(f"  [FAIL] JAMA Network Playwright 失敗")
        return None

    # MSSE: tokenized journals.lww.com endpoint (pure HTTP, 3 hops).
    if is_msse:
        print(f"  [{step}] LWW tokenized PDF (MSSE)...")
        content = _try_lww_direct(doi)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        print(f"  [FAIL] MSSE LWW direct 失敗")
        return None

    # Springer Sports Medicine: link.springer.com gates plain HTTP with a JS
    # challenge; Playwright + article warmup is the only thing that works.
    if is_sports_medicine:
        print(f"  [{step}] Springer Playwright (Sports Medicine)...")
        content = _try_springer_playwright(doi)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        print(f"  [FAIL] Sports Medicine Springer Playwright 失敗")
        return None

    # [1] Direct PDF URL
    print(f"  [{step}] Direct PDF URL...")
    content = _try_direct(doi, journal)
    if content and _is_lww_paywall_pdf(content):
        print("  [reject] LWW paywall stub from direct URL")
        content = None
    step += 1

    # [2] DOI redirect
    if not content:
        print(f"  [{step}] DOI redirect...")
        content = _try_doi_redirect(doi)
        if content and _is_lww_paywall_pdf(content):
            print("  [reject] LWW paywall stub from DOI redirect")
            content = None
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

    # BMJ journals (Heart, BJSM): most closed-access articles are available via
    # ProQuest under NCKU IP auth after the cheaper OA/direct methods fail.
    if not content and (is_heart or is_bjsm):
        print(f"  [{step}] ProQuest fallback ({'Heart' if is_heart else 'BJSM'})...")
        content = _try_proquest_playwright(doi)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        step += 1

    # [7] Playwright NEJM session (persistent Chrome + homepage warmup + session cookies)
    is_nejm = doi.startswith("10.1056/") or "nejm" in journal.lower() or "new england" in journal.lower()
    if not content and is_nejm:
        print(f"  [{step}] Playwright NEJM session...")
        fake_article = {"doi": doi, "pmid": "manual", "authors": [], "year": "0000"}
        results = playwright_nejm_batch_download([fake_article], out_dir)
        content = results.get(doi)
        step += 1

    # [8] Playwright OUP session (EHJ etc.; Crossref -> article-pdf URL + browser cookies)
    is_oup = doi.startswith("10.1093/")
    if not content and is_oup:
        print(f"  [{step}] Playwright OUP session...")
        fake_article = {"doi": doi, "pmid": "manual", "authors": [], "year": "0000"}
        results = playwright_oup_batch_download([fake_article], out_dir)
        content = results.get(doi)
        step += 1

    # [9] nodriver (Cloudflare-protected sites: JAMA etc.; NEJM/OUP last resort)
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
    normalized_source = _normalize_doi(source)
    if re.match(r"^10\.\d{4,}/\S+", normalized_source):
        return [normalized_source]

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
        line = _normalize_doi(line)
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
