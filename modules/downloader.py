"""Phase 3: PDF download — institution IP auth, Chrome TLS fingerprint via curl_cffi."""

import os
import re
import time
import logging
import warnings
from pathlib import Path
from urllib.parse import urljoin, urlparse

# Suppress noisy asyncio warning when nodriver closes Chrome
warnings.filterwarnings("ignore", message=".*Loop.*that handles pid.*is closed.*")
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

from dotenv import load_dotenv
load_dotenv()

try:
    from curl_cffi import requests
    IMPERSONATE = "chrome120"
except ImportError:
    import requests
    IMPERSONATE = None
    print("[warn] curl_cffi not found, falling back to requests (may fail on Cloudflare sites)")

UNPAYWALL_EMAIL = "researcher@example.com"
ELSEVIER_API_KEY = os.getenv("ELSEVIER_API_KEY", "")
TIMEOUT = 30
PDF_DIR = Path("output/pdfs")
FAILURES_LOG = Path("output/download_failures.log")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

logger = logging.getLogger(__name__)


def _get(url: str, **kwargs) -> requests.Response:
    """HTTP GET with Chrome TLS fingerprint (bypasses Cloudflare bot detection)."""
    if IMPERSONATE:
        return requests.get(url, impersonate=IMPERSONATE, timeout=TIMEOUT, **kwargs)
    return requests.get(url, headers=HEADERS, timeout=TIMEOUT, **kwargs)


def _is_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"


def _pdf_filename(article: dict) -> str:
    pmid = article.get("pmid", "unknown")
    authors = article.get("authors", [])
    first_author = re.sub(r"[^\w]", "", authors[0].split()[0]) if authors else "unknown"
    year = article.get("year", "0000")
    return f"{pmid}_{first_author}_{year}.pdf"


def _is_eurointervention_journal(journal: str) -> bool:
    return "eurointervention" in journal.lower()


def _direct_pdf_urls(doi: str, journal: str) -> list[str]:
    """Return known direct PDF URL candidates for this journal."""
    j = journal.lower()
    if "engl j med" in j or "nejm" in j or "new england journal" in j:
        return [
            f"https://www.nejm.org/doi/pdf/{doi}",
            f"https://www.nejm.org/doi/pdf/{doi}?articleTools=true",
        ]
    if "jama" in j:
        return [
            f"https://jamanetwork.com/journals/jama/fullarticle/{doi}",
        ]
    if "lancet" in j:
        return [
            f"https://www.thelancet.com/action/showPdf?pii={doi.split('/')[-1]}",
        ]
    if "am coll cardiol" in j or "jacc" in j or "american college of cardiology" in j:
        return [
            f"https://www.jacc.org/doi/pdf/{doi}",
        ]
    if "eurointervention" in j:
        return [
            f"https://eurointervention.pcronline.com/doi/{doi}/pdf",
            f"https://eurointervention.pcronline.com/doi/pdf/{doi}",
        ]
    return []


def _try_direct(doi: str, journal: str) -> bytes | None:
    """Try known direct PDF URLs (no landing page needed)."""
    for url in _direct_pdf_urls(doi, journal):
        try:
            resp = _get(url, allow_redirects=True)
            if resp.status_code == 200 and _is_pdf(resp.content):
                logger.debug(f"Direct PDF hit: {url}")
                return resp.content
        except Exception as e:
            logger.debug(f"Direct PDF failed ({url}): {e}")
    return None


def _primo_login(page) -> bool:
    """
    Log in to NCKU Primo via ADFS SSO.
    Requires PRIMO_USER and PRIMO_PASS in .env.
    Returns True if login succeeded.
    """
    primo_user = os.getenv("PRIMO_USER", "")
    primo_pass = os.getenv("PRIMO_PASS", "")
    if not primo_user or not primo_pass:
        logger.debug("Primo credentials not set (PRIMO_USER / PRIMO_PASS)")
        return False

    try:
        page.goto(
            "https://ncku.primo.exlibrisgroup.com/discovery/search"
            "?vid=886NCKU_INST:886NCKU_INST",
            timeout=30000,
        )
        page.wait_for_load_state("networkidle", timeout=15000)

        # Click login button
        login_btn = page.query_selector(
            '[data-testid="sign-in-button"], a:has-text("Sign In"), button:has-text("登入")'
        )
        if not login_btn:
            logger.debug("Primo: login button not found")
            return False
        login_btn.click()
        page.wait_for_load_state("networkidle", timeout=15000)

        # Fill ADFS form
        page.fill('input[name="UserName"]', primo_user)
        page.fill('input[name="Password"]', primo_pass)
        page.keyboard.press("Enter")
        # Wait for SAML redirect to complete back to Primo
        try:
            page.wait_for_url("**primo.exlibrisgroup.com**", timeout=30000)
            logger.debug("Primo login successful")
            return True
        except Exception:
            # May already be on Primo after fast redirect
            if "primo.exlibrisgroup.com" in page.url:
                logger.debug("Primo login successful (already redirected)")
                return True
            logger.debug(f"Primo login failed or timed out, URL: {page.url}")
            return False
    except Exception as e:
        logger.debug(f"Primo login error: {e}")
        return False


def _resolve_pii(doi: str) -> str | None:
    """Resolve Elsevier DOI to PII via linkinghub (no browser needed)."""
    try:
        resp = _get(f"https://doi.org/{doi}", allow_redirects=True)
        m = re.search(r"/pii/([A-Z0-9]+)", resp.url)
        if m:
            return m.group(1)
        m = re.search(r"/pii/([A-Z0-9]+)", resp.text)
        if m:
            return m.group(1)
    except Exception as e:
        logger.debug(f"PII resolve failed for {doi}: {e}")
    return None


async def _nodriver_wait_for_cloudflare(tab, max_wait: int = 30) -> bool:
    """Wait for Cloudflare challenge to auto-resolve. Returns True if page is accessible."""
    import asyncio

    for _ in range(max_wait // 2):
        title = await tab.evaluate("document.title")
        body_start = await tab.evaluate("document.body.innerText.substring(0, 100)")
        if "robot" not in body_start.lower() and "請稍候" not in title and "just a moment" not in title.lower():
            return True
        logger.debug(f"Cloudflare challenge active, waiting... (title={title})")
        await asyncio.sleep(2)
    return False


async def _nodriver_download_one(browser, pii: str) -> bytes | None:
    """Download a single Elsevier PDF using an existing nodriver browser instance."""
    import base64

    try:
        # Step 1: open article page (institution IP grants access)
        article_url = f"https://www.sciencedirect.com/science/article/pii/{pii}"
        tab = await browser.get(article_url)
        await tab.sleep(5)

        # Wait for Cloudflare challenge if present
        if not await _nodriver_wait_for_cloudflare(tab):
            logger.debug("nodriver: Cloudflare challenge did not resolve")
            return None

        await tab.sleep(3)
        title = await tab.evaluate("document.title")
        logger.debug(f"Article page: {title}")

        # Check for access
        has_access = await tab.evaluate(
            'document.body.innerText.includes("Download PDF")'
            ' || document.body.innerText.includes("View PDF")'
        )
        if not has_access:
            logger.debug("nodriver: no institutional access on article page")
            return None

        # Step 2: navigate to /pdf viewer
        pdf_viewer_url = f"https://www.sciencedirect.com/science/article/pii/{pii}/pdf"
        tab2 = await browser.get(pdf_viewer_url)
        await tab2.sleep(5)

        if not await _nodriver_wait_for_cloudflare(tab2):
            logger.debug("nodriver: Cloudflare challenge on PDF viewer")
            return None

        await tab2.sleep(5)
        ct = await tab2.evaluate("document.contentType")
        if ct != "application/pdf":
            logger.debug(f"nodriver: /pdf page not PDF (ct={ct})")
            return None

        # Step 3: extract PDF bytes via sync XHR (same-origin)
        pdf_b64 = await tab2.evaluate('''
            (() => {
                const xhr = new XMLHttpRequest();
                xhr.open('GET', window.location.href, false);
                xhr.overrideMimeType('text/plain; charset=x-user-defined');
                xhr.send(null);
                if (xhr.status === 200) {
                    let binary = '';
                    const text = xhr.responseText;
                    for (let i = 0; i < text.length; i++) {
                        binary += String.fromCharCode(text.charCodeAt(i) & 0xff);
                    }
                    return btoa(binary);
                }
                return null;
            })()
        ''')

        if pdf_b64 and isinstance(pdf_b64, str):
            content = base64.b64decode(pdf_b64)
            if _is_pdf(content):
                logger.debug(f"nodriver: got PDF ({len(content)} bytes)")
                return content
        logger.debug("nodriver: failed to extract PDF bytes")
        return None
    except Exception as e:
        logger.debug(f"nodriver download_one failed for {pii}: {e}")
        return None



def _try_nodriver(doi: str) -> bytes | None:
    """
    Use nodriver (undetected Chrome, visible window) to download a single Elsevier PDF.
    For batch downloads, prefer nodriver_batch_download() to share one browser session.
    """
    import asyncio

    try:
        import nodriver as uc
    except ImportError:
        logger.debug("nodriver not installed, skipping")
        return None

    pii = _resolve_pii(doi)
    if not pii:
        logger.debug(f"nodriver: cannot resolve PII for {doi}")
        return None
    logger.debug(f"Resolved PII: {pii}")

    async def _download():
        browser = await uc.start(headless=False)
        try:
            return await _nodriver_download_one(browser, pii)
        finally:
            browser.stop()

    try:
        return asyncio.run(_download())
    except Exception as e:
        logger.debug(f"nodriver failed for {doi}: {e}")
        return None


def nodriver_batch_download(articles: list[dict], out_dir: Path = PDF_DIR) -> dict[str, bytes]:
    """
    Download multiple Elsevier/OUP PDFs using a SINGLE nodriver browser instance.
    Avoids Cloudflare rate-limiting by reusing the same Chrome session.
    Returns {doi: pdf_bytes} for successful downloads.
    """
    import asyncio

    try:
        import nodriver as uc
    except ImportError:
        logger.debug("nodriver not installed, skipping batch")
        return {}

    # Resolve all PIIs first (via curl, no browser needed)
    doi_pii = {}
    for article in articles:
        doi = article.get("doi", "")
        if not doi or not doi.startswith("10.1016/"):
            continue
        dest = out_dir / _pdf_filename(article)
        if dest.exists() and dest.stat().st_size > 10_000:
            continue
        pii = _resolve_pii(doi)
        if pii:
            doi_pii[doi] = pii

    if not doi_pii:
        return {}

    async def _batch():
        results = {}
        failed_dois = []
        browser = await uc.start(headless=False)
        try:
            for i, (doi, pii) in enumerate(doi_pii.items()):
                logger.debug(f"nodriver batch [{i+1}/{len(doi_pii)}]: {doi} (PII={pii})")
                content = await _nodriver_download_one(browser, pii)
                if content:
                    results[doi] = content
                else:
                    failed_dois.append((doi, pii))
                if i < len(doi_pii) - 1:
                    await asyncio.sleep(2)  # polite delay between articles

            # Retry failed articles — Cloudflare cookie may now be valid
            # after the first successful page load cleared the challenge
            if failed_dois and results:
                logger.debug(f"Retrying {len(failed_dois)} failed article(s)...")
                for doi, pii in failed_dois:
                    await asyncio.sleep(2)
                    content = await _nodriver_download_one(browser, pii)
                    if content:
                        results[doi] = content
        finally:
            browser.stop()
        return results

    try:
        return asyncio.run(_batch())
    except Exception as e:
        logger.debug(f"nodriver batch failed: {e}")
        return {}


def _try_elsevier_api(doi: str) -> bytes | None:
    """Try Elsevier Article Retrieval API (requires ELSEVIER_API_KEY in .env)."""
    if not ELSEVIER_API_KEY:
        return None
    try:
        # Extract PII from DOI redirect to ScienceDirect
        resp = _get(f"https://doi.org/{doi}", allow_redirects=True)
        pii_match = re.search(r"/pii/([A-Z0-9]+)", resp.url)
        if not pii_match:
            # Try to get PII from page content
            pii_match = re.search(r'"pii"\s*:\s*"([A-Z0-9]+)"', resp.text)
        if not pii_match:
            logger.debug(f"Elsevier API: cannot extract PII for {doi}")
            return None

        pii = pii_match.group(1)
        pdf_url = (
            f"https://api.elsevier.com/content/article/pii/{pii}"
            f"?apiKey={ELSEVIER_API_KEY}&httpAccept=application/pdf"
        )
        pdf_resp = _get(pdf_url, allow_redirects=True)
        if pdf_resp.status_code == 200 and _is_pdf(pdf_resp.content):
            return pdf_resp.content
        logger.debug(f"Elsevier API returned {pdf_resp.status_code} for {pii}")
    except Exception as e:
        logger.debug(f"Elsevier API failed for {doi}: {e}")
    return None


def _try_doi_redirect(doi: str) -> bytes | None:
    """Follow DOI → landing page → find PDF link."""
    try:
        resp = _get(f"https://doi.org/{doi}", allow_redirects=True)
        resp.raise_for_status()
        if _is_pdf(resp.content):
            return resp.content

        final_url = resp.url
        base = f"{urlparse(final_url).scheme}://{urlparse(final_url).netloc}"
        if "eurointervention.pcronline.com" in urlparse(final_url).netloc and not final_url.rstrip("/").endswith("/pdf"):
            pdf_resp = _get(final_url.rstrip("/") + "/pdf", allow_redirects=True)
            pdf_resp.raise_for_status()
            if _is_pdf(pdf_resp.content):
                return pdf_resp.content
        pdf_url = _find_pdf_link(resp.text, base, final_url)
        if pdf_url:
            pdf_resp = _get(pdf_url, allow_redirects=True)
            pdf_resp.raise_for_status()
            if _is_pdf(pdf_resp.content):
                return pdf_resp.content
    except Exception as e:
        logger.debug(f"DOI redirect failed for {doi}: {e}")
    return None


def _find_pdf_link(html: str, base: str, page_url: str) -> str | None:
    # 1. citation_pdf_url meta tag (JAMA, many publishers)
    m = re.search(r'citation_pdf_url[^>]*content=["\']([^"\']+)["\']', html)
    if not m:
        m = re.search(r'content=["\']([^"\']+)["\'][^>]*citation_pdf_url', html)
    if m:
        url = m.group(1)
        if url.startswith("http"):
            return url

    # 2. href patterns
    patterns = [
        r'href=["\']([^"\']*\.pdf[^"\']*)["\']',
        r'href=["\']([^"\']*[Pp][Dd][Ff][^"\']*)["\']',
        r'href=["\']([^"\']*/pdf/[^"\']*)["\']',
        r'href=["\']([^"\']*article[^"\']*pdf[^"\']*)["\']',
    ]
    for pattern in patterns:
        for match in re.findall(pattern, html):
            if match.startswith("http"):
                return match
            elif match.startswith("/"):
                return base + match
            else:
                return urljoin(page_url, match)
    return None


def _resolve_oup_pdf_url(doi: str) -> str | None:
    """Get OUP PDF URL from Crossref API (no Cloudflare)."""
    try:
        resp = _get(f"https://api.crossref.org/works/{doi}", allow_redirects=True)
        resp.raise_for_status()
        data = resp.json()["message"]
        for link in data.get("link", []):
            url = link.get("URL", "")
            if "article-pdf" in url:
                return url
    except Exception as e:
        logger.debug(f"Crossref lookup failed for {doi}: {e}")
    return None


def playwright_oup_batch_download(
    articles: list[dict], out_dir: Path = PDF_DIR
) -> dict[str, bytes]:
    """Download multiple OUP PDFs using a SINGLE Playwright Chrome session.
    Uses system Chrome + persistent context to bypass Cloudflare.
    Returns {doi: pdf_bytes} for successful downloads.
    """
    import base64
    import tempfile
    import shutil

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.debug("playwright not installed, skipping OUP batch")
        return {}

    # Resolve PDF URLs via Crossref first (no browser needed)
    doi_pdf_url: dict[str, str] = {}
    for article in articles:
        doi = article.get("doi", "")
        if not doi or not doi.startswith("10.1093/"):
            continue
        dest = out_dir / _pdf_filename(article)
        if dest.exists() and dest.stat().st_size > 10_000:
            continue
        pdf_url = _resolve_oup_pdf_url(doi)
        if pdf_url:
            doi_pdf_url[doi] = pdf_url

    if not doi_pdf_url:
        return {}

    user_data = tempfile.mkdtemp(prefix="pw_oup_")
    results: dict[str, bytes] = {}
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data,
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
            page = context.pages[0] if context.pages else context.new_page()

            # Visit first article page to establish Cloudflare cookie
            first_doi = next(iter(doi_pdf_url))
            page.goto(f"https://doi.org/{first_doi}", timeout=30000)
            page.wait_for_load_state("domcontentloaded", timeout=15000)
            time.sleep(3)
            logger.debug(f"OUP session established: {page.title()[:60]}")

            for i, (doi, pdf_url) in enumerate(doi_pdf_url.items()):
                logger.debug(f"OUP batch [{i+1}/{len(doi_pdf_url)}]: {doi}")
                try:
                    page.goto(pdf_url, timeout=60000, wait_until="commit")
                    time.sleep(5)

                    ct = page.evaluate("document.contentType")
                    if ct != "application/pdf":
                        logger.debug(f"OUP: not PDF (ct={ct}) for {doi}")
                        continue

                    pdf_b64 = page.evaluate("""
                        (() => {
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', window.location.href, false);
                            xhr.overrideMimeType('text/plain; charset=x-user-defined');
                            xhr.send(null);
                            if (xhr.status === 200) {
                                let binary = '';
                                const text = xhr.responseText;
                                for (let i = 0; i < text.length; i++) {
                                    binary += String.fromCharCode(text.charCodeAt(i) & 0xff);
                                }
                                return btoa(binary);
                            }
                            return null;
                        })()
                    """)
                    if pdf_b64 and isinstance(pdf_b64, str):
                        content = base64.b64decode(pdf_b64)
                        if _is_pdf(content):
                            results[doi] = content
                            logger.debug(f"OUP: got PDF ({len(content)} bytes)")
                except Exception as e:
                    logger.debug(f"OUP batch failed for {doi}: {e}")
                time.sleep(2)

            context.close()
    except Exception as e:
        logger.debug(f"Playwright OUP batch failed: {e}")
    finally:
        shutil.rmtree(user_data, ignore_errors=True)
    return results


def _resolve_pmcid(doi: str) -> str | None:
    """Convert DOI to PMCID via NCBI ID converter."""
    try:
        resp = _get(
            f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
            f"?ids={doi}&format=json&email={UNPAYWALL_EMAIL}",
        )
        resp.raise_for_status()
        records = resp.json().get("records", [])
        if records and "pmcid" in records[0]:
            return records[0]["pmcid"]
    except Exception as e:
        logger.debug(f"PMCID lookup failed for {doi}: {e}")
    return None



def _try_unpaywall(doi: str) -> bytes | None:
    """Try Unpaywall OA PDF locations."""
    try:
        resp = _get(f"https://api.unpaywall.org/v2/{doi}?email={UNPAYWALL_EMAIL}")
        resp.raise_for_status()
        data = resp.json()
        locations = data.get("oa_locations") or []
        best = data.get("best_oa_location")
        if best:
            locations = [best] + [l for l in locations if l != best]
        for loc in locations:
            pdf_url = loc.get("url_for_pdf")
            if not pdf_url:
                continue
            try:
                pdf_resp = _get(pdf_url, allow_redirects=True)
                pdf_resp.raise_for_status()
                if _is_pdf(pdf_resp.content):
                    return pdf_resp.content
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"Unpaywall failed for {doi}: {e}")
    return None


def download_pdf(article: dict, out_dir: Path = PDF_DIR) -> Path | None:
    """
    Download PDF for one article. Returns path if successful, None otherwise.
    Strategy: 1) Direct PDF URL  2) DOI redirect  3) nodriver  4) Elsevier API  5) Unpaywall OA
    nodriver uses real Chrome (visible) to bypass Cloudflare on ScienceDirect/JACC.
    """
    doi = article.get("doi", "")
    if not doi:
        _log_failure(article, "No DOI")
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / _pdf_filename(article)

    if dest.exists() and dest.stat().st_size > 10_000:
        print(f"  [skip] Already downloaded: {dest.name}")
        return dest

    journal = article.get("journal", "")
    is_eurointervention = _is_eurointervention_journal(journal)
    is_elsevier = doi.startswith("10.1016/")
    content = None

    print(f"  [1] Direct PDF URL ({journal[:30]})...")
    content = _try_direct(doi, journal)

    if not content:
        print(f"  [2] DOI redirect...")
        content = _try_doi_redirect(doi)

    if not content and is_eurointervention:
        _log_failure(article, "EuroIntervention blocked by subscription wall")
        print(f"  [FAIL] {doi} (EuroIntervention subscription wall)")
        return None

    if not content and is_elsevier:
        print(f"  [3] nodriver (ScienceDirect)...")
        content = _try_nodriver(doi)

    if not content and ELSEVIER_API_KEY and is_elsevier:
        print(f"  [4] Elsevier API...")
        content = _try_elsevier_api(doi)

    if not content:
        content = _try_unpaywall(doi)

    if content:
        dest.write_bytes(content)
        print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
        return dest

    _log_failure(article, "PDF not found via all methods")
    print(f"  [FAIL] {doi}")
    return None


def _log_failure(article: dict, reason: str):
    FAILURES_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(FAILURES_LOG, "a") as f:
        f.write(f"{article.get('pmid','?')} | {article.get('doi','?')} | {reason}\n")


def download_articles(articles: list[dict], out_dir: Path = PDF_DIR) -> dict[str, Path | None]:
    """Download PDFs for multiple articles. Returns {pmid: path_or_None}.

    Elsevier articles (DOI 10.1016/*) that fail direct/DOI-redirect download
    are batched into a single nodriver Chrome session to avoid Cloudflare
    rate-limiting from opening multiple browser instances.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    # ── Pass 1: try non-browser methods for all articles ──────────────
    elsevier_pending: list[dict] = []
    oup_pending: list[dict] = []

    for i, article in enumerate(articles, 1):
        title = article.get("title", "")[:60]
        pmid = article.get("pmid", "?")
        doi = article.get("doi", "")
        print(f"\n[{i}/{len(articles)}] {title}...")

        dest = out_dir / _pdf_filename(article)
        if dest.exists() and dest.stat().st_size > 10_000:
            print(f"  [skip] Already downloaded: {dest.name}")
            results[pmid] = dest
            continue

        journal = article.get("journal", "")
        is_eurointervention = _is_eurointervention_journal(journal)
        content = None

        print(f"  [1] Direct PDF URL ({journal[:30]})...")
        content = _try_direct(doi, journal)

        if not content:
            print(f"  [2] DOI redirect...")
            content = _try_doi_redirect(doi)

        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            results[pmid] = dest
        elif is_eurointervention:
            _log_failure(article, "EuroIntervention blocked by subscription wall")
            print(f"  [FAIL] {doi} (EuroIntervention subscription wall)")
            results[pmid] = None
        elif doi.startswith("10.1016/"):
            if ELSEVIER_API_KEY:
                print(f"  [3] Elsevier API...")
                content = _try_elsevier_api(doi)
            if not content:
                print(f"  [4] Unpaywall...")
                content = _try_unpaywall(doi)
            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                print(f"  [pending] queued for nodriver batch (Elsevier, last resort)")
                elsevier_pending.append(article)
        elif doi.startswith("10.1093/"):
            print(f"  [pending] queued for Playwright batch (OUP)")
            oup_pending.append(article)
        else:
            # Other publishers: try Unpaywall as last resort
            content = _try_unpaywall(doi)
            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                _log_failure(article, "PDF not found via all methods")
                print(f"  [FAIL] {doi}")
                results[pmid] = None

        time.sleep(1)

    # ── Pass 2a: batch download OUP articles via Playwright ────────────
    if oup_pending:
        print(f"\n{'─'*50}")
        print(f"  Playwright batch: downloading {len(oup_pending)} OUP PDF(s)...")
        print(f"  (opening Chrome, please wait)\n")

        oup_results = playwright_oup_batch_download(oup_pending, out_dir)

        for article in oup_pending:
            pmid = article.get("pmid", "?")
            doi = article.get("doi", "")
            dest = out_dir / _pdf_filename(article)
            content = oup_results.get(doi)

            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                content = _try_unpaywall(doi)
                if content:
                    dest.write_bytes(content)
                    print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                    results[pmid] = dest
                else:
                    _log_failure(article, "PDF not found via all methods")
                    print(f"  [FAIL] {doi}")
                    results[pmid] = None

    # ── Pass 2b: batch download Elsevier articles via nodriver ─────────
    if elsevier_pending:
        print(f"\n{'─'*50}")
        print(f"  nodriver batch: downloading {len(elsevier_pending)} Elsevier PDF(s)...")
        print(f"  (opening one Chrome window, please wait)\n")

        batch_results = nodriver_batch_download(elsevier_pending, out_dir)

        for article in elsevier_pending:
            pmid = article.get("pmid", "?")
            doi = article.get("doi", "")
            dest = out_dir / _pdf_filename(article)
            content = batch_results.get(doi)

            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                _log_failure(article, "PDF not found via all methods")
                print(f"  [FAIL] {doi}")
                results[pmid] = None

    return results


if __name__ == "__main__":
    test_articles = [
        {
            "pmid": "39820077",
            "title": "Echocardiographic Diastolic Function Grading in HFpEF",
            "doi": "10.1016/j.jacc.2025.11.024",
            "authors": ["TestAuthor"],
            "journal": "Journal of the American College of Cardiology",
            "year": "2026",
        },
        {
            "pmid": "41811272",
            "title": "Designing Next-Generation Cardiometabolic Outcome Trials",
            "doi": "10.1016/j.jacc.2026.01.020",
            "authors": ["Platz"],
            "journal": "Journal of the American College of Cardiology",
            "year": "2026",
        },
        {
            "pmid": "41778690",
            "title": "Beta-Blocker vs Calcium-Channel Blocker in Non-Obstructive HCM",
            "doi": "10.1016/j.jacc.2025.11.028",
            "authors": ["Bjerregaard"],
            "journal": "Journal of the American College of Cardiology",
            "year": "2026",
        },
    ]
    print("=== Testing JACC PDF download (batch mode) ===\n")
    results = download_articles(test_articles, out_dir=Path("output/test_pdfs"))
    print("\n--- Summary ---")
    for pmid, path in results.items():
        print(f"  {pmid}: {'OK → ' + str(path) if path else 'FAILED'}")
