"""Phase 3: PDF download — institution IP auth, Chrome TLS fingerprint via curl_cffi."""

import os
import re
import time
import logging
import shutil
import tempfile
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
PRIMO_CIRCULATION_URL = (
    "https://ncku.primo.exlibrisgroup.com/discovery/fulldisplay"
    "?docid=alma991011715469707978&context=L&vid=886NCKU_INST:886NCKU_INST"
    "&lang=zh-tw&search_scope=MyInst_and_CI&adaptor=Local%20Search%20Engine"
    "&tab=Everything&query=any,contains,circulation&offset=0"
)


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


def _is_jacc_article(article: dict) -> bool:
    journal = article.get("journal", "").lower()
    doi = article.get("doi", "").lower()
    return (
        "jacc" in journal
        or "am coll cardiol" in journal
        or "american college of cardiology" in journal
        or doi.startswith("10.1016/j.jacc.")
    )


def _is_circulation_article(article: dict) -> bool:
    journal = article.get("journal", "").lower()
    return "circulation" in journal


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


def _fill_sso_credentials(page) -> bool:
    primo_user = os.getenv("PRIMO_USER", "")
    primo_pass = os.getenv("PRIMO_PASS", "")
    if not primo_user or not primo_pass:
        return False

    selectors = [
        ('input[placeholder="帳號:"]', 'input[placeholder="密碼:"]'),
        ('input[placeholder*="帳號"]', 'input[placeholder*="密碼"]'),
        ('input[name="UserName"]', 'input[name="Password"]'),
        ('input[name="username"]', 'input[name="password"]'),
        ('input[id*="user"]', 'input[id*="pass"]'),
        ('input[name*="user"]', 'input[name*="pass"]'),
        ('input[type="email"]', 'input[type="password"]'),
        ('input[type="text"]', 'input[type="password"]'),
    ]

    try:
        page.wait_for_load_state("domcontentloaded", timeout=30000)
    except Exception:
        pass

    for user_selector, pass_selector in selectors:
        try:
            page.wait_for_selector(user_selector, state="visible", timeout=10000)
            page.wait_for_selector(pass_selector, state="visible", timeout=10000)
            page.fill(user_selector, primo_user)
            page.fill(pass_selector, primo_pass)
            _submit_login_form(page)
            _dismiss_post_login_notice(page)
            return True
        except Exception:
            continue

    try:
        user_input = page.locator("input").filter(has_not=page.locator('input[type="hidden"]')).first
        password_input = page.locator('input[type="password"]').first
        if user_input.count() and password_input.count():
            user_input.fill(primo_user)
            password_input.fill(primo_pass)
            _submit_login_form(page)
            _dismiss_post_login_notice(page)
            return True
    except Exception as e:
        logger.debug(f"SSO credential fill failed: {e}")
    return False


def _submit_login_form(page):
    button_patterns = [
        re.compile("登入"),
        re.compile("login", re.I),
        re.compile("sign in", re.I),
    ]
    for pattern in button_patterns:
        try:
            button = page.get_by_role("button", name=pattern).first
            if button.count() and button.is_visible():
                button.click()
                return
        except Exception:
            continue
    page.keyboard.press("Enter")


def _dismiss_post_login_notice(page):
    patterns = [
        re.compile(r"^OK$", re.I),
        re.compile("提醒"),
    ]
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            for pattern in patterns:
                button = page.get_by_role("button", name=pattern).first
                if button.count() and button.is_visible():
                    button.click()
                    time.sleep(1)
                    return
            ok_text = page.get_by_text(re.compile(r"^OK$", re.I)).first
            if ok_text.count() and ok_text.is_visible():
                ok_text.click()
                time.sleep(1)
                return
        except Exception:
            pass
        time.sleep(0.5)


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


def _page_has_pdf_full_text(page) -> bool:
    try:
        locator = page.get_by_text("PDF Full Text", exact=False)
        return locator.count() > 0 and locator.first.is_visible()
    except Exception:
        return False


def _click_ovid_link(page):
    link = page.get_by_role("link", name=re.compile("ovid", re.I)).first
    context = page.context
    existing_pages = len(context.pages)
    try:
        with context.expect_page(timeout=10000) as popup_info:
            link.click()
        popup = popup_info.value
        popup.wait_for_load_state("domcontentloaded", timeout=30000)
        return popup
    except Exception:
        pass

    try:
        with page.expect_popup(timeout=5000) as popup_info:
            link.click()
        popup = popup_info.value
        popup.wait_for_load_state("domcontentloaded", timeout=30000)
        return popup
    except Exception:
        try:
            link.click()
        except Exception:
            pass
        deadline = time.time() + 10
        while time.time() < deadline:
            if len(context.pages) > existing_pages:
                popup = context.pages[-1]
                try:
                    popup.wait_for_load_state("domcontentloaded", timeout=30000)
                except Exception:
                    pass
                return popup
            time.sleep(0.5)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        return page


def _find_search_input(page):
    preferred_selectors = [
        "#jb-search-keywords-textbox",
        'input[name="jb-search-keywords-textbox"]',
        'input[placeholder="Enter Keywords"]',
    ]
    for selector in preferred_selectors:
        try:
            candidate = page.locator(selector).first
            if candidate.count() and candidate.is_visible() and candidate.is_enabled():
                return candidate
        except Exception:
            continue

    inputs = page.locator("input")
    for i in range(inputs.count()):
        candidate = inputs.nth(i)
        try:
            if not candidate.is_visible() or not candidate.is_enabled():
                continue
            attrs = " ".join(
                filter(
                    None,
                    [
                        candidate.get_attribute("type"),
                        candidate.get_attribute("name"),
                        candidate.get_attribute("id"),
                        candidate.get_attribute("placeholder"),
                        candidate.get_attribute("aria-label"),
                    ],
                )
            ).lower()
            if any(token in attrs for token in ["search", "query", "keyword"]):
                return candidate
            if (candidate.get_attribute("type") or "").lower() in {"search", "text", ""}:
                return candidate
        except Exception:
            continue
    return None


def _search_ovid_advanced_title(page, title: str) -> bool:
    if not title:
        return False

    title_input = page.locator("#jb-search-title-textbox").first
    submit_button = page.locator('input[name="submit:Journal Browse Perform Search|1"]').first
    all_issues_radio = page.locator('input[name="search_area"][value="ai"]').first

    if not title_input.count() or not title_input.is_visible():
        return False

    try:
        advanced_radio = page.locator("#advance").first
        if advanced_radio.count() and advanced_radio.is_visible():
            advanced_radio.check(force=True)
            time.sleep(1)
        if all_issues_radio.count() and all_issues_radio.is_visible():
            all_issues_radio.check(force=True)
        title_input.click()
        title_input.fill("")
        title_input.fill(title)
        if submit_button.count() and submit_button.is_visible():
            submit_button.click()
        else:
            title_input.press("Enter")
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        time.sleep(3)
        return True
    except Exception as e:
        logger.debug(f"Ovid advanced title search failed for {title}: {e}")
        return False


def _search_ovid_article(page, article: dict) -> bool:
    title = article.get("title", "").strip()
    search_titles = _ovid_title_queries(title)

    for candidate_title in search_titles:
        if _open_ovid_article_pdf(page, candidate_title):
            return True
        if _open_ovid_article_link(page, candidate_title):
            return _page_has_pdf_full_text(page)

    for query in search_titles:
        if not _search_ovid_advanced_title(page, query):
            continue

        for candidate_title in search_titles:
            if _open_ovid_article_pdf(page, candidate_title):
                return True
            if _open_ovid_article_link(page, candidate_title):
                return _page_has_pdf_full_text(page)

    return False


def _ovid_title_queries(title: str) -> list[str]:
    if not title:
        return []
    queries = [title]
    normalized = re.sub(r"[^A-Za-z0-9\\s-]", " ", title)
    normalized = re.sub(r"\\s+", " ", normalized).strip()
    if normalized and normalized != title:
        queries.append(normalized)
    words = normalized.split() if normalized else title.split()
    if len(words) >= 8:
        queries.append(" ".join(words[:8]))
    if len(words) >= 5:
        queries.append(" ".join(words[:5]))
    deduped = []
    for query in queries:
        if query and query not in deduped:
            deduped.append(query)
    return deduped


def _open_ovid_article_link(page, title: str) -> bool:
    if not title:
        return False

    title_patterns = [title]
    words = title.split()
    if len(words) >= 6:
        title_patterns.append(" ".join(words[:6]))
    if len(words) >= 10:
        title_patterns.append(" ".join(words[:10]))

    for pattern in title_patterns:
        try:
            link = page.get_by_role("link", name=re.compile(re.escape(pattern), re.I)).first
            if link.count() and link.is_visible():
                link.click()
                page.wait_for_load_state("domcontentloaded", timeout=30000)
                time.sleep(2)
                return True
        except Exception as e:
            logger.debug(f"Ovid title link click failed for {pattern}: {e}")
            continue

    return False


def _open_ovid_article_pdf(page, title: str) -> bool:
    if not title:
        return False

    try:
        result_checkbox = page.locator('input[name="R"]').filter(
            has=page.locator(f'xpath=following::*[contains(normalize-space(.), "{title[:40]}")]')
        ).first
        if result_checkbox.count():
            article_row = result_checkbox.locator(
                'xpath=ancestor::*[self::tr or self::li or self::div][1]'
            )
            pdf_link = article_row.get_by_role("link", name=re.compile("PDF Full Text", re.I)).first
            if pdf_link.count() and pdf_link.is_visible():
                pdf_link.click()
                page.wait_for_load_state("domcontentloaded", timeout=30000)
                time.sleep(3)
                return True
    except Exception as e:
        logger.debug(f"Ovid row PDF click failed: {e}")

    try:
        article_link = page.get_by_role("link", name=re.compile(re.escape(title[:40]), re.I)).first
        if article_link.count() and article_link.is_visible():
            article_row = article_link.locator('xpath=ancestor::*[self::tr or self::li or self::div][1]')
            pdf_link = article_row.get_by_role("link", name=re.compile("PDF Full Text", re.I)).first
            if pdf_link.count() and pdf_link.is_visible():
                pdf_link.click()
                page.wait_for_load_state("domcontentloaded", timeout=30000)
                time.sleep(3)
                return True
    except Exception as e:
        logger.debug(f"Ovid article-title row PDF click failed: {e}")

    return False


def _try_ovid_pdf_link(page) -> bytes | None:
    try:
        download_buttons = [
            'button[aria-label*="Download" i]',
            'cr-icon-button[aria-label*="Download" i]',
            '#download',
            '#download-button',
        ]
        for selector in download_buttons:
            button = page.locator(selector).first
            if button.count() and button.is_visible():
                with page.expect_download(timeout=15000) as download_info:
                    button.click()
                download = download_info.value
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                download.save_as(str(tmp_path))
                content = tmp_path.read_bytes()
                tmp_path.unlink(missing_ok=True)
                if _is_pdf(content):
                    return content
    except Exception as e:
        logger.debug(f"Ovid viewer download button failed: {e}")

    try:
        content_type = page.evaluate("document.contentType")
        if content_type == "application/pdf":
            pdf_url = page.url
            cookies = {cookie["name"]: cookie["value"] for cookie in page.context.cookies([pdf_url])}
            resp = _get(pdf_url, allow_redirects=True, cookies=cookies)
            if resp.status_code == 200 and _is_pdf(resp.content):
                return resp.content
    except Exception as e:
        logger.debug(f"Ovid current-page PDF fetch failed: {e}")

    try:
        pdf_link = page.get_by_role("link", name=re.compile("PDF Full Text", re.I)).first
        href = pdf_link.get_attribute("href")
        if href:
            pdf_url = urljoin(page.url, href)
            cookies = {cookie["name"]: cookie["value"] for cookie in page.context.cookies([pdf_url])}
            resp = _get(pdf_url, allow_redirects=True, cookies=cookies)
            if resp.status_code == 200 and _is_pdf(resp.content):
                return resp.content
    except Exception as e:
        logger.debug(f"Ovid PDF href fetch failed: {e}")

    try:
        pdf_link = page.get_by_role("link", name=re.compile("PDF Full Text", re.I)).first
        with page.expect_download(timeout=15000) as download_info:
            pdf_link.click()
        download = download_info.value
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        download.save_as(str(tmp_path))
        content = tmp_path.read_bytes()
        tmp_path.unlink(missing_ok=True)
        if _is_pdf(content):
            return content
    except Exception as e:
        logger.debug(f"Ovid PDF click download failed: {e}")
    return None


def _try_circulation_via_primo(article: dict) -> bytes | None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.debug("playwright not installed, skipping Circulation Ovid flow")
        return None

    primo_user = os.getenv("PRIMO_USER", "")
    primo_pass = os.getenv("PRIMO_PASS", "")
    if not primo_user or not primo_pass:
        logger.debug("Primo credentials not set for Circulation Ovid flow")
        return None

    user_data = tempfile.mkdtemp(prefix="pw_circulation_")
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data,
                headless=False,
                channel="chrome",
                accept_downloads=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(PRIMO_CIRCULATION_URL, timeout=60000)
            page.wait_for_load_state("domcontentloaded", timeout=30000)

            target_page = _click_ovid_link(page)
            time.sleep(2)

            if _fill_sso_credentials(target_page):
                try:
                    target_page.wait_for_load_state("domcontentloaded", timeout=30000)
                    time.sleep(3)
                except Exception:
                    pass

            if "primo.exlibrisgroup.com" in target_page.url and not _primo_login(target_page):
                logger.debug("Circulation flow: Primo login failed")
                context.close()
                return None

            if not _search_ovid_article(target_page, article):
                logger.debug("Circulation flow: article not found in Ovid")
                context.close()
                return None

            content = _try_ovid_pdf_link(target_page)
            context.close()
            return content
    except Exception as e:
        logger.debug(f"Circulation Primo/Ovid flow failed: {e}")
        return None
    finally:
        shutil.rmtree(user_data, ignore_errors=True)


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
    Strategy: JACC uses Elsevier API first; other journals use the existing direct/redirect flow.
    """
    doi = article.get("doi", "")

    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / _pdf_filename(article)

    if dest.exists() and dest.stat().st_size > 10_000:
        print(f"  [skip] Already downloaded: {dest.name}")
        return dest

    journal = article.get("journal", "")
    is_eurointervention = _is_eurointervention_journal(journal)
    is_jacc = _is_jacc_article(article)
    is_circulation = _is_circulation_article(article)
    if not doi and not is_circulation:
        _log_failure(article, "No DOI")
        return None
    is_elsevier = doi.startswith("10.1016/")
    content = None

    if is_circulation:
        print("  [1] Primo/Ovid (Circulation)...")
        content = _try_circulation_via_primo(article)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        _log_failure(article, "Circulation PDF not found via Primo/Ovid")
        print(f"  [FAIL] {doi or article.get('title', '')} (Circulation Primo/Ovid)")
        return None

    if is_jacc:
        print("  [1] Elsevier API (JACC)...")
        content = _try_elsevier_api(doi)

        if not content:
            print("  [2] DOI redirect...")
            content = _try_doi_redirect(doi)

        if not content:
            content = _try_unpaywall(doi)

        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest

        _log_failure(article, "JACC PDF not found via Elsevier API")
        print(f"  [FAIL] {doi} (JACC Elsevier API)")
        return None

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

    JACC articles use Elsevier API directly. Other Elsevier articles that fail
    direct/DOI-redirect download are batched into a single nodriver Chrome
    session to avoid Cloudflare rate-limiting.
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
        is_jacc = _is_jacc_article(article)
        is_circulation = _is_circulation_article(article)
        content = None

        if is_circulation:
            print("  [1] Primo/Ovid (Circulation)...")
            content = _try_circulation_via_primo(article)
            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                _log_failure(article, "Circulation PDF not found via Primo/Ovid")
                print(f"  [FAIL] {doi or title} (Circulation Primo/Ovid)")
                results[pmid] = None
            time.sleep(1)
            continue

        if is_jacc:
            print("  [1] Elsevier API (JACC)...")
            content = _try_elsevier_api(doi)

            if not content:
                print("  [2] DOI redirect...")
                content = _try_doi_redirect(doi)

            if not content:
                content = _try_unpaywall(doi)

            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                _log_failure(article, "JACC PDF not found via Elsevier API")
                print(f"  [FAIL] {doi} (JACC Elsevier API)")
                results[pmid] = None
            time.sleep(1)
            continue

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
            print(f"  [pending] queued for nodriver batch (Elsevier)")
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
                # Last resort: Elsevier API / Unpaywall
                if ELSEVIER_API_KEY and doi.startswith("10.1016/"):
                    content = _try_elsevier_api(doi)
                if not content:
                    content = _try_unpaywall(doi)
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
