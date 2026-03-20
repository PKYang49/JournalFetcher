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
    if "circulation" in j:
        return [
            f"https://www.ahajournals.org/doi/pdf/{doi}",
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


def _switch_to_ovid_advanced(page) -> bool:
    """Switch the Ovid search panel from Keyword to Advanced mode.
    Returns True if Advanced fields are now visible."""
    for selector in [
        'label[for="advance"]',
        'label:has(#advance)',
        '.wk-segmented-control-item:has(#advance)',
    ]:
        try:
            el = page.locator(selector).first
            if el.count() and el.is_visible():
                print(f"    [debug] Clicking Advanced via: {selector}")
                el.click()
                time.sleep(2)
                return True
        except Exception:
            continue

    # Fallback: click visible "Advanced" text
    try:
        adv_text = page.get_by_text("Advanced", exact=True).first
        if adv_text.count() and adv_text.is_visible():
            print(f"    [debug] Clicking 'Advanced' text...")
            adv_text.click()
            time.sleep(2)
            return True
    except Exception:
        pass

    print(f"    [debug] Could not find Advanced tab to click")
    return False


def _find_ovid_field(page, field_name: str):
    """Find an input field in the Ovid Advanced search panel by label text."""
    # Try common ID patterns
    field_lower = field_name.lower().replace(".", "").strip()
    for sel in [
        f"#jb-search-{field_lower}-textbox",
        f'input[name*="{field_lower}" i]',
        f'input[id*="{field_lower}" i]',
    ]:
        try:
            candidate = page.locator(sel).first
            if candidate.count() and candidate.is_visible():
                return candidate
        except Exception:
            continue

    # Find by label text -> next input
    try:
        label = page.get_by_text(field_name, exact=True).first
        if label.count() and label.is_visible():
            sibling_input = label.locator("xpath=following::input[1]")
            if sibling_input.count() and sibling_input.is_visible():
                return sibling_input
    except Exception:
        pass

    return None


def _submit_ovid_search(page):
    """Click Search/submit button on Ovid search panel."""
    for sel in [
        'input[name="submit:Journal Browse Perform Search|1"]',
        'input[value="Search"]',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() and btn.is_visible():
                print(f"    [debug] Clicking submit: {sel}")
                btn.click()
                page.wait_for_load_state("domcontentloaded", timeout=30000)
                time.sleep(3)
                return
        except Exception:
            continue

    try:
        search_btn = page.get_by_role("button", name="Search").first
        if search_btn.count() and search_btn.is_visible():
            print(f"    [debug] Clicking Search button")
            search_btn.click()
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            time.sleep(3)
            return
    except Exception:
        pass

    print(f"    [debug] No submit button found, pressing Enter")
    page.keyboard.press("Enter")
    page.wait_for_load_state("domcontentloaded", timeout=30000)
    time.sleep(3)


def _search_ovid_advanced(page, article: dict) -> bool:
    """Search Ovid Advanced using article metadata (volume/issue/page/year).
    Falls back to title search if those fields aren't available."""

    if not _switch_to_ovid_advanced(page):
        return False

    volume = article.get("volume", "")
    issue = article.get("issue", "")
    pages = article.get("pages", "")
    year = article.get("year", "")
    title = article.get("title", "").strip()

    # Extract first page number from range like "795-797" or "e936-e970"
    first_page = pages.split("-")[0].strip() if pages else ""

    print(f"    [debug] Advanced fields: vol={volume} issue={issue} page={first_page} year={year}")

    try:
        # Select "All Issues" to search across all volumes
        all_issues_radio = page.locator('input[name="search_area"][value="ai"]').first
        if all_issues_radio.count() and all_issues_radio.is_visible():
            all_issues_radio.click(force=True)
            print(f"    [debug] Selected 'All Issues'")

        # Strategy 1: Search by volume + page (most precise)
        if volume and first_page:
            vol_input = _find_ovid_field(page, "Vol.")
            page_input = _find_ovid_field(page, "Page")
            if vol_input and page_input:
                print(f"    [debug] Filling Vol={volume}, Page={first_page}")
                vol_input.fill(volume)
                page_input.fill(first_page)

                # Also fill issue and year if available
                if issue:
                    issue_input = _find_ovid_field(page, "Issue")
                    if issue_input:
                        issue_input.fill(issue)
                if year:
                    year_input = _find_ovid_field(page, "Year")
                    if year_input:
                        year_input.fill(year)

                _submit_ovid_search(page)
                print(f"    [debug] Search done, URL: {page.url[:120]}")
                return True

        # Strategy 2: Search by title (shorter queries)
        title_input = _find_ovid_field(page, "Title")
        if title_input and title:
            # Use first few words to avoid mismatch
            words = title.split()
            short_title = " ".join(words[:6]) if len(words) > 6 else title
            print(f"    [debug] Filling Title='{short_title[:50]}'")
            title_input.fill(short_title)

            if year:
                year_input = _find_ovid_field(page, "Year")
                if year_input:
                    year_input.fill(year)

            _submit_ovid_search(page)
            print(f"    [debug] Search done, URL: {page.url[:120]}")
            return True

        # Dump what's visible for debugging
        print(f"    [debug] Could not find Vol/Page or Title fields")
        try:
            inputs = page.locator("input:visible").all()
            for inp in inputs[:15]:
                attrs = {k: inp.get_attribute(k) for k in ["id", "name", "type", "placeholder", "class"] if inp.get_attribute(k)}
                print(f"    [debug]   visible input: {attrs}")
        except Exception:
            pass
        return False

    except Exception as e:
        print(f"    [debug] Advanced search exception: {e}")
        return False


def _search_ovid_advanced_title(page, title: str) -> bool:
    """Legacy wrapper — search by title only."""
    return _search_ovid_advanced(page, {"title": title})


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


def _click_ovid_fulltext_and_get_pdf(page, title: str) -> bytes | None:
    """
    From Ovid search results or issue listing page, find the article
    and download its PDF. Priorities:
      1. Find "PDF Full Text" link on the page (directly available in search results)
      2. Find "Ovid Full Text" → navigate to article page → get PDF there
    """
    title_words = title.split()

    # ── Strategy 1: Find "PDF Full Text" directly on this page ────────
    # After Advanced search, the results page shows "PDF Full Text" inline
    pdf_ft = None
    try:
        pdf_ft_links = page.get_by_text("PDF Full Text", exact=True)
        count = pdf_ft_links.count()
        print(f"    [debug] 'PDF Full Text' text elements on page: {count}")
        if count == 1:
            pdf_ft = pdf_ft_links.first
        elif count > 1:
            # Multiple results — ONLY use the one near our article title
            for n in [min(len(title_words), 10), 6, 3]:
                partial = " ".join(title_words[:n])
                try:
                    title_el = page.get_by_text(re.compile(re.escape(partial), re.I)).first
                    if title_el.count() and title_el.is_visible():
                        container = title_el.locator("xpath=ancestor::*[position() <= 5]")
                        nearby_pdf = container.get_by_text("PDF Full Text", exact=True).first
                        if nearby_pdf.count() and nearby_pdf.is_visible():
                            pdf_ft = nearby_pdf
                            print(f"    [debug] Matched 'PDF Full Text' near title: '{partial[:40]}'")
                            break
                except Exception:
                    continue
            if not pdf_ft:
                print(f"    [debug] {count} 'PDF Full Text' links but none matched our title — skipping")
    except Exception as e:
        print(f"    [debug] PDF Full Text search exception: {e}")

    if pdf_ft:
        try:
            if pdf_ft.count() and pdf_ft.is_visible():
                # Check if it's a link with href
                tag = pdf_ft.evaluate("el => el.tagName")
                href = pdf_ft.get_attribute("href") if tag.upper() == "A" else None

                # If not a link, try to find the parent <a>
                if not href:
                    try:
                        parent_a = pdf_ft.locator("xpath=ancestor::a[1]")
                        if parent_a.count():
                            href = parent_a.get_attribute("href")
                            pdf_ft = parent_a  # use the link element instead
                    except Exception:
                        pass

                print(f"    [debug] Found 'PDF Full Text' (tag={tag}, href={href[:80] if href else 'None'})")
                return _click_pdf_fulltext_link(page, pdf_ft)
        except Exception as e:
            print(f"    [debug] PDF Full Text click exception: {e}")

    # ── Strategy 2: Find article title → click "Ovid Full Text" → article page ──
    for n in [len(title_words), 10, 6, 3]:
        if n > len(title_words):
            continue
        partial = " ".join(title_words[:n])
        try:
            title_link = page.get_by_role(
                "link", name=re.compile(re.escape(partial), re.I)
            ).first
            if not title_link.count() or not title_link.is_visible():
                continue
            print(f"    [debug] Found title link for: '{partial[:50]}'")

            # Click "Ovid Full Text" in the same row
            row = title_link.locator(
                'xpath=ancestor::*[self::tr or self::div[contains(@class,"row")'
                ' or contains(@class,"result")]'
                " or self::li][1]"
            )
            ovid_ft = row.get_by_role(
                "link", name=re.compile("Ovid Full Text", re.I)
            ).first
            if ovid_ft.count() and ovid_ft.is_visible():
                href = ovid_ft.get_attribute("href")
                print(f"    [debug] Clicking 'Ovid Full Text', href: {href[:80] if href else 'None'}")
                if href:
                    page.goto(urljoin(page.url, href), timeout=30000)
                else:
                    ovid_ft.click()
                page.wait_for_load_state("domcontentloaded", timeout=30000)
                time.sleep(5)
                return _try_ovid_pdf_link(page)
        except Exception as e:
            print(f"    [debug] Row search exception: {e}")
            continue

    print(f"    [debug] No PDF Full Text or Ovid Full Text found")
    return None


def _click_pdf_fulltext_link(page, pdf_link) -> bytes | None:
    """Click a PDF link, handling popup/new-tab/frameset/download cases."""
    context = page.context
    href = None
    try:
        href = pdf_link.get_attribute("href")
    except Exception:
        pass

    # Navigate to the PDF link URL (Ovid uses framesets, not direct PDF)
    if href:
        pdf_url = urljoin(page.url, href)
        print(f"    [debug] Navigating to PDF link: {pdf_url[:120]}")
        page.goto(pdf_url, timeout=60000)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        time.sleep(5)

        # Check if we got a frameset with embedded PDF
        content = _extract_pdf_from_frameset(page)
        if content:
            return content

        # Check if page itself is PDF
        content = _extract_pdf_from_page(page)
        if content:
            return content

    # Try clicking instead (for non-href or JS-driven links)
    try:
        with context.expect_page(timeout=10000) as popup_info:
            pdf_link.click()
        popup = popup_info.value
        popup.wait_for_load_state("domcontentloaded", timeout=30000)
        time.sleep(5)
        content = _extract_pdf_from_frameset(popup)
        if not content:
            content = _extract_pdf_from_page(popup)
        if content:
            try:
                popup.close()
            except Exception:
                pass
            return content
    except Exception:
        pass

    # Try expect_download
    try:
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
    except Exception:
        pass

    return None


def _extract_pdf_from_frameset(page) -> bytes | None:
    """Extract PDF bytes from an Ovid frameset page (PDF embedded in iframe)."""
    frames = page.frames
    print(f"    [debug] Page has {len(frames)} frame(s)")

    for frame in frames:
        frame_url = frame.url
        if not frame_url or frame_url == "about:blank":
            continue
        print(f"    [debug]   frame: {frame_url[:120]}")

        # Check if frame content is PDF
        try:
            ct = frame.evaluate("document.contentType")
            if ct == "application/pdf":
                print(f"    [debug]   -> frame is PDF! Downloading...")
                cookies = {
                    c["name"]: c["value"]
                    for c in page.context.cookies([frame_url])
                }
                resp = _get(frame_url, allow_redirects=True, cookies=cookies)
                if resp.status_code == 200 and _is_pdf(resp.content):
                    print(f"    [debug]   -> got PDF via cookies ({len(resp.content)} bytes)")
                    return resp.content

                # Try XHR inside the frame
                import base64
                pdf_b64 = frame.evaluate("""
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
                        print(f"    [debug]   -> got PDF via XHR ({len(content)} bytes)")
                        return content
        except Exception as e:
            logger.debug(f"Frame PDF check failed: {e}")

    # Also check for embed/object/iframe with PDF src
    for sel in ['embed[type="application/pdf"]', 'embed[src*=".pdf"]',
                'iframe[src*="pdf"]', 'object[data*="pdf"]']:
        try:
            el = page.locator(sel).first
            if el.count():
                src = el.get_attribute("src") or el.get_attribute("data")
                if src:
                    pdf_url = urljoin(page.url, src)
                    print(f"    [debug] Found embedded PDF element: {pdf_url[:120]}")
                    cookies = {
                        c["name"]: c["value"]
                        for c in page.context.cookies([pdf_url])
                    }
                    resp = _get(pdf_url, allow_redirects=True, cookies=cookies)
                    if resp.status_code == 200 and _is_pdf(resp.content):
                        return resp.content
        except Exception:
            continue

    # Try the Chrome PDF viewer download button inside any frame
    for frame in frames:
        try:
            for btn_sel in [
                'button[aria-label*="Download" i]',
                'cr-icon-button[aria-label*="Download" i]',
                '#download', '#download-button',
            ]:
                button = frame.locator(btn_sel).first
                if button.count() and button.is_visible():
                    print(f"    [debug] Found download button in frame: {btn_sel}")
                    with page.expect_download(timeout=15000) as dl_info:
                        button.click()
                    download = dl_info.value
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        tmp_path = Path(tmp.name)
                    download.save_as(str(tmp_path))
                    content = tmp_path.read_bytes()
                    tmp_path.unlink(missing_ok=True)
                    if _is_pdf(content):
                        return content
        except Exception:
            continue

    return None


def _extract_pdf_from_page(page) -> bytes | None:
    """Extract PDF bytes from a page that is displaying a PDF."""
    # Check if the page content is directly a PDF
    try:
        ct = page.evaluate("document.contentType")
        if ct == "application/pdf":
            pdf_url = page.url
            cookies = {
                c["name"]: c["value"] for c in page.context.cookies([pdf_url])
            }
            resp = _get(pdf_url, allow_redirects=True, cookies=cookies)
            if resp.status_code == 200 and _is_pdf(resp.content):
                return resp.content
    except Exception:
        pass

    # Try XHR fetch on same-origin PDF
    try:
        pdf_b64 = page.evaluate(
            """
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
            """
        )
        if pdf_b64 and isinstance(pdf_b64, str):
            import base64
            content = base64.b64decode(pdf_b64)
            if _is_pdf(content):
                return content
    except Exception:
        pass

    # Try Chrome PDF viewer download button
    try:
        for selector in [
            'button[aria-label*="Download" i]',
            'cr-icon-button[aria-label*="Download" i]',
            "#download",
            "#download-button",
        ]:
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
    except Exception:
        pass

    return None


def _ovid_download_article_pdf(page, article: dict, browse_url: str) -> bytes | None:
    """
    From the Ovid journal browse page, search for the article using
    Advanced search (vol/issue/page), navigate to it, and download the PDF.
    Navigates back to browse_url when done.
    """
    title = article.get("title", "").strip()
    if not title:
        return None

    volume = article.get("volume", "")
    issue = article.get("issue", "")
    pages = article.get("pages", "")
    year = article.get("year", "")
    print(f"    [debug] Article: vol={volume} issue={issue} pages={pages} year={year}")

    # Always use Advanced search to find the exact article first
    _navigate_back(page, browse_url)
    if _search_ovid_advanced(page, article):
        # After search, try to find and download the PDF
        content = _click_ovid_fulltext_and_get_pdf(page, title)
        if content:
            print(f"    [debug] Got PDF after search! Size: {len(content)}")
            _navigate_back(page, browse_url)
            return content
        print(f"    [debug] Could not get PDF from search results")
    else:
        print(f"    [debug] Advanced search failed")

    _navigate_back(page, browse_url)
    return None


def _navigate_back(page, url: str):
    """Navigate back to the given URL."""
    try:
        if url and url != page.url:
            page.goto(url, timeout=30000)
            page.wait_for_load_state("domcontentloaded", timeout=15000)
            time.sleep(2)
    except Exception as e:
        logger.debug(f"Navigate back failed: {e}")


def playwright_circulation_batch_download(
    articles: list[dict], out_dir: Path = PDF_DIR
) -> dict[str, bytes]:
    """
    Download multiple Circulation PDFs using a SINGLE Playwright browser session
    via NCKU Primo -> Ovid.
    Returns {doi: pdf_bytes} for successful downloads.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.debug("playwright not installed, skipping Circulation batch")
        return {}

    primo_user = os.getenv("PRIMO_USER", "")
    primo_pass = os.getenv("PRIMO_PASS", "")
    if not primo_user or not primo_pass:
        logger.debug("Primo credentials not set for Circulation batch")
        return {}

    # Filter articles that still need downloading
    to_download = []
    for article in articles:
        dest = out_dir / _pdf_filename(article)
        if dest.exists() and dest.stat().st_size > 10_000:
            continue
        to_download.append(article)

    if not to_download:
        return {}

    user_data = tempfile.mkdtemp(prefix="pw_circulation_batch_")
    results: dict[str, bytes] = {}
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

            # Step 1: Navigate to Primo Circulation page
            print(f"    [debug] Navigating to Primo: {PRIMO_CIRCULATION_URL[:80]}...")
            page.goto(PRIMO_CIRCULATION_URL, timeout=60000)
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            print(f"    [debug] Primo loaded, URL: {page.url[:100]}")

            # Step 2: Click Ovid link (may open new tab)
            print(f"    [debug] Clicking Ovid link...")
            ovid_page = _click_ovid_link(page)
            time.sleep(2)
            print(f"    [debug] After Ovid click, URL: {ovid_page.url[:100]}")

            # Step 3: Handle SSO login if needed
            sso_filled = _fill_sso_credentials(ovid_page)
            print(f"    [debug] SSO credentials filled: {sso_filled}")
            if sso_filled:
                try:
                    ovid_page.wait_for_load_state("domcontentloaded", timeout=30000)
                    time.sleep(3)
                except Exception:
                    pass
                print(f"    [debug] After SSO login, URL: {ovid_page.url[:100]}")

            # Dismiss any post-login notices
            _dismiss_post_login_notice(ovid_page)

            # Check if still on Primo (login redirect failed)
            if "primo.exlibrisgroup.com" in ovid_page.url:
                print(f"    [debug] Still on Primo, attempting login...")
                if not _primo_login(ovid_page):
                    print(f"    [debug] Primo login FAILED")
                    context.close()
                    return {}

            # Wait for Ovid page to fully load
            time.sleep(3)
            browse_url = ovid_page.url
            print(f"    [debug] Ovid browse page: {browse_url[:120]}")

            # Step 4: Download each article
            for i, article in enumerate(to_download):
                title = article.get("title", "")[:60]
                doi = article.get("doi", "")
                print(f"  Ovid [{i+1}/{len(to_download)}]: {title}...")

                content = _ovid_download_article_pdf(ovid_page, article, browse_url)
                if content:
                    results[doi] = content
                    print(f"    -> PDF downloaded ({len(content)//1024} KB)")
                else:
                    print(f"    -> PDF not found")

                time.sleep(2)

            context.close()
    except Exception as e:
        logger.debug(f"Circulation batch failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
    finally:
        shutil.rmtree(user_data, ignore_errors=True)

    return results


def _ovid_title_queries(title: str) -> list[str]:
    if not title:
        return []
    queries = [title]
    normalized = re.sub(r"[^A-Za-z0-9\s-]", " ", title)
    normalized = re.sub(r"\s+", " ", normalized).strip()
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
    """Try to get PDF bytes from the current Ovid page (article full-text page)."""
    print(f"    [debug] _try_ovid_pdf_link called, URL: {page.url[:120]}")

    # 1. Check if current page is already a PDF
    try:
        content_type = page.evaluate("document.contentType")
        print(f"    [debug] Page content-type: {content_type}")
        if content_type == "application/pdf":
            content = _extract_pdf_from_page(page)
            if content:
                return content
    except Exception as e:
        logger.debug(f"Ovid current-page PDF check failed: {e}")

    # 2. Scroll down to make sure bottom toolbar is visible, then wait
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)
    except Exception:
        pass

    # 3. Find "PDF Full Text" link — try multiple strategies
    pdf_link = None

    # 3a. By role + name
    try:
        candidate = page.get_by_role("link", name=re.compile("PDF Full Text", re.I)).first
        if candidate.count() and candidate.is_visible():
            pdf_link = candidate
            print(f"    [debug] Found 'PDF Full Text' via role+name")
    except Exception:
        pass

    # 3b. By text content (handles non-<a> or differently accessible elements)
    if not pdf_link:
        try:
            candidate = page.get_by_text("PDF Full Text", exact=False).first
            if candidate.count() and candidate.is_visible():
                tag = candidate.evaluate("el => el.tagName")
                print(f"    [debug] Found 'PDF Full Text' via text, tag={tag}")
                pdf_link = candidate
        except Exception:
            pass

    # 3c. By CSS selectors common on Ovid
    if not pdf_link:
        for sel in [
            'a[href*="pdf" i]',
            'a:has-text("PDF Full Text")',
            'a:has-text("PDF")',
            '.fulltext-pdf a',
            '#pdf-link',
        ]:
            try:
                candidate = page.locator(sel).first
                if candidate.count() and candidate.is_visible():
                    txt = candidate.inner_text()[:40]
                    print(f"    [debug] Found PDF element via '{sel}': '{txt}'")
                    pdf_link = candidate
                    break
            except Exception:
                continue

    if not pdf_link:
        print(f"    [debug] 'PDF Full Text' NOT found on page")
        # Dump visible links for debugging
        try:
            links = page.locator("a:visible").all()
            for lnk in links[:20]:
                txt = lnk.inner_text().strip()[:60]
                href = (lnk.get_attribute("href") or "")[:60]
                if txt:
                    print(f"    [debug]   link: '{txt}' -> {href}")
        except Exception:
            pass
        return None

    # 4. Try to get the PDF via the found link
    href = pdf_link.get_attribute("href") if pdf_link else None
    print(f"    [debug] PDF link href: {href[:120] if href else 'None'}")
    content = _click_pdf_fulltext_link(page, pdf_link)
    if content:
        return content
    print(f"    [debug] _click_pdf_fulltext_link returned None")

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
    if not doi:
        _log_failure(article, "No DOI")
        return None
    is_elsevier = doi.startswith("10.1016/")
    content = None

    if is_circulation:
        print("  [1] Direct PDF (Circulation)...")
        content = _try_direct(doi, journal)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        print("  [2] Primo/Ovid fallback (Circulation)...")
        circ_results = playwright_circulation_batch_download([article], out_dir)
        content = circ_results.get(doi)
        if content:
            dest.write_bytes(content)
            print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
            return dest
        _log_failure(article, "Circulation PDF not found")
        print(f"  [FAIL] {doi or article.get('title', '')} (Circulation)")
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
    circulation_pending: list[dict] = []

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
            print(f"  [1] Direct PDF URL (Circulation)...")
            content = _try_direct(doi, journal)
            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
                time.sleep(1)
                continue
            print(f"  [pending] queued for Playwright batch (Circulation/Ovid fallback)")
            circulation_pending.append(article)
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

    # ── Pass 2: batch download Circulation articles via Playwright/Ovid ─
    if circulation_pending:
        print(f"\n{'─'*50}")
        print(f"  Playwright batch: downloading {len(circulation_pending)} Circulation PDF(s) via Ovid...")
        print(f"  (opening Chrome → Primo → Ovid, please wait)\n")

        circ_results = playwright_circulation_batch_download(circulation_pending, out_dir)

        for article in circulation_pending:
            pmid = article.get("pmid", "?")
            doi = article.get("doi", "")
            dest = out_dir / _pdf_filename(article)
            content = circ_results.get(doi)

            if content:
                dest.write_bytes(content)
                print(f"  [OK] {dest.name} ({len(content)//1024} KB)")
                results[pmid] = dest
            else:
                _log_failure(article, "Circulation PDF not found via Primo/Ovid")
                print(f"  [FAIL] {doi or article.get('title', '')} (Circulation Primo/Ovid)")
                results[pmid] = None

    # ── Pass 3a: batch download OUP articles via Playwright ────────────
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

    # ── Pass 3b: batch download Elsevier articles via nodriver ─────────
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
