"""ego lite browser adapter — fetch PDFs that need a real browser profile.

Some publishers gate the article PDF behind state that a headless HTTP client
cannot reproduce:

- Ovid (MSSE) requires an httpOnly entitlement cookie; `document.cookie` never
  exposes it, so a cookie-bridge into curl_cffi is impossible.
- ScienceDirect (Elsevier) serves a Cloudflare JS challenge that only a real
  navigation can solve; `fetch()` on the same URL returns the challenge HTML.

ego lite runs a real Chromium profile that already carries both, and its
`ego-browser nodejs` CLI gives us a Node runtime inside that profile — so the
bytes can be written straight to disk without the local-receiver hop the
Claude-in-Chrome helper needs.

Every entry point degrades to None when ego lite is unavailable, so callers can
keep their existing fallback cascade (and the on-demand `deferred` retry) intact.
"""

import json
import os
import shutil
import subprocess
import tempfile
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

EGO_BIN = "ego-browser"
TASK_SPACE = "journalfetcher-download"

# Bytes cross the CDP boundary as base64, so a huge PDF inflates ~4/3 in
# transit. Ordinary articles are 1-4 MB; refuse anything absurd rather than
# hang the subprocess.
MAX_PDF_BYTES = 80 * 1024 * 1024

# Runs inside the page. Returns base64 because CDP cannot hand back binary.
_PAGE_FETCH_JS = """(async () => {
  const target = __PDF_URL_JS__;
  const r = await fetch(target, { credentials: 'include' });
  const buf = new Uint8Array(await r.arrayBuffer());
  let bin = '';
  for (let i = 0; i < buf.length; i += 0x8000) {
    bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
  }
  return {
    status: r.status,
    type: r.headers.get('content-type'),
    size: buf.length,
    head: bin.slice(0, 5),
    b64: btoa(bin),
  };
})()"""

# Runs in Node, inside the ego task space.
_DRIVER_JS = """
import fs from 'fs'

const task = await useOrCreateTaskSpace(__TASK_SPACE__)
try {
  await openOrReuseTab(__NAV_URL__, { wait: true, timeout: __NAV_TIMEOUT__ })
  await wait(__SETTLE__)

  // Optional second hop: read a link off the landing page and navigate to it.
  // Needed where the PDF URL is not derivable from the DOI (ScienceDirect
  // signs it with an md5) and where only a real navigation clears the
  // publisher's JS challenge — an in-page fetch just returns the challenge.
  const linkJs = __LINK_JS__
  if (linkJs) {
    const next = await js(linkJs)
    if (!next) {
      cliLog('EGO_FAIL no link matched on landing page')
      throw new Error('no-link')
    }
    await gotoAndWait(next, { timeout: __NAV_TIMEOUT__ })
    await wait(__SETTLE__)
  }

  const waitFor = __WAIT_FOR_URL__
  if (waitFor) {
    const re = new RegExp(waitFor)
    for (let i = 0; i < 15; i++) {
      const info = await pageInfo()
      if (re.test(info.url || '')) break
      await wait(2)
    }
  }

  const res = await js(__PAGE_FETCH__)
  if (res.head !== '%PDF-') {
    cliLog('EGO_FAIL status=' + res.status + ' type=' + res.type + ' size=' + res.size)
  } else if (res.size > __MAX_BYTES__) {
    cliLog('EGO_FAIL oversized size=' + res.size)
  } else {
    fs.writeFileSync(__OUT_PATH__, Buffer.from(res.b64, 'base64'))
    cliLog('EGO_OK ' + res.size)
  }
} finally {
  // Always release the task space: Ovid counts an open fulltext tab as one of
  // three shared institutional seats.
  await completeTaskSpace(task.id, { keep: false })
}
"""


@lru_cache(maxsize=1)
def ego_available() -> bool:
    """True when the ego-browser CLI is on PATH and not disabled by env."""
    if os.getenv("JOURNAL_FETCHER_EGO", "1") == "0":
        return False
    return shutil.which(EGO_BIN) is not None


def _render(template: str, values: dict[str, str]) -> str:
    out = template
    for key, value in values.items():
        out = out.replace(key, value)
    return out


def fetch_pdf_via_ego(
    nav_url: str,
    *,
    link_js: str | None = None,
    pdf_url_js: str = "location.href",
    wait_for_url: str | None = None,
    settle: float = 3.0,
    nav_timeout: int = 60,
    timeout: int = 180,
) -> bytes | None:
    """Navigate to `nav_url` in ego lite, then fetch the PDF from page context.

    `link_js` (optional) is evaluated on the landing page and must return a URL
    to navigate to before fetching — use it when the PDF URL is only
    discoverable from the page, or when the publisher's JS challenge only
    clears on a real navigation.

    `pdf_url_js` is a JavaScript expression evaluated in the final page; the
    default re-fetches whatever the navigation landed on (ScienceDirect, where
    the challenge redirects to a signed asset URL). Ovid instead derives the
    PDF URL from the fulltext URL.

    `wait_for_url` is a regex polled against the page URL before fetching, for
    publishers that bounce through an interstitial.

    Returns PDF bytes, or None if ego is unavailable or the page did not yield
    a PDF (paywall, expired cookie, captcha).
    """
    if not ego_available():
        logger.debug("ego-browser not available; skipping")
        return None

    fd, tmp_path = tempfile.mkstemp(suffix=".pdf", prefix="ego_")
    os.close(fd)
    out_path = Path(tmp_path)

    page_fetch = _render(_PAGE_FETCH_JS, {"__PDF_URL_JS__": pdf_url_js})
    script = _render(
        _DRIVER_JS,
        {
            "__TASK_SPACE__": json.dumps(TASK_SPACE),
            "__NAV_URL__": json.dumps(nav_url),
            "__NAV_TIMEOUT__": str(nav_timeout),
            "__SETTLE__": str(settle),
            "__LINK_JS__": json.dumps(link_js) if link_js else "null",
            "__WAIT_FOR_URL__": json.dumps(wait_for_url) if wait_for_url else "null",
            "__PAGE_FETCH__": json.dumps(page_fetch),
            "__OUT_PATH__": json.dumps(str(out_path)),
            "__MAX_BYTES__": str(MAX_PDF_BYTES),
        },
    )

    try:
        proc = subprocess.run(
            [EGO_BIN, "nodejs"],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        # cliLog writes to stderr, so read both streams before deciding.
        output = f"{proc.stdout or ''}\n{proc.stderr or ''}".strip()
        if "EGO_OK" not in output:
            logger.debug(f"ego fetch failed for {nav_url}: {output[:300]}")
            return None
        if not out_path.exists() or out_path.stat().st_size == 0:
            logger.debug(f"ego reported OK but wrote nothing for {nav_url}")
            return None
        return out_path.read_bytes()
    except subprocess.TimeoutExpired:
        logger.debug(f"ego fetch timed out after {timeout}s for {nav_url}")
        return None
    except Exception as e:
        logger.debug(f"ego fetch errored for {nav_url}: {e}")
        return None
    finally:
        out_path.unlink(missing_ok=True)


# Supplement links on an article landing page. A candidate must look like a
# downloadable file (so in-page "#supplementary-materials" anchors and
# reference DOIs such as 10.2337/dc24-0491 drop out) and carry a supplement
# hint in either its URL or its link text. The article PDF itself is excluded.
# Covers NEJM/AHA (/doi/suppl/.../suppl_file/), JAMA (cdn ...supp1...pdf),
# Elsevier (mmcN), Springer (MOESM), BMJ (DCN/inline-supplementary-material).
_SUPPLEMENT_LINKS_JS = """(() => {
  const URL_HINT = /supp|appendix|protocol|MOESM|mmc\\d|\\/DC\\d|data[-_]?sharing|disclosure/i;
  const TEXT_HINT = /supplement|appendix|protocol|statistical analysis plan|data sharing|disclosure|eMethods|eTable|eFigure|supporting information/i;
  const FILE = /\\.(pdf|docx?|xlsx?|zip|pptx?)(\\?|#|$)|\\/suppl_file\\//i;
  const meta = document.querySelector('meta[name="citation_pdf_url"]');
  const articlePdf = meta && meta.content;
  const seen = new Set();
  const out = [];
  for (const a of document.querySelectorAll('a[href]')) {
    const h = a.href;
    if (!h || seen.has(h) || h === articlePdf || /\\/doi\\/(e?pdf|epdf|pdfdirect)\\//i.test(h)) continue;
    if (!FILE.test(h)) continue;
    const text = [a.textContent, a.getAttribute('aria-label'), a.title]
      .filter(Boolean).join(' ').replace(/\\s+/g, ' ').trim();
    if (!URL_HINT.test(h) && !TEXT_HINT.test(text)) continue;
    seen.add(h);
    out.push({ url: h, text: text.slice(0, 200) });
  }
  return out;
})()"""

# Like _PAGE_FETCH_JS but never throws: a cross-origin CDN without CORS
# headers rejects the fetch, and the caller retries those from Python.
_SUPPLEMENT_FETCH_JS = """(async () => {
  try {
    const r = await fetch(__URL__, { credentials: 'include' });
    const buf = new Uint8Array(await r.arrayBuffer());
    if (buf.length > __MAX_BYTES__) {
      return { status: r.status, type: r.headers.get('content-type'), size: buf.length, error: 'oversized' };
    }
    let bin = '';
    for (let i = 0; i < buf.length; i += 0x8000) {
      bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
    }
    return { status: r.status, type: r.headers.get('content-type'), size: buf.length, b64: btoa(bin) };
  } catch (e) {
    return { error: String(e) };
  }
})()"""

_SUPPLEMENT_DRIVER_JS = """
import fs from 'fs'
import path from 'path'

const task = await useOrCreateTaskSpace(__TASK_SPACE__)
try {
  await openOrReuseTab(__NAV_URL__, { wait: true, timeout: __NAV_TIMEOUT__ })
  await wait(__SETTLE__)
  const info = await pageInfo()
  cliLog('EGO_PAGE ' + JSON.stringify({ url: info.url || '' }))
  const links = (await js(__LINKS_JS__)) || []
  cliLog('EGO_LINKS ' + links.length)
  const fetchTpl = __FETCH_JS__
  for (let i = 0; i < Math.min(links.length, __MAX_FILES__); i++) {
    const link = links[i]
    let res = await js(fetchTpl.replace('__URL__', JSON.stringify(link.url)))
    let via = 'fetch'
    // NEJM (Cloudflare) answers an in-page fetch of suppl_file with a
    // "Just a moment..." challenge; a real navigation to the file clears it.
    // Attachments (docx/zip) start a download instead, so the re-fetch is
    // accepted only if it is a non-HTML 200.
    const blocked = res.status !== 200 || /text\\/html/i.test(res.type || '')
    if (blocked && res.error !== 'oversized') {
      try {
        await gotoAndWait(link.url, { timeout: __NAV_TIMEOUT__ })
        await wait(__SETTLE__)
        const again = await js(fetchTpl.replace('__URL__', 'location.href'))
        if (again.status === 200 && !/text\\/html/i.test(again.type || '')) {
          res = again
          via = 'nav'
        }
      } catch (e) {
        res.error = (res.error || 'http_' + res.status) + '; nav: ' + String(e)
      }
    }
    const rec = { url: link.url, text: link.text, status: res.status ?? null, via,
                  type: res.type ?? null, size: res.size ?? null, error: res.error ?? null, path: null }
    if (res.b64 && res.status === 200 && !/text\\/html/i.test(res.type || '')) {
      rec.error = null
      rec.path = path.join(__OUT_DIR__, 'supp_' + String(i + 1).padStart(2, '0') + '.bin')
      fs.writeFileSync(rec.path, Buffer.from(res.b64, 'base64'))
    } else if (!rec.error) {
      rec.error = 'http_' + res.status
    }
    cliLog('EGO_FILE ' + JSON.stringify(rec))
  }
} finally {
  await completeTaskSpace(task.id, { keep: false })
}
"""


@dataclass(frozen=True)
class SupplementLink:
    """One supplement link found on a landing page, and what fetching it gave.

    `path` is set when ego fetched the bytes in-page; otherwise `error` says
    why (typically a CORS rejection from a publisher CDN).
    """

    url: str
    text: str
    status: int | None
    via: str
    content_type: str | None
    size: int | None
    error: str | None
    path: Path | None


@dataclass(frozen=True)
class SupplementScan:
    landing_url: str | None
    links: list[SupplementLink]


def fetch_supplements_via_ego(
    nav_url: str,
    out_dir: Path,
    *,
    max_files: int = 12,
    settle: float = 5.0,
    nav_timeout: int = 60,
    timeout: int = 420,
) -> SupplementScan | None:
    """Open `nav_url` in ego lite, collect supplement links, fetch each in-page.

    Files are written to `out_dir` as supp_NN.bin; the caller sniffs the real
    format. Returns None when ego is unavailable or the run failed outright
    (timeout, crash); a scan with no links means the page loaded but listed
    no supplements (or a challenge page was served instead).
    """
    if not ego_available():
        logger.debug("ego-browser not available; skipping supplement scan")
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    script = _render(
        _SUPPLEMENT_DRIVER_JS,
        {
            "__TASK_SPACE__": json.dumps(TASK_SPACE),
            "__NAV_URL__": json.dumps(nav_url),
            "__NAV_TIMEOUT__": str(nav_timeout),
            "__SETTLE__": str(settle),
            "__LINKS_JS__": json.dumps(_SUPPLEMENT_LINKS_JS),
            "__FETCH_JS__": json.dumps(
                _SUPPLEMENT_FETCH_JS.replace("__MAX_BYTES__", str(MAX_PDF_BYTES))
            ),
            "__MAX_FILES__": str(max_files),
            "__OUT_DIR__": json.dumps(str(out_dir.resolve())),
        },
    )
    try:
        proc = subprocess.run(
            [EGO_BIN, "nodejs"],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        logger.debug(f"ego supplement scan timed out after {timeout}s for {nav_url}")
        return None

    output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    landing_url: str | None = None
    links: list[SupplementLink] = []
    saw_links_line = False
    for line in output.splitlines():
        if line.startswith("EGO_PAGE "):
            landing_url = json.loads(line[len("EGO_PAGE "):]).get("url") or None
        elif line.startswith("EGO_LINKS "):
            saw_links_line = True
        elif line.startswith("EGO_FILE "):
            rec = json.loads(line[len("EGO_FILE "):])
            links.append(
                SupplementLink(
                    url=rec["url"],
                    text=rec.get("text") or "",
                    status=rec.get("status"),
                    via=rec.get("via") or "fetch",
                    content_type=rec.get("type"),
                    size=rec.get("size"),
                    error=rec.get("error"),
                    path=Path(rec["path"]) if rec.get("path") else None,
                )
            )
    if not saw_links_line:
        logger.debug(f"ego supplement scan failed for {nav_url}: {output.strip()[:300]}")
        return None
    return SupplementScan(landing_url=landing_url, links=links)
