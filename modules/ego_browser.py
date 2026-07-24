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
    pdf_url_js: str = "location.href",
    wait_for_url: str | None = None,
    settle: float = 3.0,
    nav_timeout: int = 60,
    timeout: int = 180,
) -> bytes | None:
    """Navigate to `nav_url` in ego lite, then fetch the PDF from page context.

    `pdf_url_js` is a JavaScript expression evaluated in the loaded page; the
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
