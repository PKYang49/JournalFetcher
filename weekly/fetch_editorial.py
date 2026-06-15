#!/usr/bin/env python3
"""Fetch an editorial / commentary full text by DOI for appraisal SECTION-0 §7.

This is a *local tool* the appraisal model invokes during `claude -p`. The
model runs in the cloud and cannot read paywalled editorials via WebFetch,
but this script runs on the maintainer's machine (institutional network) and
turns a DOI into plain-text markdown that the model can actually summarise:

    python3 weekly/fetch_editorial.py 10.1016/j.jacc.2026.04.001

Pipeline: dlbydoi.py's download_one fallback chain (DOI → full-text PDF over
the institutional network) → appraise_selected._convert_pdf_to_markdown (PDF
→ markdown, refs stripped). The markdown is printed to stdout; all diagnostic
chatter goes to stderr so the model reads a clean body. Non-zero exit on
download/convert failure so the model knows to fall back to `[無法取得全文]`.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Editorials/comments are short; this cap only guards against a runaway
# conversion (bad OCR, attached supplements) flooding the model context.
DEFAULT_MAX_CHARS = 60000

# Append-only trace of every invocation. Gives the launchd pipeline visibility
# into whether the appraisal model actually reached for the tool and whether
# the download succeeded (the model runs with cwd=tmpdir, so use an abs path).
_LOG_PATH = ROOT / "output" / "logs" / "editorial_fetch.log"


def _log(message: str) -> None:
    from datetime import datetime

    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().isoformat(timespec='seconds')}\t{message}\n")
    except OSError:
        pass


@contextmanager
def _stdout_to_stderr():
    """Redirect fd 1 → fd 2 for the duration of the block.

    dlbydoi spawns browser drivers (nodriver / Playwright) that write to the
    real stdout file descriptor, which contextlib.redirect_stdout (a Python-
    level swap) does not capture. An OS-level dup2 keeps the model-facing
    stdout clean so only the final markdown lands there.
    """
    sys.stdout.flush()
    saved = os.dup(1)
    try:
        os.dup2(2, 1)
        yield
    finally:
        sys.stdout.flush()
        os.dup2(saved, 1)
        os.close(saved)


def fetch(doi: str, max_chars: int = DEFAULT_MAX_CHARS) -> str | None:
    """Download + convert one editorial by DOI via dlbydoi.py."""
    import dlbydoi
    from weekly.appraise_selected import _convert_pdf_to_markdown

    doi = dlbydoi._normalize_doi(doi)
    with tempfile.TemporaryDirectory(prefix="editorial_dl_") as tmp:
        out_dir = Path(tmp)
        # dlbydoi + the converter print progress to stdout; redirect to stderr
        # so the stdout the model reads stays clean markdown.
        with _stdout_to_stderr():
            try:
                pdf = dlbydoi.download_one(doi, out_dir)
            except Exception as e:  # noqa: BLE001 - tool must report, not crash
                print(f"[fetch-editorial] download error for {doi}: {e}", file=sys.stderr)
                return None
            if not pdf or not pdf.exists():
                return None
            try:
                md = _convert_pdf_to_markdown(pdf)
            except Exception as e:  # noqa: BLE001
                print(f"[fetch-editorial] convert error for {doi}: {e}", file=sys.stderr)
                return None

    md = (md or "").strip()
    if not md:
        return None
    if len(md) > max_chars:
        md = md[:max_chars].rstrip() + "\n\n[...truncated...]"
    return md


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch an editorial/comment full text by DOI (prints markdown to stdout)."
    )
    parser.add_argument("doi", help="DOI of the editorial / commentary")
    parser.add_argument(
        "--max-chars",
        type=int,
        default=DEFAULT_MAX_CHARS,
        help=f"truncate body to this many chars (default {DEFAULT_MAX_CHARS})",
    )
    args = parser.parse_args()

    _log(f"CALL\t{args.doi}")
    md = fetch(args.doi, max_chars=args.max_chars)
    if md is None:
        _log(f"FAIL\t{args.doi}")
        print(
            f"[fetch-editorial] FAILED: could not retrieve full text for {args.doi}",
            file=sys.stderr,
        )
        return 1
    _log(f"OK\t{args.doi}\t{len(md)} chars")

    if len(md) < 200:
        print(
            f"[fetch-editorial] WARNING: extracted text is very short "
            f"({len(md)} chars); may be a paywall stub",
            file=sys.stderr,
        )
    print(f"# Editorial/Comment full text — DOI {args.doi}\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
