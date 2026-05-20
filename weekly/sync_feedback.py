"""Sync weekly highlight feedback from the Google Apps Script relay.

The weekly report's "本週精選評讀" cards carry 👍/👎 buttons that POST to a
Google Apps Script web app (a Google Sheet acts as storage). This module pulls
those rows back and merges them into ``data/interest_feedback.jsonl``, which
``weekly/select_articles.py`` feeds into the article-selection prompt.

Run standalone:  python -m weekly.sync_feedback
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FEEDBACK_PATH = DATA_DIR / "interest_feedback.jsonl"

VERDICT_MAP = {"up": "interested", "down": "not_interested"}


def _known_pmids() -> set[str]:
    """PMIDs shown in any past weekly report — used to drop spam against the public endpoint."""
    pmids: set[str] = set()
    weekly_dir = ROOT / "output" / "weekly"
    if not weekly_dir.exists():
        return pmids
    for pattern in ("*/articles.json", "*/selected_articles.json"):
        for meta in weekly_dir.glob(pattern):
            try:
                data = json.loads(meta.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if isinstance(data, list):
                for art in data:
                    pmid = str(art.get("pmid", "")).strip()
                    if pmid:
                        pmids.add(pmid)
    return pmids


def _load_existing() -> dict[tuple[str, str], dict]:
    """Existing feedback keyed by (week, pmid); newest ``ts`` wins per key."""
    records: dict[tuple[str, str], dict] = {}
    if not FEEDBACK_PATH.exists():
        return records
    for line in FEEDBACK_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        key = (str(obj.get("week", "")), str(obj.get("pmid", "")))
        prev = records.get(key)
        if prev is None or str(obj.get("ts", "")) >= str(prev.get("ts", "")):
            records[key] = obj
    return records


def sync_feedback(timeout: int = 30) -> int:
    """Fetch remote feedback rows and merge into ``interest_feedback.jsonl``.

    Returns the number of records added or updated. Raises on transport or
    relay errors so the caller can decide whether to continue.
    """
    load_dotenv(ROOT / ".env")
    url = os.getenv("FEEDBACK_ENDPOINT_URL", "").strip()
    token = os.getenv("FEEDBACK_SYNC_TOKEN", "").strip()
    if not url:
        print("[feedback] FEEDBACK_ENDPOINT_URL not set; skip sync")
        return 0

    resp = requests.post(
        url,
        json={"action": "sync", "token": token},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(f"relay rejected request: {payload.get('error')}")

    known = _known_pmids()
    records = _load_existing()
    changed = 0

    for row in payload.get("rows", []):
        pmid = str(row.get("pmid", "")).strip()
        week = str(row.get("week", "")).strip()
        verdict = VERDICT_MAP.get(str(row.get("verdict", "")).strip())
        if not pmid or not verdict:
            continue
        if known and pmid not in known:
            continue  # unknown PMID — likely spam against the public endpoint

        record = {
            "ts": str(row.get("ts", "")).strip(),
            "week": week,
            "pmid": pmid,
            "journal": str(row.get("journal", "")).strip(),
            "title": str(row.get("title", "")).strip(),
            "verdict": verdict,
            "note": str(row.get("note", "")).strip(),
        }
        key = (week, pmid)
        prev = records.get(key)
        if prev is not None and str(prev.get("ts", "")) >= record["ts"]:
            continue  # already stored this verdict or a newer one
        records[key] = record
        changed += 1

    if changed:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ordered = sorted(records.values(), key=lambda r: str(r.get("ts", "")))
        FEEDBACK_PATH.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in ordered) + "\n",
            encoding="utf-8",
        )
    print(f"[feedback] synced {changed} new/updated feedback row(s)")
    return changed


if __name__ == "__main__":
    try:
        sync_feedback()
    except Exception as e:  # noqa: BLE001 — standalone CLI: report and exit
        print(f"[feedback] sync failed: {e}", file=sys.stderr)
        sys.exit(1)
