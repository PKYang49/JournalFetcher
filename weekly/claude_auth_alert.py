"""Track an expired `claude` OAuth login and nag the user on Discord.

An expired login never recovers on its own (unlike a usage limit), so the
appraisal paths record it here instead of waiting out a 5h window:

* ``report_auth_expired`` writes a marker file and sends a Discord alert,
  throttled to one per ``JOURNAL_FETCHER_AUTH_ALERT_INTERVAL`` seconds (6h).
* ``is_auth_blocked`` lets the 15-minute worker run a cheap Haiku probe
  instead of the full download/convert/classify pipeline while blocked.
* ``clear_auth_blocked`` removes the marker once claude answers again, so the
  next expiry alerts immediately.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MARKER_PATH = ROOT / "data" / "claude_auth_alert.json"

ALERT_INTERVAL_SECONDS = int(
    os.getenv("JOURNAL_FETCHER_AUTH_ALERT_INTERVAL", str(6 * 60 * 60))
)


def _read_marker() -> dict:
    try:
        return json.loads(MARKER_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as e:
        print(f"[auth-alert] unreadable marker {MARKER_PATH}: {e}", file=sys.stderr)
        return {}


def is_auth_blocked() -> bool:
    return MARKER_PATH.exists()


def report_auth_expired(context: str, error: str, pending: int) -> None:
    """Record the expired login and send a (throttled) Discord alert."""
    from weekly import publish

    now = time.time()
    marker = _read_marker()
    marker.setdefault("since", now)
    marker["context"] = context
    marker["error"] = error[:400]

    last_alert = float(marker.get("last_alert", 0))
    if now - last_alert >= ALERT_INTERVAL_SECONDS:
        if publish.send_claude_auth_alert(context, error, pending):
            marker["last_alert"] = now
    else:
        print(
            f"[auth-alert] claude login still expired; Discord alert already sent "
            f"{(now - last_alert) / 3600:.1f}h ago"
        )

    MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKER_PATH.write_text(json.dumps(marker, ensure_ascii=False), encoding="utf-8")


def clear_auth_blocked() -> None:
    if MARKER_PATH.exists():
        MARKER_PATH.unlink()
        print("[auth-alert] claude login works again; cleared auth-expired marker")
