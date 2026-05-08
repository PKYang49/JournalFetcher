"""Send Discord notification for the latest rendered weekly report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from weekly import publish, render  # noqa: E402


def _load_latest_index_entry() -> dict:
    if not render.INDEX_DATA.exists():
        raise FileNotFoundError(f"Missing index data: {render.INDEX_DATA}")

    entries = json.loads(render.INDEX_DATA.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("docs/_index.json has no weekly report entries")

    latest = entries[0]
    if not isinstance(latest, dict):
        raise ValueError("latest docs/_index.json entry is not an object")
    return latest


def main() -> int:
    parser = argparse.ArgumentParser(description="Notify Discord about the latest weekly report.")
    parser.add_argument("--dry-run", action="store_true", help="Print latest report without sending")
    args = parser.parse_args()

    latest = _load_latest_index_entry()
    filename = str(latest.get("filename", ""))
    label = str(latest.get("label", ""))
    count = int(latest.get("count", 0))

    if not filename or not label:
        raise ValueError("latest report entry must include filename and label")

    if args.dry_run:
        print(f"[dry-run] latest report: {label} ({filename}), count={count}")
        return 0

    ok = publish.send_discord_index_notice(label=label, filename=filename, count=count)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
