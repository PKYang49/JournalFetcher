"""Replace the baked-in feedback relay URL in generated weekly HTML."""

from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"


def matching_files(old_url: str) -> list[Path]:
    matches: list[Path] = []
    for path in sorted(DOCS_DIR.rglob("*.html")):
        try:
            if old_url in path.read_text(encoding="utf-8"):
                matches.append(path)
        except OSError as error:
            raise RuntimeError(f"cannot read {path}: {error}") from error
    return matches


def replace_endpoint(old_url: str, new_url: str, *, apply: bool) -> list[Path]:
    if not old_url.startswith("https://") or not new_url.startswith("https://"):
        raise ValueError("both relay URLs must use https://")
    if old_url == new_url:
        raise ValueError("old and new relay URLs are identical")

    matches = matching_files(old_url)
    if apply:
        for path in matches:
            content = path.read_text(encoding="utf-8")
            path.write_text(content.replace(old_url, new_url), encoding="utf-8")
    return matches


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preview or apply an exact feedback endpoint replacement in docs/*.html.",
    )
    parser.add_argument("old_url")
    parser.add_argument("new_url")
    parser.add_argument("--apply", action="store_true", help="Write changes; default is preview only")
    args = parser.parse_args()

    try:
        matches = replace_endpoint(args.old_url, args.new_url, apply=args.apply)
    except (ValueError, RuntimeError) as error:
        parser.error(str(error))

    action = "updated" if args.apply else "would update"
    print(f"{action} {len(matches)} HTML file(s)")
    for path in matches:
        print(path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
