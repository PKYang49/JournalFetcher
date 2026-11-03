#!/usr/bin/env python3
"""PostToolUse hook: append every file the model Read to an audit log.

`claude -p --output-format json` reports only the final text, not which tool
calls ran. The appraisal hands figure pages to claude as files it must Read
(codex gets them attached with `-i` instead), and in 2026-W45 Opus read 2 of
6. Registered via the temp --settings file in claude_exec, this hook lets the
caller check afterwards which images were actually opened.

Usage (referenced from a temp --settings hooks entry):

    python3 read_audit_hook.py <log path>

Never blocks: whatever happens, it exits 0 with no decision.
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    if len(sys.argv) != 2:
        return 0
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    path = (payload.get("tool_input") or {}).get("file_path")
    if payload.get("tool_name") == "Read" and path:
        with open(sys.argv[1], "a", encoding="utf-8") as log:
            log.write(path + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
