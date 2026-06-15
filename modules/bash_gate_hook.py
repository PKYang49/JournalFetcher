#!/usr/bin/env python3
"""PreToolUse hook: allow only whitelisted Bash command prefixes, deny the rest.

`claude -p` cannot sub-restrict the Bash tool via --allowedTools alone (a
`Bash(prefix:*)` specifier does not block non-matching commands in headless
mode). To expose Bash for a single helper (the editorial full-text fetcher)
without granting an arbitrary shell, we register this script as a PreToolUse
hook scoped to the Bash matcher.

Usage (referenced from a temp --settings hooks entry):

    python3 bash_gate_hook.py "<allowed prefix 1>" "<allowed prefix 2>" ...

Reads the PreToolUse payload on stdin and emits a permission decision:
- non-Bash tool calls → allow (this hook only gates Bash)
- Bash command starting with an allowed prefix → allow
- anything else (or an unparseable payload) → deny  (fail closed)
"""

from __future__ import annotations

import json
import sys


def _decision(decision: str, reason: str) -> None:
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": decision,
                    "permissionDecisionReason": reason,
                }
            }
        )
    )


def main() -> int:
    allowed = sys.argv[1:]
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        _decision("deny", "bash_gate: unparseable PreToolUse payload")
        return 0

    if payload.get("tool_name", "") != "Bash":
        _decision("allow", "bash_gate: non-Bash tool")
        return 0

    command = ((payload.get("tool_input") or {}).get("command") or "").strip()
    if allowed and any(command.startswith(prefix) for prefix in allowed):
        _decision("allow", "bash_gate: editorial fetch allowed")
        return 0

    _decision("deny", "Bash 僅允許 editorial 全文擷取指令；其餘指令已封鎖。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
