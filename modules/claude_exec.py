"""Run `claude -p` non-interactively, with codex fallback on limit exhaustion.

Primary backend for the weekly pipeline:
- summaries:  claude -p with Haiku 4.5
- appraisals: claude -p with Opus 4.6 (same per-token price as 4.7 but uses
  the older tokenizer, ~15-25% fewer tokens for the same medical text)

Fallback: when claude reports a rate-limit / credit-exhausted error the
caller switches to the existing `codex exec` path for the rest of this
process. The Agent SDK credit resets monthly, so the next process invocation
tries claude again. We do not track spend locally — the authoritative signal
is the error claude itself returns when the credit runs out.

Auth: ANTHROPIC_API_KEY is deliberately stripped from the subprocess env so
claude uses the user's keychain OAuth login (Claude subscription / Agent SDK
credit), not a raw-API account.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable

BASH_GATE_HOOK = Path(__file__).resolve().parent / "bash_gate_hook.py"

CLAUDE_SUMMARY_MODEL_ENV = "JOURNAL_FETCHER_CLAUDE_SUMMARY_MODEL"
CLAUDE_APPRAISAL_MODEL_ENV = "JOURNAL_FETCHER_CLAUDE_APPRAISAL_MODEL"

FALLBACK_CLAUDE_SUMMARY_MODEL = "claude-haiku-4-5"
FALLBACK_CLAUDE_APPRAISAL_MODEL = "claude-opus-4-6"

# Env passed to claude subprocess: full ambient env minus ANTHROPIC_API_KEY,
# so claude uses macOS keychain OAuth (subscription / Agent SDK credit)
# rather than a raw-API account. A minimal allowlist breaks keychain access
# because the keychain depends on session/user context (USER, LOGNAME, etc.)
# that we can't trim safely.

# Substrings (lowercased) in the claude error response that we treat as
# "Agent SDK credit exhausted / rate limited" → permanent switch to codex
# for the rest of this process.
LIMIT_ERROR_SIGNALS = (
    "rate_limit",
    "rate limit",
    "billing_error",
    "billing error",
    "credit",
    "monthly limit",
    "usage limit",
    "agent sdk",
    "quota",
)


# Process-local: flips to True on first ClaudeLimitError, stays True for the
# rest of this process. The Agent SDK credit refreshes monthly, so the next
# pipeline run starts fresh.
_session = {"claude_exhausted": False}


def _claude_only() -> bool:
    """When JOURNAL_FETCHER_CLAUDE_ONLY is truthy, never fall back to codex:
    a claude failure re-raises so the caller can defer/retry instead of
    silently producing a codex result. Default behaviour (unset) is unchanged.
    """
    return os.environ.get("JOURNAL_FETCHER_CLAUDE_ONLY", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


class ClaudeError(RuntimeError):
    """Base for claude -p call failures."""


class ClaudeLimitError(ClaudeError):
    """Claude -p reported rate-limit / credit exhaustion. Caller switches
    permanently to codex for the remainder of this process."""


def resolve_claude_cli() -> str:
    """Return the claude CLI path; cover launchd-missing PATH locations."""
    found = shutil.which("claude")
    if found:
        return found
    for path in (
        Path.home() / ".local" / "bin" / "claude",
        Path("/opt/homebrew/bin/claude"),
    ):
        if path.exists():
            return str(path)
    raise FileNotFoundError("claude CLI not found")


def claude_env() -> dict[str, str]:
    """Build env for the claude subprocess.

    - Strips ANTHROPIC_API_KEY so claude uses keychain OAuth (subscription /
      Agent SDK credit), not a raw-API account.
    - Backfills USER / LOGNAME from the current uid if they are missing.
      launchd-spawned processes get a near-empty env (just PATH/HOME from
      the plist), and macOS Keychain auth fails with "Not logged in" unless
      USER/LOGNAME identify the keychain owner.
    """
    env = dict(os.environ)
    env.pop("ANTHROPIC_API_KEY", None)
    if not env.get("USER") or not env.get("LOGNAME"):
        try:
            import pwd

            name = pwd.getpwuid(os.getuid()).pw_name
            env.setdefault("USER", name)
            env.setdefault("LOGNAME", name)
        except (ImportError, KeyError):
            pass
    return env


def get_claude_summary_model() -> str:
    return os.getenv(CLAUDE_SUMMARY_MODEL_ENV) or FALLBACK_CLAUDE_SUMMARY_MODEL


def get_claude_appraisal_model() -> str:
    return os.getenv(CLAUDE_APPRAISAL_MODEL_ENV) or FALLBACK_CLAUDE_APPRAISAL_MODEL


def _is_limit_signal(text: str) -> bool:
    lower = text.lower()
    return any(signal in lower for signal in LIMIT_ERROR_SIGNALS)


def run_claude_prompt(
    prompt: str,
    *,
    model: str,
    timeout: int = 1800,
    system_prompt: str | None = None,
    enable_web_search: bool = False,
    read_dirs: list[Path | str] | None = None,
    bash_commands: list[str] | None = None,
) -> tuple[str, float]:
    """Run `claude -p --model <model> --output-format json` once.

    The full caller prompt goes through stdin (not ARG_MAX-bounded). cwd is
    a tmpdir so claude does not auto-load the project's CLAUDE.md / skills.

    `system_prompt`: when provided, it is written to a tempfile and appended
    to claude's default system prompt via `--append-system-prompt-file`.
    Claude auto-caches the system block (1-hour ephemeral cache), so passing
    a stable system prompt across articles in the same week turns most calls
    into cache reads (~10% of full input price) instead of full ingests.

    `enable_web_search`: when True, exposes WebSearch + WebFetch tools and
    pre-grants permission. Adds a server-side fee per search (~$0.01-0.03)
    and 30-90s runtime per call, but lets the model actually fulfil SKILL.md
    SECTION-0 external-search requirements (author background, journal IF,
    editorial/commentary) instead of hallucinating from memory.

    `read_dirs`: when provided, each directory is passed via --add-dir and
    the Read tool is added to the allowed set so the model can lazily pull
    on-disk references (e.g. JAMA Users' Guides) only when relevant. Cwd is
    still a tmpdir, so Read is constrained to these explicit dirs.

    `bash_commands`: when provided, the Bash tool is exposed but gated by a
    PreToolUse hook (registered via a temp --settings file) that allows only
    commands starting with one of these prefixes and denies everything else.
    A `Bash(prefix:*)` allowlist alone does NOT restrict Bash in headless
    mode, so the hook is what makes this a tight, single-helper capability
    (e.g. the editorial-fetch helper) rather than a general shell.

    Returns (result_text, total_cost_usd).
    Raises ClaudeLimitError on rate/credit exhaustion; ClaudeError otherwise.
    """
    try:
        cli = resolve_claude_cli()
    except FileNotFoundError as e:
        raise ClaudeError(str(e)) from e

    sys_file: tempfile._TemporaryFileWrapper | None = None
    cmd = [
        cli,
        "-p",
        "請依照輸入內容完成任務，只輸出指定的最終結果。",
        "--model",
        model,
        "--output-format",
        "json",
        # Strip per-machine sections (cwd, env, git status, memory paths) from
        # the system prompt so the cache key is identical across processes /
        # machines / weeks. Required for the cache to actually hit on reruns.
        "--exclude-dynamic-system-prompt-sections",
    ]
    if system_prompt:
        sys_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".md",
            prefix="claude_sys_",
            delete=False,
        )
        try:
            sys_file.write(system_prompt)
            sys_file.flush()
        finally:
            sys_file.close()
        cmd.extend(["--append-system-prompt-file", sys_file.name])

    # Compose the visible tool list from feature flags. --tools restricts the
    # visible set (no Bash/Edit leakage); --allowedTools pre-grants permission
    # so -p mode does not stall on a permission prompt. Both are required:
    # --tools alone still hits permission gating in non-interactive runs.
    tool_names: list[str] = []
    allowed: list[str] = []
    settings_file: tempfile._TemporaryFileWrapper | None = None
    if enable_web_search:
        tool_names.extend(["WebSearch", "WebFetch"])
        allowed.extend(["WebSearch", "WebFetch"])
    if read_dirs:
        for d in read_dirs:
            cmd.extend(["--add-dir", str(d)])
        tool_names.append("Read")
        allowed.append("Read")
    if bash_commands:
        # Expose Bash (--tools) but gate it with a PreToolUse hook that allows
        # only the listed command prefixes — a Bash(prefix:*) allowlist does
        # not restrict Bash in headless mode, the hook does.
        tool_names.append("Bash")
        hook_cmd = " ".join(
            [shlex.quote(sys.executable or "python3"), shlex.quote(str(BASH_GATE_HOOK))]
            + [shlex.quote(prefix) for prefix in bash_commands]
        )
        settings_payload = {
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "Bash",
                        "hooks": [{"type": "command", "command": hook_cmd}],
                    }
                ]
            }
        }
        settings_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            prefix="claude_settings_",
            delete=False,
        )
        try:
            json.dump(settings_payload, settings_file)
            settings_file.flush()
        finally:
            settings_file.close()
        cmd.extend(["--settings", settings_file.name])
    if tool_names:
        cmd.extend(["--tools", ",".join(tool_names)])
        if allowed:
            cmd.extend(["--allowedTools", ",".join(allowed)])

    try:
        result = subprocess.run(
            cmd,
            input=prompt,
            cwd=tempfile.gettempdir(),
            env=claude_env(),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raise ClaudeError(f"claude -p timed out after {timeout}s") from e
    finally:
        for tmp in (sys_file, settings_file):
            if tmp is not None:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

    if result.returncode != 0:
        msg = (result.stderr or result.stdout or "").strip()[:400]
        if _is_limit_signal(msg):
            raise ClaudeLimitError(msg or "rate_limit")
        raise ClaudeError(f"claude -p exit {result.returncode}: {msg}")

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise ClaudeError(
            f"failed to parse claude -p JSON: {e}; stderr={result.stderr[:200]}"
        ) from e

    if payload.get("is_error"):
        err_text = str(payload.get("result", "")).strip()
        if _is_limit_signal(err_text):
            raise ClaudeLimitError(err_text or "rate_limit (no message)")
        raise ClaudeError(f"claude -p reported error: {err_text[:400]}")

    text = str(payload.get("result", "")).strip()
    cost = float(payload.get("total_cost_usd") or 0.0)
    if not text:
        raise ClaudeError("claude -p returned empty result")
    return text, cost


def try_claude_or_fallback(
    prompt: str,
    *,
    claude_model: str,
    fallback: Callable[[], str | None],
    timeout: int = 1800,
    label: str = "",
    system_prompt: str | None = None,
    enable_web_search: bool = False,
    read_dirs: list[Path | str] | None = None,
    bash_commands: list[str] | None = None,
) -> tuple[str | None, str]:
    """Try claude first; on error fall back to `fallback()`.

    - On ClaudeLimitError: flips the process-local exhausted flag → all
      later calls in this process skip claude entirely.
    - On other ClaudeError (timeout, parse, transient): falls back this
      one call only, claude will be tried again on the next call.

    `system_prompt` is forwarded to `run_claude_prompt` for prompt caching;
    `enable_web_search` exposes WebSearch+WebFetch; `read_dirs` exposes the
    Read tool over those dirs. The codex fallback receives a single combined
    prompt — the caller is responsible for joining system + user content and
    wiring any codex-side file/web access on its own.

    Returns (text, backend) where backend is "claude" or "codex".
    """
    if not _session["claude_exhausted"]:
        try:
            text, _cost = run_claude_prompt(
                prompt,
                model=claude_model,
                timeout=timeout,
                system_prompt=system_prompt,
                enable_web_search=enable_web_search,
                read_dirs=read_dirs,
                bash_commands=bash_commands,
            )
            return text, "claude"
        except ClaudeLimitError as e:
            _session["claude_exhausted"] = True
            if _claude_only():
                print(
                    f"  [info] {label or 'claude'} {claude_model}: 用量已達上限 "
                    f"({str(e)[:120]});claude-only 模式,不 fallback codex。",
                    file=sys.stderr,
                )
                raise
            print(
                f"  [info] {label or 'claude'} {claude_model}: 用量已達上限 "
                f"({str(e)[:120]});本次 process 後續改用 codex。",
                file=sys.stderr,
            )
        except ClaudeError as e:
            if _claude_only():
                print(
                    f"  [warn] {label or 'claude'} {claude_model}: {str(e)[:120]};"
                    f" claude-only 模式,不 fallback codex。",
                    file=sys.stderr,
                )
                raise
            print(
                f"  [warn] {label or 'claude'} {claude_model}: {str(e)[:120]};"
                f" 本次改走 codex,後續仍會嘗試 claude。",
                file=sys.stderr,
            )
    return fallback(), "codex"


def claude_exhausted_this_run() -> bool:
    """Test helper: whether the exhausted flag is set in this process."""
    return _session["claude_exhausted"]
