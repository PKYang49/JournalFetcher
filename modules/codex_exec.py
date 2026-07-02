"""Single entry point for `codex exec` subprocess invocations.

Parallel to `modules/claude_exec.py`: claude has one dispatch point
(`try_claude_or_fallback`), and this module gives codex the same. Before
this, the `codex exec ...` argv + tempdir + output-file boilerplate was
copy-pasted in five places (summarize, summarize_weekly, select_articles,
classify_article, appraise_selected), so any change to sandboxing, output
capture, or flags had to be made five times.

All callers now funnel through `run_codex_exec`. The prompt is passed on
stdin (proven by the appraisal path, which handles very large prompts) so
there is no ARG_MAX ceiling.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from modules.codex_model import codex_exec_env, resolve_codex_cli


def run_codex_exec(
    prompt: str,
    *,
    model: str,
    sandbox: str = "read-only",
    image_paths: Sequence[Path] | None = None,
    timeout: int = 180,
    tmp_prefix: str = "codex_",
    extra_env: dict[str, str] | None = None,
    set_tmpdir_env: bool = False,
    label: str = "codex",
    err_truncate: int = 300,
) -> str | None:
    """Run `codex exec` once and return the final message text (or None).

    Args:
        prompt: full prompt, delivered on stdin (no ARG_MAX limit).
        model: codex model id (e.g. from get_summary_model / get_appraisal_model).
        sandbox: codex `--sandbox` mode (read-only / workspace-write /
            danger-full-access).
        image_paths: optional images appended as `-i <path>` flags.
        timeout: subprocess timeout in seconds.
        tmp_prefix: prefix for the ephemeral working / output tempdir.
        extra_env: extra environment variables merged over `codex_exec_env()`.
        set_tmpdir_env: when True, point TMPDIR at the codex tempdir so any
            helper the model shells out to (e.g. dlbydoi) writes there.
        label: prefix used in the `[warn] <label> error: ...` diagnostic.
        err_truncate: how many chars of stderr/stdout to surface on failure.

    Returns:
        The `--output-last-message` file contents (preferred) or stdout,
        stripped; None on non-zero exit. subprocess.TimeoutExpired / OSError
        propagate to the caller (callers that want to swallow them wrap the
        call themselves, preserving pre-refactor behavior).
    """
    import tempfile

    with tempfile.TemporaryDirectory(prefix=tmp_prefix) as tmp_dir:
        output_path = Path(tmp_dir) / "last_message.txt"
        cmd = [
            resolve_codex_cli(),
            "exec",
            "--model",
            model,
            "--sandbox",
            sandbox,
            "--skip-git-repo-check",
            "--color",
            "never",
            "--ephemeral",
            "--output-last-message",
            str(output_path),
        ]
        for img in image_paths or []:
            cmd.extend(["-i", str(img)])

        env = codex_exec_env()
        if set_tmpdir_env:
            env["TMPDIR"] = tmp_dir
        if extra_env:
            env.update(extra_env)

        result = subprocess.run(
            cmd,
            cwd=tmp_dir,
            env=env,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout).strip()
            print(f"  [warn] {label} error: {err[:err_truncate]}", file=sys.stderr)
            return None
        if output_path.exists():
            return output_path.read_text(encoding="utf-8").strip()
        return result.stdout.strip() or None
