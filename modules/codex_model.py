"""Resolve the Codex model used for journal summarization."""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

FALLBACK_SUMMARY_MODEL = "gpt-5.4"
SUMMARY_MODEL_ENV = "JOURNAL_FETCHER_CODEX_MODEL"
CODEX_CONFIG_PATH = Path.home() / ".codex" / "config.toml"
COMMON_CODEX_PATHS = (
    Path("/opt/homebrew/bin/codex"),
    Path("/usr/local/bin/codex"),
)


def get_summary_model() -> str:
    """Return one minor version below the Codex default model when possible."""
    override = os.getenv(SUMMARY_MODEL_ENV)
    if override:
        return override

    default_model = _read_codex_default_model()
    if not default_model:
        return FALLBACK_SUMMARY_MODEL

    return _previous_minor_model(default_model) or FALLBACK_SUMMARY_MODEL


def resolve_codex_cli() -> str:
    """Return the Codex CLI path, including common launchd-missing PATH locations."""
    found = shutil.which("codex")
    if found:
        return found

    for path in COMMON_CODEX_PATHS:
        if path.exists():
            return str(path)

    raise FileNotFoundError("codex CLI not found")


def _read_codex_default_model() -> str | None:
    if not CODEX_CONFIG_PATH.exists():
        return None

    for line in CODEX_CONFIG_PATH.read_text().splitlines():
        match = re.fullmatch(r'\s*model\s*=\s*"([^"]+)"\s*', line)
        if match:
            return match.group(1)
    return None


def _previous_minor_model(model: str) -> str | None:
    match = re.fullmatch(r"(gpt-\d+)\.(\d+)", model)
    if not match:
        return None

    minor = int(match.group(2))
    if minor <= 0:
        return None
    return f"{match.group(1)}.{minor - 1}"
