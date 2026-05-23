"""Phase 2a: Generate 3-sentence Traditional Chinese summaries.

Primary backend: `claude -p` with Haiku (Agent SDK credit). Fallback: `codex
exec` with GPT 5.4, used automatically when claude reports a rate-limit /
credit-exhausted error."""

import subprocess
import sys
import tempfile
from pathlib import Path

from modules import claude_exec
from modules.codex_model import codex_exec_env, get_summary_model, resolve_codex_cli

SYSTEM_PROMPT = (
    "你是醫學文獻摘要助手。用繁體中文，以三句話摘要這篇文章："
    "第一句說研究設計和族群，第二句說主要發現和數字，第三句說臨床意義。"
    "不超過 120 字，不使用條列，不加標題。"
    "只輸出三句摘要，不要任何其他內容。"
)


def _run_codex_prompt(prompt: str, timeout: int = 180) -> str | None:
    """Run Codex CLI non-interactively and return the final message only."""
    with tempfile.TemporaryDirectory(prefix="codex_summary_") as tmp_dir:
        output_path = Path(tmp_dir) / "last_message.txt"
        result = subprocess.run(
            [
                resolve_codex_cli(),
                "exec",
                "--model",
                get_summary_model(),
                "--sandbox",
                "read-only",
                "--skip-git-repo-check",
                "--color",
                "never",
                "--ephemeral",
                "--output-last-message",
                str(output_path),
                prompt,
            ],
            cwd=tmp_dir,
            env=codex_exec_env(),
            input="",
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout).strip()
            print(f"  [warn] codex exec error: {err[:200]}", file=sys.stderr)
            return None
        if output_path.exists():
            return output_path.read_text().strip()
        stdout = result.stdout.strip()
        return stdout or None


def _run_summary_prompt(prompt: str, timeout: int = 180) -> str | None:
    """Try claude -p Haiku first; fall back to codex GPT 5.4 on rate/credit error."""
    text, _backend = claude_exec.try_claude_or_fallback(
        prompt,
        claude_model=claude_exec.get_claude_summary_model(),
        fallback=lambda: _run_codex_prompt(prompt, timeout=timeout),
        timeout=timeout,
        label="summarize",
    )
    return text


def summarize_one(abstract: str, title: str = "") -> str:
    """Summarize a single abstract (claude Haiku primary, codex GPT 5.4 fallback)."""
    if not abstract:
        return "[無摘要]"

    user_content = f"Title: {title}\n\nAbstract:\n{abstract}" if title else abstract
    prompt = f"{SYSTEM_PROMPT}\n\n{user_content}"

    try:
        summary = _run_summary_prompt(prompt)
        if summary:
            return summary
        return "[摘要生成失敗]"
    except FileNotFoundError:
        print("  [warn] no usable backend (claude / codex CLI not found)", file=sys.stderr)
        return "[需要 claude 或 codex CLI 才能生成摘要]"
    except subprocess.TimeoutExpired:
        return "[摘要生成逾時]"


def summarize_articles(articles: list[dict]) -> list[dict]:
    """
    Batch summarize, adding 'summary' key to each article.
    Processes sequentially to avoid multiple Codex sessions competing.
    """
    total = len(articles)
    for i, article in enumerate(articles, 1):
        title = article.get("title", "")
        abstract = article.get("abstract", "")
        print(f"  [{i}/{total}] 生成摘要：{title[:50]}...")
        article["summary"] = summarize_one(abstract, title)
    return articles


if __name__ == "__main__":
    test_abstract = (
        "Background: Caffeine is the most widely consumed psychoactive substance. "
        "Methods: We conducted a randomized, double-blind, placebo-controlled trial "
        "in 200 adults comparing daily caffeine intake (200 mg) vs placebo over 12 weeks. "
        "Results: Caffeine significantly reduced fatigue scores (mean difference -3.2; 95% CI, "
        "-4.1 to -2.3; P<.001) and improved cognitive performance on the Stroop test "
        "(effect size 0.45). Adverse effects were mild and transient. "
        "Conclusions: Regular moderate caffeine intake improves fatigue and cognition "
        "in healthy adults without significant adverse effects."
    )
    test_title = "Effects of Daily Caffeine on Fatigue and Cognition: A Randomized Trial"
    print("Testing summarize_one via codex exec...\n")
    summary = summarize_one(test_abstract, test_title)
    print(f"摘要:\n{summary}")
