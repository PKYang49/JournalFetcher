"""Weekly report summarizer using `codex exec`."""

import subprocess
import sys
import tempfile
from pathlib import Path

SYSTEM_PROMPT = (
    "你是醫學文獻摘要助手。用繁體中文（台灣用語）整理這篇文章，"
    "輸出必須剛好兩行，格式如下：\n"
    "摘要：1-2 句話，交代研究設計、族群或樣本數、主要介入/比較，以及最重要數據；不超過 130 字。\n"
    "短評：用輕鬆、個人化但專業的語氣，一句話評論臨床意義或後續可能影響；不超過 60 字。\n"
    "不要使用條列，不要加 Markdown，不要輸出其他內容。"
)


def _run_codex_prompt(prompt: str, timeout: int = 180) -> str | None:
    """Run Codex CLI non-interactively and return the final message only."""
    with tempfile.TemporaryDirectory(prefix="codex_weekly_summary_") as tmp_dir:
        output_path = Path(tmp_dir) / "last_message.txt"
        result = subprocess.run(
            [
                "codex",
                "exec",
                "--sandbox",
                "read-only",
                "--color",
                "never",
                "--ephemeral",
                "--output-last-message",
                str(output_path),
                prompt,
            ],
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


def _parse_summary_response(text: str) -> tuple[str, str]:
    """Parse the two-line weekly summary response."""
    summary = ""
    commentary = ""
    fallback_lines: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("摘要："):
            summary = line.removeprefix("摘要：").strip()
        elif line.startswith("摘要:"):
            summary = line.removeprefix("摘要:").strip()
        elif line.startswith("短評："):
            commentary = line.removeprefix("短評：").strip()
        elif line.startswith("短評:"):
            commentary = line.removeprefix("短評:").strip()
        else:
            fallback_lines.append(line)

    if not summary and fallback_lines:
        summary = fallback_lines[0]
    if not commentary and len(fallback_lines) > 1:
        commentary = fallback_lines[1]
    return summary.strip(), commentary.strip()


def summarize_one(abstract: str, title: str = "") -> tuple[str, str]:
    """Generate a Traditional Chinese summary and commentary via `codex exec`."""
    if not abstract:
        return "[無摘要]", ""

    user_content = f"Title: {title}\n\nAbstract:\n{abstract}" if title else abstract
    prompt = f"{SYSTEM_PROMPT}\n\n{user_content}"

    try:
        response = _run_codex_prompt(prompt)
        if response:
            summary, commentary = _parse_summary_response(response)
            if summary:
                return summary, commentary
            return "[摘要解析失敗]", ""
        return "[摘要生成失敗]", ""
    except FileNotFoundError:
        print("  [warn] codex CLI not found", file=sys.stderr)
        return "[需要 codex CLI 才能生成摘要]", ""
    except subprocess.TimeoutExpired:
        return "[摘要生成逾時]", ""


def summarize_articles(articles: list[dict]) -> list[dict]:
    """Add 'summary' and 'commentary' keys to each article in-place."""
    total = len(articles)
    for i, article in enumerate(articles, 1):
        title = article.get("title", "")
        abstract = article.get("abstract", "")
        print(f"  [{i}/{total}] 生成摘要：{title[:50]}...")
        summary, commentary = summarize_one(abstract, title)
        article["summary"] = summary
        article["commentary"] = commentary
    return articles


if __name__ == "__main__":
    test_abstract = (
        "Background: HFpEF patients with obesity have limited proven therapies. "
        "Methods: We randomized 731 patients with HFpEF and BMI >=30 to tirzepatide "
        "or placebo for 52 weeks. Primary outcome was a composite of cardiovascular "
        "death and worsening HF events. "
        "Results: Tirzepatide reduced the primary composite (HR 0.62; 95% CI, 0.41-0.95; "
        "P=0.026) and improved KCCQ-CSS by 6.9 points. "
        "Conclusions: In HFpEF with obesity, tirzepatide reduced HF events and improved "
        "symptoms compared with placebo."
    )
    test_title = "Tirzepatide in Heart Failure with Preserved Ejection Fraction"
    print("Testing summarize_one...\n")
    summary, commentary = summarize_one(test_abstract, test_title)
    print(f"摘要:\n{summary}")
    print(f"短評:\n{commentary}")
