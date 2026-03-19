"""Phase 2a: Generate 3-sentence Traditional Chinese summaries via Claude Code CLI."""

import subprocess
import sys

SYSTEM_PROMPT = (
    "你是醫學文獻摘要助手。用繁體中文，以三句話摘要這篇文章："
    "第一句說研究設計和族群，第二句說主要發現和數字，第三句說臨床意義。"
    "不超過 120 字，不使用條列，不加標題。"
    "只輸出三句摘要，不要任何其他內容。"
)


def summarize_one(abstract: str, title: str = "") -> str:
    """Summarize a single abstract using Claude Code CLI (no API key needed)."""
    if not abstract:
        return "[無摘要]"

    user_content = f"Title: {title}\n\nAbstract:\n{abstract}" if title else abstract
    prompt = f"{SYSTEM_PROMPT}\n\n{user_content}"

    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"  [warn] claude CLI error: {result.stderr[:100]}", file=sys.stderr)
            return "[摘要生成失敗]"
    except FileNotFoundError:
        print("  [warn] claude CLI not found, skipping summary", file=sys.stderr)
        return "[需要 claude CLI 才能生成摘要]"
    except subprocess.TimeoutExpired:
        return "[摘要生成逾時]"


def summarize_articles(articles: list[dict]) -> list[dict]:
    """
    Batch summarize, adding 'summary' key to each article.
    Processes sequentially (claude CLI is already fast enough).
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
    print("Testing summarize_one via claude CLI...\n")
    summary = summarize_one(test_abstract, test_title)
    print(f"摘要:\n{summary}")
