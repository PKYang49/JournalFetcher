"""Weekly report 4-sentence summarizer using `claude -p`.

Each summary covers: background / methods / results / conclusion (one sentence each).
"""

import subprocess
import sys

SYSTEM_PROMPT = (
    "你是醫學文獻摘要助手。用繁體中文（台灣用語），以剛好四句話摘要這篇文章，"
    "每句一個主題，依序為："
    "第一句講研究背景與動機；"
    "第二句講研究設計與方法（族群、樣本數、介入或比較）；"
    "第三句講主要結果（含關鍵數字、效應量、統計顯著性）；"
    "第四句講結論與臨床意義。"
    "整段不超過 200 字，不使用條列、不加標題、不重複句首主詞。"
    "只輸出這四句中文摘要，不要任何其他內容、不要加引號或括號標示。"
)


def summarize_one(abstract: str, title: str = "") -> str:
    """Generate a 4-sentence Traditional Chinese summary via `claude -p`."""
    if not abstract:
        return "[無摘要]"

    user_content = f"Title: {title}\n\nAbstract:\n{abstract}" if title else abstract
    prompt = f"{SYSTEM_PROMPT}\n\n{user_content}"

    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=90,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        print(
            f"  [warn] claude CLI error: {result.stderr[:200]}", file=sys.stderr
        )
        return "[摘要生成失敗]"
    except FileNotFoundError:
        print("  [warn] claude CLI not found", file=sys.stderr)
        return "[需要 claude CLI 才能生成摘要]"
    except subprocess.TimeoutExpired:
        return "[摘要生成逾時]"


def summarize_articles(articles: list[dict]) -> list[dict]:
    """Add 'summary' key to each article in-place. Sequential execution."""
    total = len(articles)
    for i, article in enumerate(articles, 1):
        title = article.get("title", "")
        abstract = article.get("abstract", "")
        print(f"  [{i}/{total}] 生成摘要：{title[:50]}...")
        article["summary"] = summarize_one(abstract, title)
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
    summary = summarize_one(test_abstract, test_title)
    print(f"摘要:\n{summary}")
