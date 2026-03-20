"""Phase 2b: Terminal checkbox UI for article selection."""

import shutil
import questionary
from questionary import Choice


def _display_journal_name(journal: str) -> str:
    lower = journal.lower()
    if "eurointervention" in lower:
        return "EuroIntervention"
    return journal


def _build_choices(articles: list[dict]) -> list[Choice]:
    """Build checkbox choices with truncated titles that fit terminal width."""
    term_width = shutil.get_terminal_size().columns
    max_title_len = term_width - 25  # reserve space for "  ○ XX. [JOURNAL] "

    choices = []
    for idx, a in enumerate(articles, 1):
        journal = _display_journal_name(a.get("journal", "Unknown"))
        title = a.get("title", "(無標題)")
        tag = f"[{journal}]"
        avail = max_title_len - len(tag) - 1
        short_title = title if len(title) <= avail else title[: avail - 1] + "…"
        label = f"{idx:>2}. {tag} {short_title}"
        choices.append(Choice(title=label, value=a))
    return choices


def select_for_summary(articles: list[dict]) -> list[dict]:
    """Checkbox: pick which articles to generate summaries for."""
    if not articles:
        print("沒有文章可供選擇。")
        return []

    choices = _build_choices(articles)
    selected = questionary.checkbox(
        "請勾選要產生摘要的文章（空白鍵勾選，Enter 確認）：",
        choices=choices,
    ).ask()
    return selected if selected else []


def print_summaries(articles: list[dict]) -> None:
    """Print full summaries (not truncated)."""
    print("\n" + "─" * 60)
    print("摘要：")
    print("─" * 60)
    for idx, a in enumerate(articles, 1):
        journal = _display_journal_name(a.get("journal", "Unknown"))
        title = a.get("title", "(無標題)")
        summary = a.get("summary", "（無摘要）")
        print(f"\n  {idx:>2}. [{journal}] {title}")
        print(f"      {summary}")
    print("\n" + "─" * 60)


def select_for_download(articles: list[dict]) -> list[dict]:
    """Checkbox: pick which articles to download."""
    if not articles:
        print("沒有文章可供選擇。")
        return []

    choices = _build_choices(articles)
    selected = questionary.checkbox(
        "請勾選要下載的文章（空白鍵勾選，Enter 確認）：",
        choices=choices,
    ).ask()
    return selected if selected else []


if __name__ == "__main__":
    fake_articles = [
        {
            "pmid": "11111",
            "journal": "JAMA",
            "title": "Effects of Daily Caffeine on Fatigue and Cognition",
            "doi": "10.1001/jama.2025.00001",
            "summary": "本研究為雙盲隨機對照試驗，納入 200 名健康成人比較每日咖啡因（200 mg）與安慰劑共 12 週的效果。咖啡因組的疲勞評分顯著改善（平均差 -3.2，95% CI -4.1 至 -2.3，P<.001），且認知測驗表現提升（效應量 0.45）。適度規律攝取咖啡因可安全改善健康成人的疲勞與認知功能。",
        },
        {
            "pmid": "22222",
            "journal": "NEJM",
            "title": "Restrictive vs Liberal Fluid Strategy in Septic Shock",
            "doi": "10.1056/NEJMoa2025.00002",
            "summary": "本研究為多中心隨機試驗，比較膿毒性休克患者限制性與自由性輸液策略（共納入 1500 名患者）。限制性輸液組 28 天死亡率為 22.3%，自由性輸液組為 24.1%（風險比 0.92，95% CI 0.81–1.05）。兩組死亡率未達統計顯著差異，但限制性輸液可減少機械通氣天數。",
        },
        {
            "pmid": "33333",
            "journal": "Lancet",
            "title": "Novel GLP-1 Agonist for Type 2 Diabetes: Phase 3 Trial",
            "doi": "10.1016/S0140-6736(25)00003-1",
            "summary": "本第三期隨機試驗納入 2000 名第二型糖尿病患者，評估新型 GLP-1 促效劑每週皮下注射的療效與安全性。治療 52 週後 HbA1c 從基線下降 1.8%（安慰劑組下降 0.3%，P<.001），體重平均減少 6.2 公斤。此新藥具良好血糖控制效果及心血管保護潛力，副作用主要為輕度胃腸不適。",
        },
    ]

    print("=== 測試勾選介面（假資料）===\n")
    selected = select_articles(fake_articles)
    print(f"\n已選擇 {len(selected)} 篇文章：")
    for a in selected:
        print(f"  - [{a['journal']}] {a['title']}")
