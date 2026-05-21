"""Publish weekly report: git push + Discord webhook."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"

GITHUB_USER = "pkyang49"
GITHUB_REPO = "JournalFetcher"
PAGES_BASE_URL = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}"

DISCORD_EMBED_COLOR = 0x0B5FFF
HIGHLIGHT_COUNT = 3
TITLE_MAX = 110


def _run_git(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def _docs_has_changes() -> bool:
    result = _run_git(["status", "--porcelain", "docs/"], check=False)
    return bool(result.stdout.strip())


def git_commit_and_push(filename: str, label: str) -> bool:
    """Stage docs/, commit, and push. Returns True if pushed, False if nothing to commit."""
    if not _docs_has_changes():
        print("[git] docs/ has no changes; skip commit")
        return False

    _run_git(["add", "docs/"])
    msg = f"weekly: {label} ({filename})"
    _run_git(["commit", "-m", msg])
    print(f"[git] committed: {msg}")

    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip()
    push = _run_git(["push", "-u", "origin", branch], check=False)
    if push.returncode != 0:
        print(f"[git] push failed:\n{push.stderr}")
        return False
    print(f"[git] pushed to origin/{branch}")
    return True


def _truncate(text: str, limit: int = TITLE_MAX) -> str:
    text = (text or "").strip().rstrip(".")
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _pick_highlights(articles: list[dict], n: int = HIGHLIGHT_COUNT) -> list[dict]:
    """Pick first article from each unique journal, capped at n."""
    selected = [a for a in articles if a.get("selected_for_appraisal")]
    if selected:
        return selected[:n]

    seen: set[str] = set()
    picked: list[dict] = []
    for a in articles:
        key = a.get("journal_key", "")
        if key in seen:
            continue
        seen.add(key)
        picked.append(a)
        if len(picked) >= n:
            break
    return picked


def _build_embed(
    label: str,
    articles: list[dict],
    journal_counts: list[dict],
    filename: str,
) -> dict:
    weekly_url = f"{PAGES_BASE_URL}/{filename}"
    counts_line = " · ".join(
        f"**{c['name']}** {c['count']}" for c in journal_counts
    )

    highlights = _pick_highlights(articles)
    if highlights:
        bullet_lines = []
        for a in highlights:
            j = a.get("journal_key", "")
            t = _truncate(a.get("title", ""))
            doi = a.get("doi", "")
            link = f"https://doi.org/{doi}" if doi else f"https://pubmed.ncbi.nlm.nih.gov/{a.get('pmid', '')}/"
            bullet_lines.append(f"• `{j}` [{t}]({link})")
        highlights_value = "\n".join(bullet_lines)
    else:
        highlights_value = "（本週無文章）"

    embed = {
        "title": f"📚 期刊週報 {label}",
        "url": weekly_url,
        "description": counts_line,
        "color": DISCORD_EMBED_COLOR,
        "fields": [
            {"name": "🔝 本週亮點", "value": highlights_value, "inline": False},
            {"name": "🔗 完整週報", "value": weekly_url, "inline": False},
        ],
        "footer": {"text": f"JournalFetcher · 共 {len(articles)} 篇"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return embed


def send_discord(
    label: str,
    articles: list[dict],
    journal_counts: list[dict],
    filename: str,
) -> bool:
    load_dotenv(ROOT / ".env")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("[discord] DISCORD_WEBHOOK_URL not set in .env; skip")
        return False

    embed = _build_embed(label, articles, journal_counts, filename)
    payload = {"embeds": [embed]}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
    except requests.RequestException as e:
        print(f"[discord] webhook request failed: {type(e).__name__}")
        return False

    if resp.status_code in (200, 204):
        print(f"[discord] webhook sent OK ({resp.status_code})")
        return True
    print(f"[discord] webhook failed: {resp.status_code} {resp.text[:200]}")
    return False


def send_discord_index_notice(label: str, filename: str, count: int) -> bool:
    """Send a Discord notice for an already-rendered weekly report."""
    load_dotenv(ROOT / ".env")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("[discord] DISCORD_WEBHOOK_URL not set in .env; skip")
        return False

    weekly_url = f"{PAGES_BASE_URL}/{filename}"
    payload = {
        "embeds": [
            {
                "title": f"📚 期刊週報 {label}",
                "url": weekly_url,
                "description": f"本週週報已更新，共 {count} 篇。",
                "color": DISCORD_EMBED_COLOR,
                "fields": [
                    {"name": "🔗 完整週報", "value": weekly_url, "inline": False},
                ],
                "footer": {"text": "JournalFetcher"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ]
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
    except requests.RequestException as e:
        print(f"[discord] webhook request failed: {type(e).__name__}")
        return False

    if resp.status_code in (200, 204):
        print(f"[discord] webhook sent OK ({resp.status_code})")
        return True
    print(f"[discord] webhook failed: {resp.status_code} {resp.text[:200]}")
    return False


def send_appraisal_notice(label: str, article: dict, appraisal_url: str) -> bool:
    """Send a Discord notice with a single completed appraisal link."""
    load_dotenv(ROOT / ".env")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("[discord] DISCORD_WEBHOOK_URL not set in .env; skip")
        return False

    full_url = appraisal_url
    if not full_url.startswith(("http://", "https://")):
        full_url = f"{PAGES_BASE_URL}/{appraisal_url.lstrip('/')}"

    doi = str(article.get("doi", "")).strip()
    pmid = str(article.get("pmid", "")).strip()
    source_url = f"https://doi.org/{doi}" if doi else (
        f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
    )
    fields = [
        {"name": "🔗 完整評讀", "value": full_url, "inline": False},
    ]
    if source_url:
        fields.append({"name": "原文", "value": source_url, "inline": False})

    payload = {
        "embeds": [
            {
                "title": "文獻評讀完成",
                "url": full_url,
                "description": _truncate(article.get("title", ""), 180),
                "color": DISCORD_EMBED_COLOR,
                "fields": fields,
                "footer": {
                    "text": f"JournalFetcher · {label} · {article.get('journal_key') or article.get('journal', '')}"
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ]
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
    except requests.RequestException as e:
        print(f"[discord] webhook request failed: {type(e).__name__}")
        return False

    if resp.status_code in (200, 204):
        print(f"[discord] appraisal webhook sent OK ({resp.status_code})")
        return True
    print(f"[discord] webhook failed: {resp.status_code} {resp.text[:200]}")
    return False


if __name__ == "__main__":
    # Quick test: build and print an example embed without sending
    sample_articles = [
        {
            "journal_key": "NEJM",
            "title": "Tirzepatide in Heart Failure with Preserved Ejection Fraction.",
            "doi": "10.1056/NEJMoaSAMPLE",
            "pmid": "12345678",
        },
        {
            "journal_key": "Lancet",
            "title": "AI-assisted CXR interpretation: a multicenter RCT.",
            "doi": "",
            "pmid": "12345679",
        },
        {
            "journal_key": "JACC",
            "title": "Five-year follow-up of TAVI vs SAVR in low-risk patients.",
            "doi": "10.1016/j.jacc.SAMPLE",
            "pmid": "12345680",
        },
    ]
    sample_counts = [
        {"name": "NEJM", "count": 5},
        {"name": "Lancet", "count": 4},
        {"name": "JACC", "count": 8},
    ]
    embed = _build_embed("2026-W19", sample_articles, sample_counts, "2026-W19.html")
    import json as _json
    print(_json.dumps(embed, ensure_ascii=False, indent=2))
