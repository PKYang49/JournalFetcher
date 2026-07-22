"""Select weekly highlighted articles using local feedback and Codex CLI."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

from modules.codex_exec import run_codex_exec
from modules.codex_model import get_summary_model

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FEEDBACK_PATH = DATA_DIR / "interest_feedback.jsonl"
PROFILE_PATH = DATA_DIR / "interest_profile.md"

DEFAULT_PROFILE = """# User Interest Profile

## Strong positive signals
- Cardiology with direct clinical decision impact
- Cardiovascular devices, procedures, imaging, electrophysiology, heart failure, prevention, and outcomes
- Practice-changing RCTs, high-quality observational studies, guidelines, systematic reviews, and clinically useful diagnostics
- Studies with clear effect sizes, patient-important outcomes, safety tradeoffs, or implementation implications

## Negative signals
- Purely basic science without near-term clinical interpretation
- Editorials, news, narrative opinion pieces, and very small hypothesis-generating reports unless unusually important
- Surrogate-only studies with weak clinical relevance
"""

SELECTION_PROMPT = """你是心臟科醫師的每週文獻策展助手。

任務：從候選文章中精選 {limit} 篇最值得完整下載 PDF 並評讀的文章。

偏好設定：
{profile}

近期回饋 JSONL（可為空）：
{feedback}

選文原則：
- 優先心臟科、醫療設備、臨床決策、治療/診斷流程會改變的文章。
- 優先 RCT、guideline、systematic review/meta-analysis、高品質觀察性研究、重要 diagnostic/prognostic/model 文章。
- 可選非心臟文章，但必須有跨科臨床重要性或方法學價值。
- 避免 editorial/news/commentary，除非它本身就是本週最重要的臨床脈絡。
- 重視「近期回饋」：那是使用者對過去週報文章（含精選與一般摘要文章）按下的
  真實興趣評價。verdict 為 not_interested 的文章方向（期刊、主題、文章類型）
  要明顯降低權重，interested 的要提高；尤其使用者對「未被選為精選」的文章標記
  interested，代表那類文章下次應優先考慮。回饋與上方偏好設定衝突時，以回饋為準。
- 每篇給 score 0-100 與 reason，reason 用繁體中文，具體說明為什麼值得評讀。

候選文章 JSON：
{articles_json}

只輸出 JSON，不要 Markdown，不要解釋。格式：
{{
  "selected": [
    {{"pmid": "...", "doi": "...", "score": 92, "reason": "...", "tags": ["..."]}}
  ]
}}
"""


def ensure_profile() -> Path:
    """Create the local interest profile if missing."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not PROFILE_PATH.exists():
        PROFILE_PATH.write_text(DEFAULT_PROFILE, encoding="utf-8")
    if not FEEDBACK_PATH.exists():
        FEEDBACK_PATH.write_text("", encoding="utf-8")
    return PROFILE_PATH


def load_profile() -> str:
    ensure_profile()
    return PROFILE_PATH.read_text(encoding="utf-8").strip()


def load_recent_feedback(max_lines: int = 80) -> str:
    ensure_profile()
    lines = [
        line.strip()
        for line in FEEDBACK_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return "\n".join(lines[-max_lines:])


def _article_payload(articles: list[dict]) -> list[dict]:
    payload = []
    for i, article in enumerate(articles):
        payload.append(
            {
                "index": i,
                "pmid": article.get("pmid", ""),
                "doi": article.get("doi", ""),
                "journal_key": article.get("journal_key", ""),
                "journal": article.get("journal", ""),
                "year": article.get("year", ""),
                "pub_type": article.get("pub_type", ""),
                "title": article.get("title", ""),
                "authors": article.get("authors", [])[:6],
                "abstract": article.get("abstract", ""),
                "summary": article.get("summary", ""),
                "commentary": article.get("commentary", ""),
            }
        )
    return payload


def _run_codex_prompt(prompt: str, timeout: int = 240) -> str | None:
    return run_codex_exec(
        prompt,
        model=get_summary_model(),
        timeout=timeout,
        tmp_prefix="codex_weekly_select_",
        label="codex selection",
        err_truncate=300,
    )


def _parse_selection(text: str) -> list[dict]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end >= start:
        cleaned = cleaned[start : end + 1]
    data = json.loads(cleaned)
    selected = data.get("selected", [])
    if not isinstance(selected, list):
        raise ValueError("selection JSON field 'selected' must be a list")
    return [item for item in selected if isinstance(item, dict)]


def _fallback_select(articles: list[dict], limit: int) -> list[dict]:
    preferred_types = {
        "RCT",
        "Trial",
        "Phase III Trial",
        "Guideline",
        "Systematic Review",
        "Meta-Analysis",
        "Observational",
        "Original",
    }
    preferred_journals = {"NEJM", "Lancet", "JAMA", "JACC", "Circulation", "EHJ"}

    ranked = []
    for article in articles:
        score = 40
        if article.get("journal_key") in preferred_journals:
            score += 15
        if article.get("pub_type") in preferred_types:
            score += 20
        text = f"{article.get('title', '')} {article.get('abstract', '')}".lower()
        for term in (
            "cardio",
            "heart",
            "coronary",
            "myocard",
            "atrial",
            "valve",
            "device",
            "implant",
            "stent",
            "heart failure",
        ):
            if term in text:
                score += 5
        ranked.append((score, article))

    ranked.sort(key=lambda item: item[0], reverse=True)
    selected = []
    for score, article in ranked[:limit]:
        selected.append(
            {
                "pmid": article.get("pmid", ""),
                "doi": article.get("doi", ""),
                "score": score,
                "reason": "規則式 fallback 選出：期刊、文章類型與心臟科關鍵字整體較符合偏好。",
                "tags": ["fallback"],
            }
        )
    return selected


def _write_selection_raw(
    out_dir: Path,
    *,
    response: str | None,
    items: list[dict],
) -> Path:
    """Persist the selector's raw reply so a bad pick can be audited later."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "selection_raw.json"
    payload = {
        "model": get_summary_model(),
        "response": response,
        "parsed": items,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _normalize_doi(value: object) -> str:
    """Return a lowercase bare DOI suitable for identity comparisons."""
    doi = unquote(str(value or "")).strip()
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE)
    doi = re.sub(
        r"^(?:https?://)?(?:dx\.)?doi\.org/",
        "",
        doi,
        flags=re.IGNORECASE,
    )
    return doi.strip().rstrip(".").lower()


def _resolve_item(
    item: dict,
    article_by_pmid: dict[str, dict],
    article_by_doi: dict[str, dict],
) -> dict | None:
    """Map one selection item back to a candidate article.

    The model emits pmid and doi for the same paper, so they must agree. A
    transposed digit in the pmid can still land on a different-but-real article
    in the pool (seen in 2026-W30: 42024568 -> 42024048), which silently
    attaches one paper's reason to another. When both identifiers resolve but
    disagree, reject the item; neither model-emitted identifier is inherently
    trustworthy enough to choose over the other.
    """
    pmid = str(item.get("pmid", ""))
    doi = _normalize_doi(item.get("doi", ""))
    by_pmid = article_by_pmid.get(pmid) if pmid else None
    by_doi = article_by_doi.get(doi) if doi else None

    if by_pmid is not None and by_doi is not None and by_pmid is not by_doi:
        print(
            f"  [warn] selection pmid/doi disagree: pmid={pmid} -> "
            f"{by_pmid.get('title', '')[:60]!r}, doi={doi} -> "
            f"{by_doi.get('title', '')[:60]!r}; rejecting item",
            file=sys.stderr,
        )
        return None
    if by_pmid is not None:
        if doi and by_doi is None:
            print(
                f"  [warn] selection doi {doi} not in candidate pool "
                f"(pmid={pmid}); cannot cross-check",
                file=sys.stderr,
            )
        return by_pmid
    return by_doi


def select_top_articles(
    articles: list[dict],
    limit: int = 2,
    out_dir: Path | None = None,
) -> list[dict]:
    """Annotate and return selected articles."""
    if limit <= 0:
        return []

    payload = _article_payload(articles)
    prompt = SELECTION_PROMPT.format(
        limit=limit,
        profile=load_profile(),
        feedback=load_recent_feedback() or "(no feedback yet)",
        articles_json=json.dumps(payload, ensure_ascii=False),
    )

    selection_items: list[dict]
    response: str | None = None
    try:
        response = _run_codex_prompt(prompt)
        selection_items = _parse_selection(response) if response else []
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError, ValueError) as e:
        print(f"  [warn] selection via codex failed; using fallback: {e}", file=sys.stderr)
        selection_items = []

    if out_dir is not None:
        _write_selection_raw(out_dir, response=response, items=selection_items)

    if not selection_items:
        selection_items = _fallback_select(articles, limit)

    article_by_pmid = {str(a.get("pmid", "")): a for a in articles if a.get("pmid")}
    article_by_doi = {
        normalized: article
        for article in articles
        if (normalized := _normalize_doi(article.get("doi", "")))
    }
    selected_articles: list[dict] = []
    seen_ids: set[str] = set()

    for item in selection_items:
        article = _resolve_item(item, article_by_pmid, article_by_doi)
        if article is None:
            continue

        key = str(article.get("pmid") or article.get("doi") or id(article))
        if key in seen_ids:
            continue
        seen_ids.add(key)
        article["selected_for_appraisal"] = True
        article["selection_score"] = item.get("score")
        article["selection_reason"] = item.get("reason", "")
        article["selection_tags"] = item.get("tags", [])
        selected_articles.append(article)
        if len(selected_articles) >= limit:
            break

    if len(selected_articles) < limit:
        for item in _fallback_select(articles, limit):
            pmid = str(item.get("pmid", ""))
            article = article_by_pmid.get(pmid)
            if not article:
                continue
            key = str(article.get("pmid") or article.get("doi") or id(article))
            if key in seen_ids:
                continue
            seen_ids.add(key)
            article["selected_for_appraisal"] = True
            article["selection_score"] = item.get("score")
            article["selection_reason"] = item.get("reason", "")
            article["selection_tags"] = item.get("tags", [])
            selected_articles.append(article)
            if len(selected_articles) >= limit:
                break

    return selected_articles


def write_selected_metadata(selected: list[dict], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "selected_articles.json"
    path.write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
