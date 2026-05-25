"""Classify an article into one of ~10 routes so appraise_selected can load
only the relevant SKILL fragment instead of the full 1180-line SKILL.md.

Layered fallback (matches the rest of the pipeline's philosophy: "don't track
spend locally; let the error signal drive switching"):

  1. claude -p Haiku 4.5 (primary, ~$0.005/article, 30-60ms)
  2. codex exec gpt-5.4 (limit-hit fallback, via try_claude_or_fallback)
  3. local heuristic on title + journal (covers ~95% by simple keywords)
  4. "default" → caller loads the full SKILL.md (= pre-fragmentation behavior)

The classification is stateless here; the caller persists it on the article
dict (article["appraisal_route"] / ["appraisal_route_source"]) so subsequent
runs (e.g. manual re-appraisal requests) skip the LLM call.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from modules import claude_exec
from modules.codex_model import codex_exec_env, get_summary_model, resolve_codex_cli

# Routes correspond 1:1 with skill fragment files. Keep in sync with
# weekly/appraise_selected.py::_FRAGMENT_BY_ROUTE.
VALID_ROUTES = (
    "rct",
    "observational",
    "preclinical",
    "sr",
    "nma",
    "cpg",
    "consensus",
    "narrative",
    "diagnostic",
    "default",
)

# Cap markdown sent to the classifier. 3k chars ≈ abstract + intro opening,
# enough to identify article type without paying Haiku context cost for the
# full guideline / SR.
_HEAD_CHARS = 3000

_CLASSIFY_PROMPT = """你是醫學文獻分類助手。根據以下文章 metadata 與前 {head_chars} 字內容，判斷文章類型，從以下 10 個 route 中擇一：

- rct: 隨機對照試驗、cross-over、pilot study、任何 RCT 設計
- observational: 世代研究、病例對照、橫斷面、registry 分析、target-trial emulation
- preclinical: 動物、細胞、ex vivo、機轉研究、phase I first-in-human
- sr: systematic review、meta-analysis、scoping review、umbrella review
- nma: network meta-analysis、multiple treatment comparison
- cpg: clinical practice guideline、scientific statement、學會發布的 recommendations（即使含部分共識成分也歸 cpg）
- consensus: 純 consensus statement、Delphi、position paper（不含正式 CPG 框架）
- narrative: narrative review、state-of-the-art、focus seminar、教育綜述、editorial、commentary、perspective、viewpoint
- diagnostic: diagnostic accuracy、AI/ML 模型、clinical prediction rule、prognostic model 驗證
- default: 上述都不符合、或同時涵蓋多型而難以單選

判斷優先序：
1. 看文章自述（abstract / introduction）的設計描述
2. 自稱 review 但缺乏 PRISMA / 預先 PICO / 雙人篩選 → narrative
3. 同時是 guideline 又含 Delphi → cpg（cpg fragment 也涵蓋共識評估）
4. 含 sensitivity/specificity/AUC/calibration plot/ML model validation → diagnostic
5. 不確定 → confidence: low

只輸出 JSON，不加任何其他文字、不加 markdown code fence：
{{"route": "xxx", "confidence": "high|medium|low", "reason": "1 句話理由"}}

Title: {title}
Journal: {journal}

前 {head_chars} 字 markdown：
{head}
"""


def _run_codex_classify(prompt: str, timeout: int = 60) -> str | None:
    """Fallback path: codex exec with summary model (gpt-5.4)."""
    with tempfile.TemporaryDirectory(prefix="codex_classify_") as tmp_dir:
        output_path = Path(tmp_dir) / "last_message.txt"
        try:
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
        except FileNotFoundError:
            print("  [warn] codex CLI not found; classifier falling through to heuristic", file=sys.stderr)
            return None
        except subprocess.TimeoutExpired:
            print("  [warn] codex classify timed out; falling through to heuristic", file=sys.stderr)
            return None
        if result.returncode != 0:
            err = (result.stderr or result.stdout).strip()
            print(f"  [warn] codex classify error: {err[:200]}", file=sys.stderr)
            return None
        if output_path.exists():
            return output_path.read_text().strip()
        return result.stdout.strip() or None


def _parse_route(text: str) -> tuple[str | None, str | None]:
    """Extract (route, confidence) from a model response. Returns (None, None)
    if the response isn't usable. Confidence is "high" / "medium" / "low" or None.
    """
    if not text:
        return None, None
    # Strip common wrappers: ```json ... ``` fences, leading prose
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```\s*$", "", cleaned)
    # First {...} block in the response
    m = re.search(r"\{[^{}]*\}", cleaned, re.DOTALL)
    if not m:
        return None, None
    try:
        payload = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None, None
    route = str(payload.get("route", "")).strip().lower()
    if route not in VALID_ROUTES:
        return None, None
    confidence = str(payload.get("confidence", "")).strip().lower() or None
    return route, confidence


def _classify_by_heuristic(title: str, journal: str) -> str | None:
    """Rule-based fallback. Conservative — return None on no clear match."""
    t = (title or "").lower()

    # CPG / scientific statement — checked first because guideline titles
    # often also contain "consensus" or "expert" keywords
    if any(k in t for k in (
        "guideline", "guidelines",
        "scientific statement",
        "recommendations for",
        "recommendation for",
        "aha/acc", "acc/aha",
        "esc guidelines", "esc guideline",
        "nice guideline",
        "expert consensus document",
        "expert consensus decision pathway",
    )):
        return "cpg"

    # Pure consensus (no formal CPG framing)
    if any(k in t for k in (
        "consensus statement",
        "delphi study", "delphi survey", "delphi consensus", "delphi process",
        "position paper", "position statement",
    )):
        return "consensus"

    # SR / NMA
    if "network meta-analysis" in t:
        return "nma"
    if any(k in t for k in (
        "meta-analysis", "meta analysis",
        "systematic review",
        "scoping review",
        "umbrella review",
    )):
        return "sr"

    # Narrative / editorial / perspective
    if any(k in t for k in (
        "narrative review",
        "state of the art", "state-of-the-art",
        "primer on", "primer for",
        "in brief",
        "editorial",
        "viewpoint", "perspective on",
    )):
        return "narrative"

    # Diagnostic / prediction model
    if any(k in t for k in (
        "diagnostic accuracy", "diagnostic performance",
        "prediction model", "prognostic model",
        "clinical prediction rule",
        "development and validation",
        "external validation",
    )):
        return "diagnostic"

    # RCT
    if any(k in t for k in (
        "randomized trial", "randomised trial",
        "randomized clinical trial", "randomised clinical trial",
        "randomized controlled trial", "randomised controlled trial",
        "double-blind", "double blind",
        "placebo-controlled",
        "phase iii", "phase 3 trial",
        "phase ii trial", "phase 2 trial",
        "non-inferiority trial",
    )):
        return "rct"

    # Observational
    if any(k in t for k in (
        "cohort study", "prospective cohort",
        "case-control", "case control",
        "cross-sectional",
        "registry analysis", "registry study",
        "real-world",
    )):
        return "observational"

    return None


def classify(article: dict, markdown_head: str) -> tuple[str, str]:
    """Classify an article. Returns (route, source).

    source ∈ {"haiku", "codex", "heuristic", "fallback"}
    route is one of VALID_ROUTES; "default" means no confident classification.
    """
    title = article.get("title", "") or ""
    journal = article.get("journal") or article.get("journal_key", "") or ""
    head = (markdown_head or "")[:_HEAD_CHARS]

    prompt = _CLASSIFY_PROMPT.format(
        head_chars=_HEAD_CHARS,
        title=title,
        journal=journal,
        head=head,
    )

    text, backend = claude_exec.try_claude_or_fallback(
        prompt,
        claude_model=claude_exec.get_claude_summary_model(),
        fallback=lambda: _run_codex_classify(prompt, timeout=60),
        timeout=60,
        label="classify",
    )

    if text:
        route, confidence = _parse_route(text)
        # Trust high/medium confidence from the LLM. low-confidence → drop to
        # heuristic so we don't lock in a guess.
        if route and confidence != "low":
            source = "haiku" if backend == "claude" else "codex"
            return route, source

    # Heuristic on title + journal
    heuristic_route = _classify_by_heuristic(title, journal)
    if heuristic_route:
        return heuristic_route, "heuristic"

    # No confident classification — caller loads the full skill
    return "default", "fallback"


if __name__ == "__main__":
    samples = [
        {
            "title": "2026 ACC/AHA Guideline for the Management of Patients With Dyslipidemia",
            "journal": "JACC",
            "expected": "cpg",
        },
        {
            "title": "Effect of Daily Caffeine on Cardiovascular Events: A Randomized Trial",
            "journal": "NEJM",
            "expected": "rct",
        },
        {
            "title": "Statins for Primary Prevention: A Systematic Review and Meta-Analysis",
            "journal": "Lancet",
            "expected": "sr",
        },
        {
            "title": "Comparative Effectiveness of SGLT2 Inhibitors: A Network Meta-Analysis",
            "journal": "JAMA",
            "expected": "nma",
        },
        {
            "title": "External Validation of the PREVENT Risk Prediction Model",
            "journal": "Circulation",
            "expected": "diagnostic",
        },
        {
            "title": "Pathophysiology of HFpEF: A State-of-the-Art Review",
            "journal": "EHJ",
            "expected": "narrative",
        },
        {
            "title": "Cardiac Rehabilitation in Athletes: An International Delphi Consensus",
            "journal": "BJSM",
            "expected": "consensus",
        },
        {
            "title": "Some random article with no clear cue",
            "journal": "JAMA",
            "expected": None,
        },
    ]
    print("Testing _classify_by_heuristic:")
    for s in samples:
        got = _classify_by_heuristic(s["title"], s["journal"])
        ok = "OK" if got == s["expected"] else f"FAIL (expected {s['expected']})"
        print(f"  {ok:>30s}  got={got!r:>15s}  title={s['title'][:60]}")
