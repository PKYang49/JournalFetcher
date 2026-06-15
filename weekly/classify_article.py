"""Classify an article into one of ~10 routes so appraise_selected can load
only the relevant SKILL fragment instead of the full 1180-line SKILL.md.

Resolution chain:

  1. local heuristic on title + journal + pub_type — covers ~50% of articles
     directly because PubMed already tags Meta-Analysis / Systematic Review /
     Practice Guideline / RCT / Editorial etc. authoritatively.
  2. claude -p Haiku 4.5 (~$0.005/article, 30-60ms) — only on heuristic miss.
  3. codex exec GPT 5.4 — when Haiku is unavailable (claude exhausted / rate
     limit / transient error). Mirrors the summary backend's claude→codex
     fallback so an ambiguous article still gets a real route instead of
     loading the full SKILL.
  4. "default" → caller loads the full SKILL.md (= pre-fragmentation behavior),
     only when both LLM backends are unavailable or unsure.

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


def _classify_with_codex(prompt: str, timeout: int = 120) -> str | None:
    """Classify via `codex exec` GPT 5.4 when Haiku is unavailable.

    Returns a valid route, or None on codex failure / unparseable output.
    Confidence is ignored here: codex is already the degraded path, so a
    parseable route is preferred over falling through to the full SKILL.
    """
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
        except (OSError, subprocess.TimeoutExpired) as e:
            print(f"  [warn] codex classify error: {str(e)[:160]}", file=sys.stderr)
            return None
        if result.returncode != 0:
            err = (result.stderr or result.stdout).strip()
            print(f"  [warn] codex classify exit {result.returncode}: {err[:160]}", file=sys.stderr)
            return None
        text = output_path.read_text().strip() if output_path.exists() else result.stdout.strip()
    route, _confidence = _parse_route(text)
    return route


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


# PubMed pub_type label → route. Labels come from modules.pubmed.PUB_TYPE_PRIORITY.
# Exclude the ambiguous ones ("Review" alone could be narrative or SR-without-MA-tag;
# "Original" is just Journal Article and tells us nothing about study type) — those
# fall through to title heuristics or the LLM.
_PUB_TYPE_ROUTE = {
    "Meta-Analysis": "sr",
    "Systematic Review": "sr",
    "Guideline": "cpg",
    "RCT": "rct",
    "Phase IV Trial": "rct",
    "Phase III Trial": "rct",
    "Phase II Trial": "rct",
    "Phase I Trial": "rct",
    "Trial": "rct",
    "Editorial": "narrative",
    "Comment": "narrative",
    "Letter": "narrative",
    "News": "narrative",
    "Observational": "observational",
    "Case Report": "observational",
}


def _classify_by_heuristic(
    title: str, journal: str, pub_type: str = ""
) -> str | None:
    """Rule-based fallback. Conservative — return None on no clear match.

    Resolution order (most-specific first):
      1. Title-only NMA detection (PubMed has no NMA pub_type — it tags as Meta-Analysis).
      2. CPG / scientific-statement title patterns. CPG outranks the pub_type "Review"
         tag that ACC/AHA / ESC documents sometimes also carry.
      3. PubMed pub_type for clear-cut tags (Meta-Analysis, Systematic Review,
         Practice Guideline, RCT, Editorial, Observational, etc.).
      4. Title heuristics for everything pub_type does not cover (scoping review,
         narrative review, diagnostic accuracy, observational design names, etc.).
      5. Last-resort: pub_type "Review" alone → narrative (no SR/CPG signal found
         above, so it is almost certainly a narrative / educational review).
    """
    t = (title or "").lower()
    pt = (pub_type or "").strip()

    # 1. NMA: PubMed has no separate NMA tag; the title is the only reliable signal.
    if "network meta-analysis" in t:
        return "nma"

    # 2. CPG title patterns — must run before pub_type because guideline documents
    # are sometimes tagged only as "Review" in PubMed, and ACC/AHA / ESC framing
    # is more authoritative than the generic Review tag.
    #
    # Bare "guideline" alone is too greedy ("guideline-recommended LDL",
    # "guideline-directed therapy", "off-guideline", etc. all appear in
    # observational / RCT titles). Strip these compounds before matching.
    t_for_cpg = t
    for compound in (
        "guideline-recommended",
        "guideline-directed",
        "guideline-based",
        "guideline-concordant",
        "guideline-indicated",
        "guideline-eligible",
        "guideline-adherent",
        "off-guideline",
        "non-guideline",
    ):
        t_for_cpg = t_for_cpg.replace(compound, "")
    if any(k in t_for_cpg for k in (
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

    # 3. PubMed pub_type — authoritative for the labels in _PUB_TYPE_ROUTE.
    if pt in _PUB_TYPE_ROUTE:
        return _PUB_TYPE_ROUTE[pt]

    # 4. Title-based patterns for routes pub_type does not cover.
    # Pure consensus (no formal CPG framing)
    if any(k in t for k in (
        "consensus statement",
        "delphi study", "delphi survey", "delphi consensus", "delphi process",
        "position paper", "position statement",
    )):
        return "consensus"

    # SR via title (scoping / umbrella reviews lack a dedicated pub_type)
    if any(k in t for k in (
        "meta-analysis", "meta analysis",
        "systematic review",
        "scoping review",
        "umbrella review",
    )):
        return "sr"

    # Narrative / editorial / perspective (title-side; pub_type already handled
    # the Editorial/Comment/Letter/News cases above)
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

    # RCT title patterns (catches trials that PubMed has not yet retagged)
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

    # Observational title patterns
    if any(k in t for k in (
        "cohort study", "prospective cohort",
        "case-control", "case control",
        "cross-sectional",
        "registry analysis", "registry study",
        "real-world",
    )):
        return "observational"

    # 5. pub_type "Review" with no SR/CPG/narrative title cues → narrative.
    # ("Original" / "" / unknown stay as None so the caller can ask the LLM.)
    if pt == "Review":
        return "narrative"

    return None


def classify(article: dict, markdown_head: str) -> tuple[str, str]:
    """Classify an article. Returns (route, source).

    source ∈ {"heuristic", "haiku", "codex", "fallback"}
    route is one of VALID_ROUTES; "default" means no confident classification.

    Heuristic runs first because PubMed's pub_type tagging is authoritative for
    Meta-Analysis / Systematic Review / Practice Guideline / RCT / Editorial /
    etc. Only when title + journal + pub_type leave the route ambiguous do we
    pay for an LLM call: claude Haiku first, then codex GPT 5.4 when Haiku is
    unavailable (exhausted / rate limit / transient error). Only when both
    backends fail or are unsure do we return "default" (full SKILL backstop).
    """
    title = article.get("title", "") or ""
    journal = article.get("journal") or article.get("journal_key", "") or ""
    pub_type = article.get("pub_type", "") or ""

    heuristic_route = _classify_by_heuristic(title, journal, pub_type)
    if heuristic_route:
        return heuristic_route, "heuristic"

    # Heuristic missed — title is genuinely ambiguous (e.g. pub_type=Original
    # with no design keywords). Build the LLM prompt once; both backends share it.
    head = (markdown_head or "")[:_HEAD_CHARS]
    prompt = _CLASSIFY_PROMPT.format(
        head_chars=_HEAD_CHARS,
        title=title,
        journal=journal,
        head=head,
    )

    # Haiku first, but only if claude is still available this process.
    if not claude_exec.claude_exhausted_this_run():
        try:
            text, _cost = claude_exec.run_claude_prompt(
                prompt,
                model=claude_exec.get_claude_summary_model(),
                timeout=60,
            )
            route, confidence = _parse_route(text)
            # Trust high/medium confidence. low-confidence → "default" so we
            # don't lock in a guess; caller loads the full SKILL. (We do not
            # second-guess a usable Haiku answer with codex.)
            if route and confidence != "low":
                return route, "haiku"
            return "default", "fallback"
        except claude_exec.ClaudeLimitError:
            # run_claude_prompt does not flip the flag itself; set it so later
            # classify() calls skip Haiku and go straight to codex.
            claude_exec._session["claude_exhausted"] = True
        except claude_exec.ClaudeError:
            # Transient (timeout / parse / 5xx): fall through to codex this call.
            pass

    # Haiku unavailable → codex GPT 5.4 fallback. Only "default" if codex also
    # fails to produce a valid route.
    route = _classify_with_codex(prompt)
    if route:
        return route, "codex"
    return "default", "fallback"


if __name__ == "__main__":
    samples = [
        # Title-driven (no pub_type needed)
        {
            "title": "2026 ACC/AHA Guideline for the Management of Patients With Dyslipidemia",
            "journal": "JACC",
            "pub_type": "Guideline",
            "expected": "cpg",
        },
        {
            "title": "Comparative Effectiveness of SGLT2 Inhibitors: A Network Meta-Analysis",
            "journal": "JAMA",
            "pub_type": "Meta-Analysis",
            "expected": "nma",
        },
        {
            "title": "Cardiac Rehabilitation in Athletes: An International Delphi Consensus",
            "journal": "BJSM",
            "pub_type": "Review",
            "expected": "consensus",
        },
        {
            "title": "Pathophysiology of HFpEF: A State-of-the-Art Review",
            "journal": "EHJ",
            "pub_type": "Review",
            "expected": "narrative",
        },
        {
            "title": "External Validation of the PREVENT Risk Prediction Model",
            "journal": "Circulation",
            "pub_type": "Original",
            "expected": "diagnostic",
        },
        # pub_type drives classification when title is generic
        {
            "title": "Effect of Daily Caffeine on Cardiovascular Events",
            "journal": "NEJM",
            "pub_type": "RCT",
            "expected": "rct",
        },
        {
            "title": "Statins for Primary Prevention",
            "journal": "Lancet",
            "pub_type": "Systematic Review",
            "expected": "sr",
        },
        {
            "title": "Editorial on the SGLT2 Era",
            "journal": "Circulation",
            "pub_type": "Editorial",
            "expected": "narrative",
        },
        {
            "title": "Outcomes in TAVR Recipients: A Multi-Center Cohort",
            "journal": "JACC",
            "pub_type": "Observational",
            "expected": "observational",
        },
        # pub_type "Review" alone with no SR/CPG/narrative title cue → narrative
        {
            "title": "Inflammation in Atherosclerosis",
            "journal": "EHJ",
            "pub_type": "Review",
            "expected": "narrative",
        },
        # Regression: "guideline-recommended" in an observational title must NOT
        # be misrouted to cpg (W22 Lipoprotein(a) paper).
        {
            "title": "Lipoprotein(a) and residual cardiovascular risks in patients "
                     "who achieve guideline-recommended LDL cholesterol goals",
            "journal": "EHJ",
            "pub_type": "Original",
            "expected": None,
        },
        # Scientific statement should still hit cpg even with pub_type "Review"
        {
            "title": "Cardiac Intensive Care Unit Appropriate Patient Selection and "
                     "Triage: A Scientific Statement From the American Heart Association",
            "journal": "Circulation",
            "pub_type": "Review",
            "expected": "cpg",
        },
        # No pub_type, generic title → None (LLM)
        {
            "title": "Some random article with no clear cue",
            "journal": "JAMA",
            "pub_type": "",
            "expected": None,
        },
        # Original article, no title cue → None (LLM decides; could be RCT/obs)
        {
            "title": "Long-term outcomes after percutaneous coronary intervention",
            "journal": "Circulation",
            "pub_type": "Original",
            "expected": None,
        },
    ]
    print("Testing _classify_by_heuristic (title + journal + pub_type):")
    failures = 0
    for s in samples:
        got = _classify_by_heuristic(s["title"], s["journal"], s.get("pub_type", ""))
        ok = got == s["expected"]
        if not ok:
            failures += 1
        marker = "OK" if ok else f"FAIL (expected {s['expected']!r})"
        print(
            f"  {marker:>30s}  got={got!r:>15s}  pt={s.get('pub_type', '')!r:>20s}  "
            f"title={s['title'][:55]}"
        )
    print(f"\n{len(samples) - failures}/{len(samples)} passed")
    raise SystemExit(1 if failures else 0)
