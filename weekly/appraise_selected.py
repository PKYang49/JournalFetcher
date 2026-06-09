"""Generate weekly appraisals for selected downloaded PDFs.

Primary backend: `claude -p` with Opus 4.6 (Agent SDK credit; identical
per-token price to 4.7 but uses the older, more efficient tokenizer).
Fallback: `codex exec` with GPT 5.5, used automatically when claude reports
a rate-limit / credit-exhausted error.

PDF→markdown: pymupdf4llm (preserves heading levels, multi-column flow, and
table structure that MarkItDown collapses into runs of text). MarkItDown is
retained as a safety fallback in case pymupdf4llm fails on a specific PDF.
Reference lists are stripped after conversion — they're 30-40% of markdown
on guidelines / SR and contribute nothing to critical appraisal.

Skill routing: `classify_article.classify()` pre-determines the route
(rct/sr/cpg/narrative/diagnostic/…). It tries the local heuristic (title +
journal + PubMed pub_type) first, then falls back to claude Haiku 4.5 only
when the heuristic is ambiguous; if claude is exhausted the route becomes
"default" (loads the full SKILL = pre-fragmentation behavior). Once a route
is picked, only its fragment from `skills/literature-appraisal/fragments/`
is loaded into the system prompt instead of the full SKILL.md.

Prompt structure (v3.6): the cacheable system prompt is composed of
SKILL.md + route fragment + output-quality style guide + per-route JAMA
references catalog (`_build_references_section`). Passed to claude via
`--append-system-prompt-file` so it auto-caches (1h ephemeral). When the
route has a JAMA mapping, claude is also given `--add-dir <references>`
and the Read tool to lazily pull individual reference files; codex sees the
same absolute paths in the prompt and uses its read-only sandbox shell.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from contextlib import nullcontext
from pathlib import Path

from modules import claude_exec
from modules.codex_model import codex_exec_env, get_appraisal_model, resolve_codex_cli
from weekly import classify_article
from weekly.figure_extract import extract_figure_pages

ROOT = Path(__file__).resolve().parent.parent
SKILL_DIR = ROOT / "skills" / "literature-appraisal"
SKILL_PATH = SKILL_DIR / "SKILL.md"
FRAGMENTS_DIR = SKILL_DIR / "fragments"
REFERENCES_DIR = SKILL_DIR / "references"
JAMA_REFERENCES_DIR = REFERENCES_DIR / "jamaevidence"
STYLE_GUIDE_PATH = REFERENCES_DIR / "output_quality_style_guide.md"

# route → list of JAMA Users' Guides filenames that match this route's
# methodology. Files live under JAMA_REFERENCES_DIR. An empty list means
# "no specific JAMA guide applies to this route"; the model can still consult
# the full catalog via `references/index.md` if needed.
#
# Maintained alongside the file inventory in references/index.md — if a new
# JAMA guide is added, update both.
_REFERENCES_BY_ROUTE: dict[str, tuple[str, ...]] = {
    "rct": (
        "jama_271_9_039.md",
        "jug130002.md",
        "jug120004_2605_2611.md",
        "jama_park_2022_ug_210002_1642635648.12041.md",
        "ssc160002.md",
    ),
    "observational": (
        "jama_agoritsas_2017_ug_160001.md",
        "hernanrobins_WhatIf_2jan25.md",
    ),
    "preclinical": (),
    "sr": (
        "jug140001.md",
        "jug120001_1246_1253.md",
    ),
    "nma": (
        "jug140001.md",
        "jug120001_1246_1253.md",
    ),
    "cpg": (
        "jama_brignardellopetersen_2021_ug_210001_1643153219.93024.md",
    ),
    "consensus": (
        "jama_brignardellopetersen_2021_ug_210001_1643153219.93024.md",
    ),
    "narrative": (),
    "diagnostic": (
        "jama_alba_2017_ug_170001.md",
        "jama_liu_2019_ug_190001.md",
        "jml90008.md",
    ),
    "default": (),
}

# route → fragment filename(s). Keep in sync with classify_article.VALID_ROUTES
# and the actual files in skills/literature-appraisal/fragments/. "default"
# concatenates all fragments → equivalent to the pre-fragmentation full SKILL.md
# (zero-risk safety net when classification is uncertain).
_FRAGMENT_BY_ROUTE: dict[str, tuple[str, ...]] = {
    "rct": ("skill_a.md",),
    "observational": ("skill_a.md",),
    "preclinical": ("skill_a.md",),
    "sr": ("skill_b_sr.md",),
    "nma": ("skill_b_sr.md",),
    "cpg": ("skill_b_cpg.md",),
    "consensus": ("skill_b_cpg.md",),
    "narrative": ("skill_b_narrative.md",),
    "diagnostic": ("skill_c.md",),
    "default": (
        "skill_a.md",
        "skill_b_sr.md",
        "skill_b_cpg.md",
        "skill_b_narrative.md",
        "skill_c.md",
    ),
}

# Size backstop for the article markdown fed into one appraisal. This is a
# safety limit, NOT a routine truncation cap: normal papers (~40-90k chars)
# and full clinical practice guidelines (the 2026 ACC/AHA dyslipidemia
# guideline is ~1.05M chars) all pass through untouched. It only catches
# pathological MarkItDown output — bad OCR, repeated page furniture, runaway
# supplements — that would overflow the model context, blow up cost, or hit
# the subprocess timeout. ~1.5M chars is roughly 400-450k tokens of English
# medical text. An article over the backstop is flagged `too_large` for
# manual appraisal instead of being appraised on a misleading fragment.
# Override via JOURNAL_FETCHER_APPRAISAL_CHAR_BACKSTOP if the appraisal
# model's context window differs.
ARTICLE_CHAR_BACKSTOP = int(
    os.getenv("JOURNAL_FETCHER_APPRAISAL_CHAR_BACKSTOP", "1500000")
)


class ArticleTooLargeError(RuntimeError):
    """Raised when PDF-converted markdown exceeds ARTICLE_CHAR_BACKSTOP."""


# System prompt: stable across all articles sharing the same route in a given
# week. Goes into claude's cached system prompt via --append-system-prompt-file,
# so a route's second-and-onward articles pay cache-read price (~10% of full
# input) for these ~26k tokens of skill + style guide instead of re-ingesting.
APPRAISAL_SYSTEM_PROMPT = """你是醫學文獻批判性評讀助手。對於使用者送進來的每一篇文章，請依照下方 Skill 與 Output Quality Style Guide 完成完整批判性評讀。

執行原則：
- Output Quality Style Guide 只作為補充品質規格；若與 Skill 衝突，以 Skill 為準。
- 使用者輸入的「文章 metadata」與「PDF converted markdown」是該次評讀的目標文章。

Skill:
{skill}

Output Quality Style Guide:
{style_guide}
"""

# User prompt: variable per-article portion. Lives outside the cache so each
# article gets fresh input ingestion of its own content.
APPRAISAL_USER_PROMPT = """請對下方文章執行完整批判性評讀，遵守 system prompt 中的 Skill 與 Style Guide。本文輸入是 PDF converted markdown。

文章 metadata:
Title: {title}
Journal: {journal}
Year: {year}
DOI: {doi}
PMID: {pmid}

PDF converted markdown:
{article_markdown}
"""

CODEX_REFERENCE_INSTRUCTIONS = """You are running under `codex exec --sandbox read-only`. You have read access
to any file the user can read on this filesystem. If the appraisal task
below references JAMA Users' Guides by absolute path, you may consult them
with shell tools (`cat`, `rg`, `head`) — but only when a specific
methodological question warrants it, and only excerpt the passages directly
relevant to the article under review. Use the citation format defined in
the references catalog later in this prompt.
"""

FIGURE_APPRAISAL_INSTRUCTIONS = """\
本文另有 {n} 張從同一份 PDF 抽出的圖頁 PNG,位於:
{figdir}

圖檔:
{filelist}

請在完成文字評讀時一併讀取並判讀這些圖頁,把「圖傳達但文字沒講清楚的證據」整合進正文。
尤其留意:Kaplan-Meier 曲線分離時點與 number-at-risk 後期是否銳減、forest plot
點估計與 CI 是否跨越無效線、CONSORT/flow diagram 人數是否一致、座標軸是否從非零起點
誇大效果。只根據圖中實際可見內容評讀;看不清楚就明說「圖解析度不足無法判讀」,不要臆測。
"""

CODEX_FIGURE_INSTRUCTIONS = """The prompt includes figure-page context, and {n}
PNG image(s) from the article PDF are attached with `codex exec -i`. Inspect
the attached images and integrate their visible evidence into the appraisal.
"""


def _run_codex_prompt(
    prompt: str,
    timeout: int = 1800,
    has_references: bool = False,
    image_paths: list[Path] | None = None,
) -> str | None:
    if has_references:
        prompt = CODEX_REFERENCE_INSTRUCTIONS.rstrip() + "\n\n" + prompt
    if image_paths:
        prompt = CODEX_FIGURE_INSTRUCTIONS.format(n=len(image_paths)).rstrip() + "\n\n" + prompt

    with tempfile.TemporaryDirectory(prefix="codex_weekly_appraise_") as tmp_dir:
        output_path = Path(tmp_dir) / "last_message.md"
        cmd = [
            resolve_codex_cli(),
            "exec",
            "--model",
            get_appraisal_model(),
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
            "--color",
            "never",
            "--ephemeral",
            "--output-last-message",
            str(output_path),
        ]
        for img in image_paths or []:
            cmd.extend(["-i", str(img)])
        cmd.append(prompt)
        result = subprocess.run(
            cmd,
            cwd=tmp_dir,
            env=codex_exec_env(),
            input="",
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout).strip()
            print(f"  [warn] codex appraisal error: {err[:400]}", file=sys.stderr)
            return None
        if output_path.exists():
            return output_path.read_text(encoding="utf-8").strip()
        return result.stdout.strip() or None


def _appraisal_web_search_enabled() -> bool:
    """Read JOURNAL_FETCHER_APPRAISAL_WEB_SEARCH; default on.

    Set to "0" / "false" / "no" / "off" to disable. Useful when claude's
    Agent SDK credit is low (web search adds server-side tool fees) or when
    you specifically want offline / cache-only behavior.
    """
    raw = os.getenv("JOURNAL_FETCHER_APPRAISAL_WEB_SEARCH", "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _appraisal_figures_enabled() -> bool:
    """Read JOURNAL_FETCHER_APPRAISAL_FIGURES; default on for full appraisals."""
    raw = os.getenv("JOURNAL_FETCHER_APPRAISAL_FIGURES", "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


# Figure-page budget per route. Guidelines/statements and evidence syntheses
# carry many labelled figures (key-takeaway panels, multiple forest plots), so
# they get a larger budget; primary studies need only the core few. Combined
# with figure_extract's caption-first ordering, this keeps a late key figure
# from being crowded out in long documents without inflating cost everywhere.
DEFAULT_FIGURE_MAX_PAGES = 6
FIGURE_MAX_PAGES_BY_ROUTE: dict[str, int] = {
    "cpg": 12,
    "consensus": 12,
    "sr": 10,
    "nma": 10,
}


def _figure_max_pages(route: str | None) -> int:
    return FIGURE_MAX_PAGES_BY_ROUTE.get(route or "", DEFAULT_FIGURE_MAX_PAGES)


def _build_figure_instruction(figdir: Path, files: list[Path]) -> str:
    filelist = "\n".join(f"- {p.name}" for p in files)
    return FIGURE_APPRAISAL_INSTRUCTIONS.format(
        n=len(files),
        figdir=figdir,
        filelist=filelist,
    )


def _run_appraisal_prompt(
    system_prompt: str,
    user_prompt: str,
    timeout: int = 1800,
    read_dirs: list[Path] | None = None,
    has_references: bool = False,
    image_paths: list[Path] | None = None,
) -> tuple[str | None, str, str]:
    """Try claude -p Opus 4.6 first (with cached system prompt + WebSearch +
    optional reference Read access); fall back to codex GPT 5.5 on rate /
    credit error.

    WebSearch lets claude actually fulfil SKILL.md SECTION-0's external-
    search requirements (author background, journal IF, editorial). When
    `read_dirs` is set, claude can also Read JAMA Users' Guides on demand.

    Codex has its own built-in web access and read-only sandbox access; this
    helper only forwards the combined prompt — the codex side of `--add-dir`
    / explicit reference injection is handled in `_run_codex_prompt`.

    Returns `(text, backend, model)` so the caller can record which backend
    actually produced the appraisal (claude vs codex fallback) and the model
    name. `model` reflects the env-resolved appraisal model for that backend
    at call time; if `text` is None the model still reflects what was tried.
    """
    codex_combined = system_prompt.rstrip() + "\n\n---\n\n" + user_prompt
    enable_web = _appraisal_web_search_enabled()

    claude_model = claude_exec.get_claude_appraisal_model()
    text, backend = claude_exec.try_claude_or_fallback(
        user_prompt,
        claude_model=claude_model,
        fallback=lambda: _run_codex_prompt(
            codex_combined,
            timeout=timeout,
            has_references=has_references,
            image_paths=image_paths,
        ),
        timeout=timeout,
        label="appraise",
        system_prompt=system_prompt,
        enable_web_search=enable_web,
        read_dirs=read_dirs,
    )
    model = claude_model if backend == "claude" else get_appraisal_model()
    return text, backend, model


_REFERENCES_HEADING_RE = re.compile(
    r"^#{1,3}\s+\**\s*("
    r"references?"
    r"|bibliography"
    r"|works\s+cited"
    r"|cited\s+literature"
    r"|literature\s+cited"
    r"|reference\s+list"
    r"|references\s+and\s+notes"
    r")\s*\**\s*$",
    re.MULTILINE | re.IGNORECASE,
)


def _strip_references(markdown: str) -> str:
    """Cut markdown from the first References-style heading to end of document.

    Most journal articles end with a reference list that takes up 30-40% of
    the markdown but contributes nothing to critical appraisal. Stripping it
    frees model context and lowers token cost.

    Safety: only strip when the cut keeps ≥2,000 chars of leading content,
    so a misidentified heading near the top can't blank out the whole article.
    """
    match = _REFERENCES_HEADING_RE.search(markdown)
    if not match:
        return markdown
    stripped = markdown[: match.start()].rstrip()
    if len(stripped) < 2000:
        return markdown
    removed = len(markdown) - len(stripped)
    print(f"  [strip-refs] removed {removed:,} chars from References onward")
    return stripped


def _convert_with_pymupdf4llm(pdf_path: Path) -> str | None:
    """Primary path: pymupdf4llm preserves headings, multi-column flow, tables."""
    try:
        import pymupdf4llm
    except ImportError:
        print("  [warn] pymupdf4llm not installed; falling back to markitdown", file=sys.stderr)
        return None
    try:
        md = pymupdf4llm.to_markdown(
            str(pdf_path),
            ignore_images=True,
            show_progress=False,
        )
    except Exception as e:
        print(
            f"  [warn] pymupdf4llm failed for {pdf_path.name}: {e}; falling back to markitdown",
            file=sys.stderr,
        )
        return None
    md = (md or "").strip()
    if len(md) < 1000:
        print(
            f"  [warn] pymupdf4llm output suspiciously short ({len(md)} chars); "
            f"falling back to markitdown",
            file=sys.stderr,
        )
        return None
    return md


def _convert_with_markitdown(pdf_path: Path) -> str:
    """Fallback path: MarkItDown via subprocess. Raises RuntimeError on failure."""
    with tempfile.TemporaryDirectory(prefix="markitdown_pdf_") as tmp_dir:
        md_path = Path(tmp_dir) / "article.md"
        result = subprocess.run(
            ["python3", "-m", "markitdown", str(pdf_path), "-o", str(md_path)],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout).strip()
            raise RuntimeError(f"markitdown failed for {pdf_path.name}: {err[:400]}")
        return md_path.read_text(encoding="utf-8").strip()


def _convert_pdf_to_markdown(pdf_path: Path) -> str:
    md = _convert_with_pymupdf4llm(pdf_path)
    if md is None:
        md = _convert_with_markitdown(pdf_path)
    return _strip_references(md)


def _build_references_section(route: str) -> str:
    """Emit a markdown catalog of JAMA reference files for this route.

    The output is used by both backends:
      - claude: with --add-dir + Read tool, the model can lazily Read the
        listed files when relevant.
      - codex: with --sandbox read-only it can `cat`/`rg` the same absolute
        paths from a shell.

    Absolute paths mean the same string works for both backends without
    needing per-route path rewriting. Per-machine cache effect: the absolute
    path differs across machines, so the cache key is machine-local — fine
    for a single-user setup, would need adjustment for shared CI.

    The references directory is .gitignored (local-only on the maintainer's
    Mac). On CI / fresh clones the directory is absent; in that case this
    helper returns "" and the pipeline runs without the references layer —
    equivalent to pre-v3.6 behavior.

    Returns "" when the route has no specific JAMA guide (preclinical /
    narrative / default) so callers can skip the system-prompt append.
    """
    if not JAMA_REFERENCES_DIR.is_dir():
        return ""
    specific_files = _REFERENCES_BY_ROUTE.get(route, ())
    if not specific_files and route != "default":
        return ""
    index_path = REFERENCES_DIR / "index.md"
    if not index_path.exists():
        return ""

    lines: list[str] = []
    lines.append("## 可用權威參考（按需讀取，非必讀）")
    lines.append("")
    lines.append(
        f"以下為當前文章 route=`{route}` 對應的 JAMA Users' Guides / 工具書。"
        "**只在批判涉及對應方法學議題時取用**，不要一律展開。"
        "引用時在輸出明確標註「[參考: <filename> 第 X 節]」，並只擷取與本文相關段落。"
    )
    lines.append("")

    if specific_files:
        lines.append("### route 對應重點參考")
        lines.append("")
        for filename in specific_files:
            abs_path = JAMA_REFERENCES_DIR / filename
            if not abs_path.exists():
                continue
            lines.append(f"- `{abs_path}`")
        lines.append("")

    lines.append("### 完整索引（含其他主題）")
    lines.append("")
    lines.append(f"完整檔案清單與用途說明見 `{index_path}`。")
    return "\n".join(lines)


def _load_skill_for_route(route: str) -> str:
    """Compose the skill text: entry SKILL.md + fragment(s) for this route.

    Unknown / "default" routes fall back to concatenating all fragments so the
    output is identical to the pre-fragmentation full skill (zero-risk fallback).
    """
    fragments = _FRAGMENT_BY_ROUTE.get(route, _FRAGMENT_BY_ROUTE["default"])
    parts = [SKILL_PATH.read_text(encoding="utf-8")]
    for name in fragments:
        path = FRAGMENTS_DIR / name
        if not path.exists():
            print(
                f"  [warn] missing fragment {path}; route={route} will degrade output",
                file=sys.stderr,
            )
            continue
        parts.append(path.read_text(encoding="utf-8"))
    return "\n\n".join(parts)


def _safe_report_name(article: dict) -> str:
    pmid = article.get("pmid") or "no-pmid"
    first_author = "unknown"
    authors = article.get("authors") or []
    if authors:
        first_author = str(authors[0]).split()[0]
    year = article.get("year") or "unknown-year"
    return f"{pmid}_{first_author}_{year}_appraisal.md"


def appraise_pdf(article: dict, pdf_path: Path, out_dir: Path) -> Path | None:
    """Create one appraisal report.

    Returns the report path, or None on appraisal failure. Raises
    ArticleTooLargeError when the PDF markdown exceeds the size backstop.
    """
    if not SKILL_PATH.exists():
        raise FileNotFoundError(f"missing skill: {SKILL_PATH}")
    if not FRAGMENTS_DIR.exists():
        raise FileNotFoundError(f"missing skill fragments dir: {FRAGMENTS_DIR}")
    if not STYLE_GUIDE_PATH.exists():
        raise FileNotFoundError(f"missing output quality style guide: {STYLE_GUIDE_PATH}")

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / _safe_report_name(article)
    if report_path.exists() and report_path.stat().st_size > 1000:
        print(f"  [skip] appraisal exists: {report_path.name}")
        return report_path

    article_markdown = _convert_pdf_to_markdown(pdf_path)
    if len(article_markdown) > ARTICLE_CHAR_BACKSTOP:
        raise ArticleTooLargeError(
            f"{pdf_path.name}: markdown is {len(article_markdown):,} chars, "
            f"over the {ARTICLE_CHAR_BACKSTOP:,}-char backstop; flagged for "
            f"manual appraisal (truncating would yield a misleading partial "
            f"review)"
        )

    # Classify once and cache on the article dict so manual re-runs skip
    # the LLM call entirely.
    route = article.get("appraisal_route")
    if route and route in _FRAGMENT_BY_ROUTE:
        print(f"  [classify] route={route} (cached)")
    else:
        route, source = classify_article.classify(article, article_markdown[:3000])
        article["appraisal_route"] = route
        article["appraisal_route_source"] = source
        print(f"  [classify] route={route} (source={source})")

    skill_text = _load_skill_for_route(route)

    system_prompt = APPRAISAL_SYSTEM_PROMPT.format(
        skill=skill_text,
        style_guide=STYLE_GUIDE_PATH.read_text(encoding="utf-8"),
    )
    references_block = _build_references_section(route)
    if references_block:
        system_prompt = system_prompt.rstrip() + "\n\n" + references_block + "\n"

    user_prompt = APPRAISAL_USER_PROMPT.format(
        title=article.get("title", ""),
        journal=article.get("journal") or article.get("journal_key", ""),
        year=article.get("year", ""),
        doi=article.get("doi", ""),
        pmid=article.get("original_pmid", article.get("pmid", "")),
        article_markdown=article_markdown,
    )

    temp_context = (
        tempfile.TemporaryDirectory(prefix="appraisal_figures_")
        if _appraisal_figures_enabled()
        else nullcontext(None)
    )
    with temp_context as fig_tmp:
        figure_paths: list[Path] = []
        figure_dir = Path(fig_tmp) if fig_tmp else None
        if figure_dir is not None:
            try:
                figure_paths = extract_figure_pages(
                    pdf_path, figure_dir, max_pages=_figure_max_pages(route)
                )
            except Exception as e:
                print(
                    f"  [warn] figure extraction failed for {pdf_path.name}: {e}",
                    file=sys.stderr,
                )
            if figure_paths:
                print(f"  [figures] extracted {len(figure_paths)} page(s)")
                user_prompt = (
                    user_prompt.rstrip()
                    + "\n\n"
                    + _build_figure_instruction(figure_dir, figure_paths)
                    + "\n"
                )

        # Give claude Read access to the references dir only when this route
        # actually has corresponding JAMA guides. Figure pages, when enabled,
        # are added as a separate temporary Read directory. Codex receives the
        # same figures via explicit `-i <path>` flags.
        read_dirs = [REFERENCES_DIR] if references_block else []
        if figure_dir is not None and figure_paths:
            read_dirs.append(figure_dir)
        report, backend, model = _run_appraisal_prompt(
            system_prompt,
            user_prompt,
            read_dirs=read_dirs or None,
            has_references=bool(references_block),
            image_paths=figure_paths or None,
        )
    if not report:
        return None

    # Persist backend/model/timestamp on the article so render can show them
    # in the per-appraisal HTML footer (and on-demand re-renders read them
    # straight from selected_articles.json without needing the model again).
    from datetime import datetime
    from zoneinfo import ZoneInfo

    article["appraisal_backend"] = backend
    article["appraisal_model"] = model
    article["appraised_at"] = datetime.now(ZoneInfo("Asia/Taipei")).strftime(
        "%Y-%m-%d %H:%M %Z"
    )

    report_path.write_text(report + "\n", encoding="utf-8")
    return report_path


def appraise_selected(
    selected: list[dict],
    download_results: dict[str, Path | None],
    out_dir: Path,
) -> dict[str, Path | None]:
    """Appraise all selected articles that have PDFs."""
    results: dict[str, Path | None] = {}
    total = len(selected)
    for i, article in enumerate(selected, 1):
        pmid = str(article.get("pmid", ""))
        title = article.get("title", "")[:70]
        pdf_path = download_results.get(pmid)
        print(f"\n[{i}/{total}] 完整評讀：{title}...")
        if pdf_path is None:
            print("  [skip] PDF not downloaded")
            results[pmid] = None
            article["appraisal_status"] = "pdf_failed"
            continue
        try:
            report_path = appraise_pdf(article, pdf_path, out_dir)
        except ArticleTooLargeError as e:
            print(f"  [skip] {e}", file=sys.stderr)
            results[pmid] = None
            article["appraisal_path"] = ""
            article["appraisal_status"] = "too_large"
            continue
        except Exception as e:
            print(f"  [warn] appraisal failed: {e}", file=sys.stderr)
            report_path = None
        results[pmid] = report_path
        article["appraisal_path"] = str(report_path) if report_path else ""
        article["appraisal_status"] = "done" if report_path else "failed"
    return results
