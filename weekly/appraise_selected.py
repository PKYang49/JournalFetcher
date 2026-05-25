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
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from modules import claude_exec
from modules.codex_model import codex_exec_env, get_appraisal_model, resolve_codex_cli

ROOT = Path(__file__).resolve().parent.parent
SKILL_PATH = ROOT / "skills" / "literature-appraisal" / "SKILL.md"
STYLE_GUIDE_PATH = ROOT / "skills" / "literature-appraisal" / "references" / "output_quality_style_guide.md"

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


APPRAISAL_PROMPT = """請依照下方文獻評讀 Skill，對目標文章做完整批判性評讀。

Output Quality Style Guide 只作為補充品質規格；若與 Skill 衝突，以 Skill 為準。
本文輸入是 PDF converted markdown。

Skill:
{skill}

Output Quality Style Guide:
{style_guide}

文章 metadata:
Title: {title}
Journal: {journal}
Year: {year}
DOI: {doi}
PMID: {pmid}

PDF converted markdown:
{article_markdown}
"""


def _run_codex_prompt(prompt: str, timeout: int = 1800) -> str | None:
    with tempfile.TemporaryDirectory(prefix="codex_weekly_appraise_") as tmp_dir:
        output_path = Path(tmp_dir) / "last_message.md"
        result = subprocess.run(
            [
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
            print(f"  [warn] codex appraisal error: {err[:400]}", file=sys.stderr)
            return None
        if output_path.exists():
            return output_path.read_text(encoding="utf-8").strip()
        return result.stdout.strip() or None


def _run_appraisal_prompt(prompt: str, timeout: int = 1800) -> str | None:
    """Try claude -p Opus 4.6 first; fall back to codex GPT 5.5 on rate/credit error."""
    text, _backend = claude_exec.try_claude_or_fallback(
        prompt,
        claude_model=claude_exec.get_claude_appraisal_model(),
        fallback=lambda: _run_codex_prompt(prompt, timeout=timeout),
        timeout=timeout,
        label="appraise",
    )
    return text


_REFERENCES_HEADING_RE = re.compile(
    r"^#{1,3}\s+("
    r"references?"
    r"|bibliography"
    r"|works\s+cited"
    r"|cited\s+literature"
    r"|literature\s+cited"
    r"|reference\s+list"
    r"|references\s+and\s+notes"
    r")\s*$",
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

    prompt = APPRAISAL_PROMPT.format(
        skill=SKILL_PATH.read_text(encoding="utf-8"),
        style_guide=STYLE_GUIDE_PATH.read_text(encoding="utf-8"),
        title=article.get("title", ""),
        journal=article.get("journal") or article.get("journal_key", ""),
        year=article.get("year", ""),
        doi=article.get("doi", ""),
        pmid=article.get("original_pmid", article.get("pmid", "")),
        article_markdown=article_markdown,
    )
    report = _run_appraisal_prompt(prompt)
    if not report:
        return None

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
