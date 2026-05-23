"""Generate weekly appraisals for selected downloaded PDFs.

Primary backend: `claude -p` with Opus 4.6 (Agent SDK credit; identical
per-token price to 4.7 but uses the older, more efficient tokenizer).
Fallback: `codex exec` with GPT 5.5, used automatically when claude reports
a rate-limit / credit-exhausted error.
"""

from __future__ import annotations

import os
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


def _convert_pdf_to_markdown(pdf_path: Path) -> str:
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
