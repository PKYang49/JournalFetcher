"""Generate weekly appraisals for selected downloaded PDFs."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

from modules.codex_model import codex_exec_env, get_appraisal_model, resolve_codex_cli

ROOT = Path(__file__).resolve().parent.parent
SKILL_PATH = ROOT / "skills" / "literature-appraisal" / "SKILL.md"
STYLE_GUIDE_PATH = ROOT / "skills" / "literature-appraisal" / "references" / "output_quality_style_guide.md"

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


def _run_codex_prompt(prompt: str, timeout: int = 900) -> str | None:
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
    """Create one appraisal report. Returns report path or None on failure."""
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
    if len(article_markdown) > 120_000:
        article_markdown = (
            article_markdown[:120_000]
            + "\n\n[TRUNCATED: PDF markdown exceeded 120000 characters]\n"
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
    report = _run_codex_prompt(prompt)
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
        except Exception as e:
            print(f"  [warn] appraisal failed: {e}", file=sys.stderr)
            report_path = None
        results[pmid] = report_path
        article["appraisal_path"] = str(report_path) if report_path else ""
        article["appraisal_status"] = "done" if report_path else "failed"
    return results
