"""Render weekly report HTML using Jinja2."""

import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
DOCS_DIR = ROOT / "docs"
INDEX_DATA = DOCS_DIR / "_index.json"

TPE = timezone(timedelta(hours=8))


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(["html"]),
    )


def _authors_display(authors: list[str], max_show: int = 3) -> str:
    if not authors:
        return ""
    if len(authors) <= max_show:
        return ", ".join(authors)
    return ", ".join(authors[:max_show]) + f" 等 {len(authors)} 人"


def _summary_is_error(summary: str) -> bool:
    return bool(summary) and summary.startswith("[") and summary.endswith("]")


PUB_TYPE_CLASS = {
    "Meta-Analysis": "type-evidence",
    "Systematic Review": "type-evidence",
    "Guideline": "type-guideline",
    "RCT": "type-trial",
    "Phase I Trial": "type-trial",
    "Phase II Trial": "type-trial",
    "Phase III Trial": "type-trial",
    "Phase IV Trial": "type-trial",
    "Trial": "type-trial",
    "Review": "type-review",
    "Editorial": "type-editorial",
    "Comment": "type-editorial",
    "Letter": "type-editorial",
    "Case Report": "type-other",
    "News": "type-other",
    "Observational": "type-original",
    "Original": "type-original",
}


def _pub_type_class(pub_type: str) -> str:
    return PUB_TYPE_CLASS.get(pub_type, "type-other")


def render_weekly(
    articles: list[dict],
    week_label: str,
    journal_counts: list[dict],
) -> str:
    """Render a single weekly report HTML.

    `articles` should already include a `summary` and a `journal_key` field.
    Returns the rendered HTML string.
    """
    env = _env()
    tmpl = env.get_template("weekly.html")
    now = datetime.now(TPE).strftime("%Y-%m-%d %H:%M %Z")

    prepared = []
    for a in articles:
        prepared.append(
            {
                **a,
                "authors_display": _authors_display(a.get("authors", [])),
                "summary_error": _summary_is_error(a.get("summary", "")),
                "pub_type_class": _pub_type_class(a.get("pub_type", "")),
            }
        )

    return tmpl.render(
        week_label=week_label,
        generated_at=now,
        total_count=len(articles),
        articles=prepared,
        journal_counts=journal_counts,
    )


def write_weekly(html: str, filename: str) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    path = DOCS_DIR / filename
    path.write_text(html, encoding="utf-8")
    return path


def update_index(filename: str, label: str, count: int, date: str) -> Path:
    """Append/update an entry in docs/_index.json and re-render docs/index.html."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    entries: list[dict] = []
    if INDEX_DATA.exists():
        try:
            entries = json.loads(INDEX_DATA.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            entries = []

    entries = [e for e in entries if e.get("filename") != filename]
    entries.insert(
        0,
        {"filename": filename, "label": label, "count": count, "date": date},
    )

    INDEX_DATA.write_text(
        json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    env = _env()
    tmpl = env.get_template("index.html")
    html = tmpl.render(entries=entries)
    index_path = DOCS_DIR / "index.html"
    index_path.write_text(html, encoding="utf-8")
    return index_path


def iso_week_label(dt: datetime | None = None) -> tuple[str, str, str]:
    """Return (label='2026-W19', filename='2026-W19.html', date='2026-05-08')."""
    dt = dt or datetime.now(TPE)
    year, week, _ = dt.isocalendar()
    label = f"{year}-W{week:02d}"
    return label, f"{label}.html", dt.strftime("%Y-%m-%d")


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)
