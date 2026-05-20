"""Render weekly report HTML using Jinja2."""

import json
import re
from html import escape
from datetime import datetime, timezone, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markdown_it import MarkdownIt

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
    selected_articles: list[dict] | None = None,
    feedback_endpoint: str = "",
) -> str:
    """Render a single weekly report HTML.

    `articles` should already include a `summary` and a `journal_key` field.
    `feedback_endpoint` is the Apps Script web app URL; when set, the selected
    cards render 👍/👎 feedback buttons that POST to it.
    Returns the rendered HTML string.
    """
    env = _env()
    tmpl = env.get_template("weekly.html")
    now = datetime.now(TPE).strftime("%Y-%m-%d %H:%M %Z")

    def prepare(article: dict) -> dict:
        return (
            {
                **article,
                "authors_display": _authors_display(article.get("authors", [])),
                "summary_error": _summary_is_error(article.get("summary", "")),
                "commentary": article.get("commentary", ""),
                "pub_type_class": _pub_type_class(article.get("pub_type", "")),
            }
        )

    prepared = [prepare(a) for a in articles]
    prepared_selected = [prepare(a) for a in (selected_articles or [])]
    selected_pmids = {
        str(a.get("pmid")) for a in (selected_articles or []) if a.get("pmid")
    }

    return tmpl.render(
        week_label=week_label,
        generated_at=now,
        total_count=len(articles),
        articles=prepared,
        journal_counts=journal_counts,
        selected_articles=prepared_selected,
        selected_pmids=selected_pmids,
        feedback_endpoint=feedback_endpoint,
    )


def write_weekly(html: str, filename: str) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    path = DOCS_DIR / filename
    path.write_text(html, encoding="utf-8")
    return path


def publish_appraisals(
    selected_articles: list[dict],
    week_label: str,
) -> list[Path]:
    """Render selected appraisal markdown reports into docs/appraisals/<week>/."""
    if not selected_articles:
        return []

    out_dir = DOCS_DIR / "appraisals" / week_label
    out_dir.mkdir(parents=True, exist_ok=True)
    published: list[Path] = []
    md = MarkdownIt("commonmark", {"html": False, "linkify": True}).enable("table")

    for article in selected_articles:
        source = article.get("appraisal_path")
        if not source:
            continue
        source_path = Path(source)
        if not source_path.exists():
            continue

        name = sanitize_filename(source_path.stem) + ".html"
        html_path = out_dir / name
        body = md.render(source_path.read_text(encoding="utf-8"))
        title = article.get("title") or source_path.stem
        page = _render_appraisal_page(
            title=title,
            week_label=week_label,
            body_html=body,
        )
        html_path.write_text(page, encoding="utf-8")
        article["appraisal_url"] = f"appraisals/{week_label}/{name}"
        published.append(html_path)

    return published


def _render_appraisal_page(title: str, week_label: str, body_html: str) -> str:
    safe_title = escape(title)
    safe_week = escape(week_label)
    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{safe_title}</title>
<style>
  :root {{
    --bg: #fafafa;
    --card: #ffffff;
    --text: #1a1a1a;
    --muted: #6b6b6b;
    --border: #e5e5e5;
    --accent: #0b5fff;
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang TC",
      "Microsoft JhengHei", sans-serif;
    background: var(--bg);
    color: var(--text);
    margin: 0;
    padding: 24px 16px 64px;
    line-height: 1.72;
  }}
  main {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    margin: 0 auto;
    max-width: 840px;
    padding: 22px 26px;
  }}
  nav {{
    margin: 0 auto 14px;
    max-width: 840px;
    color: var(--muted);
    font-size: 14px;
  }}
  a {{ color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  h1, h2, h3, h4 {{ line-height: 1.35; }}
  h1 {{ font-size: 26px; margin-top: 0; }}
  h2 {{ border-top: 1px solid var(--border); padding-top: 18px; }}
  code {{
    background: rgba(127, 127, 127, 0.12);
    border-radius: 4px;
    padding: 1px 4px;
  }}
  table {{
    border-collapse: collapse;
    display: block;
    overflow-x: auto;
    width: 100%;
  }}
  th, td {{
    border: 1px solid var(--border);
    padding: 6px 8px;
    text-align: left;
    vertical-align: top;
  }}
  blockquote {{
    border-left: 3px solid var(--accent);
    color: var(--muted);
    margin-left: 0;
    padding-left: 14px;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg: #1a1a1a;
      --card: #242424;
      --text: #e8e8e8;
      --muted: #999;
      --border: #333;
      --accent: #6ea8ff;
    }}
  }}
</style>
</head>
<body>
<nav><a href="../../{safe_week}.html">← 回到 {safe_week} 週報</a></nav>
<main>
<h1>{safe_title}</h1>
{body_html}
</main>
</body>
</html>
"""


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
