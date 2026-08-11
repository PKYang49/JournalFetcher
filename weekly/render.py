"""Render weekly report HTML using Jinja2."""

import json
import re
from html import escape
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markdown_it import MarkdownIt

from weekly.appraisal_status import AppraisalStatus

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


def _feedback_id(article: dict) -> str:
    pmid = str(article.get("pmid", "")).strip()
    if pmid:
        return f"pmid:{pmid}"
    doi = str(article.get("doi", "")).strip().lower()
    if doi:
        return f"doi:{doi}"
    return ""


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


# Why an appraisal card can exist without a published page, keyed by the
# article's `appraisal_status`. "" covers articles the appraisal phase never
# reached (e.g. run_weekly gave up after exhausting its usage-limit retry
# cycles), which leaves the field unset. AppraisalStatus.DONE appears here
# only in the anomalous case where the report file went missing before
# publish_appraisals ran.
APPRAISAL_STATE_DISPLAY: dict[str, dict[str, str]] = {
    "": {
        "label": "評讀進行中",
        "note": "評讀尚未產出，系統會在後續排程續跑。",
        "card_class": "status-card-processing",
        "chip_class": "is-processing",
    },
    AppraisalStatus.DONE: {
        "label": "評讀頁待產出",
        "note": "評讀已完成，但頁面尚未產生；下次發布時會補上。",
        "card_class": "status-card-deferred",
        "chip_class": "is-deferred",
    },
    AppraisalStatus.FAILED: {
        "label": "評讀失敗",
        "note": "評讀產出失敗，將於後續排程重新嘗試。",
        "card_class": "status-card-failed",
        "chip_class": "is-failed",
    },
    AppraisalStatus.PDF_FAILED: {
        "label": "PDF 下載失敗",
        "note": "無法取得全文 PDF，暫時無法評讀。",
        "card_class": "status-card-failed",
        "chip_class": "is-failed",
    },
    AppraisalStatus.TOO_LARGE: {
        "label": "全文過長",
        "note": "全文超過長度上限，未進行評讀。",
        "card_class": "status-card-failed",
        "chip_class": "is-failed",
    },
}

_UNKNOWN_APPRAISAL_STATE: dict[str, str] = {
    "label": "評讀未完成",
    "note": "評讀尚未產出，系統會在後續排程續跑。",
    "card_class": "status-card-deferred",
    "chip_class": "is-deferred",
}


def _appraisal_state(article: dict) -> dict[str, str]:
    """Describe why an appraisal card has no page yet.

    Returns {} once `appraisal_url` exists (the card links to the report and
    needs no status). Otherwise the card would render as a silently empty
    pick, so every no-page reason gets a visible chip + note, mirroring how
    on-demand requests surface 評讀中 / 評讀失敗.
    """
    if article.get("appraisal_url"):
        return {}
    status = str(article.get("appraisal_status") or "").strip()
    return APPRAISAL_STATE_DISPLAY.get(status, _UNKNOWN_APPRAISAL_STATE)


def _journal_sections(articles: list[dict], journal_counts: list[dict]) -> list[dict]:
    """Group summary cards by journal and assign stable in-page anchors."""
    grouped: dict[str, list[dict]] = {}
    for article in articles:
        name = str(article.get("journal_key") or article.get("journal") or "").strip()
        name = name or "其他"
        grouped.setdefault(name, []).append(article)

    ordered_names = [
        str(item.get("name", "")).strip()
        for item in journal_counts
        if str(item.get("name", "")).strip() in grouped
    ]
    ordered_names.extend(name for name in grouped if name not in ordered_names)

    sections: list[dict] = []
    used_anchors: set[str] = set()
    for name in ordered_names:
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "other"
        anchor = f"journal-{slug}"
        suffix = 2
        while anchor in used_anchors:
            anchor = f"journal-{slug}-{suffix}"
            suffix += 1
        used_anchors.add(anchor)
        sections.append(
            {
                "name": name,
                "anchor": anchor,
                "articles": grouped[name],
                "count": len(grouped[name]),
            }
        )
    return sections


def render_weekly(
    articles: list[dict],
    week_label: str,
    journal_counts: list[dict],
    selected_articles: list[dict] | None = None,
    feedback_endpoint: str = "",
) -> str:
    """Render a single weekly report HTML.

    `articles` should already include a `summary` and a `journal_key` field.
    `feedback_endpoint` is the relay URL (Cloudflare Worker or legacy Apps
    Script); when set, cards render 👍/👎 buttons that POST to it.

    `selected_articles` is split into two top sections by the `manual_request`
    tag: AI-curated picks render under 本週精選評讀, reader-requested appraisals
    under 已評讀. 本週文章摘要 then drops every article shown above so each
    article appears in exactly one section.
    Returns the rendered HTML string.
    """
    env = _env()
    tmpl = env.get_template("weekly.html")
    now = datetime.now(TPE).strftime("%Y-%m-%d %H:%M %Z")

    def prepare(article: dict) -> dict:
        appraise_request_url = ""
        if feedback_endpoint and article.get("doi"):
            appraise_request_url = (
                feedback_endpoint
                + "?"
                + urlencode(
                    {
                        "view": "appraise",
                        "week": week_label,
                        "pmid": article.get("pmid", ""),
                        "doi": article.get("doi", ""),
                        "journal": article.get("journal_key") or article.get("journal") or "",
                        "title": article.get("title", ""),
                    }
                )
            )
        return (
            {
                **article,
                "authors_display": _authors_display(article.get("authors", [])),
                "summary_error": _summary_is_error(article.get("summary", "")),
                "commentary": article.get("commentary", ""),
                "pub_type_class": _pub_type_class(article.get("pub_type", "")),
                "appraise_request_url": appraise_request_url,
                "appraisal_state": _appraisal_state(article),
                "feedback_id": _feedback_id(article),
            }
        )

    selected_meta = {
        str(a.get("pmid")): a for a in (selected_articles or []) if a.get("pmid")
    }

    enriched_articles: list[dict] = []
    for article in articles:
        pmid = str(article.get("pmid", ""))
        selected = selected_meta.get(pmid)
        if selected:
            merged = {**article}
            for key in (
                "appraisal_url",
                "appraisal_path",
                "appraisal_status",
                "selected_for_appraisal",
                "selection_reason",
                "selection_tags",
            ):
                if selected.get(key):
                    merged[key] = selected[key]
            enriched_articles.append(merged)
        else:
            enriched_articles.append(article)

    prepared = [prepare(a) for a in enriched_articles]

    article_meta = {
        a.get("feedback_id"): a for a in prepared if a.get("feedback_id")
    }
    prepared_selected = []
    for article in selected_articles or []:
        selected = prepare(article)
        base = article_meta.get(selected.get("feedback_id"))
        if base:
            selected = {**base, **selected}
            for key in ("summary", "commentary"):
                if not selected.get(key) and base.get(key):
                    selected[key] = base[key]
            selected["summary_error"] = _summary_is_error(selected.get("summary", ""))
        prepared_selected.append(selected)
    selected_ids = {a.get("feedback_id") for a in prepared_selected if a.get("feedback_id")}

    # Split appraised articles: AI-curated picks (本週精選評讀) vs. articles a
    # reader requested via the weekly HTML (已評讀). process_appraisal_requests
    # tags the latter with `manual_request`.
    def _is_manual(article: dict) -> bool:
        return "manual_request" in (article.get("selection_tags") or [])

    featured_articles = [a for a in prepared_selected if not _is_manual(a)]
    appraised_articles = [a for a in prepared_selected if _is_manual(a)]

    # 本週文章摘要 drops everything already shown above, so each article
    # appears in exactly one section.
    summary_articles = [
        a for a in prepared if a.get("feedback_id") not in selected_ids
    ]
    journal_sections = _journal_sections(summary_articles, journal_counts)

    return tmpl.render(
        week_label=week_label,
        generated_at=now,
        total_count=len(articles),
        articles=summary_articles,
        journal_sections=journal_sections,
        journal_counts=journal_counts,
        featured_articles=featured_articles,
        appraised_articles=appraised_articles,
        selected_ids=selected_ids,
        feedback_endpoint=feedback_endpoint,
    )


def write_weekly(html: str, filename: str) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    path = DOCS_DIR / filename
    # Jinja control-flow indentation can leave whitespace-only lines. Keep
    # generated reports stable so small template changes do not create noisy
    # trailing-whitespace diffs across every article card.
    clean_html = "\n".join(line.rstrip() for line in html.splitlines())
    if html.endswith("\n"):
        clean_html += "\n"
    path.write_text(clean_html, encoding="utf-8")
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
            backend=article.get("appraisal_backend"),
            model=article.get("appraisal_model"),
            route=article.get("appraisal_route"),
            appraised_at=article.get("appraised_at"),
        )
        html_path.write_text(page, encoding="utf-8")
        article["appraisal_url"] = f"appraisals/{week_label}/{name}"
        published.append(html_path)

    return published


def _build_appraisal_footer(
    backend: str | None,
    model: str | None,
    route: str | None,
    appraised_at: str | None,
) -> str:
    """Return a small HTML footer block listing which backend/model/route
    produced this appraisal. Returns "" when nothing useful is recorded
    (e.g. appraisals from before the metadata-plumbing was added)."""
    parts: list[str] = []
    if model:
        backend_label = f" ({escape(backend)})" if backend else ""
        parts.append(f"模型：{escape(model)}{backend_label}")
    if route:
        parts.append(f"route：{escape(route)}")
    if appraised_at:
        parts.append(f"產出時間：{escape(appraised_at)}")
    if not parts:
        return ""
    return (
        '<footer class="appraisal-meta">評讀資訊：'
        + "  ·  ".join(parts)
        + "</footer>"
    )


def _render_appraisal_page(
    title: str,
    week_label: str,
    body_html: str,
    backend: str | None = None,
    model: str | None = None,
    route: str | None = None,
    appraised_at: str | None = None,
) -> str:
    safe_title = escape(title)
    safe_week = escape(week_label)
    footer_html = _build_appraisal_footer(backend, model, route, appraised_at)
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
  .appraisal-meta {{
    border-top: 1px solid var(--border);
    margin-top: 28px;
    padding-top: 14px;
    color: var(--muted);
    font-size: 13px;
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
{footer_html}
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
    entries.append({"filename": filename, "label": label, "count": count, "date": date})
    # Sort by label, newest first. Not insert(0): re-rendering a past week
    # (resume_weekly, backfills) would otherwise hoist it above newer ones.
    entries.sort(key=lambda e: e.get("label", ""), reverse=True)

    INDEX_DATA.write_text(
        json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    env = _env()
    tmpl = env.get_template("index.html")
    html = tmpl.render(entries=entries)
    index_path = DOCS_DIR / "index.html"
    index_path.write_text(html, encoding="utf-8")
    return index_path


def render_and_write_weekly(
    articles: list[dict],
    *,
    week_label: str,
    filename: str,
    date_str: str,
    journal_counts: list[dict],
    selected_articles: list[dict],
    feedback_endpoint: str,
) -> tuple[Path, Path]:
    """Render one week's page, write docs/<filename>, refresh docs/index.html.

    The full-re-render tail shared by the two batch entry points
    (weekly.run_weekly and weekly.resume_weekly). Returns (weekly_path,
    index_path). git push / Discord stay in the callers — they differ
    (run_weekly sends a digest, resume_weekly does not).
    """
    html = render_weekly(
        articles,
        week_label=week_label,
        journal_counts=journal_counts,
        selected_articles=selected_articles,
        feedback_endpoint=feedback_endpoint,
    )
    weekly_path = write_weekly(html, filename)
    index_path = update_index(filename, week_label, len(articles), date_str)
    return weekly_path, index_path


def iso_week_label(dt: datetime | None = None) -> tuple[str, str, str]:
    """Return (label='2026-W19', filename='2026-W19.html', date='2026-05-08').

    Weeks start on **Sunday** (not ISO's Monday): the Sunday 08:50 launchd run
    should open a NEW week rather than append to the just-ended one. We reuse
    ISO's robust year/week numbering (handles year-end W52/W53 edges) by
    shifting the date +1 day, so ISO's Monday boundary lands on Sunday. E.g.
    Sun 2026-07-05 → W28 (a fresh week), Sat 2026-07-04 → W27.
    """
    dt = dt or datetime.now(TPE)
    year, week, _ = (dt + timedelta(days=1)).isocalendar()
    label = f"{year}-W{week:02d}"
    return label, f"{label}.html", dt.strftime("%Y-%m-%d")


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)
