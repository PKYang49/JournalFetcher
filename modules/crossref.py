"""Crossref-backed article discovery for journals where PubMed lags online-first."""

from __future__ import annotations

import html
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

CROSSREF_BASE_URL = "https://api.crossref.org"
BJSM_ISSN = "0306-3674"
REQUEST_TIMEOUT = 30
USER_AGENT = (
    "JournalFetcher/1.0 "
    "(mailto:{email})"
)


class CrossrefFetchError(RuntimeError):
    """Raised when Crossref discovery fails."""


def fetch_bjsm_articles(
    count: int = 10,
    date_range: tuple[int, int] = (90, 100),
) -> list[dict]:
    """Fetch BJSM online-first articles from Crossref.

    `date_range` is (min_days_ago, max_days_ago), matching modules.pubmed.
    Only returns articles with an abstract after the Crossref -> BMJ-page
    fallback cascade.
    """
    items = _fetch_crossref_journal_items(
        BJSM_ISSN,
        count=count * 3,
        date_range=date_range,
    )
    articles: list[dict] = []
    for item in items:
        article = _crossref_item_to_article(item)
        if not article["abstract"]:
            article["abstract"] = _fetch_bmj_abstract(article["doi"])
        if not article["abstract"]:
            continue
        articles.append(article)
        if len(articles) >= count:
            break
    return articles


def _fetch_crossref_journal_items(
    issn: str,
    count: int,
    date_range: tuple[int, int],
) -> list[dict[str, Any]]:
    today = datetime.now(timezone.utc).date()
    min_days_ago, max_days_ago = min(date_range), max(date_range)
    from_date = today - timedelta(days=max_days_ago)
    until_date = today - timedelta(days=min_days_ago)

    email = os.getenv("PUBMED_EMAIL", "researcher@example.com")
    params = {
        "filter": (
            f"from-online-pub-date:{from_date.isoformat()},"
            f"until-online-pub-date:{until_date.isoformat()},"
            "type:journal-article"
        ),
        "rows": max(count, 1),
        "mailto": email,
    }
    headers = {"User-Agent": USER_AGENT.format(email=email)}
    resp = requests.get(
        f"{CROSSREF_BASE_URL}/journals/{issn}/works",
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise CrossrefFetchError(f"Crossref request failed: {resp.status_code}") from e
    data = resp.json()
    return data.get("message", {}).get("items", [])


def _crossref_item_to_article(item: dict[str, Any]) -> dict:
    title = _first_text(item.get("title"))
    doi = item.get("DOI", "")
    abstract = _clean_html(item.get("abstract", ""))
    authors = _authors_from_crossref(item.get("author", []))
    year = _year_from_crossref(item)
    pub_type = _classify_bjsm_article(title, abstract)

    return {
        "pmid": "",
        "title": title,
        "abstract": abstract,
        "doi": doi,
        "journal": (
            _first_text(item.get("container-title"))
            or "British Journal of Sports Medicine"
        ),
        "authors": authors,
        "year": year,
        "volume": item.get("volume", ""),
        "issue": item.get("issue", ""),
        "pages": item.get("page", ""),
        "pub_types": [pub_type] if pub_type else [],
        "pub_type": pub_type,
        "source": "crossref",
    }


def _fetch_bmj_abstract(doi: str) -> str:
    if not doi:
        return ""
    email = os.getenv("PUBMED_EMAIL", "researcher@example.com")
    headers = {"User-Agent": USER_AGENT.format(email=email)}
    try:
        resp = requests.get(
            f"https://bjsm.bmj.com/lookup/doi/{doi}",
            headers=headers,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException:
        return ""

    text = resp.text
    for pattern in (
        r'<meta[^>]+name=["\']citation_abstract["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']citation_abstract["\']',
        r'<section[^>]+class=["\'][^"\']*abstract[^"\']*["\'][^>]*>(.*?)</section>',
        r'<div[^>]+class=["\'][^"\']*abstract[^"\']*["\'][^>]*>(.*?)</div>',
    ):
        match = re.search(pattern, text, flags=re.I | re.S)
        if match:
            cleaned = _clean_html(match.group(1))
            if cleaned:
                return cleaned
    return ""


def _clean_html(value: str) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _first_text(values: Any) -> str:
    if isinstance(values, list) and values:
        return str(values[0]).strip()
    if isinstance(values, str):
        return values.strip()
    return ""


def _authors_from_crossref(values: list[dict[str, Any]]) -> list[str]:
    authors: list[str] = []
    for author in values:
        family = str(author.get("family", "")).strip()
        given = str(author.get("given", "")).strip()
        name = " ".join(part for part in (family, given) if part)
        if name:
            authors.append(name)
    return authors


def _year_from_crossref(item: dict[str, Any]) -> str:
    for key in ("published-online", "published-print", "published", "issued"):
        parts = item.get(key, {}).get("date-parts")
        if parts and parts[0]:
            return str(parts[0][0])
    return ""


def _classify_bjsm_article(title: str, abstract: str) -> str:
    text = f"{title} {abstract}".lower()
    if "meta-analysis" in text:
        return "Meta-Analysis"
    if "systematic review" in text or "umbrella review" in text:
        return "Systematic Review"
    if "randomised" in text or "randomized" in text or " trial" in text:
        return "RCT"
    if "review" in title.lower():
        return "Review"
    if "cross-sectional" in text or "cohort" in text or "case-control" in text:
        return "Observational"
    return "Original"
