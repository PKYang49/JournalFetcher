"""Deterministically find editorial/comment articles for an appraisal target."""

from __future__ import annotations

import html
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import requests

from modules import pubmed


@dataclass(frozen=True)
class EditorialCandidate:
    doi: str
    title: str = ""
    journal: str = ""
    year: str = ""
    pmid: str = ""
    source: str = ""
    pub_types: tuple[str, ...] = ()


_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"'<>?#]+", re.I)
_NEJM_EDITORIAL_RE = re.compile(r"10\.1056/NEJMe[0-9A-Za-z]+", re.I)


def normalize_doi(value: str) -> str:
    doi = html.unescape(str(value or "").strip().strip("<>`"))
    doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi, flags=re.I)
    return doi.rstrip(".,);]>'\"/")


def find_editorial_candidates(article: dict) -> list[EditorialCandidate]:
    """Find likely editorial/comment DOIs before model invocation.

    Sources are intentionally deterministic and conservative:
    1. PubMed linked comment relationships when available.
    2. NEJM article-page `dc.Relation` metadata, which NEJM uses for linked
       editorials but PubMed may not expose as comments.
    3. PubMed title/topic fallback query for editorial/comment publication
       types.
    """
    doi = normalize_doi(str(article.get("doi", "")))
    title = str(article.get("title", "") or "")
    pmid = str(article.get("original_pmid") or article.get("pmid") or "").strip()

    found: dict[str, EditorialCandidate] = {}
    for candidate in _pubmed_linked_candidates(pmid):
        found.setdefault(candidate.doi.lower(), candidate)

    if doi.lower().startswith("10.1056/"):
        for candidate in _nejm_relation_candidates(doi):
            found.setdefault(candidate.doi.lower(), candidate)

    for candidate in _pubmed_query_candidates(title, doi):
        found.setdefault(candidate.doi.lower(), candidate)

    target = doi.lower()
    return [
        c for c in found.values()
        if c.doi and c.doi.lower() != target and _is_editorial_like(c)
    ]


def _is_editorial_like(candidate: EditorialCandidate) -> bool:
    pub_types = {p.lower() for p in candidate.pub_types}
    if {"editorial", "comment", "letter"} & pub_types:
        return True
    if candidate.doi.lower().startswith("10.1056/nejme"):
        return True
    title = candidate.title.lower()
    return "editorial" in title or "comment" in title


def _pubmed_linked_candidates(pmid: str) -> list[EditorialCandidate]:
    if not pmid or not pmid.isdigit():
        return []
    ids: set[str] = set()
    for linkname in ("pubmed_pubmed_comments", "pubmed_pubmed_comment_in"):
        try:
            resp = requests.get(
                f"{pubmed.BASE_URL}/elink.fcgi",
                params={
                    "dbfrom": "pubmed",
                    "db": "pubmed",
                    "id": pmid,
                    "retmode": "xml",
                    "linkname": linkname,
                    "email": pubmed.EMAIL,
                    "tool": pubmed.TOOL,
                },
                timeout=30,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
        except (requests.RequestException, ET.ParseError):
            continue
        ids.update(
            node.text or "" for node in root.findall(".//Link/Id") if node.text
        )
        time.sleep(0.34)
    return _pubmed_candidates_by_pmids(sorted(ids), source="PubMed linked comment")


def _pubmed_query_candidates(title: str, doi: str) -> list[EditorialCandidate]:
    terms = _title_terms(title)
    if not terms:
        return []
    queries = [
        f'("{terms}"[Title] OR "{terms}"[All Fields]) AND '
        '(editorial[Publication Type] OR comment[Publication Type])'
    ]
    # DOI stem helps when the editorial title does not quote the article title.
    if doi.lower().startswith("10.1056/") and "NEJM" not in terms:
        queries.append(
            f'"{terms}"[All Fields] AND "N Engl J Med"[Journal] AND '
            '(editorial[Publication Type] OR comment[Publication Type])'
        )

    ids: set[str] = set()
    for query in queries:
        try:
            resp = requests.get(
                f"{pubmed.BASE_URL}/esearch.fcgi",
                params={
                    "db": "pubmed",
                    "term": query,
                    "retmode": "json",
                    "retmax": 10,
                    "email": pubmed.EMAIL,
                    "tool": pubmed.TOOL,
                },
                timeout=30,
            )
            resp.raise_for_status()
            ids.update(resp.json().get("esearchresult", {}).get("idlist", []))
        except (requests.RequestException, ValueError):
            continue
        time.sleep(0.34)
    return _pubmed_candidates_by_pmids(sorted(ids), source="PubMed title query")


def _title_terms(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", title or "").strip()
    if not cleaned:
        return ""
    # Keep the distinctive prefix; very long quoted title queries often miss.
    parts = re.split(r"[:?.]", cleaned, maxsplit=1)
    return parts[0][:120].strip()


def _pubmed_candidates_by_pmids(pmids: list[str], source: str) -> list[EditorialCandidate]:
    if not pmids:
        return []
    try:
        articles = pubmed.fetch_articles(pmids[:10])
    except Exception:
        return []
    candidates = []
    for item in articles:
        doi = normalize_doi(str(item.get("doi", "")))
        if not doi:
            continue
        candidates.append(
            EditorialCandidate(
                doi=doi,
                title=str(item.get("title", "")),
                journal=str(item.get("journal", "")),
                year=str(item.get("year", "")),
                pmid=str(item.get("pmid", "")),
                source=source,
                pub_types=tuple(item.get("pub_types") or []),
            )
        )
    return candidates


def _nejm_relation_candidates(doi: str) -> list[EditorialCandidate]:
    related = _nejm_relation_dois(doi)
    candidates = []
    for rel_doi in related:
        meta = pubmed.fetch_article_by_doi(rel_doi)
        if meta:
            candidates.append(
                EditorialCandidate(
                    doi=rel_doi,
                    title=str(meta.get("title", "")),
                    journal=str(meta.get("journal", "")),
                    year=str(meta.get("year", "")),
                    pmid=str(meta.get("pmid", "")),
                    source="NEJM dc.Relation + PubMed",
                    pub_types=tuple(meta.get("pub_types") or []),
                )
            )
        else:
            candidates.append(
                EditorialCandidate(
                    doi=rel_doi,
                    source="NEJM dc.Relation",
                )
            )
        time.sleep(0.34)
    return candidates


def _nejm_relation_dois(doi: str) -> list[str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []

    url = f"https://www.nejm.org/doi/full/{doi}"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
                )
            )
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            html_text = page.content()
            body_text = page.locator("body").inner_text(timeout=10000)
            browser.close()
    except Exception:
        return []

    relations = set(
        re.findall(
            r'<meta\s+name=["\']dc\.Relation["\']\s+content=["\']([^"\']+)["\']',
            html_text,
            flags=re.I,
        )
    )
    # Fallback: keep only NEJM editorial DOI patterns from the page. The
    # relation metadata is preferred because recirculation widgets include old,
    # topically related editorials that are not about this article.
    if not relations:
        relations = set(_NEJM_EDITORIAL_RE.findall(html_text + "\n" + body_text))
    return sorted({normalize_doi(x) for x in relations if _NEJM_EDITORIAL_RE.fullmatch(x)})

