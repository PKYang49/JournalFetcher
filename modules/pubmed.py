"""Phase 1: Fetch article list from PubMed E-utilities API."""

import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# NCBI asks every E-utilities client to identify itself via `tool` + `email`.
# Doing so puts us in a higher-trust bucket and surfaces a real contact in
# their logs if we ever misbehave (lower chance of silent throttling).
EMAIL = os.getenv("PUBMED_EMAIL", "researcher@example.com")
TOOL = "JournalFetcher"
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# Retry policy for transient NCBI failures (HTTP 5xx, network blip, or 200 OK
# with an `ERROR` payload / missing `idlist`). With ATTEMPTS=3 and BASE=3.0,
# waits between attempts are 1s and 3s (no sleep after the final attempt).
SEARCH_MAX_ATTEMPTS = 3
SEARCH_BACKOFF_BASE = 3.0


class PubmedSearchError(RuntimeError):
    """Raised when PubMed esearch keeps failing after all retries."""

JOURNAL_QUERIES = {
    "NEJM": '"N Engl J Med"[Journal]',
    "Lancet": '"Lancet"[Journal]',
    "JAMA": '"JAMA"[Journal]',
    "JACC": '"J Am Coll Cardiol"[Journal]',
    "EHJ": '"Eur Heart J"[Journal]',
    "EuroIntervention": '"EuroIntervention"[Journal]',
    "Circulation": '"Circulation"[Journal]',
    "BJSM": '"Br J Sports Med"[Journal]',
    "MSSE": '"Med Sci Sports Exerc"[Journal]',
    "SportsMed": '"Sports Med"[Journal]',
    "JAP": '"J Appl Physiol (1985)"[Journal]',
    "JAMACardio": '"JAMA Cardiol"[Journal]',
    "Heart": '"Heart"[Journal]',
}

# Per-journal publication-date window override, as (min_days_ago, max_days_ago).
# Articles are restricted to that historical window — e.g. BJSM picks up papers
# published between 90 and 100 days ago, giving each weekly run a historical
# slice of content rather than the bleeding-edge ePub stream.
# Journals not listed here use the caller-supplied `days` argument.
JOURNAL_DEFAULT_WINDOW: dict[str, tuple[int, int]] = {
    "BJSM": (90, 100),
    "Heart": (90, 97),
}

# Priority order for picking a representative publication type.
# Each entry: (PubMed PublicationType string, display label)
PUB_TYPE_PRIORITY = [
    ("Meta-Analysis", "Meta-Analysis"),
    ("Systematic Review", "Systematic Review"),
    ("Practice Guideline", "Guideline"),
    ("Guideline", "Guideline"),
    ("Randomized Controlled Trial", "RCT"),
    ("Clinical Trial, Phase IV", "Phase IV Trial"),
    ("Clinical Trial, Phase III", "Phase III Trial"),
    ("Clinical Trial, Phase II", "Phase II Trial"),
    ("Clinical Trial, Phase I", "Phase I Trial"),
    ("Clinical Trial", "Trial"),
    ("Review", "Review"),
    ("Editorial", "Editorial"),
    ("Comment", "Comment"),
    ("Letter", "Letter"),
    ("Case Reports", "Case Report"),
    ("News", "News"),
    ("Observational Study", "Observational"),
]

# PubMed sometimes labels guideline/consensus documents only as "Review" and
# may not provide an abstract. Keep these high-value documents in the weekly
# feed without admitting every abstract-less article.
GUIDELINE_TITLE_MARKERS = (
    "guideline",
    "consensus",
    "scientific statement",
    "decision pathway",
)
GUIDELINE_PUB_TYPES = {
    "Practice Guideline",
    "Guideline",
    "Consensus Development Conference",
    "Consensus Development Conference, NIH",
}


def is_guideline_or_consensus(article: dict) -> bool:
    """Return whether an article is a guideline/consensus candidate."""
    pub_types = set(article.get("pub_types") or [])
    if pub_types & GUIDELINE_PUB_TYPES:
        return True
    title = (article.get("title") or "").casefold()
    return any(marker in title for marker in GUIDELINE_TITLE_MARKERS)


def _classify_pub_type(pub_types: list[str]) -> str:
    """Pick the most informative publication type from PubMed's list."""
    pub_set = set(pub_types)
    for key, label in PUB_TYPE_PRIORITY:
        if key in pub_set:
            return label
    if "Journal Article" in pub_set:
        return "Original"
    return pub_types[0] if pub_types else ""


def _build_esearch_params(
    journal: str,
    days: int,
    count: int,
    date_range: tuple[int, int] | None,
) -> dict:
    query = JOURNAL_QUERIES[journal]
    today = datetime.now(timezone.utc).date()
    params = {
        "db": "pubmed",
        "retmax": count,
        "retmode": "json",
        "sort": "pub date",
        "email": EMAIL,
        "tool": TOOL,
    }
    if date_range is not None:
        a, b = date_range
        min_days_ago, max_days_ago = min(a, b), max(a, b)
        mindate = today - timedelta(days=max_days_ago)
        maxdate = today - timedelta(days=min_days_ago)
    else:
        # Use publication date so "recent" means recently published, not
        # recently added/updated inside PubMed's indexing pipeline. Explicit
        # date parameters are more stable than a relative `"last N days"[dp]`
        # term, which can interleave older issue-assigned Lancet records.
        mindate = today - timedelta(days=days)
        maxdate = today
    # Include guideline/consensus title and publication-type candidates even
    # when PubMed has not supplied an abstract for them.
    abstract_or_guideline = (
        "(hasabstract[text] OR guideline[Title] OR consensus[Title] "
        'OR "scientific statement"[Title] OR "decision pathway"[Title] '
        'OR "Practice Guideline"[pt] OR "Consensus Development Conference"[pt])'
    )
    params["term"] = f"{query} AND {abstract_or_guideline}"
    params["datetype"] = "pdat"
    params["mindate"] = mindate.strftime("%Y/%m/%d")
    params["maxdate"] = maxdate.strftime("%Y/%m/%d")
    return params


def search_pmids(
    journal: str,
    days: int = 30,
    count: int = 20,
    date_range: tuple[int, int] | None = None,
) -> list[str]:
    """Search PubMed and return list of PMIDs for the journal.

    `date_range`, when given as (min_days_ago, max_days_ago), restricts results
    to articles whose publication date falls inside that historical window and
    takes precedence over `days`.

    Retries on transient failures: HTTP 5xx, network errors, and 200 OK
    responses whose `esearchresult` is missing `idlist` or carries an `ERROR`
    field. Raises `PubmedSearchError` after all retries are exhausted so the
    caller can decide whether to abort or fall back.
    """
    params = _build_esearch_params(journal, days, count, date_range)
    last_reason = ""
    for attempt in range(1, SEARCH_MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(
                f"{BASE_URL}/esearch.fcgi", params=params, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            result = data.get("esearchresult", {})
            # NCBI sometimes returns 200 OK with an explicit ERROR payload or
            # an empty body lacking `idlist` — treat both as transient.
            if "ERROR" in result:
                last_reason = f"NCBI ERROR: {result['ERROR']!r}"
            elif "idlist" not in result:
                last_reason = (
                    f"missing 'idlist'; body keys={sorted(result.keys())}"
                )
            else:
                return result["idlist"]
        except requests.RequestException as e:
            last_reason = f"{type(e).__name__}: {e}"
        except ValueError as e:  # json decode failure
            last_reason = f"JSON decode failed: {e}"

        if attempt < SEARCH_MAX_ATTEMPTS:
            sleep_s = SEARCH_BACKOFF_BASE ** (attempt - 1)
            print(
                f"  [retry {attempt}/{SEARCH_MAX_ATTEMPTS - 1}] {journal}: "
                f"{last_reason}; sleeping {sleep_s:.0f}s"
            )
            time.sleep(sleep_s)

    raise PubmedSearchError(
        f"esearch failed for {journal} after {SEARCH_MAX_ATTEMPTS} attempts: "
        f"{last_reason}"
    )


def fetch_articles(pmids: list[str]) -> list[dict]:
    """Fetch article metadata for a list of PMIDs."""
    if not pmids:
        return []
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "rettype": "abstract",
        "email": EMAIL,
        "tool": TOOL,
    }
    resp = requests.get(f"{BASE_URL}/efetch.fcgi", params=params, timeout=60)
    resp.raise_for_status()
    return _parse_articles(resp.text)


def fetch_article_by_doi(doi: str) -> dict | None:
    """Resolve a DOI to a single PubMed article metadata dict.

    Returns None when PubMed has no record for the DOI (e.g. an ahead-of-print
    that is not yet indexed). The caller can then fall back to Crossref or to a
    minimal hand-built dict.
    """
    doi = (doi or "").strip()
    if not doi:
        return None
    params = {
        "db": "pubmed",
        "term": f"{doi}[AID]",
        "retmode": "json",
        "email": EMAIL,
        "tool": TOOL,
    }
    try:
        resp = requests.get(f"{BASE_URL}/esearch.fcgi", params=params, timeout=30)
        resp.raise_for_status()
        idlist = resp.json().get("esearchresult", {}).get("idlist", [])
    except (requests.RequestException, ValueError):
        return None
    if not idlist:
        return None
    articles = fetch_articles(idlist[:1])
    return articles[0] if articles else None


def _parse_articles(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    articles = []
    for article_node in root.findall(".//PubmedArticle"):
        try:
            articles.append(_parse_single(article_node))
        except Exception as e:
            print(f"  [warn] parse error: {e}")
    return articles


def _parse_single(node: ET.Element) -> dict:
    medline = node.find("MedlineCitation")
    article = medline.find("Article")

    pmid = medline.findtext("PMID", "")

    # Title and abstract both carry inline markup (<i>, <sup>, <sub>, <b>).
    # Use itertext(): .text/.findtext() stop at the first child element, which
    # silently truncates e.g. "Chronic PM<sub>2.5</sub> Exposure..." to "Chronic PM".
    title_node = article.find("ArticleTitle")
    title = "".join(title_node.itertext()).strip() if title_node is not None else ""

    # Abstract — may be structured.
    abstract_parts = article.findall(".//AbstractText")
    abstract = " ".join(
        (p.get("Label", "") + ": " if p.get("Label") else "") + "".join(p.itertext())
        for p in abstract_parts
    ).strip()

    # DOI
    doi = ""
    for id_node in node.findall(".//ArticleId"):
        if id_node.get("IdType") == "doi":
            doi = id_node.text or ""
            break

    # Journal
    journal = article.findtext(".//Journal/Title", "")

    # Authors
    authors = []
    for author in article.findall(".//Author"):
        last = author.findtext("LastName", "")
        fore = author.findtext("ForeName", "")
        if last:
            authors.append(f"{last} {fore}".strip())

    # Prefer the electronic publication year when PubMed provides one. This
    # avoids labelling older ePub articles with a later issue-assignment year.
    year = _extract_publication_year(article)
    volume = article.findtext(".//Journal/JournalIssue/Volume", "")
    issue = article.findtext(".//Journal/JournalIssue/Issue", "")
    pages = article.findtext(".//Pagination/MedlinePgn", "")

    pub_types = [
        (t.text or "")
        for t in article.findall(".//PublicationTypeList/PublicationType")
    ]
    pub_type = _classify_pub_type(pub_types)

    return {
        "pmid": pmid,
        "title": title,
        "abstract": abstract,
        "doi": doi,
        "journal": journal,
        "authors": authors,
        "year": year,
        "volume": volume,
        "issue": issue,
        "pages": pages,
        "pub_types": pub_types,
        "pub_type": pub_type,
    }


def _extract_publication_year(article: ET.Element) -> str:
    article_dates = article.findall(".//ArticleDate")
    for date_node in article_dates:
        year = date_node.findtext("Year", "")
        if year:
            return year

    pub_date = article.find(".//Journal/JournalIssue/PubDate")
    if pub_date is None:
        return ""

    return (
        pub_date.findtext("Year", "")
        or pub_date.findtext("MedlineDate", "")[:4]
        or ""
    )


def fetch_journal_articles(
    journal: str,
    days: int = 30,
    count: int = 20,
    date_range: tuple[int, int] | None = None,
) -> list[dict]:
    """High-level: search + fetch for one journal.

    Ordinary articles require an abstract. Guideline/consensus candidates are
    retained without one because PubMed does not consistently provide
    abstracts for these documents.

    Pass `date_range=(min_days_ago, max_days_ago)` to restrict to a historical
    publication-date window; otherwise `days` is the "last N days" lookback.
    """
    # Fetch extra records because ordinary abstract-less records are removed
    # after metadata parsing.
    pmids = search_pmids(
        journal, days=days, count=count * 4, date_range=date_range
    )
    articles = fetch_articles(pmids)
    articles = [
        a
        for a in articles
        if a.get("abstract", "").strip() or is_guideline_or_consensus(a)
    ]
    return articles[:count]


if __name__ == "__main__":
    print("Fetching JAMA latest 5 articles...")
    articles = fetch_journal_articles("JAMA", count=5)
    for i, a in enumerate(articles, 1):
        print(f"\n[{i}] PMID: {a['pmid']}")
        print(f"    Title: {a['title']}")
        print(f"    DOI:   {a['doi']}")
        print(f"    Authors: {', '.join(a['authors'][:3])}{'...' if len(a['authors']) > 3 else ''}")
