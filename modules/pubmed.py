"""Phase 1: Fetch article list from PubMed E-utilities API."""

import xml.etree.ElementTree as ET
from typing import Optional
import requests

EMAIL = "researcher@example.com"
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

JOURNAL_QUERIES = {
    "NEJM": '"N Engl J Med"[Journal]',
    "Lancet": '"Lancet"[Journal]',
    "JAMA": '"JAMA"[Journal]',
    "JACC": '"J Am Coll Cardiol"[Journal]',
    "EHJ": '"Eur Heart J"[Journal]',
    "EuroIntervention": '"EuroIntervention"[Journal]',
}


def search_pmids(journal: str, year: str = "2026", count: int = 20) -> list[str]:
    """Search PubMed and return list of PMIDs for the journal."""
    query = JOURNAL_QUERIES[journal]
    term = f'{query} AND "{year}"[Date - Publication] AND hasabstract[text]'
    params = {
        "db": "pubmed",
        "term": term,
        "retmax": count,
        "retmode": "json",
        "sort": "pub date",
        "email": EMAIL,
    }
    resp = requests.get(f"{BASE_URL}/esearch.fcgi", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["esearchresult"]["idlist"]


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
    }
    resp = requests.get(f"{BASE_URL}/efetch.fcgi", params=params, timeout=60)
    resp.raise_for_status()
    return _parse_articles(resp.text)


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
    title = article.findtext("ArticleTitle", "")

    # Abstract — may be structured
    abstract_parts = article.findall(".//AbstractText")
    abstract = " ".join(
        (p.get("Label", "") + ": " if p.get("Label") else "") + (p.text or "")
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

    # Publication year
    year = (
        article.findtext(".//Journal/JournalIssue/PubDate/Year")
        or article.findtext(".//Journal/JournalIssue/PubDate/MedlineDate", "")[:4]
        or ""
    )

    return {
        "pmid": pmid,
        "title": title,
        "abstract": abstract,
        "doi": doi,
        "journal": journal,
        "authors": authors,
        "year": year,
    }


def fetch_journal_articles(
    journal: str, year: str = "2026", count: int = 20
) -> list[dict]:
    """High-level: search + fetch for one journal. Only returns articles with abstracts."""
    # Fetch more than needed to account for post-filter
    pmids = search_pmids(journal, year=year, count=count * 2)
    articles = fetch_articles(pmids)
    articles = [a for a in articles if a.get("abstract", "").strip()]
    return articles[:count]


if __name__ == "__main__":
    print("Fetching JAMA latest 5 articles...")
    articles = fetch_journal_articles("JAMA", count=5)
    for i, a in enumerate(articles, 1):
        print(f"\n[{i}] PMID: {a['pmid']}")
        print(f"    Title: {a['title']}")
        print(f"    DOI:   {a['doi']}")
        print(f"    Authors: {', '.join(a['authors'][:3])}{'...' if len(a['authors']) > 3 else ''}")
