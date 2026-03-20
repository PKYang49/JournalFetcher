#!/usr/bin/env python3
"""Quick test: download one Circulation article via Ovid."""

from pathlib import Path
from modules.pubmed import fetch_articles

# First fetch the real metadata from PubMed (includes volume/issue/pages)
print("Fetching article metadata from PubMed...")
articles = fetch_articles(["41674444"])
if articles:
    test_article = articles[0]
    print(f"  Title: {test_article['title'][:80]}")
    print(f"  DOI: {test_article['doi']}")
    print(f"  Vol: {test_article.get('volume', '?')} Issue: {test_article.get('issue', '?')} Pages: {test_article.get('pages', '?')} Year: {test_article.get('year', '?')}")
else:
    # Fallback with hardcoded data
    test_article = {
        "pmid": "41674444",
        "title": "Prospective Associations of Obesity and Obesity Severity With 9 Cardiovascular Conditions Across Adiposity Measures",
        "doi": "10.1161/CIRCULATIONAHA.125.075327",
        "authors": ["Dardari"],
        "journal": "Circulation",
        "year": "2026",
        "volume": "153",
        "issue": "11",
        "pages": "843-854",
    }
    print(f"  (using fallback data)")

from modules.downloader import playwright_circulation_batch_download

out_dir = Path("output/test_circ")
out_dir.mkdir(parents=True, exist_ok=True)

print("\n=== Testing Circulation PDF download via Ovid ===\n")
results = playwright_circulation_batch_download([test_article], out_dir=out_dir)
print(f"\n--- Result: {'OK' if results else 'FAILED'} ---")
for doi, content in results.items():
    fname = f"{test_article['pmid']}_{test_article['authors'][0]}_{test_article['year']}.pdf"
    dest = out_dir / fname
    dest.write_bytes(content)
    print(f"  {doi}: {len(content)} bytes -> {dest}")
