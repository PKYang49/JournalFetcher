from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from modules import crossref, pubmed
from weekly import run_weekly


class PubmedFetchLimitTests(unittest.TestCase):
    def test_no_limit_returns_every_filtered_article(self) -> None:
        articles = [
            {"pmid": str(i), "abstract": "abstract", "pub_types": []}
            for i in range(12)
        ]
        with (
            patch("modules.pubmed.search_pmids", return_value=[str(i) for i in range(12)]) as search,
            patch("modules.pubmed.fetch_articles", return_value=articles),
        ):
            result = pubmed.fetch_journal_articles("EHJ", days=7, count=None)

        self.assertEqual(result, articles)
        search.assert_called_once_with("EHJ", days=7, count=None, date_range=None)

    def test_explicit_limit_keeps_debug_behavior(self) -> None:
        articles = [
            {"pmid": str(i), "abstract": "abstract", "pub_types": []}
            for i in range(5)
        ]
        with (
            patch("modules.pubmed.search_pmids", return_value=[str(i) for i in range(5)]) as search,
            patch("modules.pubmed.fetch_articles", return_value=articles),
        ):
            result = pubmed.fetch_journal_articles("EHJ", days=7, count=2)

        self.assertEqual(result, articles[:2])
        search.assert_called_once_with("EHJ", days=7, count=8, date_range=None)

    def test_no_limit_raises_instead_of_silent_safety_truncation(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "esearchresult": {
                "count": str(pubmed.UNBOUNDED_SEARCH_SAFETY_LIMIT + 1),
                "idlist": ["1"],
            }
        }
        with patch("modules.pubmed.requests.get", return_value=response):
            with self.assertRaisesRegex(pubmed.PubmedSearchError, "refusing to truncate"):
                pubmed.search_pmids("EHJ", days=7, count=None)


class CrossrefFetchLimitTests(unittest.TestCase):
    def test_no_limit_returns_all_abstracted_bjsm_items(self) -> None:
        items = [
            {"DOI": f"10.1136/test-{i}", "title": [f"Title {i}"], "abstract": "Abstract"}
            for i in range(12)
        ]
        with patch("modules.crossref._fetch_crossref_journal_items", return_value=items) as fetch:
            result = crossref.fetch_bjsm_articles(count=None)

        self.assertEqual(len(result), 12)
        fetch.assert_called_once_with(
            crossref.BJSM_ISSN,
            count=None,
            date_range=(90, 100),
        )


class WeeklyFetchLimitTests(unittest.TestCase):
    def test_weekly_no_limit_is_forwarded_to_pubmed(self) -> None:
        with patch("weekly.run_weekly.pubmed.fetch_journal_articles", return_value=[]) as fetch:
            result = run_weekly.fetch_all(["EHJ"], days=None, count=None)

        self.assertEqual(result, [])
        fetch.assert_called_once_with("EHJ", count=None, days=7)


if __name__ == "__main__":
    unittest.main()
