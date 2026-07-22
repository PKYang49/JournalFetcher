from __future__ import annotations

import contextlib
import io
import unittest
from unittest.mock import patch

from weekly.select_articles import _normalize_doi, _resolve_item, select_top_articles


class SelectArticleIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.first = {
            "pmid": "42024048",
            "doi": "10.1016/j.jacc.2026.01.030",
            "title": "Sudden Cardiac Death Due to Myocardial Infarction",
        }
        self.second = {
            "pmid": "42024568",
            "doi": "10.1016/j.jacc.2026.03.161",
            "title": "Left Bundle Branch Pacing vs Right Ventricular Pacing",
        }
        self.by_pmid = {
            self.first["pmid"]: self.first,
            self.second["pmid"]: self.second,
        }
        self.by_doi = {
            _normalize_doi(self.first["doi"]): self.first,
            _normalize_doi(self.second["doi"]): self.second,
        }

    def test_normalizes_common_doi_forms(self) -> None:
        expected = "10.1016/s0140-6736(26)01278-x"
        for value in (
            "10.1016/S0140-6736(26)01278-X",
            "https://doi.org/10.1016/S0140-6736(26)01278-X",
            "http://dx.doi.org/10.1016/S0140-6736(26)01278-X.",
            "doi: 10.1016/S0140-6736%2826%2901278-X",
            "DOI: https://doi.org/10.1016/S0140-6736(26)01278-X",
        ):
            with self.subTest(value=value):
                self.assertEqual(_normalize_doi(value), expected)

    def test_accepts_matching_pmid_and_url_form_doi(self) -> None:
        selected = _resolve_item(
            {
                "pmid": self.first["pmid"],
                "doi": f"https://doi.org/{self.first['doi'].upper()}",
            },
            self.by_pmid,
            self.by_doi,
        )
        self.assertIs(selected, self.first)

    def test_rejects_pmid_doi_disagreement(self) -> None:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            selected = _resolve_item(
                {"pmid": self.first["pmid"], "doi": self.second["doi"]},
                self.by_pmid,
                self.by_doi,
            )
        self.assertIsNone(selected)
        self.assertIn("rejecting item", stderr.getvalue())

    def test_rejected_conflict_is_filled_by_fallback(self) -> None:
        candidates = [
            {**self.first, "journal_key": "JACC", "pub_type": "Original"},
            {**self.second, "journal_key": "JACC", "pub_type": "Original"},
        ]
        model_reply = (
            '{"selected":[{"pmid":"42024048",'
            '"doi":"10.1016/j.jacc.2026.03.161",'
            '"score":99,"reason":"mismatched","tags":["wrong"]}]}'
        )
        with (
            patch("weekly.select_articles._run_codex_prompt", return_value=model_reply),
            patch("weekly.select_articles.load_profile", return_value="profile"),
            patch("weekly.select_articles.load_recent_feedback", return_value=""),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            selected = select_top_articles(candidates, limit=1)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["pmid"], self.first["pmid"])
        self.assertEqual(selected[0]["selection_tags"], ["fallback"])


if __name__ == "__main__":
    unittest.main()
