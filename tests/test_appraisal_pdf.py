from __future__ import annotations

import unittest
from unittest.mock import patch

from weekly.appraise_selected import (
    _convert_with_pymupdf4llm,
    _has_intact_table,
    _strip_references,
)

# One reference entry carrying the markers _CITATION_MARKER_RE looks for.
_CITATION = "Smith J, Doe A, et al. A trial of something. Lancet 2024; 403: 1-9.\n"


class AppraisalPdfTests(unittest.TestCase):
    def test_reference_section_does_not_remove_following_tables(self) -> None:
        markdown = (
            "A" * 2200
            + "\n\n## **REFERENCES**\n\n1. Reference one.\n"
            + "\n## **FIGURE LEGENDS**\n\nFigure 1. Flowchart.\n"
            + "\n## **Table 1. Outcomes**\n\n|value|\n|---|\n|42|\n"
        )

        stripped = _strip_references(markdown)

        self.assertNotIn("Reference one", stripped)
        self.assertIn("FIGURE LEGENDS", stripped)
        self.assertIn("Table 1. Outcomes", stripped)
        self.assertIn("|42|", stripped)

    def test_reference_section_at_end_is_removed(self) -> None:
        markdown = "A" * 2200 + "\n\n## References\n\n1. Reference one.\n"

        stripped = _strip_references(markdown)

        self.assertNotIn("References", stripped)
        self.assertNotIn("Reference one", stripped)
        self.assertEqual(stripped, "A" * 2200)

    def test_headings_inside_the_reference_list_do_not_end_it(self) -> None:
        # Guidelines group their citations under the section that cites them,
        # so the list is punctuated by headings that must not stop the cut.
        markdown = (
            "A" * 2200
            + "\n\n## REFERENCES\n\n"
            + "## Preamble\n\n"
            + _CITATION * 5
            + "\n## 1.5. Scope of the Guideline\n\n"
            + _CITATION * 5
            + "\n## **Appendix 1. Writing Committee**\n\n|name|\n|---|\n|kept|\n"
        )

        stripped = _strip_references(markdown)

        self.assertNotIn("Smith J", stripped)
        self.assertNotIn("1.5. Scope", stripped)
        self.assertIn("Appendix 1. Writing Committee", stripped)
        self.assertIn("|kept|", stripped)

    def test_empty_block_does_not_end_the_reference_list(self) -> None:
        # A heading immediately followed by another heading carries no text to
        # score; the list continues under the next one.
        markdown = (
            "A" * 2200
            + "\n\n## REFERENCES\n\n"
            + "## 4.2. Risk Enhancers\n\n## 4.1. Risk Assessment\n\n"
            + _CITATION * 5
            + "\n## **FIGURE LEGENDS**\n\n"
            + "Figure 1. Study flowchart showing enrolment and follow-up.\n" * 4
        )

        stripped = _strip_references(markdown)

        self.assertNotIn("Smith J", stripped)
        self.assertNotIn("4.2. Risk Enhancers", stripped)
        self.assertIn("FIGURE LEGENDS", stripped)

    def test_collapsed_table_does_not_count_as_parsed(self) -> None:
        intact = "## Table 1\n\n|arm|n|\n|---|---|\n|placebo|20|\n|drug|21|\n"
        # pymupdf4llm folds every row into one cell when it cannot find the
        # column boundaries; the values survive but the alignment does not.
        collapsed = (
            "## Table 2\n\n|T0<br>T1<br>T2<br>P|\n|---|\n"
            + "|" + "<br>".join(str(i) for i in range(40)) + "|\n"
        )
        header_only = "## Table 3\n\n|" + "<br>".join("abcdefghijkl") + "|\n|---|\n\n"

        self.assertTrue(_has_intact_table(intact))
        self.assertFalse(_has_intact_table(collapsed))
        self.assertFalse(_has_intact_table(header_only))
        self.assertFalse(_has_intact_table("## Discussion\n\nNo table here.\n"))

    @patch("pymupdf4llm.to_markdown")
    def test_table_pages_are_reconciled_after_reference_stripping(
        self, to_markdown
    ) -> None:
        to_markdown.return_value = [
            {"text": "A" * 2200 + "\n"},
            {
                "text": (
                    "## **REFERENCES**\n\n"
                    "1. Reference table with a separator.\n"
                    "|---|\n|removed-table|\n"
                )
            },
            {"text": "## **FIGURE LEGENDS**\n\nFigure 1. Flowchart.\n"},
            {"text": "## **Table 1. Outcomes**\n\n|value|\n|---|\n|kept-table|\n"},
        ]

        converted = _convert_with_pymupdf4llm("article.pdf")

        self.assertIsNotNone(converted)
        assert converted is not None
        self.assertEqual(converted.parsed_table_pages, frozenset({4}))
        self.assertNotIn("removed-table", converted.markdown)
        self.assertIn("kept-table", converted.markdown)


if __name__ == "__main__":
    unittest.main()
