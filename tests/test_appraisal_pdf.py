from __future__ import annotations

import unittest
from unittest.mock import patch

from weekly.appraise_selected import (
    _convert_with_pymupdf4llm,
    _strip_references,
)


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
