"""Typed vocabularies for appraisal / request state.

Before this module the state values were bare string literals scattered
across appraise_selected, process_appraisal_requests and render. A typo
(``"deferred"`` vs ``"deferred_claude_limit"``, ``"pdf_failed"`` vs
``"pdf-failed"``) produced no error, only silent wrong behavior. These
StrEnums keep the on-disk / on-dict values byte-identical (they ARE str
subclasses, so ``AppraisalStatus.DONE == "done"`` and they JSON-serialize
and f-string-render as their plain value) while turning a typo into an
AttributeError at import time.

Two distinct state machines live here:

* ``AppraisalStatus`` — set on the article dict by ``appraise_selected``,
  persisted to ``selected_articles.json``, and surfaced in the weekly HTML.
* ``RequestStatus`` — the on-demand request lifecycle recorded in
  ``data/appraisal_requests_processed.jsonl`` by ``process_appraisal_requests``.
"""

from __future__ import annotations

from enum import StrEnum


class AppraisalStatus(StrEnum):
    """Status of a single article's full appraisal (article-dict field)."""

    DONE = "done"
    FAILED = "failed"
    PDF_FAILED = "pdf_failed"
    TOO_LARGE = "too_large"


# Appraisal outcomes a re-request cannot fix: the PDF could not be fetched, or
# the converted markdown is over the size backstop. process_appraisal_requests
# records these as a terminal request failure instead of retrying forever.
TERMINAL_APPRAISAL_FAILURES = frozenset(
    {AppraisalStatus.PDF_FAILED, AppraisalStatus.TOO_LARGE}
)


class RequestStatus(StrEnum):
    """Lifecycle of an on-demand appraisal request (jsonl `status` field)."""

    REQUESTED = "requested"
    DEFERRED = "deferred"
    DONE = "done"
    FAILED = "failed"
