"""The real engine, marked `slow` by the directory conftest.

Needs `make spacy-model` (or the CI step that runs it). No Docker — but a 2-4 s model
load and ~600 MB of RAM, which is exactly why it does not belong in the default run.
"""

import pytest
from tests.pii import assert_no_pii, read_note

from ecet.application.redaction_policy import (
    CUSTOM_PATTERNS,
    ENTITY_REPLACEMENTS,
    SCORE_THRESHOLD,
)
from ecet.infrastructure.pii.presidio_redactor import PresidioPiiRedactor


@pytest.fixture(scope="session")
def redactor() -> PresidioPiiRedactor:
    return PresidioPiiRedactor(
        replacements=ENTITY_REPLACEMENTS,
        custom_patterns=CUSTOM_PATTERNS,
        score_threshold=SCORE_THRESHOLD,
        spacy_model="en_core_web_lg",
        concurrency=2,
    )


async def test_it_removes_every_pii_string_from_a_note(
    redactor: PresidioPiiRedactor,
) -> None:
    result = await redactor.redact(read_note("meets"))

    assert_no_pii(result.text)
    assert result.redactor == "presidio-2.2"


async def test_icd10_codes_survive(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact(read_note("meets"))

    assert "M54.5" in result.text


async def test_the_custom_recognisers_fire(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("MRN: 4482910 for member NWH2288341.")

    assert "<MRN>" in result.text
    assert "<MEMBER_ID>" in result.text
    assert result.entity_counts["MRN"] == 1
    assert result.entity_counts["MEMBER_ID"] == 1


async def test_locations_are_redacted(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("Seen at Northwind Medical Center, San Francisco.")

    assert "<LOCATION>" in result.text


async def test_dates_of_service_are_kept(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("Date of service: 2026-08-14. Follow-up in six weeks.")

    assert "2026-08-14" in result.text


async def test_empty_text_is_not_an_error(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("")

    assert result.text == ""
    assert result.entity_counts == {}


async def test_text_beyond_one_chunk_is_still_redacted(
    redactor: PresidioPiiRedactor,
) -> None:
    filler = "\n\n".join("The member attended the scheduled session." for _ in range(400))
    long_note = f"{filler}\n\n{read_note('meets')}"

    result = await redactor.redact(long_note)

    assert_no_pii(result.text)


async def test_every_entity_in_the_policy_is_reported_in_the_counts(
    redactor: PresidioPiiRedactor,
) -> None:
    result = await redactor.redact(read_note("meets"))

    assert set(result.entity_counts) <= set(ENTITY_REPLACEMENTS)
    assert result.entity_counts.get("US_SSN") == 1
