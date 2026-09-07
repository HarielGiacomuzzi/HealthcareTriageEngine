"""The real engine, marked `slow` by the directory conftest.

Needs `make spacy-model` (or the CI step that runs it). No Docker — but a 2-4 s model
load and ~600 MB of RAM, which is exactly why it does not belong in the default run.
"""

import pytest
from tests.pii import assert_no_pii, read_note

from ecet.application.redaction_policy import (
    CUSTOM_PATTERNS,
    ENTITY_REPLACEMENTS,
    KEPT_ENTITIES,
    SCORE_THRESHOLD,
)
from ecet.infrastructure.pii.presidio_redactor import (
    DEFAULT_CHUNK_CHARS,
    PresidioPiiRedactor,
    _chunks,
)


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
    # Pins the exemption filter running BEFORE the counts loop: if it moved after,
    # "M54.5" would still print (LOCATION's anonymize op never touched it) but the
    # count would double to 2 (M54.5 plus the "MD" false positive), and this test
    # would still pass on the `in result.text` assertion alone.
    assert result.entity_counts["LOCATION"] == 1


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


def test_kept_entities_are_never_in_the_analysed_entity_list(
    redactor: PresidioPiiRedactor,
) -> None:
    # Ties KEPT_ENTITIES to the actual behaviour: DATE_TIME survives because it is
    # never analysed, not because anything special consults this constant. If it
    # were ever added to ENTITY_REPLACEMENTS, this fails alongside the constant's
    # own premise going stale.
    assert not KEPT_ENTITIES & set(redactor._entities)


async def test_empty_text_is_not_an_error(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("")

    assert result.text == ""
    assert result.entity_counts == {}


async def test_text_beyond_one_chunk_is_still_redacted(
    redactor: PresidioPiiRedactor,
) -> None:
    filler = "\n\n".join("The member attended the scheduled session." for _ in range(500))
    long_note = f"{filler}\n\n{read_note('meets')}"
    # Prove this test actually exercises the multi-chunk path, not the
    # `len(text) <= limit` short-circuit — a regression there must fail loudly.
    assert len(_chunks(long_note, DEFAULT_CHUNK_CHARS)) > 1

    result = await redactor.redact(long_note)

    assert_no_pii(result.text)


def test_chunks_keeps_leading_and_trailing_blank_paragraphs() -> None:
    text = "\n\nA\n\n" + "x" * 600 + "\n\nB\n\n"

    chunks = _chunks(text, limit=100)

    assert len(chunks) > 1
    assert "\n\n".join(chunks) == text


async def test_no_entity_outside_the_policy_is_reported_in_the_counts(
    redactor: PresidioPiiRedactor,
) -> None:
    result = await redactor.redact(read_note("meets"))

    assert set(result.entity_counts) <= set(ENTITY_REPLACEMENTS)
    assert result.entity_counts.get("US_SSN") == 1
