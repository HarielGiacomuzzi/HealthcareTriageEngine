from datetime import date
from typing import Any
from uuid import uuid4

import pytest

from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import CheckOutcome, Verdict
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.rules import MIN_TEXT_CHARS, aggregate, extract_icd10_codes, run_checks

LONG_NOTE = (
    "Patient reports persistent lower back pain for eight weeks. "
    "Conservative therapy completed. Diagnosis M54.5 documented."
)


def code(raw: str) -> Icd10Code:
    return Icd10Code(code=raw)


def redacted(text: str) -> RedactedText:
    return RedactedText(text=text, entity_counts={"PERSON": 1}, redactor="presidio-2.2.x")


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {code("M54.5")},
        "excluded_codes": {code("Z00.0")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


def outcome(result: Any, name: str) -> CheckOutcome:
    return next(check for check in result.checks if check.name == name)


def test_codes_are_extracted_normalised_and_deduplicated() -> None:
    found = extract_icd10_codes("Codes m54.5, M54.5 and g89.4 were noted.")
    assert [item.code for item in found] == ["M54.5", "G89.4"]


def test_words_that_are_not_codes_are_ignored() -> None:
    assert extract_icd10_codes("No diagnosis recorded during this visit at all.") == []


def test_all_four_checks_are_always_reported() -> None:
    result = run_checks(redacted(LONG_NOTE), [build_policy()])
    assert [check.name for check in result.checks] == [
        "non_empty_text",
        "icd10_present",
        "excluded_code_hit",
        "covered_code_hit",
    ]


def test_a_covered_code_in_a_long_note_passes() -> None:
    result = run_checks(redacted(LONG_NOTE), [build_policy()])
    assert result.verdict is Verdict.PASS
    assert all(check.passed for check in result.checks)


def test_short_text_is_rejected() -> None:
    result = run_checks(redacted("M54.5"), [build_policy()])
    assert outcome(result, "non_empty_text").passed is False
    assert result.verdict is Verdict.REJECT


def test_text_exactly_at_the_minimum_passes_that_check() -> None:
    text = "M54.5 " + "a" * (MIN_TEXT_CHARS - 6)
    result = run_checks(redacted(text), [build_policy()])
    assert outcome(result, "non_empty_text").passed is True


def test_no_codes_is_uncertain() -> None:
    note = "The patient describes discomfort but no diagnosis code was recorded anywhere."
    result = run_checks(redacted(note), [build_policy()])
    assert outcome(result, "icd10_present").passed is False
    assert result.verdict is Verdict.UNCERTAIN


def test_an_excluded_code_is_rejected() -> None:
    note = LONG_NOTE + " Routine examination Z00.0 was also performed for the record."
    result = run_checks(redacted(note), [build_policy()])
    assert outcome(result, "excluded_code_hit").passed is False
    assert result.verdict is Verdict.REJECT


def test_a_code_covered_by_no_policy_is_uncertain() -> None:
    note = (
        "Patient reports persistent shoulder pain for eight weeks with no improvement. "
        "Diagnosis G89.4 documented by the attending physician."
    )
    result = run_checks(redacted(note), [build_policy()])
    assert outcome(result, "covered_code_hit").passed is False
    assert result.verdict is Verdict.UNCERTAIN


def test_reject_beats_uncertain() -> None:
    result = run_checks(redacted("Z00.0"), [build_policy()])
    assert result.verdict is Verdict.REJECT


def test_codes_are_matched_across_every_policy() -> None:
    other = build_policy(name="Physio", covered_codes={code("G89.4")}, excluded_codes=set())
    note = (
        "Patient reports persistent shoulder pain for eight weeks with no improvement. "
        "Diagnosis G89.4 documented by the attending physician."
    )
    result = run_checks(redacted(note), [build_policy(), other])
    assert result.verdict is Verdict.PASS


def test_no_policies_leaves_every_code_uncovered() -> None:
    result = run_checks(redacted(LONG_NOTE), [])
    assert outcome(result, "covered_code_hit").passed is False
    assert result.verdict is Verdict.UNCERTAIN


@pytest.mark.parametrize(
    ("failed", "expected"),
    [
        ([], Verdict.PASS),
        (["icd10_present"], Verdict.UNCERTAIN),
        (["covered_code_hit"], Verdict.UNCERTAIN),
        (["excluded_code_hit"], Verdict.REJECT),
        (["non_empty_text", "icd10_present"], Verdict.REJECT),
    ],
)
def test_aggregation_precedence(failed: list[str], expected: Verdict) -> None:
    names = ["non_empty_text", "icd10_present", "excluded_code_hit", "covered_code_hit"]
    checks = [CheckOutcome(name=name, passed=name not in failed, detail="") for name in names]
    assert aggregate(checks) is expected


def test_clinical_prose_produces_false_positive_codes() -> None:
    """Documents the ceiling: pattern-only matching, no ICD-10 dictionary.

    `T12` is a vertebra named in nearly every lumbar-spine MRI report. Fixing this
    needs the seeded ICD-10 code set (Phase 2) to validate against.
    """
    found = extract_icd10_codes("Vitamin B12 low. T12 vertebral body.")
    assert [item.code for item in found] == ["B12", "T12"]
