"""The rules themselves are covered in `tests/unit/domain/test_rules.py`; this file
proves the wiring holds against the realistic note fixtures."""

from datetime import date
from typing import Any
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY
from tests.fakes import FakePiiRedactor
from tests.pii import read_note

from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.domain.evaluation import Verdict
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Policy


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


TENANT_A_MRI = {
    "id": PolicyId(uuid4()),
    "tenant_id": "tenant-a",
    "name": "MRI lumbar spine",
    "version": 2,
    "covered_codes": [{"code": "M54.5"}, {"code": "M51.26"}],
    "excluded_codes": [{"code": "Z00.00"}, {"code": "Z13.9"}],
    "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
    "effective_from": date(2026, 1, 1),
}


def policies() -> list[Policy]:
    fields: dict[str, Any] = dict(TENANT_A_MRI)
    return [Policy.model_validate(fields)]


@pytest.mark.parametrize(
    ("note", "expected"),
    [
        ("meets", Verdict.PASS),
        ("does_not_meet", Verdict.PASS),
        ("unclear", Verdict.PASS),
        ("excluded_code", Verdict.REJECT),
        ("no_codes", Verdict.UNCERTAIN),
    ],
)
async def test_each_note_fixture_reaches_its_verdict(note: str, expected: Verdict) -> None:
    redacted = await FakePiiRedactor().redact(read_note(note))

    result = await RunDeterministicChecks().execute(redacted, policies())

    assert result.verdict is expected


async def test_every_check_is_reported_not_only_the_failing_ones() -> None:
    redacted = await FakePiiRedactor().redact(read_note("meets"))

    result = await RunDeterministicChecks().execute(redacted, policies())

    assert [check.name for check in result.checks] == [
        "non_empty_text",
        "icd10_present",
        "excluded_code_hit",
        "covered_code_hit",
    ]


async def test_a_verdict_is_counted() -> None:
    before = sample("ecet_deterministic_verdict_total", verdict="PASS")
    redacted = await FakePiiRedactor().redact(read_note("meets"))

    result = await RunDeterministicChecks().execute(redacted, policies())

    assert result.verdict is Verdict.PASS
    assert sample("ecet_deterministic_verdict_total", verdict="PASS") == before + 1


async def test_a_reject_counts_an_avoided_llm_call() -> None:
    """ADR-002's whole argument, as a number: a deterministic REJECT never reaches
    the vendor."""
    before = sample("ecet_llm_calls_avoided_total")
    redacted = await FakePiiRedactor().redact(read_note("excluded_code"))

    result = await RunDeterministicChecks().execute(redacted, policies())

    assert result.verdict is Verdict.REJECT
    assert sample("ecet_llm_calls_avoided_total") == before + 1
