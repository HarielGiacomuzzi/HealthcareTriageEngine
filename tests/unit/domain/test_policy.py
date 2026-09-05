from datetime import date
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code, Policy


def code(raw: str) -> Icd10Code:
    return Icd10Code(code=raw)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {code("M54.5")},
        "excluded_codes": {code("Z00.0")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "required_evidence": ["conservative therapy >= 6 weeks", "imaging report"],
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


@pytest.mark.parametrize("raw", ["M54.5", "G89", "Z00.00", "T81.4XXA", "A01"])
def test_valid_icd10_codes_are_accepted(raw: str) -> None:
    assert code(raw).code == raw


@pytest.mark.parametrize("raw", ["U07.1", "154.5", "M5", "M54.", "M54.ABCDE", "", "MM4"])
def test_invalid_icd10_codes_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        code(raw)


def test_icd10_codes_are_normalised() -> None:
    assert code("  m54.5 ").code == "M54.5"


def test_icd10_codes_are_hashable_and_compare_by_value() -> None:
    assert {code("M54.5"), code("m54.5")} == {code("M54.5")}
    assert str(code("M54.5")) == "M54.5"


def test_covered_and_excluded_codes_may_not_overlap() -> None:
    with pytest.raises(ValidationError):
        build_policy(covered_codes={code("M54.5")}, excluded_codes={code("M54.5")})


def test_criteria_text_must_be_present() -> None:
    with pytest.raises(ValidationError):
        build_policy(criteria_text="")


def test_version_starts_at_one() -> None:
    with pytest.raises(ValidationError):
        build_policy(version=0)


def test_effective_to_may_not_precede_effective_from() -> None:
    with pytest.raises(ValidationError):
        build_policy(effective_from=date(2026, 5, 1), effective_to=date(2026, 4, 30))


@pytest.mark.parametrize(
    ("on", "expected"),
    [
        (date(2025, 12, 31), False),
        (date(2026, 1, 1), True),
        (date(2026, 6, 30), True),
        (date(2026, 7, 1), False),
    ],
)
def test_is_effective_is_inclusive_on_both_ends(on: date, expected: bool) -> None:
    policy = build_policy(effective_from=date(2026, 1, 1), effective_to=date(2026, 6, 30))
    assert policy.is_effective(on) is expected


def test_open_ended_policy_stays_effective() -> None:
    policy = build_policy(effective_to=None)
    assert policy.is_effective(date(2099, 1, 1)) is True


def test_inactive_policy_is_never_effective() -> None:
    policy = build_policy(active=False)
    assert policy.is_effective(date(2026, 6, 1)) is False
