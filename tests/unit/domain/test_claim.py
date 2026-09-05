from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import InvalidObjectKey, InvalidTransition, PdfTooLarge
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.ids import ClaimId

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
LATER = datetime(2026, 9, 5, 12, 5, tzinfo=UTC)

VALID_KEY = "tenants/tenant-a/claims/2026-09-05-mri.pdf"


def build_source(**overrides: Any) -> SourceObject:
    fields: dict[str, Any] = {
        "bucket": "claims",
        "key": VALID_KEY,
        "etag": "d41d8cd98f00b204e9800998ecf8427e",
        "size": 12_345,
    }
    fields.update(overrides)
    return SourceObject.model_validate(fields)


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": build_source(),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


def test_tenant_is_parsed_out_of_the_object_key() -> None:
    assert build_source().tenant_id() == "tenant-a"


@pytest.mark.parametrize(
    "key",
    [
        "tenant-a/claims/x.pdf",
        "tenants//claims/x.pdf",
        "tenants/TENANT-A/claims/x.pdf",
        "tenants/tenant-a/x.pdf",
        "tenants/tenant-a/claims/x.txt",
        "tenants/tenant-a/claims/nested/x.pdf",
        "",
    ],
)
def test_invalid_object_keys_raise(key: str) -> None:
    with pytest.raises(InvalidObjectKey):
        build_source(key=key)


@pytest.mark.parametrize("size", [0, -1])
def test_non_positive_size_is_rejected(size: int) -> None:
    with pytest.raises(ValidationError):
        build_source(size=size)


def test_size_limit_is_injected_not_read_from_config() -> None:
    source = build_source(size=1000)
    source.ensure_size_within(1000)
    with pytest.raises(PdfTooLarge):
        source.ensure_size_within(999)


def test_a_new_claim_starts_received() -> None:
    assert build_claim().status is ClaimStatus.RECEIVED


def test_legal_transition_bumps_updated_at() -> None:
    claim = build_claim()
    claim.transition(ClaimStatus.EXTRACTED, now=LATER)
    assert claim.status is ClaimStatus.EXTRACTED
    assert claim.updated_at == LATER


def test_illegal_transition_raises_and_changes_nothing() -> None:
    claim = build_claim()
    with pytest.raises(InvalidTransition):
        claim.transition(ClaimStatus.APPROVED_AUTO, now=LATER)
    assert claim.status is ClaimStatus.RECEIVED
    assert claim.updated_at == NOW


def test_the_happy_path_walks_end_to_end() -> None:
    claim = build_claim()
    for nxt in [
        ClaimStatus.EXTRACTED,
        ClaimStatus.REDACTED,
        ClaimStatus.POLICIES_ATTACHED,
        ClaimStatus.QUEUED,
        ClaimStatus.EVALUATED,
        ClaimStatus.APPROVED_AUTO,
    ]:
        claim.transition(nxt, now=LATER)
    assert claim.status is ClaimStatus.APPROVED_AUTO


def test_deterministic_reject_short_circuits_to_review() -> None:
    """ADR-002: a REJECT verdict skips QUEUED and EVALUATED entirely."""
    claim = build_claim(status=ClaimStatus.POLICIES_ATTACHED)
    claim.transition(ClaimStatus.REVIEW_PENDING, now=LATER)
    assert claim.status is ClaimStatus.REVIEW_PENDING


def test_failure_transition_requires_a_reason() -> None:
    claim = build_claim()
    with pytest.raises(InvalidTransition, match="reason"):
        claim.transition(ClaimStatus.EXTRACTION_FAILED, now=LATER)
    claim.transition(ClaimStatus.EXTRACTION_FAILED, reason="encrypted pdf", now=LATER)
    assert claim.failure_reason == "encrypted pdf"


def test_terminal_states_have_no_way_out() -> None:
    for terminal in (ClaimStatus.EXTRACTION_FAILED, ClaimStatus.NO_POLICIES):
        for nxt in ClaimStatus:
            assert terminal.can_transition_to(nxt) is False


def test_notify_failed_can_be_retried() -> None:
    claim = build_claim(status=ClaimStatus.NOTIFY_FAILED)
    claim.transition(ClaimStatus.APPROVED_AUTO, now=LATER)
    assert claim.status is ClaimStatus.APPROVED_AUTO


def test_transition_defaults_to_now_when_no_clock_is_given() -> None:
    claim = build_claim()
    before = datetime.now(UTC)
    claim.transition(ClaimStatus.EXTRACTED)
    assert before <= claim.updated_at <= datetime.now(UTC)


def test_claim_json_round_trip_is_stable() -> None:
    claim = build_claim(
        redacted=RedactedText(text="note", entity_counts={"PERSON": 3}, redactor="presidio-2.2.x"),
        deterministic=DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="non_empty_text", passed=True, detail="120 characters")],
        ),
    )
    assert Claim.model_validate(claim.model_dump(mode="json")) == claim


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError):
        build_claim(created_at=datetime(2026, 9, 5, 12, 0))
