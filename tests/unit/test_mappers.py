"""Mappers are pure: every case here runs without Docker and without a session."""

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from pydantic import SecretStr

from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import (
    claim_from_row,
    claim_to_row_values,
    policy_from_row,
    policy_to_row_values,
    review_task_from_row,
    review_task_to_row_values,
    tenant_from_row,
    tenant_to_row_values,
)
from ecet.infrastructure.postgres.orm import ClaimRow, PolicyRow, ReviewTaskRow, TenantRow

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/2026-09-06-mri.pdf"


def build_tenant(**overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": "tenant-a",
        "name": "Northwind Health Plan",
        "webhook_url": "http://mock-client:8081/hooks/northwind",
        "webhook_secret": SecretStr("dev-hmac-tenant-a"),
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 2,
        "covered_codes": {Icd10Code(code="M54.5"), Icd10Code(code="M51.26")},
        "excluded_codes": {Icd10Code(code="Z00.00")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "required_evidence": ["conservative therapy >= 6 weeks", "imaging report"],
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(bucket="claims", key=KEY, etag="etag-1", size=12_345),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


def build_task(**overrides: Any) -> ReviewTask:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "claim_id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "reason": ReviewReason.LOW_CONFIDENCE,
        "created_at": NOW,
    }
    fields.update(overrides)
    return ReviewTask.model_validate(fields)


def test_tenant_round_trips() -> None:
    tenant = build_tenant()
    assert tenant_from_row(TenantRow(**tenant_to_row_values(tenant))) == tenant


def test_the_webhook_secret_is_stored_unmasked() -> None:
    """`model_dump` masks a SecretStr; persistence must use `get_secret_value()`."""
    values = tenant_to_row_values(build_tenant())
    assert values["webhook_secret"] == "dev-hmac-tenant-a"


def test_policy_round_trips_including_code_sets() -> None:
    policy = build_policy()
    restored = policy_from_row(PolicyRow(**policy_to_row_values(policy)))
    assert restored == policy


def test_policy_code_sets_are_stored_sorted_for_stable_diffs() -> None:
    values = policy_to_row_values(build_policy())
    assert values["covered_codes"] == ["M51.26", "M54.5"]


def test_a_bare_claim_round_trips() -> None:
    claim = build_claim()
    assert claim_from_row(ClaimRow(**claim_to_row_values(claim))) == claim


def test_a_fully_populated_claim_round_trips() -> None:
    policy_id = PolicyId(uuid4())
    claim = build_claim(
        status=ClaimStatus.EVALUATED,
        redacted=RedactedText(text="note text", entity_counts={"PERSON": 3}, redactor="presidio"),
        policy_ids=[policy_id],
        deterministic=DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="non_empty_text", passed=True, detail="120 characters")],
        ),
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            matched_policy_id=policy_id,
            cited_codes=[Icd10Code(code="M54.5")],
            rationale="Documented eight weeks of conservative therapy.",
            model="claude-sonnet-5",
            prompt_version="v1",
            latency_ms=1200,
            input_tokens=900,
            output_tokens=120,
        ),
        notification_attempts=2,
        last_notify_error="connect timeout",
    )
    assert claim_from_row(ClaimRow(**claim_to_row_values(claim))) == claim


def test_an_empty_redacted_note_is_not_confused_with_no_redaction() -> None:
    """`redacted_text` may legitimately be ''; `redactor` is the None marker."""
    claim = build_claim(redacted=RedactedText(text="", entity_counts={}, redactor="presidio"))
    restored = claim_from_row(ClaimRow(**claim_to_row_values(claim)))
    assert restored.redacted is not None
    assert restored.redacted.text == ""


def test_review_task_round_trips_open_and_resolved() -> None:
    task = build_task()
    assert review_task_from_row(ReviewTaskRow(**review_task_to_row_values(task))) == task

    task.resolve(
        resolution=Decision.DOES_NOT_MEET,
        reviewer="nurse@tenant-a.example",
        notes="No conservative therapy documented.",
        now=NOW,
    )
    restored = review_task_from_row(ReviewTaskRow(**review_task_to_row_values(task)))
    assert restored == task
    assert restored.status is ReviewStatus.RESOLVED
