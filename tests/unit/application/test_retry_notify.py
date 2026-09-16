"""The operator retry out of NOTIFY_FAILED. It re-sends a decision that already exists,
as whoever made it — it never re-evaluates and never re-opens a review."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookPermanentError
from ecet.application.use_cases.retry_notify import RetryNotify
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import ClaimNotFound, InvalidTransition
from ecet.domain.evaluation import Decision, Evaluation, ReviewReason, ReviewTask
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=30)
KEY = "tenants/tenant-a/claims/note-1.pdf"


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        "status": ClaimStatus.NOTIFY_FAILED,
        "redacted": RedactedText(text="Patient <PERSON> with M54.5.", redactor="fake"),
        "evaluation": Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            rationale="Conservative therapy documented.",
            model="fake-deterministic",
            prompt_version="v1",
        ),
        "failure_reason": "webhook_unreachable",
        "notification_attempts": 1,
        "last_notify_error": "WebhookTransientError: 3 attempts failed, last: ConnectError",
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def build_case(
    claim: Claim, webhook: FakeWebhookClient | None = None
) -> tuple[RetryNotify, FakeUnitOfWork, FakeWebhookClient]:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    await uow.claims.add(claim)
    webhook = webhook or FakeWebhookClient()
    retry = RetryNotify(uow_factory=lambda: uow, webhook=webhook, clock=FixedClock(LATER))
    return retry, uow, webhook


async def test_an_automatic_decision_is_re_sent_as_auto_and_approves() -> None:
    claim = build_claim()
    retry, uow, webhook = await build_case(claim)

    result = await retry.execute(claim.id)

    ((_, payload),) = webhook.deliveries
    assert payload.decided_by == "auto"
    assert payload.outcome == "MEETS_NECESSITY"
    assert payload.confidence == 0.91
    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.failure_reason is None
    assert stored.last_notify_error is None
    assert stored.notification_attempts == 2
    assert result.status is ClaimStatus.APPROVED_AUTO
    assert uow.commits == 1
    assert_no_pii(payload.model_dump_json())


async def test_a_human_decision_is_re_sent_as_human_and_resolves() -> None:
    claim = build_claim()
    retry, uow, webhook = await build_case(claim)
    task = ReviewTask(
        id=uuid4(),
        claim_id=claim.id,
        tenant_id=claim.tenant_id,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )
    task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="nurse.okafor", notes=None, now=NOW)
    await uow.review_tasks.add(task)

    await retry.execute(claim.id)

    ((_, payload),) = webhook.deliveries
    assert payload.decided_by == "human"
    assert payload.outcome == "DOES_NOT_MEET"
    assert payload.confidence == 1.0
    assert uow.claims.claims[claim.id].status is ClaimStatus.REVIEW_RESOLVED
    assert_no_pii(payload.model_dump_json())


async def test_a_retry_that_fails_again_is_still_recorded() -> None:
    claim = build_claim()
    retry, uow, _ = await build_case(
        claim, FakeWebhookClient(error=WebhookPermanentError("status 410"))
    )

    result = await retry.execute(claim.id)

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.NOTIFY_FAILED
    assert stored.failure_reason == "webhook_rejected"
    assert stored.notification_attempts == 2
    assert stored.last_notify_error == "WebhookPermanentError: status 410"
    assert stored.updated_at == LATER
    assert result.status is ClaimStatus.NOTIFY_FAILED
    assert uow.commits == 1


@pytest.mark.parametrize(
    "status", [ClaimStatus.QUEUED, ClaimStatus.APPROVED_AUTO, ClaimStatus.REVIEW_PENDING]
)
async def test_only_a_notify_failed_claim_can_be_retried(status: ClaimStatus) -> None:
    claim = build_claim(status=status, failure_reason=None)
    retry, uow, webhook = await build_case(claim)

    with pytest.raises(InvalidTransition):
        await retry.execute(claim.id)

    assert webhook.deliveries == []
    assert uow.commits == 0


async def test_an_unknown_claim_is_not_found() -> None:
    retry, _, _ = await build_case(build_claim())

    with pytest.raises(ClaimNotFound):
        await retry.execute(ClaimId(uuid4()))


async def test_a_claim_with_no_decision_is_refused_rather_than_invented() -> None:
    claim = build_claim(evaluation=None)
    retry, _, webhook = await build_case(claim)

    with pytest.raises(InvalidTransition, match="no decision"):
        await retry.execute(claim.id)

    assert webhook.deliveries == []
