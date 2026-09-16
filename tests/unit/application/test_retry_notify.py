"""The operator retry out of NOTIFY_FAILED. It re-sends a decision that already exists,
as whoever made it — it never re-evaluates and never re-opens a review."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
import structlog.testing
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.use_cases.notify_client import UNREACHABLE
from ecet.application.use_cases.retry_notify import RetryNotify
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import ClaimNotFound, InvalidTransition
from ecet.domain.evaluation import Decision, Evaluation, ReviewReason, ReviewTask
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code
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
            matched_policy_id=PolicyId(uuid4()),
            cited_codes=[Icd10Code(code="M54.5")],
            rationale="Conservative therapy documented.",
            evidence_missing=[],
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


def build_retry(uow: FakeUnitOfWork, *, webhook: FakeWebhookClient) -> RetryNotify:
    return RetryNotify(uow_factory=lambda: uow, webhook=webhook, clock=FixedClock(LATER))


async def seed_notify_failed_claim(uow: FakeUnitOfWork, **overrides: Any) -> Claim:
    claim = build_claim(**overrides)
    await uow.claims.add(claim)
    return claim


async def test_an_automatic_decision_is_re_sent_as_auto_and_approves() -> None:
    claim = build_claim()
    retry, uow, webhook = await build_case(claim)

    result = await retry.execute(claim.id)

    ((_, payload),) = webhook.deliveries
    assert payload.decided_by == "auto"
    assert payload.outcome == "MEETS_NECESSITY"
    assert payload.confidence == 0.91
    # The claim handed to the webhook must still carry its evaluation after the
    # first unit of work has closed: a bare re-read would ship a blank rationale.
    assert claim.evaluation is not None
    assert payload.rationale == claim.evaluation.rationale
    assert payload.matched_policy_id == claim.evaluation.matched_policy_id
    assert payload.cited_codes == ["M54.5"]
    assert payload.evidence_missing == claim.evaluation.evidence_missing
    assert payload.source_key == KEY
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
    # The human resolution overrides the outcome, but the payload's supporting
    # evidence still comes from the claim's own evaluation.
    assert claim.evaluation is not None
    assert payload.rationale == claim.evaluation.rationale
    assert payload.matched_policy_id == claim.evaluation.matched_policy_id
    assert payload.cited_codes == ["M54.5"]
    assert payload.source_key == KEY
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


async def test_the_retry_posts_with_no_transaction_open() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient(watch=uow)
    claim = await seed_notify_failed_claim(uow)

    await build_retry(uow, webhook=webhook).execute(claim.id)

    assert webhook.uow_open_during_call == [False]
    assert uow.entries == 2


async def test_a_second_failure_still_commits_the_attempt_count() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient(error=WebhookTransientError("still refused"))
    # Seeded with a different failure token than the one under test, so the assertion
    # below fails if the write branch that sets it were deleted.
    claim = await seed_notify_failed_claim(uow, failure_reason="webhook_rejected")

    result = await build_retry(uow, webhook=webhook).execute(claim.id)

    assert result.status is ClaimStatus.NOTIFY_FAILED
    stored = await uow.claims.get(claim.id)
    assert stored.notification_attempts == claim.notification_attempts + 1
    assert stored.failure_reason == UNREACHABLE
    assert uow.commits == 1


class RacingWebhook(FakeWebhookClient):
    """Moves the stored claim to `APPROVED_AUTO` from under the retry while its POST
    is in flight — another operator's retry, a second retry from a double-click, or a
    worker, touching the same claim before this request's write lands."""

    def __init__(self, uow: FakeUnitOfWork, claim_id: ClaimId, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._uow = uow
        self._claim_id = claim_id

    async def deliver(self, tenant: Tenant, payload: Any) -> None:
        stored = self._uow.claims.claims[self._claim_id]
        self._uow.claims.claims[self._claim_id] = stored.model_copy(
            update={"status": ClaimStatus.APPROVED_AUTO, "failure_reason": None}
        )
        await super().deliver(tenant, payload)


async def test_a_retry_that_moved_while_failing_again_is_not_written() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    claim = await seed_notify_failed_claim(uow)
    webhook = RacingWebhook(uow, claim.id, error=WebhookTransientError("still refused"))

    with structlog.testing.capture_logs() as captured, pytest.raises(InvalidTransition):
        await build_retry(uow, webhook=webhook).execute(claim.id)

    # The winner's write stands untouched: no stamping a delivery-failure reason onto
    # a claim that already moved past NOTIFY_FAILED, and no commit.
    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.failure_reason is None
    assert stored.notification_attempts == claim.notification_attempts
    assert uow.commits == 0
    assert any(entry.get("event") == "notify.retry_lost_race" for entry in captured)


async def test_a_retry_that_moved_while_succeeding_is_not_written() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    claim = await seed_notify_failed_claim(uow)
    webhook = RacingWebhook(uow, claim.id)

    with pytest.raises(InvalidTransition):
        await build_retry(uow, webhook=webhook).execute(claim.id)

    # The client already got the notification; only the write is refused, so the
    # duplicate delivery is visible rather than silently repeated on the next retry.
    assert len(webhook.deliveries) == 1
    assert uow.commits == 0


async def test_a_deactivated_tenant_parks_the_claim_without_a_delivery() -> None:
    inactive_tenant = build_tenant().model_copy(update={"active": False})
    uow = FakeUnitOfWork(tenants=[inactive_tenant])
    webhook = FakeWebhookClient()
    claim = await seed_notify_failed_claim(uow)

    result = await build_retry(uow, webhook=webhook).execute(claim.id)

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.NOTIFY_FAILED
    assert stored.failure_reason == "tenant_inactive"
    assert stored.notification_attempts == claim.notification_attempts  # nothing left the process
    assert webhook.deliveries == []
    assert result.status is ClaimStatus.NOTIFY_FAILED
    assert uow.commits == 1
