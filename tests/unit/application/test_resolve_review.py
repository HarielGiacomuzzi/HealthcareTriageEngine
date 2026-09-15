"""UC-09c. A human decision is recorded even when its delivery fails — the review is
done, only the webhook is outstanding — and a task is invisible to every other tenant."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookTransientError
from ecet.application.use_cases.human_review import ResolveReview, ResolveReviewCommand
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import InvalidTransition, ReviewAlreadyResolved, ReviewTaskNotFound
from ecet.domain.evaluation import (
    Decision,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
)
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
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


def build_claim(status: ClaimStatus) -> Claim:
    evaluation = None
    if status is not ClaimStatus.EVALUATION_FAILED:
        evaluation = Evaluation(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            confidence=0.4,
            rationale="The note leaves the required criteria undocumented.",
            model="fake-deterministic",
            prompt_version="v1",
        )
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=status,
        redacted=RedactedText(
            text="Patient <PERSON> with M54.5, duration unclear.", redactor="fake"
        ),
        evaluation=evaluation,
        created_at=NOW,
        updated_at=NOW,
    )


class Case:
    """A claim waiting on its review task, wired to UC-09c over fakes."""

    def __init__(self, claim: Claim, webhook: FakeWebhookClient) -> None:
        self.claim = claim
        self.webhook = webhook
        self.uow = FakeUnitOfWork(tenants=[build_tenant()])
        self.task = ReviewTask(
            id=uuid4(),
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            reason=ReviewReason.LOW_CONFIDENCE,
            created_at=NOW,
        )
        self.use_case = ResolveReview(
            uow_factory=lambda: self.uow, webhook=webhook, clock=FixedClock(NOW)
        )

    def command(self, **overrides: Any) -> ResolveReviewCommand:
        fields: dict[str, Any] = {
            "task_id": self.task.id,
            "tenant_id": "tenant-a",
            "reviewer": "nurse.okafor",
            "resolution": "DOES_NOT_MEET",
            "notes": "No prior imaging report attached.",
        }
        fields.update(overrides)
        return ResolveReviewCommand.model_validate(fields)

    def stored_claim(self) -> Claim:
        return self.uow.claims.claims[self.claim.id]

    def stored_task(self) -> ReviewTask:
        return self.uow.review_tasks.tasks[self.task.id]


async def build_case(
    *,
    status: ClaimStatus = ClaimStatus.REVIEW_PENDING,
    webhook: FakeWebhookClient | None = None,
) -> Case:
    case = Case(build_claim(status), webhook or FakeWebhookClient())
    await case.uow.claims.add(case.claim)
    await case.uow.review_tasks.add(case.task)
    return case


async def test_resolving_records_the_decision_and_notifies_as_human() -> None:
    case = await build_case()

    result = await case.use_case.execute(case.command())

    task = case.stored_task()
    assert task.status is ReviewStatus.RESOLVED
    assert task.resolution is Decision.DOES_NOT_MEET
    assert task.reviewer == "nurse.okafor"
    assert task.resolved_at == NOW
    ((tenant, payload),) = case.webhook.deliveries
    assert tenant.id == "tenant-a"
    assert payload.decided_by == "human"
    assert payload.outcome == "DOES_NOT_MEET"
    assert payload.confidence == 1.0
    assert payload.claim_id == case.claim.id
    assert case.stored_claim().status is ClaimStatus.REVIEW_RESOLVED
    assert result.task_id == case.task.id
    assert result.claim_id == case.claim.id
    assert result.claim_status is ClaimStatus.REVIEW_RESOLVED
    assert case.uow.commits == 1
    assert_no_pii(payload.model_dump_json())


async def test_the_reviewers_notes_never_reach_the_webhook() -> None:
    case = await build_case()

    await case.use_case.execute(case.command(notes="Spoke to Dr. Ferreira's office."))

    body = case.webhook.deliveries[0][1].model_dump_json()
    assert "Ferreira" not in body
    assert "notes" not in body
    assert_no_pii(body)


async def test_resolving_twice_is_an_error_and_notifies_once() -> None:
    case = await build_case()
    await case.use_case.execute(case.command())

    with pytest.raises(ReviewAlreadyResolved):
        await case.use_case.execute(case.command(resolution="MEETS_NECESSITY"))

    assert len(case.webhook.deliveries) == 1
    assert case.stored_task().resolution is Decision.DOES_NOT_MEET


async def test_a_cross_tenant_resolve_is_not_found() -> None:
    case = await build_case()

    with pytest.raises(ReviewTaskNotFound):
        await case.use_case.execute(case.command(tenant_id="tenant-b"))

    assert case.stored_task().status is ReviewStatus.OPEN
    assert case.webhook.deliveries == []
    assert case.uow.commits == 0


async def test_an_unknown_task_is_not_found() -> None:
    case = await build_case()

    with pytest.raises(ReviewTaskNotFound):
        await case.use_case.execute(case.command(task_id=uuid4()))


async def test_a_failed_delivery_still_records_the_resolution() -> None:
    case = await build_case(webhook=FakeWebhookClient(error=WebhookTransientError("3 attempts")))

    result = await case.use_case.execute(case.command())

    assert case.stored_task().status is ReviewStatus.RESOLVED
    claim = case.stored_claim()
    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_unreachable"
    assert claim.notification_attempts == 1
    assert claim.last_notify_error is not None
    assert result.claim_status is ClaimStatus.NOTIFY_FAILED
    assert case.uow.commits == 1


async def test_a_claim_that_failed_evaluation_can_be_resolved() -> None:
    # UC-06 opens a review task for invalid LLM output; the claim has no evaluation.
    case = await build_case(status=ClaimStatus.EVALUATION_FAILED)

    await case.use_case.execute(case.command())

    assert case.stored_claim().status is ClaimStatus.REVIEW_RESOLVED
    payload = case.webhook.deliveries[0][1]
    assert payload.rationale == ""
    assert payload.cited_codes == []
    assert_no_pii(payload.model_dump_json())


async def test_a_claim_that_failed_evaluation_can_park_in_notify_failed() -> None:
    case = await build_case(
        status=ClaimStatus.EVALUATION_FAILED,
        webhook=FakeWebhookClient(error=WebhookTransientError("3 attempts")),
    )

    await case.use_case.execute(case.command())

    assert case.stored_claim().status is ClaimStatus.NOTIFY_FAILED


async def test_a_claim_not_awaiting_review_is_refused_before_any_delivery() -> None:
    # A webhook the transition then refused would reach the client and be rolled back.
    case = await build_case(status=ClaimStatus.APPROVED_AUTO)

    with pytest.raises(InvalidTransition):
        await case.use_case.execute(case.command())

    assert case.webhook.deliveries == []
    assert case.uow.commits == 0
