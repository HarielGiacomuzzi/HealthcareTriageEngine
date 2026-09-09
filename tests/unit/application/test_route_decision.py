"""UC-07. The confidence gate (ADR-003) and the two ways a decided claim can end:
delivered, or waiting for a human."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.use_cases.route_decision import RouteDecision
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
THRESHOLD = 0.85


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_evaluation(
    *, confidence: float, decision: Decision = Decision.MEETS_NECESSITY
) -> Evaluation:
    return Evaluation(
        decision=decision,
        confidence=confidence,
        matched_policy_id=PolicyId(uuid4()),
        rationale="Conservative therapy documented.",
        model="fake-deterministic",
        prompt_version="v1",
    )


def build_claim(*, confidence: float, verdict: Verdict = Verdict.PASS) -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=ClaimStatus.EVALUATED,
        redacted=RedactedText(text="Patient <PERSON> with M54.5.", redactor="fake"),
        deterministic=DeterministicResult(
            verdict=verdict,
            checks=[CheckOutcome(name="icd10_present", passed=True, detail="1 code")],
        ),
        evaluation=build_evaluation(confidence=confidence),
        created_at=NOW,
        updated_at=NOW,
    )


async def build_case(
    claim: Claim, webhook: FakeWebhookClient
) -> tuple[RouteDecision, FakeUnitOfWork]:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    await uow.claims.add(claim)
    route = RouteDecision(uow=uow, webhook=webhook, clock=FixedClock(NOW), threshold=THRESHOLD)
    return route, uow


async def test_a_confident_decision_notifies_and_approves() -> None:
    claim = build_claim(confidence=0.90)
    webhook = FakeWebhookClient()
    route, uow = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.APPROVED_AUTO
    assert len(webhook.deliveries) == 1
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_the_threshold_boundary_is_inclusive() -> None:
    claim = build_claim(confidence=THRESHOLD)
    webhook = FakeWebhookClient()
    route, _ = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.APPROVED_AUTO


async def test_a_low_confidence_decision_opens_a_review_and_does_not_notify() -> None:
    claim = build_claim(confidence=0.80)
    webhook = FakeWebhookClient()
    route, uow = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.REVIEW_PENDING
    assert webhook.deliveries == []
    tasks = list(uow.review_tasks.tasks.values())
    assert len(tasks) == 1
    assert tasks[0].reason is ReviewReason.LOW_CONFIDENCE
    assert tasks[0].claim_id == claim.id


async def test_an_uncertain_deterministic_verdict_names_the_combined_reason() -> None:
    claim = build_claim(confidence=0.80, verdict=Verdict.UNCERTAIN)
    route, uow = await build_case(claim, FakeWebhookClient())

    await route.execute(claim)

    tasks = list(uow.review_tasks.tasks.values())
    assert tasks[0].reason is ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW


async def test_insufficient_evidence_never_auto_notifies_however_confident() -> None:
    claim = build_claim(confidence=0.99)
    claim.evaluation = build_evaluation(confidence=0.99, decision=Decision.INSUFFICIENT_EVIDENCE)
    webhook = FakeWebhookClient()
    route, _ = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.REVIEW_PENDING
    assert webhook.deliveries == []


async def test_a_permanent_delivery_failure_sets_notify_failed_with_a_token() -> None:
    claim = build_claim(confidence=0.90)
    route, uow = await build_case(claim, FakeWebhookClient(error=WebhookPermanentError("400")))

    await route.execute(claim)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_rejected"
    assert claim.last_notify_error is not None
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_a_tenant_deactivated_before_delivery_parks_the_claim() -> None:
    # `NotifyClient` reads the tenant first, and the repository refuses an inactive one.
    # With no catch here, that escapes UC-06, rolls the evaluation back and leaves the
    # claim QUEUED with no failure_reason.
    claim = build_claim(confidence=0.90)
    uow = FakeUnitOfWork(tenants=[build_tenant().model_copy(update={"active": False})])
    await uow.claims.add(claim)
    route = RouteDecision(
        uow=uow, webhook=FakeWebhookClient(), clock=FixedClock(NOW), threshold=THRESHOLD
    )

    await route.execute(claim)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "tenant_inactive"
    assert claim.last_notify_error is not None
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_a_transient_delivery_failure_sets_notify_failed_with_its_own_token() -> None:
    claim = build_claim(confidence=0.90)
    route, _ = await build_case(claim, FakeWebhookClient(error=WebhookTransientError("timeout")))

    await route.execute(claim)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_unreachable"


async def test_routing_a_claim_with_no_evaluation_is_a_programming_error() -> None:
    claim = build_claim(confidence=0.90)
    claim.evaluation = None
    route, _ = await build_case(claim, FakeWebhookClient())

    with pytest.raises(ValueError, match="no evaluation"):
        await route.execute(claim)
