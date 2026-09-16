"""UC-07. The confidence gate (ADR-003) and the two ways a decided claim can end:
delivered, or waiting for a human.

`RouteDecision` is split in two — `decide` (pure) and `apply` (writes) — because the
delivery between them runs outside any unit of work (UC-06). These tests exercise
each half on its own terms: `decide` against an evaluation and a threshold, `apply`
against a route and a `Delivery` the caller already obtained.
"""

from datetime import UTC, datetime
from uuid import uuid4

from prometheus_client import REGISTRY
from tests.fakes import FakeUnitOfWork, FixedClock

from ecet.application.use_cases.notify_client import Delivery, tenant_inactive
from ecet.application.use_cases.route_decision import RouteDecision
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import TenantNotFound
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    Route,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
THRESHOLD = 0.85


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


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


def build_claim(*, verdict: Verdict = Verdict.PASS) -> Claim:
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
        created_at=NOW,
        updated_at=NOW,
    )


def build_router() -> RouteDecision:
    return RouteDecision(clock=FixedClock(NOW), threshold=THRESHOLD)


async def build_uow(claim: Claim) -> FakeUnitOfWork:
    uow = FakeUnitOfWork()
    await uow.claims.add(claim)
    return uow


def test_a_confident_decision_routes_to_auto_notify() -> None:
    router = build_router()

    route = router.decide(build_claim(), build_evaluation(confidence=0.90))

    assert route is Route.AUTO_NOTIFY


def test_the_threshold_boundary_is_inclusive() -> None:
    router = build_router()

    route = router.decide(build_claim(), build_evaluation(confidence=THRESHOLD))

    assert route is Route.AUTO_NOTIFY


def test_a_low_confidence_decision_routes_to_human_review() -> None:
    router = build_router()

    route = router.decide(build_claim(), build_evaluation(confidence=0.80))

    assert route is Route.HUMAN_REVIEW


def test_insufficient_evidence_never_auto_notifies_however_confident() -> None:
    router = build_router()

    route = router.decide(
        build_claim(), build_evaluation(confidence=0.99, decision=Decision.INSUFFICIENT_EVIDENCE)
    )

    assert route is Route.HUMAN_REVIEW


def test_the_route_is_counted() -> None:
    before = sample("ecet_triage_route_total", route="AUTO_NOTIFY")
    router = build_router()

    router.decide(build_claim(), build_evaluation(confidence=0.90))

    assert sample("ecet_triage_route_total", route="AUTO_NOTIFY") == before + 1


async def test_a_successful_delivery_approves_the_claim() -> None:
    claim = build_claim()
    uow = await build_uow(claim)
    delivery = Delivery(failure_reason=None, error=None)

    await build_router().apply(uow, claim, route=Route.AUTO_NOTIFY, delivery=delivery)

    assert claim.status is ClaimStatus.APPROVED_AUTO
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_a_permanent_delivery_failure_sets_notify_failed_with_a_token() -> None:
    claim = build_claim()
    uow = await build_uow(claim)
    delivery = Delivery(failure_reason="webhook_rejected", error="WebhookPermanentError: 400")

    await build_router().apply(uow, claim, route=Route.AUTO_NOTIFY, delivery=delivery)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_rejected"
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_a_tenant_deactivated_before_delivery_parks_the_claim() -> None:
    claim = build_claim()
    uow = await build_uow(claim)
    delivery = tenant_inactive(TenantNotFound("tenant-a"))

    await build_router().apply(uow, claim, route=Route.AUTO_NOTIFY, delivery=delivery)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "tenant_inactive"


async def test_a_low_confidence_route_opens_a_review_and_ignores_delivery() -> None:
    claim = build_claim()
    uow = await build_uow(claim)

    await build_router().apply(uow, claim, route=Route.HUMAN_REVIEW, delivery=None)

    assert claim.status is ClaimStatus.REVIEW_PENDING
    tasks = list(uow.review_tasks.tasks.values())
    assert len(tasks) == 1
    assert tasks[0].reason is ReviewReason.LOW_CONFIDENCE
    assert tasks[0].claim_id == claim.id


async def test_an_uncertain_deterministic_verdict_names_the_combined_reason() -> None:
    claim = build_claim(verdict=Verdict.UNCERTAIN)
    uow = await build_uow(claim)

    await build_router().apply(uow, claim, route=Route.HUMAN_REVIEW, delivery=None)

    tasks = list(uow.review_tasks.tasks.values())
    assert tasks[0].reason is ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW
