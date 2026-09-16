"""UC-07 RouteDecision (ADR-003).

The gate is one pure function — `triage(evaluation, threshold)` in the domain — and
everything here is the consequence: notify and approve, or open a review task and
wait. The threshold is injected, never read from config inside the domain.

UC-06 runs the delivery *outside* the unit of work, so this class is split in two:
`decide` is pure (no I/O, no `uow`) and `apply` writes. `NotifyClient` and the webhook
call happen between them, in the caller.
"""

import structlog

from ecet import metrics
from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.notify_client import Delivery
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.evaluation import Evaluation, ReviewReason, Route, Verdict, triage

log = structlog.get_logger(__name__)

#: Fallback token for `delivery is None` on a route other than `HUMAN_REVIEW` — a
#: programming error, since `AUTO_NOTIFY` always produces a `Delivery` (real or
#: `tenant_inactive`), but the `str | None` type still needs a non-`None` reason.
NO_DELIVERY = "no_delivery"


class RouteDecision:
    """UC-07, in two halves, because the delivery between them runs outside the unit of
    work: `decide` is pure and `apply` writes."""

    def __init__(self, *, clock: Clock, threshold: float) -> None:
        self._clock = clock
        self._threshold = threshold

    def decide(self, claim: Claim, evaluation: Evaluation) -> Route:
        route = triage(evaluation, self._threshold)
        metrics.TRIAGE_ROUTE_TOTAL.labels(route=route.value).inc()
        log.info(
            "claim.routed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            route=route.value,
            decision=evaluation.decision.value,
            confidence=evaluation.confidence,
        )
        return route

    async def apply(
        self, uow: UnitOfWork, claim: Claim, *, route: Route, delivery: Delivery | None
    ) -> None:
        """`delivery` is `None` for `HUMAN_REVIEW`: nothing was sent, because nobody has
        decided anything yet."""
        if route is Route.HUMAN_REVIEW:
            await RequestHumanReview(uow.review_tasks, self._clock).execute(
                claim, self._reason_for(claim)
            )
            self._advance(claim, ClaimStatus.REVIEW_PENDING)
        elif delivery is not None and delivery.succeeded:
            self._advance(claim, ClaimStatus.APPROVED_AUTO)
        else:
            reason = delivery.failure_reason if delivery is not None else NO_DELIVERY
            self._fail(claim, reason or NO_DELIVERY)
        await uow.claims.save(claim)

    @staticmethod
    def _reason_for(claim: Claim) -> ReviewReason:
        deterministic = claim.deterministic
        if deterministic is not None and deterministic.verdict is Verdict.UNCERTAIN:
            return ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW
        return ReviewReason.LOW_CONFIDENCE

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        previous = claim.status
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            **{"from": previous.value},
        )

    def _fail(self, claim: Claim, reason: str) -> None:
        previous = claim.status
        claim.transition(ClaimStatus.NOTIFY_FAILED, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=ClaimStatus.NOTIFY_FAILED.value,
            reason=reason,
            **{"from": previous.value},
        )
