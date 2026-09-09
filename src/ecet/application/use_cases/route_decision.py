"""UC-07 RouteDecision (ADR-003).

The gate is one pure function — `triage(evaluation, threshold)` in the domain — and
everything here is the consequence: notify and approve, or open a review task and
wait. The threshold is injected, never read from config inside the domain.

`NotifyClient` and `RequestHumanReview` are built here rather than injected, for the
same reason UC-01 builds `AttachTenantPolicies`: they depend on repositories that
belong to one unit of work, and a unit of work outlives one message, not a process.

A delivery failure does **not** open a review task. Nobody needs to read the note
again — the decision stands, only the delivery failed — so the claim parks in
`NOTIFY_FAILED` for an operator retry (Phase 5).
"""

import structlog

from ecet.application.errors import WebhookError, WebhookPermanentError
from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import TenantNotFound
from ecet.domain.evaluation import ReviewReason, Route, Verdict, triage

log = structlog.get_logger(__name__)

#: Short tokens, matching the `EXTRACTION_FAILED` convention. The exception detail
#: lives on `Claim.last_notify_error`, not here.
REJECTED = "webhook_rejected"
UNREACHABLE = "webhook_unreachable"
TENANT_INACTIVE = "tenant_inactive"


class RouteDecision:
    def __init__(
        self,
        *,
        uow: UnitOfWork,
        webhook: WebhookClient,
        clock: Clock,
        threshold: float,
    ) -> None:
        self._uow = uow
        self._clock = clock
        self._threshold = threshold
        self._notify = NotifyClient(uow.tenants, webhook, clock)
        self._request_review = RequestHumanReview(uow.review_tasks, clock)

    async def execute(self, claim: Claim) -> None:
        evaluation = claim.evaluation
        if evaluation is None:
            raise ValueError(f"claim {claim.id} has no evaluation to route")

        route = triage(evaluation, self._threshold)
        log.info(
            "claim.routed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            route=route.value,
            decision=evaluation.decision.value,
            confidence=evaluation.confidence,
        )

        if route is Route.HUMAN_REVIEW:
            await self._request_review.execute(claim, self._reason_for(claim))
            self._advance(claim, ClaimStatus.REVIEW_PENDING)
        else:
            try:
                await self._notify.execute(
                    claim,
                    outcome=evaluation.decision,
                    confidence=evaluation.confidence,
                    decided_by="auto",
                )
            except TenantNotFound as error:
                # `NotifyClient` reads the tenant before it can attempt a delivery, and
                # the repository refuses one deactivated between enqueue and evaluate.
                # Parking the claim keeps the evaluation; letting it escape would roll
                # the whole unit of work back and leave the claim QUEUED, reasonless.
                claim.last_notify_error = f"{type(error).__name__}: {error}"
                self._fail(claim, TENANT_INACTIVE)
            except WebhookError as error:
                reason = REJECTED if isinstance(error, WebhookPermanentError) else UNREACHABLE
                self._fail(claim, reason)
            else:
                self._advance(claim, ClaimStatus.APPROVED_AUTO)

        await self._uow.claims.save(claim)

    @staticmethod
    def _reason_for(claim: Claim) -> ReviewReason:
        deterministic = claim.deterministic
        if deterministic is not None and deterministic.verdict is Verdict.UNCERTAIN:
            return ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW
        return ReviewReason.LOW_CONFIDENCE

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
        )

    def _fail(self, claim: Claim, reason: str) -> None:
        claim.transition(ClaimStatus.NOTIFY_FAILED, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=ClaimStatus.NOTIFY_FAILED.value,
            reason=reason,
        )
