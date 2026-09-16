"""`POST /v1/claims/{id}/retry-notify` — the only way out of `NOTIFY_FAILED`.

The decision already exists; only its delivery failed. So nothing is re-evaluated and
nobody re-reviews: the retry re-sends what was decided, as whoever decided it. A claim
with a review task was decided by a human (UC-09c) — `review_tasks.claim_id` is
unique, so there is never more than one — and a claim without one was decided
automatically (UC-07).

A retry that fails again is not an error from this use case's point of view. It
leaves the claim `NOTIFY_FAILED` and still commits, because the attempt count and the
latest error are exactly what the operator reads next.
"""

from collections.abc import Callable

import structlog

from ecet.application.notifications import DecidedBy
from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.notify_client import NotifyClient, tenant_inactive
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import InvalidTransition, TenantNotFound
from ecet.domain.evaluation import Decision, ReviewTask
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant

log = structlog.get_logger(__name__)


class RetryNotify:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        webhook: WebhookClient,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._webhook = webhook
        self._clock = clock

    async def execute(self, claim_id: ClaimId) -> Claim:
        async with self._uow_factory() as uow:  # read-only: validates and decides
            claim = await uow.claims.get(claim_id)
            if claim.status is not ClaimStatus.NOTIFY_FAILED:
                raise InvalidTransition(f"claim {claim.id} is {claim.status}, not NOTIFY_FAILED")
            task = await uow.review_tasks.find_by_claim(claim.id)
            outcome, confidence, decided_by, target = self._decision(claim, task)
            tenant_or_error = await self._tenant(uow, claim)

        # Outside the transaction: one attempt, no in-request backoff, and no pooled
        # connection held for it (this use case is itself the operator's retry).
        if isinstance(tenant_or_error, TenantNotFound):
            delivery = tenant_inactive(tenant_or_error)
        else:
            delivery = await NotifyClient(self._webhook, self._clock).attempt(
                claim,
                tenant_or_error,
                outcome=outcome,
                confidence=confidence,
                decided_by=decided_by,
            )

        async with self._uow_factory() as uow:  # the write
            claim = await uow.claims.get(claim_id)
            now = self._clock.now()
            delivery.apply_to(claim)
            if delivery.succeeded:
                claim.transition(target, now=now)
            else:
                # There is no NOTIFY_FAILED -> NOTIFY_FAILED edge: the claim has not
                # moved, only the reason it is stuck may have.
                claim.failure_reason = delivery.failure_reason
                claim.updated_at = now
            await uow.claims.save(claim)
            await uow.commit()

        log.info(
            "notify.retried",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            decided_by=decided_by,
            status=claim.status.value,
            attempts=claim.notification_attempts,
        )
        return claim

    @staticmethod
    def _decision(
        claim: Claim, task: ReviewTask | None
    ) -> tuple[Decision, float, DecidedBy, ClaimStatus]:
        evaluation = claim.evaluation
        if task is not None and task.resolution is not None:
            return task.resolution, 1.0, "human", ClaimStatus.REVIEW_RESOLVED
        if task is None and evaluation is not None:
            return evaluation.decision, evaluation.confidence, "auto", ClaimStatus.APPROVED_AUTO
        # Not reachable through the use cases — NOTIFY_FAILED is only ever entered
        # once a decision exists. Refusing beats inventing an outcome.
        raise InvalidTransition(f"claim {claim.id} has no decision to deliver")

    async def _tenant(self, uow: UnitOfWork, claim: Claim) -> Tenant | TenantNotFound:
        """Read before the transaction closes, because the delivery happens after it.
        Returned as one value rather than an `(ok, error)` pair so the caller narrows
        with `isinstance` instead of an `assert`."""
        try:
            return await uow.tenants.get(claim.tenant_id)
        except TenantNotFound as error:
            return error
