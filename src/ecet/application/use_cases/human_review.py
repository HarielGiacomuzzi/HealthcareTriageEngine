"""UC-09 human review: request (09a), list (09b), resolve (09c).

UC-09a is idempotent by claim: `review_tasks.claim_id` is unique in the schema, so a
second request for a claim that already has an open task returns that task rather than
racing the constraint. The caller — not this use case — transitions the claim to
`REVIEW_PENDING`, because only the caller knows whether the claim is also being saved
in the same unit of work.

UC-09b lists a tenant's open tasks with the redacted note, the deterministic checks
and the LLM evaluation beside each.

UC-09c records the decision even when its webhook fails. The reviewer's work is done;
only the delivery is outstanding, so the claim parks in `NOTIFY_FAILED` for the
operator retry rather than rolling the resolution back. A reviewer's `notes` are
free text a person typed: they are stored on the task and never logged or sent.

The delivery runs between two units of work: a read-only one validates the request and
closes, the webhook POST runs with no transaction open, and a second one records the
resolution and the delivery's outcome. Two concurrent resolves can both reach the POST
before one loses at `task.resolve` — the receiver's dedupe key is `claim_id` in the
body, as Phase 4 deviation #3 already documents.
"""

from collections.abc import Callable
from datetime import datetime
from uuid import UUID, uuid4

import structlog
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.notify_client import (
    NotifyClient,
    tenant_for_delivery,
    tenant_inactive,
)
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import (
    InvalidTransition,
    ReviewAlreadyResolved,
    ReviewTaskNotFound,
    TenantNotFound,
)
from ecet.domain.evaluation import (
    DeterministicResult,
    Evaluation,
    HumanResolution,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
)
from ecet.domain.ids import ClaimId, TenantId, TenantIdField
from ecet.domain.ports.review_task_repository import ReviewTaskRepository

log = structlog.get_logger(__name__)


class RequestHumanReview:
    def __init__(self, review_tasks: ReviewTaskRepository, clock: Clock) -> None:
        self._review_tasks = review_tasks
        self._clock = clock

    async def execute(self, claim: Claim, reason: ReviewReason) -> ReviewTask:
        existing = await self._review_tasks.find_open_by_claim(claim.id)
        if existing is not None:
            return existing

        task = ReviewTask(
            id=uuid4(),
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            reason=reason,
            created_at=self._clock.now(),
        )
        await self._review_tasks.add(task)
        return task


class ReviewTaskView(BaseModel):
    """One open review, with what a reviewer reads to decide it (UC-09b).

    The redacted note is here and on no other API response: it is clinical content even
    after redaction, and the review queue is the one caller that needs it."""

    model_config = ConfigDict(frozen=True)

    task_id: UUID
    claim_id: ClaimId
    tenant_id: TenantIdField
    reason: ReviewReason
    created_at: datetime
    claim_status: ClaimStatus
    redacted_text: str | None
    deterministic: DeterministicResult | None
    evaluation: Evaluation | None

    @classmethod
    def of(cls, task: ReviewTask, claim: Claim) -> "ReviewTaskView":
        return cls(
            task_id=task.id,
            claim_id=task.claim_id,
            tenant_id=task.tenant_id,
            reason=task.reason,
            created_at=task.created_at,
            claim_status=claim.status,
            redacted_text=claim.redacted.text if claim.redacted else None,
            deterministic=claim.deterministic,
            evaluation=claim.evaluation,
        )


class ListOpenReviews:
    def __init__(self, *, uow_factory: Callable[[], UnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def execute(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTaskView]:
        """Tenant-scoped by the repository query itself; there is no cross-tenant list."""
        async with self._uow_factory() as uow:
            tasks = await uow.review_tasks.list_open(tenant_id, limit)
            # ponytail: one claim read per task, bounded by `limit` (the route caps it at
            # 200); a join in the repository if the queue ever gets long enough to notice.
            return [ReviewTaskView.of(task, await uow.claims.get(task.claim_id)) for task in tasks]


class ResolveReviewCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_id: UUID
    #: Plain `TenantId`, not the validated field: the value comes from a header, and a
    #: malformed one should miss (404) rather than fail validation with a different
    #: status than a well-formed wrong tenant.
    tenant_id: TenantId
    reviewer: str = Field(min_length=1, max_length=200)
    resolution: HumanResolution
    notes: str | None = Field(default=None, max_length=2000)


class ResolveReviewResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_id: UUID
    claim_id: ClaimId
    claim_status: ClaimStatus


class ResolveReview:
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

    async def execute(self, command: ResolveReviewCommand) -> ResolveReviewResult:
        async with self._uow_factory() as uow:  # read-only: validates and closes
            task = await uow.review_tasks.get(command.task_id)
            if task.tenant_id != command.tenant_id:
                # Same answer as "no such task": anything else confirms it exists.
                raise ReviewTaskNotFound(str(command.task_id))
            if task.status is ReviewStatus.RESOLVED:
                # Checked here so nothing is delivered for a decision already made;
                # `task.resolve` below raises the same error if it loses a race.
                raise ReviewAlreadyResolved(str(task.id))
            claim = await uow.claims.get(task.claim_id)
            if not claim.status.can_transition_to(ClaimStatus.REVIEW_RESOLVED):
                # Checked before the webhook: a delivery the transition then refused
                # would reach the client and be rolled back here.
                raise InvalidTransition(
                    f"claim {claim.id} is {claim.status}, which a review cannot resolve"
                )
            tenant_or_error = await tenant_for_delivery(uow.tenants, claim.tenant_id)

        # Outside the transaction: one attempt, no in-request backoff, and no pooled
        # connection held for it (`retry-notify` is the operator's retry).
        if isinstance(tenant_or_error, TenantNotFound):
            delivery = tenant_inactive(tenant_or_error)
        else:
            delivery = await NotifyClient(self._webhook, self._clock).attempt(
                claim,
                tenant_or_error,
                outcome=command.resolution,
                confidence=1.0,
                decided_by="human",
            )

        async with self._uow_factory() as uow:  # every write
            now = self._clock.now()
            task = await uow.review_tasks.get(command.task_id)
            try:
                # Raises `ReviewAlreadyResolved` if another request resolved the task
                # while the POST above was in flight — either in memory here (this
                # re-read already shows RESOLVED) or at `save`, where the repository's
                # `WHERE status = OPEN` guard catches the narrower window: the winner's
                # transaction commits between this re-read and this save. Both must
                # land in this `except`, or the second window's loss goes unlogged
                # after its webhook already reached the client.
                task.resolve(
                    resolution=command.resolution,
                    reviewer=command.reviewer,
                    notes=command.notes,
                    now=now,
                )
                await uow.review_tasks.save(task)
            except ReviewAlreadyResolved:
                log.warning(
                    "review.resolve_lost_race",
                    task_id=str(task.id),
                    claim_id=str(claim.id),
                    tenant_id=str(claim.tenant_id),
                    delivery_attempted=delivery.attempted,
                    delivery_succeeded=delivery.succeeded,
                )
                raise

            # The task guard above subsumes a claim-status re-check here: `claim_id` is
            # unique per open task, and `retry-notify` refuses a claim whose only task
            # is still OPEN, so nothing else can move a REVIEW_PENDING claim while this
            # task is open. A future edge into or out of REVIEW_PENDING would break that
            # and turn this into a bare InvalidTransition with no log line.
            claim = await uow.claims.get(task.claim_id)
            delivery.apply_to(claim)
            if delivery.succeeded:
                claim.transition(ClaimStatus.REVIEW_RESOLVED, now=now)
            else:
                claim.transition(ClaimStatus.NOTIFY_FAILED, reason=delivery.failure_reason, now=now)
            await uow.claims.save(claim)
            await uow.commit()

        log.info(
            "review.resolved",
            task_id=str(task.id),
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            resolution=command.resolution.value,
            claim_status=claim.status.value,
        )
        return ResolveReviewResult(task_id=task.id, claim_id=claim.id, claim_status=claim.status)
