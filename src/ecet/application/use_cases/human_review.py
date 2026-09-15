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
"""

from collections.abc import Callable
from datetime import datetime
from uuid import UUID, uuid4

import structlog
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import InvalidTransition, ReviewTaskNotFound
from ecet.domain.evaluation import (
    DeterministicResult,
    Evaluation,
    HumanResolution,
    ReviewReason,
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
        async with self._uow_factory() as uow:
            task = await uow.review_tasks.get(command.task_id)
            if task.tenant_id != command.tenant_id:
                # Same answer as "no such task": anything else confirms it exists.
                raise ReviewTaskNotFound(str(command.task_id))

            now = self._clock.now()
            # Raises `ReviewAlreadyResolved` before anything has been sent.
            task.resolve(
                resolution=command.resolution,
                reviewer=command.reviewer,
                notes=command.notes,
                now=now,
            )
            claim = await uow.claims.get(task.claim_id)
            if not claim.status.can_transition_to(ClaimStatus.REVIEW_RESOLVED):
                # Checked before the webhook: a delivery the transition then refused
                # would reach the client and be rolled back here.
                raise InvalidTransition(
                    f"claim {claim.id} is {claim.status}, which a review cannot resolve"
                )
            await uow.review_tasks.save(task)

            reason = await NotifyClient(uow.tenants, self._webhook, self._clock).attempt(
                claim, outcome=command.resolution, confidence=1.0, decided_by="human"
            )
            if reason is None:
                claim.transition(ClaimStatus.REVIEW_RESOLVED, now=now)
            else:
                claim.transition(ClaimStatus.NOTIFY_FAILED, reason=reason, now=now)
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
