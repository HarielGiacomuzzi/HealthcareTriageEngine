"""UC-09a RequestHumanReview.

Idempotent by claim: `review_tasks.claim_id` is unique in the schema, so a second
request for a claim that already has an open task returns that task rather than
racing the constraint. The caller — not this use case — transitions the claim to
`REVIEW_PENDING`, because only the caller knows whether the claim is also being
saved in the same unit of work.

UC-09b (list) and UC-09c (resolve) arrive in Phase 5 and will share this module.
"""

from uuid import uuid4

from ecet.application.ports.clock import Clock
from ecet.domain.claim import Claim
from ecet.domain.evaluation import ReviewReason, ReviewTask
from ecet.domain.ports.review_task_repository import ReviewTaskRepository


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
