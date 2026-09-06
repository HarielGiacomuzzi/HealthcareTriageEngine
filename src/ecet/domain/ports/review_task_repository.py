"""Review task persistence contract."""

from typing import Protocol, runtime_checkable
from uuid import UUID

from ecet.domain.evaluation import ReviewTask
from ecet.domain.ids import ClaimId, TenantId


@runtime_checkable
class ReviewTaskRepository(Protocol):
    async def add(self, task: ReviewTask) -> None: ...

    async def get(self, task_id: UUID) -> ReviewTask:
        """Raises `ReviewTaskNotFound`."""
        ...

    async def find_open_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        """UC-09a idempotency: return the claim's OPEN task instead of adding a duplicate.
        `review_tasks.claim_id` is unique in the postgres schema."""
        ...

    async def list_open(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTask]:
        """Tenant-scoped; there is no cross-tenant listing."""
        ...

    async def save(self, task: ReviewTask) -> None: ...
