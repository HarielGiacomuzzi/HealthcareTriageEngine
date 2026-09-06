"""Transaction boundary. One unit of work per HTTP request / per queue message.

Lives in `application/ports` because use cases depend on it; the SQLAlchemy
implementation lives in `infrastructure/postgres`.
"""

from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from ecet.domain.ports.claim_repository import ClaimRepository
from ecet.domain.ports.icd10_repository import Icd10CodeRepository
from ecet.domain.ports.policy_repository import PolicyRepository
from ecet.domain.ports.review_task_repository import ReviewTaskRepository
from ecet.domain.ports.tenant_repository import TenantRepository


@runtime_checkable
class UnitOfWork(Protocol):
    claims: ClaimRepository
    policies: PolicyRepository
    tenants: TenantRepository
    review_tasks: ReviewTaskRepository
    icd10_codes: Icd10CodeRepository

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Rolls back anything not committed, then closes the session."""
        ...

    async def commit(self) -> None: ...
