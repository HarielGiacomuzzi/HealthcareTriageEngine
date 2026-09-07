"""SQLAlchemy unit of work: one session, five repositories, one transaction."""

from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.ports.claim_repository import ClaimRepository
from ecet.domain.ports.icd10_repository import Icd10CodeRepository
from ecet.domain.ports.policy_repository import PolicyRepository
from ecet.domain.ports.review_task_repository import ReviewTaskRepository
from ecet.domain.ports.tenant_repository import TenantRepository
from ecet.infrastructure.postgres.repositories import (
    PostgresClaimRepository,
    PostgresIcd10CodeRepository,
    PostgresPolicyRepository,
    PostgresReviewTaskRepository,
    PostgresTenantRepository,
)


class SqlAlchemyUnitOfWork:
    """`AsyncSession` construction is lazy — no connection is taken until the first
    statement — so building the repositories in `__init__` costs nothing and keeps
    every attribute non-optional for the type checker.

    One instance per request/message. `__aexit__` rolls back and closes the session,
    but a fresh session is not created on re-entry, so reusing an instance across
    transactions is not supported."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session: AsyncSession = session_factory()
        self.tenants: TenantRepository = PostgresTenantRepository(self.session)
        self.policies: PolicyRepository = PostgresPolicyRepository(self.session)
        self.icd10_codes: Icd10CodeRepository = PostgresIcd10CodeRepository(self.session)
        # Kept concrete too: __aexit__ calls `clear_baseline`, which is not part of
        # the `ClaimRepository` protocol — the public attribute stays protocol-typed
        # so `SqlAlchemyUnitOfWork` structurally matches `UnitOfWork` (mypy checks
        # Protocol attributes invariantly).
        self._claims = PostgresClaimRepository(self.session)
        self.claims: ClaimRepository = self._claims
        self.review_tasks: ReviewTaskRepository = PostgresReviewTaskRepository(self.session)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # A rollback after a commit is a no-op, so "always roll back" is both the
        # safe default and the whole implementation.
        try:
            await self.session.rollback()
        finally:
            self._claims.clear_baseline()
            await self.session.close()

    async def commit(self) -> None:
        await self.session.commit()
