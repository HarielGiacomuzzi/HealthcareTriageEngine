"""Postgres implementations of the domain repository ports.

Each repository is thin: build a statement, run it, hand the row to a mapper. All
five share one `AsyncSession`, owned by the unit of work.
"""

from collections.abc import Iterable
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import (
    ClaimNotFound,
    ConcurrentModification,
    ReviewTaskNotFound,
    TenantNotFound,
)
from ecet.domain.evaluation import ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import (
    claim_from_row,
    claim_to_row_values,
    policy_from_row,
    review_task_from_row,
    review_task_to_row_values,
    tenant_from_row,
)
from ecet.infrastructure.postgres.orm import (
    ClaimRow,
    Icd10CodeRow,
    PolicyRow,
    ReviewTaskRow,
    TenantRow,
)


class PostgresTenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, tenant_id: TenantId) -> Tenant:
        row = await self._session.get(TenantRow, str(tenant_id))
        if row is None or not row.active:
            raise TenantNotFound(str(tenant_id))
        return tenant_from_row(row)


class PostgresPolicyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]:
        """Active, effective on `on`, highest version per name.

        `DISTINCT ON (name)` with `ORDER BY name, version DESC` is Postgres' one-pass
        way to say "highest version per name"; an empty list is the caller's cue to
        raise `NoPoliciesForTenant` (ADR-005).
        """
        statement = (
            select(PolicyRow)
            .where(
                PolicyRow.tenant_id == str(tenant_id),
                PolicyRow.active.is_(True),
                PolicyRow.effective_from <= on,
                or_(PolicyRow.effective_to.is_(None), PolicyRow.effective_to >= on),
            )
            .order_by(PolicyRow.name, PolicyRow.version.desc())
            .distinct(PolicyRow.name)
        )
        rows = (await self._session.scalars(statement)).all()
        return [policy_from_row(row) for row in rows]

    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]:
        wanted = list(ids)
        if not wanted:
            return []
        statement = select(PolicyRow).where(PolicyRow.id.in_(wanted)).order_by(PolicyRow.name)
        rows = (await self._session.scalars(statement)).all()
        return [policy_from_row(row) for row in rows]


class PostgresIcd10CodeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def known_codes(self) -> frozenset[Icd10Code]:
        # ponytail: one full-table read per call; cache it in the caller if a hot path
        # ever calls this per claim.
        codes = (await self._session.scalars(select(Icd10CodeRow.code))).all()
        return frozenset(Icd10Code(code=code) for code in codes)


class PostgresClaimRepository:
    """`save` is optimistic on `updated_at` (see the postgres spec).

    `Claim` carries no version column, and `transition()` overwrites `updated_at` in
    place, so the pre-mutation value has to be remembered here — one entry per claim
    this repository has seen. The repository lives exactly as long as its unit of
    work, so the map cannot grow unbounded.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._baseline: dict[UUID, datetime] = {}

    def _track(self, claim: Claim) -> Claim:
        self._baseline[claim.id] = claim.updated_at
        return claim

    async def add(self, claim: Claim) -> None:
        await self._session.execute(insert(ClaimRow).values(**claim_to_row_values(claim)))
        self._track(claim)

    async def get(self, claim_id: ClaimId) -> Claim:
        # populate_existing: a core UPDATE does not refresh the identity map, so a
        # second read in the same session would otherwise hand back the stale row.
        row = await self._session.get(ClaimRow, claim_id, populate_existing=True)
        if row is None:
            raise ClaimNotFound(str(claim_id))
        return self._track(claim_from_row(row))

    async def find_by_source(self, bucket: str, key: str, etag: str) -> Claim | None:
        statement = (
            select(ClaimRow)
            .where(ClaimRow.bucket == bucket, ClaimRow.key == key, ClaimRow.etag == etag)
            .execution_options(populate_existing=True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return None if row is None else self._track(claim_from_row(row))

    async def save(self, claim: Claim) -> None:
        baseline = self._baseline.get(claim.id)
        if baseline is None:
            raise ConcurrentModification(
                f"claim {claim.id} was not loaded by this unit of work; re-read it first"
            )
        result = await self._session.execute(
            update(ClaimRow)
            .where(ClaimRow.id == claim.id, ClaimRow.updated_at == baseline)
            .values(**claim_to_row_values(claim))
            .execution_options(synchronize_session=False)
        )
        if result.rowcount == 0:  # type: ignore[attr-defined]  # a core UPDATE is a CursorResult
            raise ConcurrentModification(f"claim {claim.id} changed since it was read")
        self._track(claim)

    async def list_by_status(self, status: ClaimStatus, *, limit: int = 50) -> list[Claim]:
        statement = (
            select(ClaimRow)
            .where(ClaimRow.status == status.value)
            .order_by(ClaimRow.created_at)
            .limit(limit)
            .execution_options(populate_existing=True)
        )
        rows = (await self._session.scalars(statement)).all()
        return [self._track(claim_from_row(row)) for row in rows]


class PostgresReviewTaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, task: ReviewTask) -> None:
        await self._session.execute(insert(ReviewTaskRow).values(**review_task_to_row_values(task)))

    async def get(self, task_id: UUID) -> ReviewTask:
        row = await self._session.get(ReviewTaskRow, task_id, populate_existing=True)
        if row is None:
            raise ReviewTaskNotFound(str(task_id))
        return review_task_from_row(row)

    async def find_open_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        """UC-09a idempotency. `review_tasks.claim_id` is unique, so this is at most one row."""
        statement = (
            select(ReviewTaskRow)
            .where(
                ReviewTaskRow.claim_id == claim_id,
                ReviewTaskRow.status == ReviewStatus.OPEN.value,
            )
            .execution_options(populate_existing=True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return None if row is None else review_task_from_row(row)

    async def list_open(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTask]:
        statement = (
            select(ReviewTaskRow)
            .where(
                ReviewTaskRow.tenant_id == str(tenant_id),
                ReviewTaskRow.status == ReviewStatus.OPEN.value,
            )
            .order_by(ReviewTaskRow.created_at)
            .limit(limit)
            .execution_options(populate_existing=True)
        )
        rows = (await self._session.scalars(statement)).all()
        return [review_task_from_row(row) for row in rows]

    async def save(self, task: ReviewTask) -> None:
        result = await self._session.execute(
            update(ReviewTaskRow)
            .where(ReviewTaskRow.id == task.id)
            .values(**review_task_to_row_values(task))
            .execution_options(synchronize_session=False)
        )
        if result.rowcount == 0:  # type: ignore[attr-defined]  # a core UPDATE is a CursorResult
            raise ReviewTaskNotFound(str(task.id))
