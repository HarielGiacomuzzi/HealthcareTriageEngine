"""Postgres implementations of the domain repository ports.

Each repository is thin: build a statement, run it, hand the row to a mapper. All
five share one `AsyncSession`, owned by the unit of work.
"""

from collections.abc import Iterable
from datetime import date

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ecet.domain.errors import TenantNotFound
from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import policy_from_row, tenant_from_row
from ecet.infrastructure.postgres.orm import Icd10CodeRow, PolicyRow, TenantRow


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
