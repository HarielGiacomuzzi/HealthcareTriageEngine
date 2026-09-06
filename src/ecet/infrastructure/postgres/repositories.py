"""Postgres implementations of the domain repository ports.

Each repository is thin: build a statement, run it, hand the row to a mapper. All
five share one `AsyncSession`, owned by the unit of work.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from ecet.domain.errors import TenantNotFound
from ecet.domain.ids import TenantId
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import tenant_from_row
from ecet.infrastructure.postgres.orm import TenantRow


class PostgresTenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, tenant_id: TenantId) -> Tenant:
        row = await self._session.get(TenantRow, str(tenant_id))
        if row is None or not row.active:
            raise TenantNotFound(str(tenant_id))
        return tenant_from_row(row)
