"""Tenant read contract."""

from typing import Protocol, runtime_checkable

from ecet.domain.ids import TenantId
from ecet.domain.tenant import Tenant


@runtime_checkable
class TenantRepository(Protocol):
    async def get(self, tenant_id: TenantId) -> Tenant:
        """Raises `TenantNotFound` for an unknown or inactive tenant."""
        ...
