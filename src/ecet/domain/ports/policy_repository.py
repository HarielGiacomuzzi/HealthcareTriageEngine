"""Policy read contract. Policies are seeded, never written through the app."""

from collections.abc import Iterable
from datetime import date
from typing import Protocol, runtime_checkable

from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy


@runtime_checkable
class PolicyRepository(Protocol):
    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]:
        """Active, effective on `on`, highest version per name. Empty list is the
        caller's cue to raise `NoPoliciesForTenant` (ADR-005)."""
        ...

    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]: ...
