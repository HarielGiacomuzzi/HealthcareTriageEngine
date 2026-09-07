"""UC-03 AttachTenantPolicies (ADR-005).

No active policy means no LLM call and no claim: the caller sets `NO_POLICIES` and
the request answers 422. Silently evaluating against an empty policy set would let
the model invent coverage rules.
"""

from ecet.application.ports.clock import Clock
from ecet.domain.errors import NoPoliciesForTenant
from ecet.domain.ids import TenantId
from ecet.domain.policy import Policy
from ecet.domain.ports.policy_repository import PolicyRepository


class AttachTenantPolicies:
    def __init__(self, policies: PolicyRepository, clock: Clock) -> None:
        self._policies = policies
        self._clock = clock

    async def execute(self, tenant_id: TenantId) -> list[Policy]:
        """The full `Policy` objects, not just ids: UC-04 needs the code sets and UC-05
        snapshots them into the queue message so the worker evaluates the same versions
        even if a policy changes mid-flight."""
        found = await self._policies.active_for_tenant(tenant_id, on=self._clock.now().date())
        if not found:
            raise NoPoliciesForTenant(str(tenant_id))
        return found
