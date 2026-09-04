# UC-03 AttachTenantPolicies

## Input / Output
`(tenant_id: TenantId, on: date)` → `list[Policy]` (non-empty).

## Steps
1. `policies.active_for_tenant(tenant_id, on=clock.now().date())`.
2. Empty → raise `NoPoliciesForTenant(tenant_id)` ([ADR-005](../00-overview.md#4-adrs)). Caller sets claim `NO_POLICIES`.
3. Return list. Caller stores `policy_ids` on claim; full policies passed along to [UC-04](UC-04-run-deterministic-checks.md) and [UC-05](UC-05-enqueue-evaluation.md) (message carries policy snapshot so worker evaluates against same versions even if policies change mid-flight).

## Tests
- Two active + one inactive + one expired → returns two.
- Zero → raises.
