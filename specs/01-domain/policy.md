# Domain: Policy

Module: `ecet/domain/policy.py`. Tenant-owned rule describing when a diagnosis
(ICD-10) justifies a procedure/service. Read-only in v1 (seeded, not managed via API).

## 1. Value Objects

### `Icd10Code` (frozen)
- `code: str`, regex `^[A-TV-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$`.
- Normalised upper-case, whitespace stripped.

### `PolicyId`
- `NewType("PolicyId", UUID)`.

## 2. Entity: `Policy`

| Field            | Type              | Notes |
|------------------|-------------------|-------|
| id               | PolicyId          |       |
| tenant_id        | TenantId          |       |
| name             | str               | human label, e.g. "MRI lumbar spine" |
| version          | int               | ≥ 1; only highest active version used |
| covered_codes    | set[Icd10Code]    | diagnoses that satisfy necessity |
| excluded_codes   | set[Icd10Code]    | diagnoses that void coverage |
| criteria_text    | str               | free-text criteria fed to LLM prompt |
| required_evidence| list[str]         | e.g. `["conservative therapy ≥ 6 weeks", "imaging report"]` |
| active           | bool              |       |
| effective_from   | date              |       |
| effective_to     | date \| None      |       |

Invariants:
- `covered_codes ∩ excluded_codes == ∅`.
- `criteria_text` non-empty.
- `effective_to is None or effective_to >= effective_from`.

Method: `is_effective(on: date) -> bool`.

## 3. Repository Port (`ecet/domain/ports/policy_repository.py`)

```python
class PolicyRepository(Protocol):
    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]: ...
    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]: ...
```

`active_for_tenant` returns only `active=True`, effective on date, highest
version per `name`. Empty list → caller raises `NoPoliciesForTenant` ([ADR-005](../00-overview.md#4-adrs)).

## 4. Seed Data
- `infrastructure/postgres/seed/policies.sql` (see [postgres](../03-infrastructure/postgres.md#seed), [tenant seed](tenant.md#4-seed)) — 2 tenants, ~5 policies each,
  1 tenant with zero policies (to exercise failure path).

## 5. Tests
- ICD-10 regex accept/reject table.
- Overlap invariant.
- Effectiveness date logic incl. open-ended `effective_to`.
