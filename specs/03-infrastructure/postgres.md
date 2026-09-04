# Infra: PostgreSQL Persistence

Module: `ecet/infrastructure/postgres/`. Implements [`ClaimRepository`](../01-domain/claim.md#4-repository-port-ecetdomainportsclaim_repositorypy), [`PolicyRepository`](../01-domain/policy.md#3-repository-port-ecetdomainportspolicy_repositorypy),
[`TenantRepository`](../01-domain/tenant.md#2-port), [`ReviewTaskRepository`](../01-domain/evaluation.md#4-reviewtask-human-in-the-loop), `UnitOfWork`.

## Stack
SQLAlchemy 2.x async + asyncpg; Alembic migrations in `migrations/`. Mapped classes in
`orm.py`; explicit `to_domain()` / `from_domain()` mappers — domain models never SQLAlchemy models.

## Schema

```sql
tenants(
  id text pk, name text not null, webhook_url text not null,
  webhook_secret text not null, active bool not null default true)

policies(
  id uuid pk, tenant_id text fk→tenants, name text not null, version int not null,
  covered_codes text[] not null, excluded_codes text[] not null,
  criteria_text text not null, required_evidence text[] not null,
  active bool not null, effective_from date not null, effective_to date,
  unique(tenant_id, name, version))
  index (tenant_id, active)

claims(
  id uuid pk, tenant_id text fk→tenants, bucket text, key text, etag text, size int,
  status text not null, redacted_text text, entity_counts jsonb, redactor text,
  policy_ids uuid[], deterministic jsonb, evaluation jsonb,
  failure_reason text, notification_attempts int default 0, last_notify_error text,
  created_at timestamptz, updated_at timestamptz,
  unique(bucket, key, etag))            -- ADR-006 idempotency
  index (tenant_id, status)

review_tasks(
  id uuid pk, claim_id uuid fk→claims unique, tenant_id text, reason text, status text,
  resolution text, reviewer text, notes text, created_at timestamptz, resolved_at timestamptz)
  index (tenant_id, status)
```

`deterministic` / `evaluation` stored as jsonb of the Pydantic dump — schema versioned via
`prompt_version` / model field; no separate tables until querying them is needed.

## UnitOfWork
```python
class SqlAlchemyUnitOfWork:
    async def __aenter__(self): session = factory(); expose .claims .policies .tenants .review_tasks
    async def __aexit__: rollback if not committed
    async def commit(self)
```
One session per request / per message.

Optimistic concurrency: `save` issues `UPDATE ... WHERE id=? AND updated_at=?`; 0 rows → `ConcurrentModification`.

## Seed
`seed/tenants.sql`, `seed/policies.sql` (content per [tenant seed](../01-domain/tenant.md#4-seed) / [policy seed](../01-domain/policy.md#4-seed-data)) applied by compose `postgres` init dir. Also `ecet seed` CLI for reruns.

## Tests
- testcontainers Postgres; migrations applied; repository round-trips per entity.
- `active_for_tenant` filters version/active/effective correctly.
- Unique (bucket,key,etag) violation → `find_by_source` path exercised.
