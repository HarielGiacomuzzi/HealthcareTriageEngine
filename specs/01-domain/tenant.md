# Domain: Tenant

Module: `ecet/domain/tenant.py`. Minimal. Exists to own webhook config and
isolate data ([README goal](../../README.md): "isolate tenant data").

## 1. Value Objects / Entity

### `TenantId`
- `NewType("TenantId", str)`, regex `^[a-z0-9][a-z0-9-]{1,62}$`. Matches S3 key prefix segment.

### `Tenant`
| Field           | Type         | Notes |
|-----------------|--------------|-------|
| id              | TenantId     |       |
| name            | str          |       |
| webhook_url     | HttpUrl      | client's callback |
| webhook_secret  | SecretStr    | HMAC key; never logged/serialised |
| active          | bool         |       |

## 2. Port

```python
class TenantRepository(Protocol):
    async def get(self, tenant_id: TenantId) -> Tenant: ...   # raises TenantNotFound
```

Unknown or inactive tenant at ingestion → `TenantNotFound` → HTTP 404, claim not created.

## 3. Isolation Rules
- Every repository query filters by `tenant_id`; no cross-tenant listing endpoint.
- Queue messages carry `tenant_id`; worker re-loads policies scoped by it (defence in depth).
- Logs include `tenant_id` as structured field, never `webhook_secret`.

## 4. Seed
- `tenant-a` (5 policies, webhook → local [`mock-client`](../03-infrastructure/webhook-client.md#mock-client-servicesmock-client-separate-tiny-fastapi-app-in-compose) container).
- `tenant-b` (5 policies).
- `tenant-empty` (0 policies) → ingestion fails, proves [ADR-005](../00-overview.md#4-adrs).
