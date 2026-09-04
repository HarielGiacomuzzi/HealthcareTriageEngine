# UC-05 EnqueueEvaluation

## Message Contract (`application/messages.py`)

```python
class PolicySnapshot(BaseModel):
    id: PolicyId
    name: str
    version: int
    covered_codes: list[str]
    excluded_codes: list[str]
    criteria_text: str
    required_evidence: list[str]

class EvaluationMessage(BaseModel):
    schema_version: Literal[1] = 1
    message_id: UUID                 # uuid4, for dedupe
    claim_id: ClaimId
    tenant_id: TenantId
    redacted_text: str
    entity_counts: dict[str, int]
    policies: list[PolicySnapshot]
    deterministic_verdict: Literal["PASS", "UNCERTAIN"]
    found_codes: list[str]
    enqueued_at: datetime
```

Never contains raw text, webhook URL, or secrets.

## Steps
1. Build message from claim + policies.
2. `queue.publish(msg)` — persistent delivery, `content_type=application/json`,
   `message_id`, header `x-tenant-id`, `x-schema-version`.
3. Return `message_id`.

Routing (see [queue-rabbitmq](../03-infrastructure/queue-rabbitmq.md#topology-declared-idempotently-on-startup-by-api-and-worker)): exchange `ecet` (topic), routing key `claims.evaluate`, queue `claims.evaluate`,
DLX `ecet.dlx`, DLQ `claims.evaluate.dlq`. Declared by both api and worker (idempotent).

## Tests
- Serialised message validates against schema; no key named `raw_text`, `webhook_secret`.
- Publish failure propagates (`QueuePublishError`), claim stays `POLICIES_ATTACHED`.
