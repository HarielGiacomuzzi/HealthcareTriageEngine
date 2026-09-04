# UC-08 NotifyClient

Push outcome to tenant system via webhook.

## Payload (`application/notifications.py`)

```python
class ClientNotification(BaseModel):
    event: Literal["claim.triaged"] = "claim.triaged"
    claim_id: ClaimId
    tenant_id: TenantId
    source_key: str
    outcome: Literal["MEETS_NECESSITY", "DOES_NOT_MEET", "INSUFFICIENT_EVIDENCE"]
    confidence: float
    decided_by: Literal["auto", "human"]
    matched_policy_id: PolicyId | None
    cited_codes: list[str]
    rationale: str
    evidence_missing: list[str]
    decided_at: datetime
```

No redacted text in payload (client already owns document).

## Steps
1. `tenants.get(tenant_id)` for `webhook_url` + `webhook_secret`.
2. `webhook.deliver(tenant, payload)`.
   Adapter signs: `X-ECET-Signature: sha256=<hmac(secret, body)>`, `X-ECET-Timestamp`, `X-ECET-Delivery: <uuid>`.
   Retries: 3 attempts, backoff 1 s / 4 s / 16 s, on 5xx / timeout / connection error. 4xx = permanent failure, no retry.
3. Record delivery attempt outcome on claim (`notification_attempts: int`, `last_notify_error: str | None` — add to `Claim`).

## Tests
- Fake webhook client records payload; payload validates; no `redacted_text` key.
- Permanent 4xx → single attempt, raises `WebhookPermanentError`.
