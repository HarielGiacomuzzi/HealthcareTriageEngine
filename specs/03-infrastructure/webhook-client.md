# Infra: Webhook Client & Mock Client Receiver

## Adapter `ecet/infrastructure/webhook/httpx_client.py` — implements [`WebhookClient`](../02-use-cases/UC-08-notify-client.md)
- `httpx.AsyncClient(timeout=10)`.
- Body: `payload.model_dump_json()`; headers:
  - `Content-Type: application/json`
  - `X-ECET-Delivery: <uuid4>` (same across retries)
  - `X-ECET-Timestamp: <unix seconds>`
  - `X-ECET-Signature: sha256=<hex hmac_sha256(secret, f"{timestamp}.{body}")>`
- Retry policy: attempts 3, backoff 1/4/16 s, retry on `httpx.TransportError`, 408, 429, 5xx.
  2xx → success. Other 4xx → `WebhookPermanentError`. Exhausted → `WebhookTransientError`.
- Never log secret; log status code + attempt.

## Mock Client (`services/mock-client/`, separate tiny FastAPI app in compose)
- `POST /webhooks/ecet`: verifies signature with shared secret, stores payload in memory list,
  returns 200. `GET /received` lists them. Env `FAIL_FIRST_N=1` to demo retry.
- Exists so `docker compose up` demonstrates full loop end-to-end without external system.

## Tests
- `respx` mocked 500,500,200 → 3 attempts, success.
- 400 → 1 attempt, permanent error.
- Signature verified by mock client test using shared secret.
