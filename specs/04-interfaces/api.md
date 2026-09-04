# Interface: HTTP API (FastAPI 0.141)

Module: `ecet/interfaces/api/`. Files: `app.py` (factory), `container.py` (wiring),
`routes/events.py`, `routes/claims.py`, `routes/reviews.py`, `routes/health.py`, `errors.py`.

## Endpoints

| Method | Path                                | Purpose | Auth |
|--------|-------------------------------------|---------|------|
| POST   | `/v1/events/s3`                     | S3 notification → [UC-01](../02-use-cases/UC-01-ingest-claim-document.md) | bearer `ECET_S3_EVENT_TOKEN` |
| POST   | `/v1/claims/ingest`                 | Manual ingest `{bucket,key}` (demo/dev; API fetches etag/size via HEAD) | api key |
| GET    | `/v1/claims/{claim_id}`             | [Claim](../01-domain/claim.md) view (status, deterministic, evaluation, no secrets) | api key + tenant header |
| POST   | `/v1/claims/{claim_id}/retry-notify`| Re-run [UC-08](../02-use-cases/UC-08-notify-client.md) for `NOTIFY_FAILED` | api key |
| GET    | `/v1/reviews?limit=`                | [UC-09b](../02-use-cases/UC-09-human-review.md) open review tasks | api key + tenant header |
| POST   | `/v1/reviews/{task_id}/resolve`     | [UC-09c](../02-use-cases/UC-09-human-review.md) | api key + tenant header |
| GET    | `/healthz`                          | liveness | none |
| GET    | `/readyz`                           | DB + AMQP + presidio loaded | none |
| GET    | `/metrics`                          | Prometheus | none |

Auth v1: static `X-API-Key` = [`ECET_API_KEY`](../05-platform/config.md); `X-Tenant-Id` header selects tenant
(portfolio simplification; note in README that prod = per-tenant keys / JWT).

## S3 event handling
- Parse `S3EventEnvelope` (list of records, format per [object-storage-minio](../03-infrastructure/object-storage-minio.md#event-delivery)). Each record → `IngestCommand`.
- Multiple records → process sequentially; response 207-style body listing per-record result.
  Single record (MinIO default) → 200/4xx per error table in UC-01.
- Non-`.pdf` keys or non-`ObjectCreated:*` events → 200, ignored, logged.
- Handler timeout budget: whole request must finish < 30 s (MinIO webhook timeout); expected < 1.5 s.

## Error mapping (`errors.py`)
[`DomainError`](../01-domain/claim.md#3-domain-errors-ecetdomainerrorspy) subclasses → `ProblemDetails` JSON (`type`, `title`, `status`, `detail`, `claim_id?`).
Unhandled → 500, logged with `claim_id` if known.

## Lifespan
- Startup: settings, engine, migrations check (fail fast if pending), presidio engine warm-up, AMQP connection + topology declare.
- Shutdown: close AMQP, dispose engine.

## Container (`container.py`)
Plain functions building the graph; no DI framework. `build_ingest_use_case(settings, deps)` etc.
FastAPI `Depends` used only at route edge to fetch the pre-built use case from `app.state`.

## Tests
- `httpx.AsyncClient(app=...)` with fake ports: each endpoint status + body contract.
- Auth missing → 401. Wrong tenant header on review resolve → 404.
