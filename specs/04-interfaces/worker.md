# Interface: Worker (RabbitMQ consumer)

Module: `ecet/interfaces/worker/`. Files: `main.py`, `container.py`, `handler.py`.

## Run
`ecet worker` (Typer CLI in `ecet/cli.py`; same image as [api](api.md), see [docker-compose](../05-platform/docker-compose.md)). Env identical to api minus S3 event token (see [config](../05-platform/config.md)).

## Behaviour
1. Build settings, engine, AMQP connection, `LLMGateway` via factory, `WebhookClient`.
2. Declare topology. Start consuming `claims.evaluate` with prefetch.
3. Per message: validate → [`EvaluateClaim`](../02-use-cases/UC-06-evaluate-claim.md)`.execute(msg)` inside fresh `UnitOfWork`.
   - Success → ack.
   - `LLMTransientError` / `QueuePublishError` / DB connection error → nack requeue.
   - Other → nack to DLQ; log with claim_id.
4. Structured log per message: `claim_id`, `tenant_id`, `outcome`, `duration_ms`, `retry_count`.
5. SIGTERM → graceful stop (see [queue spec](../03-infrastructure/queue-rabbitmq.md#consumer)). Exit code 0.

## Health
- No HTTP server. Liveness via `/metrics` on port 9100 (prometheus_client `start_http_server`) —
  also acts as readiness for compose `healthcheck`.

## Scaling
`docker compose up --scale worker=3` works out of the box (competing consumers, prefetch).

## Tests
- Handler unit test with fake use case: exception classes → correct ack/nack decision (pure function `classify(exc) -> AckAction`).
- Integration (slow): real RabbitMQ + Postgres containers, fake LLM, publish message → claim reaches APPROVED_AUTO and mock webhook called.
