# Infra: RabbitMQ Queue

Module: `ecet/infrastructure/queue/rabbitmq.py`. Implements `EvaluationQueue` (publisher)
and provides `consume(handler)` loop used by the [worker interface](../04-interfaces/worker.md).

## Topology (declared idempotently on startup by api and worker)
| Object                  | Type   | Args |
|-------------------------|--------|------|
| exchange `ecet`         | topic, durable | |
| exchange `ecet.dlx`     | topic, durable | |
| queue `claims.evaluate` | durable, quorum | `x-dead-letter-exchange=ecet.dlx`, `x-delivery-limit=5` |
| queue `claims.evaluate.dlq` | durable | bound to `ecet.dlx` with `claims.evaluate` |

Binding: `ecet` → `claims.evaluate` key `claims.evaluate`.

## Publisher
- aio-pika robust connection, publisher confirms on. `publish(msg)`: JSON body,
  `delivery_mode=PERSISTENT`, `message_id`, headers `x-tenant-id`, `x-schema-version`.
- Confirm not received / channel error → `QueuePublishError`.

## Consumer
- `prefetch_count = ECET_WORKER_PREFETCH` (default 4).
- Handler contract: `async def handle(msg: EvaluationMessage) -> None` ([message schema](../02-use-cases/UC-05-enqueue-evaluation.md#message-contract-applicationmessagespy)).
  - returns → `ack`.
  - raises `TransientError` → `nack(requeue=True)`; quorum `x-delivery-limit` moves to DLQ after 5.
  - raises anything else → `nack(requeue=False)` → DLQ.
- Body fails `EvaluationMessage.model_validate` → DLQ immediately.
- Graceful shutdown on SIGTERM: stop consuming, await in-flight, close.

## Ops
- Management UI exposed on 15672 in compose (portfolio demo value).
- CLI `ecet dlq-replay --limit N` republishes DLQ messages ([roadmap phase 5](../06-roadmap.md#phase-5--human-review--ops-endpoints)).

## Tests
- testcontainers RabbitMQ: publish → consume round-trip; handler raise → message on DLQ.
