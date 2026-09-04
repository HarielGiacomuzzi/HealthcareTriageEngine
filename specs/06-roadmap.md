# Roadmap — Development Phases

Each phase = one implementation plan (`docs/plans/`), one PR series, demo-able at end.
Context: [system overview](00-overview.md) · [spec index](00-overview.md#5-spec-index).
Order chosen so every phase leaves `docker compose up` working.

## Phase 0 — Skeleton & Tooling
Deliverable: empty but runnable project.
- pyproject (uv), ruff, mypy, import-linter contracts, pytest config, pre-commit, CI workflow.
- `src/ecet` package tree with empty modules per layout spec.
- `config.py` Settings + `.env.example`.
- Dockerfile + compose with postgres, rabbitmq, minio, api (`/healthz` only), worker (idle loop).
- Makefile targets.
Done when: `make up` green, `make test` runs zero-fail, CI passes.
Specs: [project-layout](05-platform/project-layout.md), [config](05-platform/config.md), [docker-compose](05-platform/docker-compose.md), [testing](05-platform/testing.md), [observability](05-platform/observability.md).

## Phase 1 — Domain Core
- `domain/`: Claim + state machine, Policy, Icd10Code, Evaluation, DeterministicResult, rules, triage, ReviewTask, Tenant, errors, repository ports.
- 100 % unit tested, mypy strict.
Done when: domain tests green; no I/O deps.
Specs: [claim](01-domain/claim.md), [policy](01-domain/policy.md), [evaluation](01-domain/evaluation.md), [tenant](01-domain/tenant.md).

## Phase 2 — Persistence
- Alembic migrations, ORM, mappers, repositories, UnitOfWork, seed data, `ecet seed`.
- Adapter tests with testcontainers.
Done when: seed loads 3 tenants; repository round-trips pass.
Specs: [postgres](03-infrastructure/postgres.md), [tenant seed](01-domain/tenant.md#4-seed).

## Phase 3 — Ingestion Path (API side)
- Ports + fakes; [UC-02](02-use-cases/UC-02-redact-pii.md) / [UC-03](02-use-cases/UC-03-attach-tenant-policies.md) / [UC-04](02-use-cases/UC-04-run-deterministic-checks.md) / [UC-05](02-use-cases/UC-05-enqueue-evaluation.md) then [UC-01](02-use-cases/UC-01-ingest-claim-document.md).
- Adapters: MinIO storage, pypdf extractor, presidio redactor, RabbitMQ publisher.
- FastAPI app: `/v1/events/s3`, `/v1/claims/ingest`, `/v1/claims/{id}`, `/readyz`, error mapping.
- MinIO webhook wiring in compose (`minio-setup`).
Done when: dropping fixture PDF in bucket → claim `QUEUED` in DB and message in queue; `tenant-empty` → 422.
Specs: [UC-01](02-use-cases/UC-01-ingest-claim-document.md)–[UC-05](02-use-cases/UC-05-enqueue-evaluation.md), [object-storage-minio](03-infrastructure/object-storage-minio.md), [pdf-text-extractor](03-infrastructure/pdf-text-extractor.md), [pii-redactor-presidio](03-infrastructure/pii-redactor-presidio.md), [queue-rabbitmq](03-infrastructure/queue-rabbitmq.md), [api](04-interfaces/api.md).

## Phase 4 — Evaluation Path (Worker side)
- LLM port, fake gateway, OpenAI-compatible gateway, prompt v1.
- [UC-06](02-use-cases/UC-06-evaluate-claim.md) / [UC-07](02-use-cases/UC-07-route-decision.md) / [UC-08](02-use-cases/UC-08-notify-client.md) / [UC-09a](02-use-cases/UC-09-human-review.md#uc-09a-requesthumanreview); webhook httpx client; mock-client service.
- Worker consumer, ack/nack classification, DLQ, graceful shutdown.
Done when: full loop `PDF drop → webhook received by mock-client` with fake LLM; low-confidence note → `REVIEW_PENDING`.
Specs: [UC-06](02-use-cases/UC-06-evaluate-claim.md)–[UC-09](02-use-cases/UC-09-human-review.md), [llm-gateway](03-infrastructure/llm-gateway.md), [webhook-client](03-infrastructure/webhook-client.md), [queue-rabbitmq](03-infrastructure/queue-rabbitmq.md), [worker](04-interfaces/worker.md).

## Phase 5 — Human Review & Ops Endpoints
- [UC-09b](02-use-cases/UC-09-human-review.md#uc-09b-listopenreviews) / [UC-09c](02-use-cases/UC-09-human-review.md#uc-09c-resolvereview), `/v1/reviews*`, `/v1/claims/{id}/retry-notify`, `ecet dlq-replay`.
- Real vendor run behind env flag; record one real evaluation output as fixture.
Done when: resolving review triggers webhook `decided_by=human`.
Specs: [UC-09](02-use-cases/UC-09-human-review.md), [api](04-interfaces/api.md).

## Phase 6 — Observability & Polish
- structlog + redaction guard, metrics, `claims_by_status` gauge, optional Prometheus/Grafana profile.
- E2E test suite; README rewrite: diagrams (workflow + integration), ADR section, quickstart verified < 2 min after image cached, cost-saving numbers from metrics.
Done when: README quickstart reproduces demo from clean clone.
Specs: [observability](05-platform/observability.md), [testing](05-platform/testing.md).

## Deferred (explicitly out of v1)
- OCR for scanned PDFs.
- Transactional outbox for publish (current: status stays `POLICIES_ATTACHED` on publish failure + manual retry).
- Per-tenant API keys / JWT.
- Policy management API (policies seeded only).
- JSON-mode fallback for servers without tool calling (only if a target server needs it).
- Distributed tracing.
