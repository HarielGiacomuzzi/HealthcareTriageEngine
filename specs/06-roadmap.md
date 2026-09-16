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
- Carried from Phase 0: `tests/unit/domain/` tree, `tests/fakes.py` (repository fakes), coverage `exclude_also` for `...` Protocol bodies.
Done when: domain tests green; no I/O deps.
Specs: [claim](01-domain/claim.md), [policy](01-domain/policy.md), [evaluation](01-domain/evaluation.md), [tenant](01-domain/tenant.md).

## Phase 2 — Persistence
Plan: [`docs/plans/2026-09-06-phase-2-persistence.md`](../docs/plans/2026-09-06-phase-2-persistence.md).
- Alembic migrations, ORM rows, pure mappers, the four repositories, `UnitOfWork` (port + SQLAlchemy adapter), dev seed data, `ecet seed`.
- `icd10_codes` table + `Icd10CodeRepository` port: the catalogue Phase 4 validates extracted codes against.
- Adapter tests with testcontainers.
- Carried from Phase 0: testcontainers dependency and the CI `slow` job (`pytest -m slow`) that runs it.
- Carried from Phase 1: optimistic saves need the pre-mutation `updated_at` that [postgres](03-infrastructure/postgres.md)'s `UPDATE … WHERE updated_at=?` compares against — `Claim.transition()` overwrites it in place and `Claim` has no `version` field, so the adapter carries it (a per-repository baseline map); deliberately not solved in the domain, because the schema has no version column. `Tenant.webhook_secret` must be persisted via `get_secret_value()` — the JSON dump masks it. `ReviewTaskRepository.find_open_by_claim` maps to the unique `review_tasks.claim_id`. `Claim` rejects a `tenant_id` that disagrees with its object key, so mappers must build both from the same row.
Done when: `make migrate && make seed` loads 4 tenants (one inactive) and 15 policies; rerunning the seed changes nothing; repository round-trips pass.
Specs: [postgres](03-infrastructure/postgres.md), [tenant seed](01-domain/tenant.md#4-seed).

## Phase 3 — Ingestion Path (API side)
Plan: [`docs/plans/2026-09-06-phase-3-ingestion-path.md`](../docs/plans/2026-09-06-phase-3-ingestion-path.md).
- Ports + fakes; [UC-02](02-use-cases/UC-02-redact-pii.md) / [UC-03](02-use-cases/UC-03-attach-tenant-policies.md) / [UC-04](02-use-cases/UC-04-run-deterministic-checks.md) / [UC-05](02-use-cases/UC-05-enqueue-evaluation.md) / [UC-09a](02-use-cases/UC-09-human-review.md#uc-09a-requesthumanreview) then [UC-01](02-use-cases/UC-01-ingest-claim-document.md). UC-09a lands here, not in Phase 4: UC-01's deterministic-`REJECT` branch opens the review task.
- Adapters: MinIO storage, pypdf extractor, presidio redactor, RabbitMQ publisher.
- FastAPI app: `/v1/events/s3`, `/v1/claims/ingest`, `/v1/claims/{id}`, `/readyz`, error mapping.
- MinIO webhook wiring in compose (`minio-setup`).
- Carried from Phase 0: spaCy `en_core_web_lg` download + `SPACY_MODEL` build arg in the Dockerfile; `minio-setup` compose service; api container healthcheck switched from `/healthz` to `/readyz`; `assert_no_pii` test helper + `tests/fixtures/` (notes, pdfs, s3_events) from the [testing spec](05-platform/testing.md); adapters must log through structlog only (see Phase 6 carry-over #12).
- Carried from Phase 1: the error mapping must not echo `InvalidObjectKey`'s message to clients verbatim — it embeds the client-supplied filename.
- Carried from Phase 2: api startup runs `alembic upgrade head` + the seed when `ECET_AUTO_MIGRATE=true` ([docker-compose](05-platform/docker-compose.md)) — Phase 2 shipped the migration, the seed and the `alembic.ini` + `migrations/` copy inside the image, but wired neither into the api process, which had no database yet. The compose `postgres` init-dir seeding path is gone: it runs before the tables exist. `ClaimRepository.get`, `ClaimRepository.list_by_status` and `PolicyRepository.get_many` take no tenant parameter, so tenant isolation is entirely the use case's responsibility — make that an explicit rule here before the first route is written. `PostgresClaimRepository.save` refuses a claim the repository never read (`ConcurrentModification`, not a silent overwrite) — `add()` also registers the baseline, so a use case that adds then saves inside one unit of work is fine, but one that constructs a claim and saves it without `add`/`get` is not.
Done when: dropping fixture PDF in bucket → claim `QUEUED` in DB and message in queue; `tenant-empty` → 422.
Specs: [UC-01](02-use-cases/UC-01-ingest-claim-document.md)–[UC-05](02-use-cases/UC-05-enqueue-evaluation.md), [object-storage-minio](03-infrastructure/object-storage-minio.md), [pdf-text-extractor](03-infrastructure/pdf-text-extractor.md), [pii-redactor-presidio](03-infrastructure/pii-redactor-presidio.md), [queue-rabbitmq](03-infrastructure/queue-rabbitmq.md), [api](04-interfaces/api.md).

## Phase 4 — Evaluation Path (Worker side)
Plan: [`docs/plans/2026-09-07-phase-4-evaluation-path.md`](../docs/plans/2026-09-07-phase-4-evaluation-path.md).
- LLM port, fake gateway, OpenAI-compatible gateway, prompt v1.
- [UC-06](02-use-cases/UC-06-evaluate-claim.md) / [UC-07](02-use-cases/UC-07-route-decision.md) / [UC-08](02-use-cases/UC-08-notify-client.md), reusing UC-09a from Phase 3; webhook httpx client; mock-client service.
- Worker consumer, ack/nack classification, DLQ, graceful shutdown.
- Carried from Phase 0: `mock-client` compose service (`services/mock-client/`, own Dockerfile).
- Carried from Phase 3: the `claims.evaluate` topology, `declare_topology()` and the queue constants live in `infrastructure/queue/rabbitmq.py` — the consumer imports them rather than redeclaring. `RequestHumanReview` (UC-09a) already exists and is built per unit of work, not injected. `EvaluationMessage` is the frozen wire contract: a change to it is a `schema_version` bump, not an edit.
- Carried from Phase 1: ICD-10 extraction in `domain/rules.py` is pattern-only and false-positives on clinical prose ("Vitamin B12", the "T12" vertebra); validate extracted codes against the catalogue Phase 2 seeded, read through `Icd10CodeRepository.known_codes()`. `T12` is itself a real code, so that one false positive survives — a note mentioning the T12 vertebra still looks like a diagnosis.
- Carried from Phase 2: the seeded tenants' webhook URLs already point at `http://mock-client:8081/hooks/...` ([tenant seed](01-domain/tenant.md#4-seed)); this is the phase that makes that hostname resolve.
- Carried from Phase 3: the "MinIO retries non-2xx forever" premise is wrong (no `queue_dir` in compose) — a 400 loses the event with no claim row and no operator signal; the code is right, the recoverability story needs revisiting. `QUEUE_ARGUMENTS` pins `x-queue-type: quorum` — a stale classic `claims.evaluate` in an existing `rabbitdata` volume makes `start()` fail `PRECONDITION_FAILED` and the API will not boot, and the Phase 4 consumer imports the same `declare_topology` and inherits it; a `make clean` note covers it. `SqlAlchemyUnitOfWork` is single-use — the worker needs the same per-message factory shape the API uses, not a shared instance. `POST /v1/claims/ingest` and `POST /v1/events/s3` never check the bucket — add a `settings.s3_bucket` guard so neither path HEADs or GETs a bucket the deployment does not own. `RabbitMqEvaluationQueue.is_healthy` returns True during an aio-pika robust reconnect, so `/readyz` can call the queue healthy while publishes fail; `AbstractRobustConnection.reconnecting` is the missing check.
Done when: full loop `PDF drop → webhook received by mock-client` with fake LLM; low-confidence note → `REVIEW_PENDING`.
Specs: [UC-06](02-use-cases/UC-06-evaluate-claim.md)–[UC-09](02-use-cases/UC-09-human-review.md), [llm-gateway](03-infrastructure/llm-gateway.md), [webhook-client](03-infrastructure/webhook-client.md), [queue-rabbitmq](03-infrastructure/queue-rabbitmq.md), [worker](04-interfaces/worker.md).

## Phase 5 — Human Review & Ops Endpoints
Plan: [`docs/plans/2026-09-15-phase-5-human-review-ops.md`](../docs/plans/2026-09-15-phase-5-human-review-ops.md).
- [UC-09b](02-use-cases/UC-09-human-review.md#uc-09b-listopenreviews) / [UC-09c](02-use-cases/UC-09-human-review.md#uc-09c-resolvereview), `/v1/reviews*`, `/v1/claims/{id}/retry-notify`, `ecet dlq-replay`.
- The OpenAI-compatible adapter stays verified against `httpx.MockTransport` only; no live vendor run, by the user's decision.
- Carried from Phase 3: a publish failure leaves the claim `POLICIES_ATTACHED` with no retry path yet, and `ClaimView` omits the redacted text that UC-09b needs.
- Carried from Phase 4: `POST /v1/claims/{id}/retry-notify` is the only exit from `NOTIFY_FAILED`, and `NOTIFY_FAILED -> APPROVED_AUTO | REVIEW_RESOLVED` is already in the state machine. `NotifyClient.execute(claim, outcome=..., confidence=1.0, decided_by="human")` is what UC-09c calls — it exists and is tested. Requeue has no delay (`RabbitMqConsumer._on_message` nacks with `requeue=True` for transient errors), so with `x-delivery-limit: 5` and `max_retries=0` in the OpenAI gateway a provider 429 burns the whole delivery budget in milliseconds and dead-letters the claim; `ecet dlq-replay` is this phase's recovery path for those.
Done when: resolving review triggers webhook `decided_by=human`.
Specs: [UC-09](02-use-cases/UC-09-human-review.md), [api](04-interfaces/api.md).

## Phase 6 — Observability
Plan: [`docs/plans/2026-09-16-phase-6-observability.md`](../docs/plans/2026-09-16-phase-6-observability.md).
- `configure_logging` grows a stdlib bridge (`structlog.stdlib.ProcessorFormatter`) so uvicorn's own records (`cli.py api` passes `log_config=None`) render through the same ADR-001 `drop_sensitive_fields` guard as ours, closing Phase 0 carry-over #12.
- `request_id` travels in `structlog.contextvars`: an HTTP middleware binds it and echoes it on the response, `RabbitMqEvaluationQueue.publish` stamps it as `x-request-id` on the message, and `RabbitMqConsumer._on_message` binds it (with `message_id`, `claim_id`, `tenant_id`) back into the worker's logs.
- `/metrics` is mounted on the api (`prometheus_client.make_asgi_app()`) and served by the worker on `:9100` (`prometheus_client.start_http_server`), each from its own process-global registry.
- `src/ecet/metrics.py` holds every `ecet_*` series the observability spec lists — `ecet_ingest_seconds`, `ecet_pdf_extract_seconds`, `ecet_pii_redaction_seconds`, `ecet_pii_entities_total`, `ecet_deterministic_verdict_total`, `ecet_llm_calls_avoided_total`, `ecet_llm_calls_total`, `ecet_llm_latency_seconds`, `ecet_llm_tokens_total`, `ecet_triage_route_total`, `ecet_webhook_attempts_total`, `ecet_claims_by_status`, `ecet_review_open` — plus `ecet_db_pool_in_use`, an addition the spec's table did not list.
- The worker refreshes `ecet_claims_by_status` and `ecet_review_open` on a timer (`interfaces/worker/gauges.py`), and its container finally has a healthcheck, probing its own `/metrics`, closing Phase 0 carry-over #5.
- The LLM call and the webhook POST move outside the unit of work in `EvaluateClaim` (UC-06), `ResolveReview` (UC-09c) and `RetryNotify`: each opens a read-only unit of work, closes it, makes the slow call with no connection held, then opens a second unit of work for the only writes — closing Phase 4 carry-over #11 and Phase 5 carry-over #2.
- Deviations from the plan, and the phase that closes each: [Carried over from Phase 6](#carried-over-from-phase-6).
Done when: a claim can be followed by one `request_id` from the api's response header into the worker's logs, and `/metrics` on both processes carries every series in the observability spec.
Specs: [observability](05-platform/observability.md), [config](05-platform/config.md), [docker-compose](05-platform/docker-compose.md), [testing](05-platform/testing.md).

## Phase 7 — Polish & E2E
Plan: [`docs/plans/2026-09-16-phase-7-polish-e2e.md`](../docs/plans/2026-09-16-phase-7-polish-e2e.md).
- `tests/e2e/` drives the compose stack from outside — a real `put_object` into MinIO, the signed webhook read back from the mock client, a review resolved through `/v1/reviews/{id}/resolve` ending in `decided_by=human`, ADR-002's deterministic reject with no worker activity, ADR-005's `NO_POLICIES`, one `request_id` across both processes' logs, both `/metrics` endpoints, and `assert_no_pii` over every service's logs. `make e2e` runs it; the CI `e2e` job runs it on push to `main` and on pull requests. The E2E suite caught a publish-before-commit race, fixed in UC-06 (`ClaimNotYetQueued` hold + requeue); `minio-setup` is idempotent on an existing volume; `make demo` polls minio-setup's exit state and drops a fourth note (deterministic reject).
- The `observability` compose profile: Prometheus scraping `api:8000` and `worker:9100`, Grafana with a provisioned "ECET" dashboard (ingest p95, LLM calls avoided, route split, claims by status, open reviews). `make observability`.
- `en_core_web_lg` pinned to 3.8.0 in the Dockerfile, Makefile and CI (a test keeps them agreeing); the CI `slow` job caches the wheel by version. The runtime image check follows the `SPACY_MODEL` build arg.
- README rewrite: measured quickstart, workflow and integration diagrams, the ADR table, measured ADR-002 savings, API and ops guide, and the v1 limitations (static key, mock-client freshness, `make clean` after a topology change, image size, `make spacy-model`).
- `failure_reason` is `no_text` for blank extracted text and `no_policies` for `NO_POLICIES`; `handle_unmapped` logs the path's `claim_id`; `/readyz` with no probes is 503; `build_container` releases what it opened when a later step fails.
- The test gaps from Phases 3–5: every UC-01 write asserted PII-free, duplicates in non-`QUEUED` statuses and before the tenant lookup, the bare-surname redaction, `head()` on a missing bucket, the 5-page <200 ms extraction budget, a genuinely behind-head and an unstamped database, the queue's reconnect-aware health check, and the consumer's drain and drain timeout.
- `IngestClaimDocument` (UC-01) holds no unit of work across the object read, pypdf, spaCy or the publish — read and insert, work with no connection, re-read and write — the ingestion-side twin of Phase 6's external-call move, handed over in PR #8.
- One `tenant_for_delivery(tenants, tenant_id)` in `notify_client.py` replaces the three `_tenant` copies in UC-06, UC-09c and `retry-notify` (Phase 6 carry-over #8's third caller).
- Deviations from the plan: [Carried over from Phase 7](#carried-over-from-phase-7).
Done when: README quickstart reproduces the demo from a clean clone (measured 2 min 16 s, see README §4); the CI `e2e` job is added and runs `pytest -m e2e` on push to `main` and on pull requests — its first green run, on the Phase 7 PR, is the merge gate.
Specs: [testing](05-platform/testing.md), [docker-compose](05-platform/docker-compose.md), [observability](05-platform/observability.md).

## Carried over from Phase 0

Every deferral recorded in [`docs/plans/2026-09-04-phase-0-skeleton-tooling.md`](../docs/plans/2026-09-04-phase-0-skeleton-tooling.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 0 | Closed by |
|---|---------------------|-----------|
| 1 | Layout-spec modules exist only as package `__init__.py`; each real module arrives with its phase | Phases 1–6 |
| 2 | spaCy `en_core_web_lg` download + `SPACY_MODEL` build arg in Dockerfile | Phase 3 |
| 3 | `minio-setup` compose service | Phase 3 |
| 3b | `mock-client` compose service | Phase 4 |
| 4 | api healthcheck probes `/healthz`; `/readyz` does not exist | Phase 3 |
| 5 | worker container has no healthcheck (needs the `/metrics` server) | Phase 6 — closed |
| 6 | Coverage thresholds enforced in CI only, not locally | accepted, permanent |
| 7 | `ruff extend-exclude = ["docs", "specs"]` — ruff 0.16 reformats Python fences in Markdown | accepted, permanent |
| 8 | `include_external_packages = true` required by import-linter 2.14 for `forbidden` contracts | accepted, permanent |
| 9 | `"ecet.config"` in `forbidden_modules` only once it is in the import graph | closed in Phase 0 |
| 10 | `_no_ambient_ecet_env` autouse fixture strips `ECET_*` from `os.environ` per test | accepted, permanent |
| 11 | pre-commit ruff rev pinned to the version `uv.lock` resolves; re-pin on every ruff bump | accepted, permanent |
| 12 | uvicorn stdlib log records bypass the structlog `drop_sensitive_fields` guard (ADR-001) | Phase 6 — closed |
| — | Alembic, seeds, `ecet seed`, `ecet dlq-replay` | Phases 2 / 5 |
| — | testcontainers + CI `slow` job | Phase 2 |
| — | CI `e2e` job | Phase 7 — added, runs on push to `main` and on pull requests; not yet run on GitHub — first green run on the Phase 7 PR is the merge gate |
| — | `tests/fakes.py`, `assert_no_pii`, `tests/fixtures/` from the [testing spec](05-platform/testing.md) | Phase 1 (repository fakes) / Phase 3 (the rest) |

## Carried over from Phase 1

Every deferral recorded in [`docs/plans/2026-09-05-phase-1-domain-core.md`](../docs/plans/2026-09-05-phase-1-domain-core.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 1 | Closed by |
|---|---------------------|-----------|
| 1 | Optimistic save needs the pre-mutation `updated_at`; `transition()` overwrites it and `Claim` has no `version` field — the adapter must carry it (identity map / `version_id_col`) | Phase 2 — not solvable in the domain, the schema has no version column |
| 2 | `Tenant.webhook_secret` is a `SecretStr`; persistence must use `get_secret_value()`, the model dump masks it | Phase 2 |
| 3 | ICD-10 extraction is pattern-only and false-positives on clinical prose (`B12`, `T12`); needs validation against a real code set | Phase 2 (`icd10_codes` table + `Icd10CodeRepository`) / Phase 4 (validate against them) |
| 4 | `InvalidObjectKey`'s message embeds the client-supplied filename; the API error mapping must not echo it verbatim | Phase 3 |
| 5 | `TenantIdField` applies `StringConstraints` to a `NewType`; pydantic-version-fragile, fallback documented in the plan | accepted, permanent |
| 6 | `Policy` is `frozen=True`, which does not prevent in-place mutation of its `set`/`list` fields | accepted, permanent |
| 7 | `rules.aggregate()` raises `KeyError` on a check name outside `_VERDICT_ON_FAIL` | accepted — the four names are internal to `run_checks` |
| 8 | `limit` is keyword-only on `ClaimRepository.list_by_status` but positional on `ReviewTaskRepository.list_open` | accepted — each matches its own spec |
| 9 | `ReviewTaskNotFound`'s docstring promises a tenant scope that `get(task_id)` cannot enforce | accepted — [UC-09c](02-use-cases/UC-09-human-review.md#uc-09c-resolvereview) has the same gap; the use case checks the tenant |

## Carried over from Phase 2

Every deferral recorded in [`docs/plans/2026-09-06-phase-2-persistence.md`](../docs/plans/2026-09-06-phase-2-persistence.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 2 | Closed by |
|---|---------------------|-----------|
| 1 | api startup runs `alembic upgrade head` + the seed when `ECET_AUTO_MIGRATE=true`; Phase 2 shipped the migration, the seed and the in-image `alembic.ini` + `migrations/` copy, but wired neither into the api process, which has no database yet | Phase 3 |
| 2 | The compose `postgres` init-dir seeding path from the [postgres spec](03-infrastructure/postgres.md#seed) is dropped: it runs before Alembic creates the tables. `ecet seed` is the only seeding path | accepted, permanent |
| 3 | The overall 85% coverage gate moved to the CI `slow` job, since the adapter code it measures is only reachable with Docker | accepted, permanent |
| 4 | Extracted ICD-10 codes are validated against the seeded catalogue through `Icd10CodeRepository.known_codes()`. `T12` is itself a real code, so "the T12 vertebra" still reads as a diagnosis even with the catalogue | Phase 4 |
| 5 | `known_codes()` reads the whole `icd10_codes` table per call (a `ponytail:` comment marks it); cache it in the caller if a per-claim path ever calls it | accepted unless it shows up hot |
| 6 | The seeded tenants' webhook URLs point at `http://mock-client:8081/hooks/...`, and the `mock-client` compose service does not exist until Phase 4 | Phase 4 |
| 7 | `ecet seed` gives no distinct error for a connection failure versus a bad statement | accepted, it is a dev-only command |

## Carried over from Phase 3

Every deferral recorded in [`docs/plans/2026-09-06-phase-3-ingestion-path.md`](../docs/plans/2026-09-06-phase-3-ingestion-path.md#deviations-from-spec-record-in-the-pr-description) and its progress ledger, with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 3 | Closed by |
|---|---------------------|-----------|
| 1 | A publish failure leaves the claim `POLICIES_ATTACHED` with no retry path (the outbox stays out of v1) | Phase 5 |
| 2 | `ClaimView` omits the redacted text that UC-09b will need | Phase 5 |
| 3 | No metrics anywhere in the ingestion path: `ecet_ingest_seconds`, `ecet_pdf_extract_seconds`, `ecet_pii_redaction_seconds`, `ecet_pii_entities_total`, `ecet_deterministic_verdict_total`, `ecet_llm_calls_avoided_total` are all unimplemented, and UC-04's metrics hook is a comment | Phase 6 — closed |
| 4 | `request_id` is neither bound to the structlog context nor propagated to the queue as `x-request-id` | Phase 6 — closed |
| 5 | README gaps: the ~1.5 GB image-size note, a `make spacy-model` mention, and a note that v1 auth is a static API key while production would use per-tenant keys or JWT | Phase 7 — closed |
| 6 | CI re-downloads the 590 MB spaCy model uncached on every `slow` run, and `spacy download` resolves an unpinned model version | Phase 7 — closed |
| 7 | `infrastructure/queue/in_memory.py` and `infrastructure/pii/fake_redactor.py` from the layout spec were not built; `tests/fakes.py` covers both needs | accepted, permanent |
| 8 | UC-01's spec sentence "wrap steps 5-11 so any unexpected exception sets a failure state" is not implementable as written: the state machine has no failure edge out of `EXTRACTED` or `POLICIES_ATTACHED`, and it contradicts the same spec's requirement that a publish failure LEAVE the claim `POLICIES_ATTACHED`. Two explicit failure paths ship instead; an unexpected mid-pipeline exception rolls back to the committed `RECEIVED` claim | accepted, permanent |
| 9 | Alembic runs in a subprocess from the API lifespan, because `migrations/env.py` calls `asyncio.run`, which cannot nest in the running loop. The project root is resolved from the working directory, not `__file__` | accepted, permanent |
| 10 | `ECET_AUTO_MIGRATE=true` also seeds, but only when `ECET_ENV=dev` | accepted, permanent |
| 11 | The presidio adapter's ICD-10 span filter can suppress a `LOCATION` whose entire span is three alphanumerics — UK postcode outward codes (`E14`, `N19`) and route designators (`I95`); irrelevant for US prior-auth claims | accepted, permanent |
| 12 | An entity straddling a `\n\n` chunk boundary is not redacted | accepted, permanent |
| 13 | The presidio adapter test needs `en_core_web_lg` present locally (`make spacy-model`) | accepted, permanent |
| 14 | `POST /v1/events/s3` treats an empty `eventName` as a creation event (fail-open) | accepted, permanent |
| 15 | `FAKE_REDACTOR_NAMES` carries a bare "Whitfield" entry no fixture exercises standalone — an untested defensive branch in the fake's name list | Phase 7 — closed |
| 16 | The UC-01 ADR-001 assertion checks the final stored claim, not every `claims.save` argument as the UC-01 spec's test list asks | Phase 7 — closed |
| 17 | No test covers a duplicate whose original status is not `QUEUED`, nor one proving the duplicate check precedes the tenant lookup | Phase 7 — closed |
| 18 | No test covers `ObjectStorage.head()` against a missing bucket | Phase 7 — closed |
| 19 | The pdf-text-extractor spec's soft budget "5-page fixture extracts in <200 ms" has no test; no perf scaffolding exists in the repo | Phase 7 — closed |
| 20 | `tests/adapters/test_migrations.py` hardcodes revision `'0001_initial'` and creates "an unknown revision" rather than a genuine behind-head state; `get_current_revision()` returning `None` on an unstamped database is untested | Phase 7 — closed |
| 21 | A failure mid-`build_container` leaks the engine; `/readyz` with an empty probes map returns 200 | Phase 7 — closed |
| 22 | `POST /v1/claims/ingest` and `POST /v1/events/s3` validate the object key but never the bucket — both paths HEAD/GET whatever bucket the caller names, with nothing constraining it to `claims`. Not privilege escalation under v1's single global API key, but bucket scoping is convention-only; a `settings.s3_bucket` check is two lines | Phase 4 |
| 23 | Two different tokens for one failure: UC-01 raises `ExtractionFailed("empty_text")` while the pypdf adapter raises `"no_text"` for the same condition, and `empty_text` is missing from the documented token list in `application/errors.py` | Phase 7 — closed |
| 24 | `failure_reason` for `NO_POLICIES` is the bare tenant slug (`str(NoPoliciesForTenant)`), so operators read `failure_reason: "tenant-a"` rather than a reason — breaking the short-token convention the same module sets for `EXTRACTION_FAILED` | Phase 7 — closed |
| 25 | `RabbitMqEvaluationQueue.is_healthy` returns True during an aio-pika robust reconnect (`is_closed` stays False while it retries), so `/readyz` can report the queue healthy when publishes would fail — and the compose healthcheck gates `minio-setup` on `/readyz` | Phase 4 |
| 26 | `handle_unmapped` logs no `claim_id`, while [api](04-interfaces/api.md) requires "Unhandled → 500, logged with `claim_id` if known"; the mapped handler does it, the catch-all does not | Phase 7 — closed |

## Carried over from Phase 4

Every deferral recorded in [`docs/plans/2026-09-07-phase-4-evaluation-path.md`](../docs/plans/2026-09-07-phase-4-evaluation-path.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 4 | Closed by |
|---|---------------------|-----------|
| 1 | No metric is emitted anywhere on the worker path: `ecet_llm_calls_total`, `ecet_llm_latency_seconds`, `ecet_llm_tokens_total`, `ecet_triage_route_total`, `ecet_webhook_attempts_total` are all unimplemented, and the worker has no `/metrics` server and therefore still no container healthcheck | Phase 6 — closed |
| 2 | `infrastructure/webhook/fake_client.py` and `infrastructure/queue/in_memory.py` from the layout spec are still not built; `tests/fakes.py` covers both | accepted, permanent |
| 3 | UC-06 commits once, after routing: a webhook delivered but not committed is re-delivered on redelivery. At-least-once is the queue's contract, and the only key a receiver can dedupe on is `claim_id` **in the body** — `X-ECET-Delivery` is a fresh `uuid4()` per `deliver()` call (`httpx_client.py:65`), so a redelivered claim arrives under a different delivery id and that header is no idempotency key | accepted, permanent |
| 4 | The adapter tests use `httpx.MockTransport` rather than `respx` | accepted, permanent |
| 5 | The vendor adapter is never exercised against a live endpoint; `ECET_LLM_PROVIDER=openai` is untested outside a mock transport | accepted for v1 — the adapter is verified against `httpx.MockTransport` only, by the user's decision not to call a real vendor |
| 6 | The mock client verifies a signature but not the timestamp's freshness, so a captured delivery replays forever | accepted — documented in the README (Phase 7) |
| 7 | `EvaluationOutput` accepts a missing `confidence` and degrades to `INSUFFICIENT_EVIDENCE`, but the tool schema still marks it required — a server that omits it is silently downgraded rather than reported | accepted, deliberate |
| 8 | A `NOTIFY_FAILED` claim has no retry path: `POST /v1/claims/{id}/retry-notify` is Phase 5 | Phase 5 |
| 9 | The worker depends on a healthy api in compose so migrations have run, rather than waiting for head itself; a worker restarted alone against a behind-head database exits | accepted, deliberate |
| 10 | Requeue has no delay: `RabbitMqConsumer._on_message` nacks with `requeue=True` for transient errors, which triggers immediate redelivery. With `x-delivery-limit: 5` and `max_retries=0` in the OpenAI gateway, a provider 429 burns the whole delivery budget in a fraction of a second and dead-letters the claim, with no recovery path until `ecet dlq-replay` exists | Phase 5 |
| 11 | The LLM call and the webhook POST run inside the open unit of work: `EvaluateClaim.execute` opens the transaction, `_load` issues the first SELECT, and then `self._llm.evaluate` (up to `llm_timeout_s=60`) plus `RouteDecision` → `NotifyClient` → up to 3 HTTP attempts with a 1s+4s backoff all run before `uow.commit()`. That is a Postgres connection idle-in-transaction for up to ~65s per message, `worker_prefetch=4` of them per worker, against a default SQLAlchemy pool. Nothing records it today — no metric, no log, no pool-exhaustion alarm | Phase 6 — closed |
| 12 | Two untested paths in the consumer: `RabbitMqConsumer.is_healthy` (`rabbitmq.py:88-98`, the reconnect fix that closes Phase 3 carry-over #25 — all three of its conditions could be reverted and the suite stays green), and `stop()`'s in-flight drain (`rabbitmq.py:190-204`, the whole SIGTERM story in compose, yet every test calls `stop()` with `_in_flight == 0`) | Phase 7 — closed (the health check lives on RabbitMqEvaluationQueue; the consumer's was removed) |

## Carried over from Phase 5

Every deferral recorded in [`docs/plans/2026-09-15-phase-5-human-review-ops.md`](../docs/plans/2026-09-15-phase-5-human-review-ops.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 5 | Closed by |
|---|---------------------|-----------|
| 1 | No metric on the review or retry paths either (reviews resolved, webhook attempts from the api), and still no `/metrics` server | Phase 6 — closed |
| 2 | UC-09c's and `retry-notify`'s webhook POST runs inside the HTTP request and inside the open unit of work — one attempt (no in-request backoff, `retry-notify` is the retry) while a connection sits idle-in-transaction; the api-side twin of Phase 4 #11. Unlike the worker's prefetch-bounded concurrency, the api path is unbounded, and `/readyz`'s database probe shares the same pool, so the Phase 6 fix must move the POST out of the request, not just measure it | Phase 6 — closed |
| 3 | Requeue still has no delay: a vendor 429 dead-letters a claim in milliseconds, and `ecet dlq-replay` is a manual recovery, not a retry policy | accepted for v1 — a delayed-retry queue is a topology change |
| 4 | `GET /v1/claims/{id}` still omits the redacted text; `GET /v1/reviews` is the only response that carries it | accepted, deliberate |
| 5 | A claim stuck `POLICIES_ATTACHED` is retried only when its object arrives again (MinIO's spooled re-send, or an operator re-posting `/v1/claims/ingest`); there is no sweeper, and `GET /v1/claims/{id}` does not show the object key an operator would re-post | accepted — the outbox stays out of v1 |
| 6 | `ListOpenReviews` reads one claim per task (bounded by `limit` ≤ 200) | accepted unless the queue gets long |
| 7 | `POST /v1/claims/{id}/retry-notify` takes the api key only, as the api spec lists it, so it is not tenant-scoped | accepted — v1 has one global key |
| 8 | A re-publish racing an in-flight ingestion of the same object can publish twice; one save loses with `ConcurrentModification` and the worker skips the second message | accepted, permanent |
| 9 | README: `/v1/reviews`, `retry-notify`, `make dlq-replay` | Phase 7 — closed |
| 10 | No E2E test of resolve → `decided_by=human`; `make demo` is the only end-to-end proof | Phase 7 — closed |
| 11 | No live-vendor recording exists or is planned; the OpenAI-compatible adapter is verified against mock transports only | accepted, not scheduled — the user decided not to call a real vendor |

## Carried over from Phase 6

Every deviation recorded in [`docs/plans/2026-09-16-phase-6-observability.md`](../docs/plans/2026-09-16-phase-6-observability.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it.

| # | Deferred in Phase 6 | Closed by |
|---|---------------------|-----------|
| 1 | The `observability` compose profile (Prometheus, Grafana, dashboard JSON) was not built — the endpoints are the deliverable, scraping them was the user's decision to leave for later, not scheduled | Phase 7 — closed |
| 2 | `ecet_db_pool_in_use` is an addition to the observability spec's metric table, not one of its rows | accepted, permanent |
| 3 | The api and the worker each expose their own `prometheus_client` registry (process-global, one per process) — a scraper needs both targets, not one | accepted, permanent |
| 4 | Two concurrent resolves of the same review can both POST the webhook before either commits; only one wins at `ClaimRepository.save`'s optimistic check in `task.resolve` — pre-existing in shape, not this phase's to solve | accepted, permanent |
| 5 | A crash between the read unit of work and the write unit of work (UC-06, UC-09c, retry-notify) leaves the claim recoverable — it is still in a retryable status — but with a delivery already sent that the system has no record of | accepted, permanent — the transactional-outbox alternative stays out of v1 |
| 6 | `configure_logging` re-runs its root-handler and uvicorn-logger reset on every call; harmless at one call per process, worth a look only if it is ever called from two places | accepted unless it becomes multi-call |
| 7 | The worker's shutdown awaits an in-flight gauge refresh with no ceiling — a refresh against a hung database delays shutdown indefinitely; matches the codebase's existing no-DB-timeout posture, not a new gap this phase opened | accepted, permanent |
| 8 | The `_tenant` helper is duplicated verbatim between `retry_notify.py` and `human_review.py` — deliberate, so the two use cases read alike; the only shared home would be `notify_client.py`, which knows nothing about a unit of work | Phase 7 — closed (third caller in UC-06; extracted as \`tenant_for_delivery\`) |
| 9 | `WEBHOOK_ATTEMPTS_TOTAL`'s `status_class` label can emit `1xx`/`3xx` for an unusual vendor response, outside the `{2xx, 4xx, 5xx, error}` vocabulary the spec names | accepted, permanent |
| 10 | `LLM_LATENCY_SECONDS` observes `latency_ms / 1000`, losing sub-millisecond precision | accepted, permanent |

## Carried over from Phase 7

Every deviation recorded in [`docs/plans/2026-09-16-phase-7-polish-e2e.md`](../docs/plans/2026-09-16-phase-7-polish-e2e.md#deviations-from-spec-record-in-the-pr-description). Phase 7 is the last v1 phase: nothing here is scheduled, each row is accepted or belongs to the deferred list below.

| # | Deviation in Phase 7 | Status |
|---|----------------------|--------|
| 1 | The roadmap's Phase 4 #12 named `RabbitMqConsumer.is_healthy`, a method that no longer exists; the reconnect-aware check it described lives on `RabbitMqEvaluationQueue.is_healthy`, which `/readyz` calls — the roadmap row is corrected in Task 10 | accepted, permanent |
| 2 | The CI `e2e` job builds the image with no Docker layer cache, so each run downloads the spaCy model inside the build | accepted unless the job's runtime becomes a problem |
| 3 | The `_Queue.built` class attribute needed a `ClassVar` annotation to satisfy ruff RUF012 (type-only) | accepted, permanent |
| 4 | `docker compose wait minio-setup` exits 1 once the one-shot has already exited (Compose v5.5), which is always the case by the time `/readyz` answers; the E2E conftest and `make demo` both poll `docker compose ps -a` for `exited 0` instead | accepted, permanent |
| 5 | `tests/e2e/stack.py`'s `eventually` uses PEP 695 `[T]` type parameters and names its deadline `within_s` (the brief's `TypeVar`/`timeout` failed ruff) | accepted, permanent |
| 6 | Product bug found by the E2E suite: UC-01 published the evaluation before committing `QUEUED`, so a fast worker could read the claim still `POLICIES_ATTACHED` and ack the message as a duplicate, stranding the claim `QUEUED` with no message. UC-06 now holds 0.5 s outside any unit of work and raises `ClaimNotYetQueued`, which the worker requeues | accepted, permanent — the transactional outbox stays out of v1 |
| 7 | `minio-setup` failed (exit 1) whenever it re-ran against an existing `miniodata` volume, because `mc event add` rejects an overlapping rule; it now skips the add when the rule exists | accepted, permanent |
| 8 | The E2E review lists use `?limit=200`; on a long-lived stack that is never `make clean`ed, open tenant-a tasks can accumulate past that and a new task falls outside the page. CI always starts from empty volumes | accepted |
| 9 | The fresh-clone quickstart (`uv sync && make demo`, image cached) measured 2 min 16 s, not under 2 minutes; the README states the measured time. Presidio's model load inside the api's start period dominates | accepted |

## Deferred (explicitly out of v1)
- OCR for scanned PDFs.
- Transactional outbox for publish (current: status stays `POLICIES_ATTACHED` on publish failure + manual retry).
- Per-tenant API keys / JWT.
- Policy management API (policies seeded only).
- JSON-mode fallback for servers without tool calling (only if a target server needs it).
- Distributed tracing.
