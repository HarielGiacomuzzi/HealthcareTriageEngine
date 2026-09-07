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
- [UC-09b](02-use-cases/UC-09-human-review.md#uc-09b-listopenreviews) / [UC-09c](02-use-cases/UC-09-human-review.md#uc-09c-resolvereview), `/v1/reviews*`, `/v1/claims/{id}/retry-notify`, `ecet dlq-replay`.
- Real vendor run behind env flag; record one real evaluation output as fixture.
- Carried from Phase 3: a publish failure leaves the claim `POLICIES_ATTACHED` with no retry path yet, and `ClaimView` omits the redacted text that UC-09b needs.
Done when: resolving review triggers webhook `decided_by=human`.
Specs: [UC-09](02-use-cases/UC-09-human-review.md), [api](04-interfaces/api.md).

## Phase 6 — Observability & Polish
- structlog + redaction guard, metrics, `claims_by_status` gauge, optional Prometheus/Grafana profile.
- E2E test suite; README rewrite: diagrams (workflow + integration), ADR section, quickstart verified < 2 min after image cached, cost-saving numbers from metrics.
- Carried from Phase 0: `/metrics` server + worker container healthcheck against it; CI `e2e` job (`pytest -m e2e`); close the uvicorn logging seam — `cli.py api` passes `log_config=None`, so `uvicorn.error`/`uvicorn.access` records go to the stdlib root handler and bypass the structlog `drop_sensitive_fields` guard (ADR-001 backstop). Route uvicorn through structlog and assert it in a log-capture test.
- Carried from Phase 3: no metric is emitted anywhere in the ingestion path (`ecet_ingest_seconds`, `ecet_pdf_extract_seconds`, `ecet_pii_redaction_seconds`, `ecet_pii_entities_total`, `ecet_deterministic_verdict_total`, `ecet_llm_calls_avoided_total`), and the UC-04 metrics hook is a comment; `request_id` is neither bound to the structlog context nor propagated to the queue as `x-request-id`; the README needs the ~1.5 GB image-size note, a `make spacy-model` mention, and a note that v1 auth is a static API key while production would use per-tenant keys or JWT; CI re-downloads the 590 MB spaCy model uncached on every `slow` run and `spacy download` resolves an unpinned model version — wants `actions/cache` keyed on a pinned version.
- Carried from Phase 3 (small review gaps, none blocking): the fake redactor's bare `Whitfield` entry has no standalone fixture; UC-01's ADR-001 assertion checks only the final stored claim, not every `claims.save` argument; no test covers a duplicate whose original status is not `QUEUED`, one proving the duplicate check precedes the tenant lookup, `ObjectStorage.head()` against a missing bucket, or the pdf extractor's "5-page fixture under 200 ms" budget; `tests/adapters/test_migrations.py` hardcodes revision `'0001_initial'` and never exercises an unstamped database; a failure mid-`build_container` leaks the engine, and `/readyz` with an empty probes map returns 200. Three naming fixes belong here too: UC-01 raises `ExtractionFailed("empty_text")` while the pypdf adapter raises `"no_text"` for the same condition and `empty_text` is absent from the token list in `application/errors.py`; `failure_reason` for `NO_POLICIES` is the bare tenant slug instead of a short token; `handle_unmapped` logs no `claim_id`, which [api](04-interfaces/api.md) requires.
Done when: README quickstart reproduces demo from clean clone.
Specs: [observability](05-platform/observability.md), [testing](05-platform/testing.md).

## Carried over from Phase 0

Every deferral recorded in [`docs/plans/2026-09-04-phase-0-skeleton-tooling.md`](../docs/plans/2026-09-04-phase-0-skeleton-tooling.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 0 | Closed by |
|---|---------------------|-----------|
| 1 | Layout-spec modules exist only as package `__init__.py`; each real module arrives with its phase | Phases 1–6 |
| 2 | spaCy `en_core_web_lg` download + `SPACY_MODEL` build arg in Dockerfile | Phase 3 |
| 3 | `minio-setup` compose service | Phase 3 |
| 3b | `mock-client` compose service | Phase 4 |
| 4 | api healthcheck probes `/healthz`; `/readyz` does not exist | Phase 3 |
| 5 | worker container has no healthcheck (needs the `/metrics` server) | Phase 6 |
| 6 | Coverage thresholds enforced in CI only, not locally | accepted, permanent |
| 7 | `ruff extend-exclude = ["docs", "specs"]` — ruff 0.16 reformats Python fences in Markdown | accepted, permanent |
| 8 | `include_external_packages = true` required by import-linter 2.14 for `forbidden` contracts | accepted, permanent |
| 9 | `"ecet.config"` in `forbidden_modules` only once it is in the import graph | closed in Phase 0 |
| 10 | `_no_ambient_ecet_env` autouse fixture strips `ECET_*` from `os.environ` per test | accepted, permanent |
| 11 | pre-commit ruff rev pinned to the version `uv.lock` resolves; re-pin on every ruff bump | accepted, permanent |
| 12 | uvicorn stdlib log records bypass the structlog `drop_sensitive_fields` guard (ADR-001) | Phase 6 — Phase 3+ adapters must not log via stdlib until then |
| — | Alembic, seeds, `ecet seed`, `ecet dlq-replay` | Phases 2 / 5 |
| — | testcontainers + CI `slow` job | Phase 2 |
| — | CI `e2e` job | Phase 6 |
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
| 3 | No metrics anywhere in the ingestion path: `ecet_ingest_seconds`, `ecet_pdf_extract_seconds`, `ecet_pii_redaction_seconds`, `ecet_pii_entities_total`, `ecet_deterministic_verdict_total`, `ecet_llm_calls_avoided_total` are all unimplemented, and UC-04's metrics hook is a comment | Phase 6 |
| 4 | `request_id` is neither bound to the structlog context nor propagated to the queue as `x-request-id` | Phase 6 |
| 5 | README gaps: the ~1.5 GB image-size note, a `make spacy-model` mention, and a note that v1 auth is a static API key while production would use per-tenant keys or JWT | Phase 6 |
| 6 | CI re-downloads the 590 MB spaCy model uncached on every `slow` run, and `spacy download` resolves an unpinned model version | Phase 6 — wants `actions/cache` keyed on a pinned version |
| 7 | `infrastructure/queue/in_memory.py` and `infrastructure/pii/fake_redactor.py` from the layout spec were not built; `tests/fakes.py` covers both needs | accepted, permanent |
| 8 | UC-01's spec sentence "wrap steps 5-11 so any unexpected exception sets a failure state" is not implementable as written: the state machine has no failure edge out of `EXTRACTED` or `POLICIES_ATTACHED`, and it contradicts the same spec's requirement that a publish failure LEAVE the claim `POLICIES_ATTACHED`. Two explicit failure paths ship instead; an unexpected mid-pipeline exception rolls back to the committed `RECEIVED` claim | accepted, permanent |
| 9 | Alembic runs in a subprocess from the API lifespan, because `migrations/env.py` calls `asyncio.run`, which cannot nest in the running loop. The project root is resolved from the working directory, not `__file__` | accepted, permanent |
| 10 | `ECET_AUTO_MIGRATE=true` also seeds, but only when `ECET_ENV=dev` | accepted, permanent |
| 11 | The presidio adapter's ICD-10 span filter can suppress a `LOCATION` whose entire span is three alphanumerics — UK postcode outward codes (`E14`, `N19`) and route designators (`I95`); irrelevant for US prior-auth claims | accepted, permanent |
| 12 | An entity straddling a `\n\n` chunk boundary is not redacted | accepted, permanent |
| 13 | The presidio adapter test needs `en_core_web_lg` present locally (`make spacy-model`) | accepted, permanent |
| 14 | `POST /v1/events/s3` treats an empty `eventName` as a creation event (fail-open) | accepted, permanent |
| 15 | `FAKE_REDACTOR_NAMES` carries a bare "Whitfield" entry no fixture exercises standalone — an untested defensive branch in the fake's name list | Phase 6 |
| 16 | The UC-01 ADR-001 assertion checks the final stored claim, not every `claims.save` argument as the UC-01 spec's test list asks | Phase 6 |
| 17 | No test covers a duplicate whose original status is not `QUEUED`, nor one proving the duplicate check precedes the tenant lookup | Phase 6 |
| 18 | No test covers `ObjectStorage.head()` against a missing bucket | Phase 6 |
| 19 | The pdf-text-extractor spec's soft budget "5-page fixture extracts in <200 ms" has no test; no perf scaffolding exists in the repo | Phase 6 |
| 20 | `tests/adapters/test_migrations.py` hardcodes revision `'0001_initial'` and creates "an unknown revision" rather than a genuine behind-head state; `get_current_revision()` returning `None` on an unstamped database is untested | Phase 6 |
| 21 | A failure mid-`build_container` leaks the engine; `/readyz` with an empty probes map returns 200 | Phase 6 |
| 22 | `POST /v1/claims/ingest` and `POST /v1/events/s3` validate the object key but never the bucket — both paths HEAD/GET whatever bucket the caller names, with nothing constraining it to `claims`. Not privilege escalation under v1's single global API key, but bucket scoping is convention-only; a `settings.s3_bucket` check is two lines | Phase 4 |
| 23 | Two different tokens for one failure: UC-01 raises `ExtractionFailed("empty_text")` while the pypdf adapter raises `"no_text"` for the same condition, and `empty_text` is missing from the documented token list in `application/errors.py` | Phase 6 |
| 24 | `failure_reason` for `NO_POLICIES` is the bare tenant slug (`str(NoPoliciesForTenant)`), so operators read `failure_reason: "tenant-a"` rather than a reason — breaking the short-token convention the same module sets for `EXTRACTION_FAILED` | Phase 6 |
| 25 | `RabbitMqEvaluationQueue.is_healthy` returns True during an aio-pika robust reconnect (`is_closed` stays False while it retries), so `/readyz` can report the queue healthy when publishes would fail — and the compose healthcheck gates `minio-setup` on `/readyz` | Phase 4 |
| 26 | `handle_unmapped` logs no `claim_id`, while [api](04-interfaces/api.md) requires "Unhandled → 500, logged with `claim_id` if known"; the mapped handler does it, the catch-all does not | Phase 6 |

## Deferred (explicitly out of v1)
- OCR for scanned PDFs.
- Transactional outbox for publish (current: status stays `POLICIES_ATTACHED` on publish failure + manual retry).
- Per-tenant API keys / JWT.
- Policy management API (policies seeded only).
- JSON-mode fallback for servers without tool calling (only if a target server needs it).
- Distributed tracing.
