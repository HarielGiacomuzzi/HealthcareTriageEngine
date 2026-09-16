# Enterprise Claims Extraction & Triage Engine (ECET)

> A deterministic-first AI pipeline that reads unformatted prior-authorization notes, decides medical necessity against each tenant's policies, and hands anything uncertain to a human — with PII redacted on local CPU before any model sees the text.

## 1. The business problem

Healthcare administrators spend 18–25 minutes per claim matching unstructured physician notes against ICD-10 coverage policies. ECET automates the reading and the first decision, and keeps a human review fail-safe for everything it is not confident about.

## 2. How it works

A PDF lands in a tenant's prefix of the `claims` bucket. MinIO notifies the api, which extracts the text, redacts PII, attaches the tenant's policies, runs the deterministic checks and — only if those do not already settle it — queues the claim. The worker asks the LLM gateway for a decision, routes it by confidence, and signs a webhook to the tenant. Low confidence and deterministic rejects become review tasks a human resolves through the api.

### 2.1 Workflow

![Claim pipeline sequence](specs/images/claim-pipeline-sequence.png)

![Claim triage decision logic](specs/images/triage-decision-flow.png)

Lifecycle: [claim state machine](specs/01-domain/claim.md#2-state-machine) ([rendered](specs/images/claim-state-machine.png)).

### 2.2 Integration

![ECET deployment topology](specs/images/platform-topology.png)

| Service | Port (host) | Role |
|---|---|---|
| api | 8000 | S3 events, manual ingest, claims, reviews, ops, `/metrics` |
| worker | — (9100 inside the network) | consumes `claims.evaluate`, calls the LLM, delivers webhooks, `/metrics` |
| postgres | 5432 | claims, policies, tenants, review tasks |
| rabbitmq | 5672, 15672 (UI) | `claims.evaluate` quorum queue + DLQ |
| minio | 9000, 9001 (console) | the `claims` bucket and its notification |
| mock-client | 8081 | a tenant's system: verifies the HMAC, keeps what it received |
| prometheus, grafana | 9090, 3000 | opt-in: `make observability` |

## 3. Architecture decisions (ADRs)

| ADR | Decision | Where |
|---|---|---|
| 001 | PII is redacted on local CPU before any external call; raw text is never persisted, queued or logged | [UC-02](specs/02-use-cases/UC-02-redact-pii.md), [presidio adapter](specs/03-infrastructure/pii-redactor-presidio.md) |
| 002 | Deterministic checks run before the LLM; a rule that settles the claim saves the call | [UC-04](specs/02-use-cases/UC-04-run-deterministic-checks.md), [rules](specs/01-domain/evaluation.md#1-deterministicresult-adr-002) |
| 003 | Confidence below 0.85 (`ECET_CONFIDENCE_THRESHOLD`) goes to a human | [UC-07](specs/02-use-cases/UC-07-route-decision.md), [UC-09](specs/02-use-cases/UC-09-human-review.md) |
| 004 | Vendor-agnostic LLM gateway; a deterministic fake is the default | [llm-gateway](specs/03-infrastructure/llm-gateway.md) |
| 005 | A tenant with no effective policy is a hard failure at ingestion — no policy context, no LLM call | [UC-03](specs/02-use-cases/UC-03-attach-tenant-policies.md) |
| 006 | Idempotency by S3 object (bucket, key, etag); a duplicate event is a no-op | [UC-01](specs/02-use-cases/UC-01-ingest-claim-document.md) |
| 007 | One image, two entrypoints (`ecet api`, `ecet worker`) | [docker-compose](specs/05-platform/docker-compose.md), [worker](specs/04-interfaces/worker.md) |

Full context and the end-to-end flow: [specs/00-overview.md](specs/00-overview.md).

### What ADR-002 saves

`ecet_llm_calls_avoided_total` (api) counts claims the deterministic checks settled; `ecet_llm_calls_total` (worker) counts the calls that were made. The share of evaluations that never reached a model is

```promql
sum(ecet_llm_calls_avoided_total)
  / (sum(ecet_llm_calls_avoided_total) + sum(ecet_llm_calls_total))
```

which the Grafana dashboard shows as "LLM calls avoided". In one `make demo` run (4 drops) that was 1 avoided against 2 made — a 33% avoided share. The demo mix is chosen to show every route, not to be representative — on real traffic the ratio is whatever share of notes cite an excluded code or no code at all, and each avoided call is one vendor request's latency and price not paid.

## 4. Quickstart

Prerequisites: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker with Compose v2, `jq`.

```bash
git clone https://github.com/HarielGiacomuzzi/HealthcareTriageEngine.git
cd HealthcareTriageEngine
uv sync
make demo
```

With the image already built, `make demo` took 2 minutes 16 seconds from a fresh clone. The first build takes longer: the image is 1.71 GB, most of it the spaCy `en_core_web_lg` model presidio needs (see [Image size](#image-size)).

`make demo` starts the stack, drops four fixture PDFs and resolves a review:

| Drop | Tenant | What happens |
|---|---|---|
| `note_simple.pdf` | tenant-a | meets the MRI policy → `APPROVED_AUTO`, webhook `decided_by: auto` |
| `note_unclear.pdf` | tenant-a | fake LLM confidence 0.4 → `REVIEW_PENDING` |
| `note_excluded_code.pdf` | tenant-a | `Z00.00` is excluded → deterministic reject, no LLM call → `REVIEW_PENDING` |
| `note_simple.pdf` | tenant-empty | no effective policy → `NO_POLICIES` (422 to MinIO) |

It then resolves the oldest open tenant-a review as `demo.reviewer` and prints what the mock client received — the last delivery is `decided_by: human`.

No API key is needed: `ECET_LLM_PROVIDER=fake` is the default, so the stack makes no external calls.

## 5. Using the API

Every route except `/healthz`, `/readyz` and `/metrics` needs `X-API-Key` (dev: `dev-api-key`). Tenant-scoped routes also need `X-Tenant-Id`. Every response carries `X-Request-Id`; the same id appears on every api and worker log line for that claim.

```bash
# The review queue (redacted note included — the only response that carries it)
curl -s -H X-API-Key:dev-api-key -H X-Tenant-Id:tenant-a \
  localhost:8000/v1/reviews | jq '[.[] | {task_id, claim_id, reason}]'

# Resolve one
curl -s -H X-API-Key:dev-api-key -H X-Tenant-Id:tenant-a -H 'Content-Type: application/json' \
  -d '{"reviewer":"jane.doe","resolution":"MEETS_NECESSITY","notes":"Dates confirmed."}' \
  localhost:8000/v1/reviews/<task_id>/resolve | jq

# A claim
curl -s -H X-API-Key:dev-api-key -H X-Tenant-Id:tenant-a \
  localhost:8000/v1/claims/<claim_id> | jq '{status, failure_reason, evaluation}'

# Re-send a decision whose webhook failed (NOTIFY_FAILED → 200 when it lands, 502 when it fails again)
curl -s -X POST -H X-API-Key:dev-api-key localhost:8000/v1/claims/<claim_id>/retry-notify | jq
```

Full surface: [api spec](specs/04-interfaces/api.md).

## 6. Operating it

| Command | What it does |
|---|---|
| `make up` / `make down` | start / stop the stack (`.env` is copied from `.env.example` on first use) |
| `make logs` | follow the api and worker logs (JSON, one line per event) |
| `make demo` | the walkthrough above |
| `make drop TENANT=tenant-b` | drop `note_simple.pdf` for a tenant |
| `make dlq-replay LIMIT=100` | move dead-lettered evaluations back onto `claims.evaluate` |
| `make observability` | the stack plus Prometheus (:9090) and Grafana (:3000, dashboard "ECET") |
| `make clean` | stop the stack **and delete its volumes** |

**Dead letters.** A message is dead-lettered after five failed deliveries. Requeue has no delay, so a vendor 429 can burn all five in milliseconds; once the vendor recovers, `make dlq-replay` puts them back.

**After a queue topology change, run `make clean`.** `claims.evaluate` is a quorum queue. A classic queue of the same name left in an old `rabbitdata` volume makes both the api and the worker fail to start with `PRECONDITION_FAILED`.

**Metrics.** The api and the worker each expose their own `/metrics` (a scraper needs both targets). Series: [observability spec](specs/05-platform/observability.md).

## 7. Development

```bash
make install        # uv sync
make spacy-model    # en_core_web_lg 3.8.0, pinned — only the presidio adapter tests need it
make check          # lint, types, import contracts, unit + api tests (no Docker)
uv run pytest -m slow   # adapter tests against real Postgres, RabbitMQ, MinIO, presidio (Docker)
make e2e            # the E2E suite against the compose stack (starts it if needed)
```

CI runs three jobs: `check` (push to `main` and pull requests), `slow` (adapters, 85 % coverage gate, the model wheel cached by version) and `e2e` (the compose stack, fake LLM). Layer rules are enforced by import-linter; the domain and application layers are `mypy --strict`. Test strategy: [testing spec](specs/05-platform/testing.md).

### Image size

The single image is 1.71 GB, dominated by `en_core_web_lg`. For a smaller demo image build with the medium model and tell the api to load it:

```bash
docker compose build --build-arg SPACY_MODEL=en_core_web_md
echo ECET_SPACY_MODEL=en_core_web_md >> .env
```

Redaction recall drops with the smaller model; keep `lg` for anything that matters.

## 8. Known limitations (v1)

- **Auth is one static API key** plus an `X-Tenant-Id` header. Production would use per-tenant keys or JWT, and `retry-notify` would become tenant-scoped.
- **The mock client does not check the timestamp's freshness** — it verifies the HMAC over `timestamp.body`, but a captured delivery would verify forever. A real receiver must reject old timestamps. `X-ECET-Delivery` is fresh per attempt; dedupe on `claim_id` in the body.
- **No OCR.** A scanned PDF fails extraction with `no_text`.
- **No transactional outbox.** A publish failure leaves the claim `POLICIES_ATTACHED` until the same object arrives again; a crash between an external call and its write can leave a delivery the system has no record of.
- **No delayed retry** on the evaluation queue (see Dead letters).
- The OpenAI-compatible gateway is verified against mock transports only.

Everything deferred, and why: [roadmap](specs/06-roadmap.md#deferred-explicitly-out-of-v1).

## 9. Specs

| Spec | What's inside |
|---|---|
| [System overview & index](specs/00-overview.md#5-spec-index) | End-to-end flow, the seven ADRs, the index of every spec |
| [Domain](specs/01-domain/claim.md) | Claim + state machine, evaluation results, tenant policies |
| [Use cases](specs/02-use-cases/README.md) | UC-01…UC-09, one per pipeline step |
| [Infrastructure](specs/03-infrastructure/postgres.md) | Postgres, RabbitMQ, MinIO, PDF extractor, Presidio, LLM gateway, webhook client |
| [Interfaces](specs/04-interfaces/api.md) | HTTP API and the queue worker |
| [Platform](specs/05-platform/project-layout.md) | Layout, config, docker-compose, observability, testing |
| [Roadmap](specs/06-roadmap.md) | Build order, every carry-over, and what is out of v1 |
