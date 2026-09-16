# Phase 6 — Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every claim can be followed end to end from the outside: one `request_id` grepped from the HTTP request through the queue into the worker's logs, `/metrics` on both processes carrying the thirteen `ecet_*` series the observability spec lists, and a worker container that finally has a healthcheck. In the same pass, the two external calls that hold a Postgres connection open — the LLM call and the webhook POST — move outside the unit of work, on the api side and the worker side alike, and a pool gauge makes the result visible.

**Architecture:** One new top-level module, `src/ecet/metrics.py`, holds every metric object. It sits beside `config.py` rather than inside a layer because the ingestion metrics belong to use cases, the vendor metrics to adapters and the gauges to the worker — no single layer owns them, and `prometheus_client`'s default registry is process-global anyway, so there is nothing to inject. The api exposes it by mounting `prometheus_client.make_asgi_app()` at `/metrics`; the worker starts `prometheus_client.start_http_server(settings.metrics_port)` in its container build, which is also what its new compose healthcheck probes. `configure_logging` grows a stdlib bridge (`structlog.stdlib.ProcessorFormatter`) so uvicorn's own records pass through the ADR-001 redaction guard instead of around it, closing Phase 0 carry-over #12. `request_id` travels in `structlog.contextvars`, not in a parameter: an HTTP middleware binds it, the queue publisher reads it back out to stamp `x-request-id` on the message, and the consumer binds it again on the other side. The external-call move is a reshaping of three use cases into "read, then call, then write": `EvaluateClaim`, `ResolveReview` and `RetryNotify` each open a read-only unit of work, close it, do the slow thing with no connection held, and open a second unit of work for the only writes. `NotifyClient` stops reading the tenant and stops mutating the claim to make that possible — it takes a `Tenant` and returns a `Delivery` the caller applies to whichever claim it is about to save.

**Tech Stack:** Python 3.12, `prometheus-client` (new dependency), structlog 25 (`structlog.stdlib.ProcessorFormatter`, `structlog.contextvars`), FastAPI 0.141 (`app.mount`, an HTTP middleware), aio-pika 9.6 (message headers), SQLAlchemy 2 (`func.count()` group-bys, `engine.pool.checkedout`), pytest with the `tests/fakes.py` fakes and testcontainers.

**Spec:** [`specs/06-roadmap.md` §Phase 6](../../specs/06-roadmap.md#phase-6--observability--polish), narrowed to observability plus the external-call move (the polish half — E2E suite, README rewrite, the small test and naming gaps — becomes Phase 7; Task 12 rewrites the roadmap to say so). It pulls in [observability](../../specs/05-platform/observability.md), [config](../../specs/05-platform/config.md), [docker-compose](../../specs/05-platform/docker-compose.md), [testing](../../specs/05-platform/testing.md), [worker](../../specs/04-interfaces/worker.md), [api](../../specs/04-interfaces/api.md), [UC-06](../../specs/02-use-cases/UC-06-evaluate-claim.md), [UC-07](../../specs/02-use-cases/UC-07-route-decision.md), [UC-08](../../specs/02-use-cases/UC-08-notify-client.md) and [UC-09](../../specs/02-use-cases/UC-09-human-review.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- Git in this environment needs `export DEVELOPER_DIR=/Library/Developer/CommandLineTools` before any `git` command (Xcode license error otherwise).
- Layer rule (`import-linter`; `make imports` must stay green): `ecet.domain` imports stdlib + pydantic only. `ecet.application` imports domain + stdlib + pydantic, and **never** `openai`, `httpx`, `aio_pika`, `sqlalchemy`, `fastapi`, `typer` or `ecet.config`. Do not weaken the `forbidden` contract. `ecet.metrics` is new and deliberately outside the layer stack — application and infrastructure may import it, **the domain may not**, and Task 1 adds the contract that enforces that.
- `mypy --strict` covers `src/ecet/domain` and `src/ecet/application`. `mypy src/ecet` (repo-wide `disallow_untyped_defs = true`) covers infrastructure and interfaces. Every function added in this phase is annotated. `prometheus_client` ships type hints; no `ignore_missing_imports` override is needed.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`.
- **ADR-001 still governs, and this phase touches logging.** No metric may carry a label whose value is claim text, a reviewer's note, a key or a secret. Label cardinality stays bounded: `status`, `verdict`, `route`, `outcome`, `entity`, `direction`, `provider`, `model`, `status_class` and `tenant` are all small closed sets — a `claim_id` label is forbidden, and so is a per-URL or per-key label. Every test that touches text or a payload still ends with `assert_no_pii(...)`.
- **Log through `structlog` only** in application, infrastructure and interfaces code. Task 2 makes the stdlib route *through* structlog so third-party records are guarded too; that is a backstop, not a licence to call `logging.getLogger` in our own modules.
- **Metric names are the spec's, exactly.** `prometheus_client` strips a trailing `_total` from a `Counter`'s name and appends it back on exposition, so `Counter("ecet_llm_calls_total", ...)` is exposed as `ecet_llm_calls_total` — correct, and not a bug to "fix". Task 1's exposition test pins every name.
- **`SqlAlchemyUnitOfWork` is single-use.** Every `async with uow_factory()` block is one unit of work; a use case that needs two opens two.
- **`PostgresClaimRepository.save` refuses a claim it never read.** After Tasks 9–11, the claim that is saved is the one re-read in the *second* unit of work, never the object the first one loaded.
- TDD: every step pair is "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Commit after every task with a conventional prefix. **No Claude attribution in commit messages.**
- `make check` (lint, typecheck, imports, tests) is green at the end of every task.

### Explicitly out of scope for Phase 6 (do not add)

- The `observability` compose profile — Prometheus, Grafana, any dashboard JSON. The endpoints are the deliverable; scraping them is the operator's business (user's decision).
- The E2E suite, `tests/e2e/`, the CI `e2e` job and the worker's end-to-end demo assertions (Phase 7).
- The README rewrite: `/v1/reviews`, `retry-notify`, `make dlq-replay`, the image-size note, the `make spacy-model` mention, the static-API-key note, the mock client's missing freshness check, the `make clean`-after-topology-change note (Phase 7).
- The CI spaCy model cache and the pinned model version (Phase 7).
- The small test and naming gaps carried from Phases 3–5: `empty_text`/`no_text`, the `NO_POLICIES` failure reason, `handle_unmapped`'s missing `claim_id`, the `Whitfield` fixture, the `claims.save` argument assertion, the duplicate-status tests, `ObjectStorage.head()` against a missing bucket, the pdf perf budget, `test_migrations.py`'s hardcoded revision, the `build_container` engine leak, empty-probes `/readyz`, `RabbitMqConsumer.is_healthy` and `stop()`'s drain (all Phase 7).
- Distributed tracing, a delayed-retry queue, a transactional outbox, per-tenant API keys.
- Any new setting beyond what `config.py` already has. `metrics_port` (9100) exists and is what the worker binds.

---

### Task 1: `ecet/metrics.py`, the dependency, and `/metrics` on the api

**Files:**
- Create: `src/ecet/metrics.py`
- Create: `tests/unit/test_metrics.py`
- Create: `tests/api/test_metrics_route.py`
- Modify: `pyproject.toml`
- Modify: `src/ecet/interfaces/api/app.py`

**Interfaces:**
- Consumes: nothing.
- Produces: the module every later task imports as `from ecet import metrics`. Names, exactly:
  `metrics.INGEST_SECONDS` (Histogram, label `outcome`), `metrics.PDF_EXTRACT_SECONDS` (Histogram), `metrics.PII_REDACTION_SECONDS` (Histogram), `metrics.PII_ENTITIES_TOTAL` (Counter, label `entity`), `metrics.DETERMINISTIC_VERDICT_TOTAL` (Counter, label `verdict`), `metrics.LLM_CALLS_AVOIDED_TOTAL` (Counter), `metrics.LLM_CALLS_TOTAL` (Counter, labels `provider, model, outcome`), `metrics.LLM_LATENCY_SECONDS` (Histogram, labels `provider, model`), `metrics.LLM_TOKENS_TOTAL` (Counter, label `direction`), `metrics.TRIAGE_ROUTE_TOTAL` (Counter, label `route`), `metrics.WEBHOOK_ATTEMPTS_TOTAL` (Counter, label `status_class`), `metrics.CLAIMS_BY_STATUS` (Gauge, label `status`), `metrics.REVIEW_OPEN` (Gauge, label `tenant`), `metrics.DB_POOL_IN_USE` (Gauge), and `metrics.start_metrics_server(port: int) -> None`.

- [ ] **Step 1: Add the dependency**

In `pyproject.toml`, add to `dependencies` (keep the list alphabetical where it already is — append after `"openai>=1.60,<2",`):

```toml
    "prometheus-client>=0.21,<1",
```

Then:

```bash
uv sync
```

- [ ] **Step 2: Write the failing exposition test**

Create `tests/unit/test_metrics.py`:

```python
"""The metric names are a contract with whoever scrapes them, so they get pinned.

`prometheus_client` strips a trailing `_total` from a Counter's name and appends it
again on exposition; these assertions are what stops someone "fixing" that.
"""

from prometheus_client import REGISTRY, generate_latest

from ecet import metrics

SPEC_NAMES = (
    "ecet_ingest_seconds",
    "ecet_pdf_extract_seconds",
    "ecet_pii_redaction_seconds",
    "ecet_pii_entities_total",
    "ecet_deterministic_verdict_total",
    "ecet_llm_calls_avoided_total",
    "ecet_llm_calls_total",
    "ecet_llm_latency_seconds",
    "ecet_llm_tokens_total",
    "ecet_triage_route_total",
    "ecet_webhook_attempts_total",
    "ecet_claims_by_status",
    "ecet_review_open",
)


def test_every_metric_the_spec_lists_is_exposed_under_its_own_name() -> None:
    metrics.INGEST_SECONDS.labels(outcome="ingested").observe(0.01)
    metrics.PDF_EXTRACT_SECONDS.observe(0.01)
    metrics.PII_REDACTION_SECONDS.observe(0.01)
    metrics.PII_ENTITIES_TOTAL.labels(entity="PERSON").inc()
    metrics.DETERMINISTIC_VERDICT_TOTAL.labels(verdict="PASS").inc()
    metrics.LLM_CALLS_AVOIDED_TOTAL.inc()
    metrics.LLM_CALLS_TOTAL.labels(provider="fake", model="m", outcome="ok").inc()
    metrics.LLM_LATENCY_SECONDS.labels(provider="fake", model="m").observe(0.01)
    metrics.LLM_TOKENS_TOTAL.labels(direction="input").inc(3)
    metrics.TRIAGE_ROUTE_TOTAL.labels(route="AUTO_NOTIFY").inc()
    metrics.WEBHOOK_ATTEMPTS_TOTAL.labels(status_class="2xx").inc()
    metrics.CLAIMS_BY_STATUS.labels(status="QUEUED").set(2)
    metrics.REVIEW_OPEN.labels(tenant="tenant-a").set(1)

    exposition = generate_latest().decode("utf-8")

    for name in SPEC_NAMES:
        assert f"{name} " in exposition or f"{name}{{" in exposition, name


def test_the_pool_gauge_reads_a_callable() -> None:
    """`DB_POOL_IN_USE` is fed by `engine.pool.checkedout`, which is a function, not a
    number a caller remembers to set."""
    metrics.DB_POOL_IN_USE.set_function(lambda: 7.0)

    assert REGISTRY.get_sample_value("ecet_db_pool_in_use") == 7.0


def test_no_metric_carries_an_unbounded_label() -> None:
    """ADR-001 and cardinality: a claim id, a tenant's URL or a key must never become
    a label. `tenant` on `ecet_review_open` is the one identifier the spec allows."""
    forbidden = {"claim_id", "key", "url", "api_key", "text", "reviewer"}

    for collector in (
        metrics.INGEST_SECONDS,
        metrics.PII_ENTITIES_TOTAL,
        metrics.LLM_CALLS_TOTAL,
        metrics.TRIAGE_ROUTE_TOTAL,
        metrics.WEBHOOK_ATTEMPTS_TOTAL,
        metrics.CLAIMS_BY_STATUS,
        metrics.REVIEW_OPEN,
    ):
        assert not forbidden & set(collector._labelnames)
```

- [ ] **Step 3: Run it to watch it fail**

Run: `uv run pytest tests/unit/test_metrics.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.metrics'`.

- [ ] **Step 4: Write the module**

Create `src/ecet/metrics.py`:

```python
"""Every `ecet_*` metric, defined once.

Top-level, beside `config.py`, rather than inside a layer: the ingestion metrics
belong to use cases, the vendor metrics to adapters and the gauges to the worker, so
no layer owns them. There is nothing to inject either — `prometheus_client`'s default
registry is process-global, and a metric object is a handle on it.

`ecet.domain` must not import this module; `make imports` enforces it. The domain is
pure functions over pure models, and a counter is I/O with extra steps.

Counter names keep the spec's `_total` suffix: `prometheus_client` strips it from the
metric name and appends it back on exposition, so the scraped name is the spec's.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

INGEST_SECONDS = Histogram(
    "ecet_ingest_seconds",
    "UC-01 wall time, from the command to the stored claim.",
    ["outcome"],
)
PDF_EXTRACT_SECONDS = Histogram("ecet_pdf_extract_seconds", "PDF text extraction wall time.")
PII_REDACTION_SECONDS = Histogram("ecet_pii_redaction_seconds", "UC-02 redaction wall time.")
PII_ENTITIES_TOTAL = Counter(
    "ecet_pii_entities_total", "Entities redacted before any egress (ADR-001).", ["entity"]
)
DETERMINISTIC_VERDICT_TOTAL = Counter(
    "ecet_deterministic_verdict_total", "UC-04 verdicts (ADR-002).", ["verdict"]
)
LLM_CALLS_AVOIDED_TOTAL = Counter(
    "ecet_llm_calls_avoided_total",
    "Claims a deterministic REJECT sent straight to a human, so the vendor was never called.",
)
LLM_CALLS_TOTAL = Counter(
    "ecet_llm_calls_total", "Vendor evaluations attempted.", ["provider", "model", "outcome"]
)
LLM_LATENCY_SECONDS = Histogram(
    "ecet_llm_latency_seconds", "Vendor round-trip time.", ["provider", "model"]
)
LLM_TOKENS_TOTAL = Counter("ecet_llm_tokens_total", "Tokens billed by the vendor.", ["direction"])
TRIAGE_ROUTE_TOTAL = Counter("ecet_triage_route_total", "UC-07 routes taken (ADR-003).", ["route"])
WEBHOOK_ATTEMPTS_TOTAL = Counter(
    "ecet_webhook_attempts_total", "Webhook POSTs, by response class.", ["status_class"]
)
CLAIMS_BY_STATUS = Gauge(
    "ecet_claims_by_status", "Claims per status; the worker refreshes it.", ["status"]
)
REVIEW_OPEN = Gauge("ecet_review_open", "Open review tasks per tenant.", ["tenant"])
DB_POOL_IN_USE = Gauge(
    "ecet_db_pool_in_use",
    "Checked-out SQLAlchemy connections. A connection held across an LLM call or a "
    "webhook POST shows up here first.",
)


def start_metrics_server(port: int) -> None:
    """Serve `/metrics` on `port` from a daemon thread (the worker; the api mounts the
    ASGI app instead). `prometheus_client` owns the thread and the socket."""
    start_http_server(port)
```

- [ ] **Step 5: Run it to watch it pass**

Run: `uv run pytest tests/unit/test_metrics.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Write the failing api-route test**

Create `tests/api/test_metrics_route.py`:

```python
"""`/metrics` is unauthenticated on purpose: it is scraped by a sidecar inside the
compose network, it carries no claim content, and an API key in a Prometheus config
is a key in one more place."""

from tests.api.conftest import ApiHarness


async def test_metrics_is_served_without_an_api_key(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert "ecet_ingest_seconds" in response.text
    assert response.headers["content-type"].startswith("text/plain")


async def test_metrics_exposes_no_claim_text(harness: ApiHarness) -> None:
    """Belt and braces for ADR-001: the exposition is a public-ish surface, and the
    only way text could reach it is a label somebody added by mistake."""
    async with harness.client() as client:
        response = await client.get("/metrics")

    assert "claim_id" not in response.text
```

- [ ] **Step 7: Run it to watch it fail**

Run: `uv run pytest tests/api/test_metrics_route.py -v`
Expected: FAIL — 404, `/metrics` is not mounted.

- [ ] **Step 8: Mount it on the api**

In `src/ecet/interfaces/api/app.py`, add the import and the mount:

```python
from prometheus_client import make_asgi_app
```

and, in `create_app`, after `app.include_router(reviews.router)`:

```python
    # Mounted rather than routed: `make_asgi_app` is a complete ASGI app, and mounting
    # keeps the exposition format (and its content type) out of FastAPI's hands.
    app.mount("/metrics", make_asgi_app())
```

- [ ] **Step 9: Run it to watch it pass**

Run: `uv run pytest tests/api/test_metrics_route.py -v`
Expected: PASS (2 tests).

- [ ] **Step 10: Forbid the domain from importing metrics**

In `pyproject.toml`, append a third import-linter contract:

```toml
[[tool.importlinter.contracts]]
name = "The domain stays metrics-free"
type = "forbidden"
source_modules = ["ecet.domain"]
forbidden_modules = ["prometheus_client", "ecet.metrics"]
```

Run: `uv run lint-imports`
Expected: all contracts KEPT.

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml uv.lock src/ecet/metrics.py src/ecet/interfaces/api/app.py tests/unit/test_metrics.py tests/api/test_metrics_route.py
git commit -m "feat(metrics): the ecet_* registry and /metrics on the api"
```

---

### Task 2: stdlib logs through structlog, and complete transition lines

**Files:**
- Modify: `src/ecet/infrastructure/observability/logging.py`
- Modify: `src/ecet/application/use_cases/ingest_claim_document.py`
- Modify: `src/ecet/application/use_cases/evaluate_claim.py`
- Modify: `src/ecet/application/use_cases/route_decision.py`
- Test: `tests/unit/test_logging.py`, `tests/unit/application/test_ingest_claim_document.py`

**Interfaces:**
- Consumes: `drop_sensitive_fields`, `SENSITIVE_FIELDS` (existing).
- Produces: `configure_logging(level: str) -> None` with the same signature, now also installing a stdlib root handler that renders through the same processor chain. Every `claim.transition` / `claim.failed` log line carries `from`, `to` and (for failures) `reason`, as the [observability spec](../../specs/05-platform/observability.md#logging) requires.

This closes Phase 0 carry-over #12: `cli.py api` passes `log_config=None` to uvicorn, so `uvicorn.error` and `uvicorn.access` records go to the stdlib root handler. Today that handler is `logging.basicConfig`'s, which never sees `drop_sensitive_fields`.

- [ ] **Step 1: Write the failing stdlib-bridge test**

Append to `tests/unit/test_logging.py`:

```python
import logging


def test_stdlib_records_are_rendered_as_json_through_the_guard(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Phase 0 carry-over #12: `cli.py api` hands uvicorn `log_config=None`, so
    `uvicorn.access` logs through the stdlib root handler. That handler has to be
    structlog's, or ADR-001's guard is bypassed by every third-party library."""
    configure_logging("INFO")

    logging.getLogger("uvicorn.access").info(
        "request finished", extra={"text": "Patient John Doe", "path": "/v1/claims/ingest"}
    )

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "request finished"
    assert payload["logger"] == "uvicorn.access"
    assert payload["path"] == "/v1/claims/ingest"
    assert "text" not in payload
    assert "John Doe" not in line


def test_the_bound_request_id_reaches_a_stdlib_record(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    structlog.contextvars.bind_contextvars(request_id="req-1")
    try:
        logging.getLogger("uvicorn.error").warning("startup complete")
    finally:
        structlog.contextvars.clear_contextvars()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["request_id"] == "req-1"
```

- [ ] **Step 2: Run it to watch it fail**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: FAIL — the stdlib record renders as plain text (`json.JSONDecodeError`), and `text` survives.

- [ ] **Step 3: Rewrite `configure_logging`**

Replace the body of `src/ecet/infrastructure/observability/logging.py` below `drop_sensitive_fields` with:

```python
def configure_logging(level: str) -> None:
    """Configure structlog *and* the stdlib root logger, so a third-party record
    (uvicorn's, above all) is rendered by the same chain — and dropped by the same
    ADR-001 guard — as one of ours. `cli.py api` passes `log_config=None` to uvicorn
    precisely so that uvicorn does not install a handler of its own here."""
    numeric_level = logging.getLevelNamesMapping()[level.upper()]
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    structlog.configure(
        # `drop_sensitive_fields` runs last on our own records: every other processor
        # has had its say, so nothing can reintroduce a dropped key afterwards.
        processors=[
            *shared,
            drop_sensitive_fields,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # `ExtraAdder` first: `logger.info(..., extra={"text": ...})` has to become an
        # event-dict key before `drop_sensitive_fields` can drop it.
        foreign_pre_chain=[structlog.stdlib.ExtraAdder(), *shared, drop_sensitive_fields],
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            drop_sensitive_fields,
            structlog.processors.JSONRenderer(),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(numeric_level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        # Own handlers would render around ours; propagation is what we want instead.
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True
```

and the imports at the top of the module:

```python
import logging
import sys
from typing import Any

import structlog
from structlog.typing import EventDict, Processor
```

Note: `structlog.PrintLoggerFactory` and `logging.basicConfig` are both gone — every record now leaves through the one stdlib handler.

- [ ] **Step 4: Run the logging tests**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: PASS, including the two pre-existing tests (`test_configure_logging_emits_json_without_secrets` still reads the last stdout line; the handler writes to `sys.stdout`, which `capsys` owns).

- [ ] **Step 5: Write the failing transition-line test**

Append to `tests/unit/application/test_ingest_claim_document.py` (it already builds a working `IngestClaimDocument`; reuse that module's harness/fixtures — do not invent a new one):

```python
async def test_a_transition_log_line_names_where_the_claim_came_from(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The observability spec asks for one `claim.transition` per state change, with
    `from`, `to` and `reason` — `to` alone does not say what moved."""
    configure_logging("INFO")

    await build_use_case().execute(build_command())

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    transitions = [line for line in lines if line["event"] == "claim.transition"]
    assert transitions, "no claim.transition line was logged"
    assert all(line["from"] and line["to"] for line in transitions)
    assert transitions[0]["from"] == "RECEIVED"
    assert transitions[0]["to"] == "EXTRACTED"
```

(`build_use_case()` / `build_command()` are that test module's existing helpers — use whatever names it already defines; add `import json` and the `configure_logging` import at the top.)

- [ ] **Step 6: Run it to watch it fail**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py -v -k transition_log`
Expected: FAIL — `KeyError: 'from'`.

- [ ] **Step 7: Add `from` to every transition line**

`from` is a Python keyword, so it goes in as a dict. In `ingest_claim_document.py`:

```python
    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        previous = claim.status
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            **{"from": previous.value},
        )

    async def _fail(self, uow: UnitOfWork, claim: Claim, status: ClaimStatus, reason: str) -> None:
        previous = claim.status
        claim.transition(status, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            reason=reason,
            **{"from": previous.value},
        )
        await uow.claims.save(claim)
        await uow.commit()
```

Apply the same `previous = claim.status` + `**{"from": previous.value}` shape to the three other transition log sites: `evaluate_claim.py` (the `EVALUATED` line and `_fail`), and `route_decision.py` (`_advance` and `_fail`).

- [ ] **Step 8: Run the suite**

Run: `uv run pytest`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/ecet/infrastructure/observability/logging.py src/ecet/application/use_cases/ingest_claim_document.py src/ecet/application/use_cases/evaluate_claim.py src/ecet/application/use_cases/route_decision.py tests/unit/test_logging.py tests/unit/application/test_ingest_claim_document.py
git commit -m "feat(logging): route stdlib records through structlog and name the from-state"
```

---

### Task 3: `request_id` from the HTTP request to the worker's logs

**Files:**
- Create: `src/ecet/interfaces/api/middleware.py`
- Create: `tests/api/test_request_id.py`
- Modify: `src/ecet/interfaces/api/app.py`
- Modify: `src/ecet/infrastructure/queue/rabbitmq.py`
- Test: `tests/unit/infrastructure/test_rabbitmq_consumer.py`

**Interfaces:**
- Consumes: `structlog.contextvars`.
- Produces:
  - `ecet.interfaces.api.middleware.REQUEST_ID_HEADER = "x-request-id"` and `async def bind_request_id(request, call_next) -> Response`.
  - `RabbitMqEvaluationQueue.publish` stamps `x-request-id` on the message when one is bound.
  - `RabbitMqConsumer._on_message` binds `request_id`, `message_id`, `claim_id` and `tenant_id` into the structlog context for the duration of one delivery.

- [ ] **Step 1: Write the failing api test**

Create `tests/api/test_request_id.py`:

```python
"""One id per request, echoed back. The [observability spec](../../specs/05-platform/observability.md)
calls tracing out of scope and this in: enough to grep one claim end to end."""

import json

import pytest
import structlog
from tests.api.conftest import ApiHarness

from ecet.infrastructure.observability.logging import configure_logging


async def test_a_caller_supplied_request_id_is_echoed(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/healthz", headers={"X-Request-Id": "req-from-caller"})

    assert response.headers["x-request-id"] == "req-from-caller"


async def test_a_request_without_one_gets_a_generated_id(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/healthz")

    assert len(response.headers["x-request-id"]) == 36  # uuid4


async def test_the_request_id_is_bound_to_every_log_line_of_that_request(
    harness: ApiHarness, api_headers: dict[str, str], capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("INFO")

    async with harness.client() as client:
        await client.post(
            "/v1/claims/ingest",
            headers={**api_headers, "X-Request-Id": "req-1"},
            json={"bucket": "claims", "key": "tenants/tenant-a/claims/note-1.pdf"},
        )

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    transitions = [line for line in lines if line["event"] == "claim.transition"]
    assert transitions, "the ingest logged no transition"
    assert all(line["request_id"] == "req-1" for line in transitions)


async def test_the_context_does_not_leak_between_requests(harness: ApiHarness) -> None:
    async with harness.client() as client:
        await client.get("/healthz", headers={"X-Request-Id": "req-1"})

    assert "request_id" not in structlog.contextvars.get_contextvars()
```

- [ ] **Step 2: Run it to watch it fail**

Run: `uv run pytest tests/api/test_request_id.py -v`
Expected: FAIL — `KeyError: 'x-request-id'`.

- [ ] **Step 3: Write the middleware and wire it**

Create `src/ecet/interfaces/api/middleware.py`:

```python
"""Request context.

`request_id` is carried in `structlog.contextvars`, not in a parameter, because the
one place it has to reach besides the log line is three layers down: the queue
publisher stamps it on the message so the worker's lines carry the same id. Threading
a field only logging reads through every use case signature would be worse.
"""

from collections.abc import Awaitable, Callable
from uuid import uuid4

import structlog
from fastapi import Request, Response

REQUEST_ID_HEADER = "x-request-id"


async def bind_request_id(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Bind one id for the request and echo it back, so a caller reporting a problem
    can quote the same string an operator greps for."""
    request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    try:
        response = await call_next(request)
    finally:
        # Starlette runs middleware in a task per request, but the context is copied,
        # not owned: clearing keeps a pooled task from inheriting a stale id.
        structlog.contextvars.clear_contextvars()
    response.headers[REQUEST_ID_HEADER] = request_id
    return response
```

In `src/ecet/interfaces/api/app.py`, inside `create_app`, right after `app.state.settings = settings`:

```python
    app.middleware("http")(bind_request_id)
```

with `from ecet.interfaces.api.middleware import bind_request_id` at the top.

- [ ] **Step 4: Run it to watch it pass**

Run: `uv run pytest tests/api/test_request_id.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Write the failing publisher and consumer tests**

Append to `tests/unit/infrastructure/test_rabbitmq_consumer.py`:

```python
async def test_the_consumer_binds_the_message_context_for_the_handler() -> None:
    """`request_id`/`message_id`, `claim_id`, `tenant_id` — the observability spec's
    per-message bound context, so a worker line can be grepped by the same id the api
    answered with."""
    seen: dict[str, object] = {}

    async def handle(message: EvaluationMessage) -> None:
        seen.update(structlog.contextvars.get_contextvars())

    consumer = build_consumer(handle)
    message = build_message()
    delivery = _Delivery(message.model_dump_json().encode("utf-8"))
    delivery.headers = {"x-request-id": "req-1"}

    await consumer._on_message(cast(Any, delivery))

    assert seen["request_id"] == "req-1"
    assert seen["claim_id"] == str(message.claim_id)
    assert seen["tenant_id"] == str(message.tenant_id)
    assert seen["message_id"] == str(message.message_id)
    # Cleared afterwards: the next delivery is a different claim.
    assert structlog.contextvars.get_contextvars() == {}


async def test_a_delivery_without_a_request_id_binds_no_null() -> None:
    """A message published outside an HTTP request (a dlq replay) has no id; a
    `request_id: null` field in the logs would be worse than its absence."""
    seen: dict[str, object] = {}

    async def handle(message: EvaluationMessage) -> None:
        seen.update(structlog.contextvars.get_contextvars())

    delivery = _Delivery(build_message().model_dump_json().encode("utf-8"))

    await build_consumer(handle)._on_message(cast(Any, delivery))

    assert "request_id" not in seen
```

(add `import structlog` to that module's imports.)

Create `tests/unit/infrastructure/test_rabbitmq_publish_headers.py`:

```python
"""The publisher stamps the bound `request_id` on the message. Driven against a stub
exchange: what is under test is the header, not the broker."""

from typing import Any
from uuid import uuid4

import structlog
from tests.unit.interfaces.test_worker_handler import build_message

from ecet.infrastructure.queue.rabbitmq import RabbitMqEvaluationQueue


class _Exchange:
    def __init__(self) -> None:
        self.published: list[Any] = []

    async def publish(self, message: Any, routing_key: str) -> None:
        self.published.append(message)


def build_queue(exchange: _Exchange) -> RabbitMqEvaluationQueue:
    queue = RabbitMqEvaluationQueue("amqp://unused/")
    queue._exchange = exchange  # type: ignore[assignment]  # the stub is the seam
    return queue


async def test_publish_stamps_the_bound_request_id() -> None:
    exchange = _Exchange()
    structlog.contextvars.bind_contextvars(request_id="req-1")
    try:
        await build_queue(exchange).publish(build_message())
    finally:
        structlog.contextvars.clear_contextvars()

    assert exchange.published[0].headers["x-request-id"] == "req-1"


async def test_publish_outside_a_request_sets_no_header() -> None:
    exchange = _Exchange()

    await build_queue(exchange).publish(build_message())

    assert "x-request-id" not in exchange.published[0].headers
```

- [ ] **Step 6: Run them to watch them fail**

Run: `uv run pytest tests/unit/infrastructure/test_rabbitmq_publish_headers.py tests/unit/infrastructure/test_rabbitmq_consumer.py -v`
Expected: FAIL — no `x-request-id` header, and `get_contextvars()` is empty inside the handler.

- [ ] **Step 7: Implement both ends**

In `src/ecet/infrastructure/queue/rabbitmq.py`, add a module-level helper and use it in `publish`:

```python
REQUEST_ID_HEADER = "x-request-id"


def _request_id() -> str | None:
    """The id the api bound for this request, if this publish happens inside one."""
    value = structlog.contextvars.get_contextvars().get("request_id")
    return str(value) if value is not None else None
```

and in `RabbitMqEvaluationQueue.publish`, build the headers first:

```python
        headers: dict[str, str] = {
            "x-tenant-id": str(message.tenant_id),
            "x-schema-version": str(message.schema_version),
        }
        request_id = _request_id()
        if request_id is not None:
            headers[REQUEST_ID_HEADER] = request_id
```

and pass `headers=headers` to `Message(...)`.

Add `REQUEST_ID_HEADER` to `REPLAYED_HEADERS` so a replayed message keeps the id that produced it:

```python
REPLAYED_HEADERS: tuple[str, ...] = ("x-tenant-id", "x-schema-version", REQUEST_ID_HEADER)
```

In `RabbitMqConsumer._on_message`, bind after the body validates and clear in the `finally`:

```python
            context: dict[str, str] = {
                "message_id": str(parsed.message_id),
                "claim_id": str(parsed.claim_id),
                "tenant_id": str(parsed.tenant_id),
            }
            request_id = (message.headers or {}).get(REQUEST_ID_HEADER)
            if request_id is not None:
                context["request_id"] = str(request_id)
            structlog.contextvars.bind_contextvars(**context)
```

and in the existing outer `finally`, before the in-flight bookkeeping:

```python
            structlog.contextvars.clear_contextvars()
```

- [ ] **Step 8: Run them to watch them pass**

Run: `uv run pytest tests/unit/infrastructure -v`
Expected: PASS.

- [ ] **Step 9: Full suite and commit**

```bash
uv run pytest
git add src/ecet/interfaces/api/middleware.py src/ecet/interfaces/api/app.py src/ecet/infrastructure/queue/rabbitmq.py tests/api/test_request_id.py tests/unit/infrastructure/test_rabbitmq_consumer.py tests/unit/infrastructure/test_rabbitmq_publish_headers.py
git commit -m "feat(observability): carry request_id from the api through the queue into the worker"
```

---

### Task 4: the ingestion path's metrics

**Files:**
- Modify: `src/ecet/application/use_cases/ingest_claim_document.py`
- Modify: `src/ecet/application/use_cases/redact_pii.py`
- Modify: `src/ecet/application/use_cases/run_deterministic_checks.py`
- Modify: `src/ecet/infrastructure/pdf/pypdf_extractor.py`
- Test: `tests/unit/application/test_ingest_claim_document.py`, `tests/unit/application/test_redact_pii.py`, `tests/unit/application/test_run_deterministic_checks.py`, `tests/unit/infrastructure/test_pypdf_extractor.py`

**Interfaces:**
- Consumes: `ecet.metrics` (Task 1).
- Produces: `IngestClaimDocument.execute` keeps its signature; its body moves to `_execute`. Nothing else changes shape.

Metric assertions read the registry through `prometheus_client.REGISTRY.get_sample_value(name, labels)`. Counters are process-global and tests share a process, so **every assertion is on a delta**, never an absolute: read the value before, read it after, assert the difference.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/application/test_run_deterministic_checks.py`:

```python
from prometheus_client import REGISTRY


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_a_verdict_is_counted() -> None:
    before = sample("ecet_deterministic_verdict_total", verdict="PASS")

    result = await RunDeterministicChecks().execute(build_redacted(), [build_policy()])

    assert result.verdict is Verdict.PASS
    assert sample("ecet_deterministic_verdict_total", verdict="PASS") == before + 1


async def test_a_reject_counts_an_avoided_llm_call() -> None:
    """ADR-002's whole argument, as a number: a deterministic REJECT never reaches
    the vendor."""
    before = sample("ecet_llm_calls_avoided_total")

    result = await RunDeterministicChecks().execute(build_excluded_redacted(), [build_policy()])

    assert result.verdict is Verdict.REJECT
    assert sample("ecet_llm_calls_avoided_total") == before + 1
```

(`build_redacted()`, `build_excluded_redacted()` and `build_policy()` are that module's existing helpers — use the names it already has; if it builds its inputs inline, build them inline the same way.)

Append to `tests/unit/application/test_redact_pii.py`:

```python
async def test_redaction_counts_its_entities_and_its_time() -> None:
    before_person = sample("ecet_pii_entities_total", entity="PERSON")
    before_count = sample("ecet_pii_redaction_seconds_count")

    await RedactPii(FakePiiRedactor()).execute(read_note("meets"))

    assert sample("ecet_pii_entities_total", entity="PERSON") > before_person
    assert sample("ecet_pii_redaction_seconds_count") == before_count + 1
```

Append to `tests/unit/application/test_ingest_claim_document.py`:

```python
async def test_an_ingest_is_timed_under_its_outcome() -> None:
    before_ok = sample("ecet_ingest_seconds_count", outcome="ingested")
    before_dup = sample("ecet_ingest_seconds_count", outcome="duplicate")

    use_case = build_use_case()
    await use_case.execute(build_command())
    await use_case.execute(build_command())  # same object: ADR-006 duplicate

    assert sample("ecet_ingest_seconds_count", outcome="ingested") == before_ok + 1
    assert sample("ecet_ingest_seconds_count", outcome="duplicate") == before_dup + 1


async def test_a_failed_ingest_is_timed_as_failed() -> None:
    before = sample("ecet_ingest_seconds_count", outcome="failed")

    with pytest.raises(ExtractionFailed):
        await build_use_case(extractor=FakeTextExtractor(error=ExtractionFailed("no_text"))).execute(
            build_command()
        )

    assert sample("ecet_ingest_seconds_count", outcome="failed") == before + 1
```

Append to `tests/unit/infrastructure/test_pypdf_extractor.py`:

```python
async def test_extraction_is_timed() -> None:
    before = sample("ecet_pdf_extract_seconds_count")

    await PypdfTextExtractor(max_pages=50).extract(read_pdf("note_simple.pdf"))

    assert sample("ecet_pdf_extract_seconds_count") == before + 1
```

(each of these four modules needs the `REGISTRY` import and the small `sample()` helper above; copy it rather than sharing it — four lines beats a fixture module.)

- [ ] **Step 2: Run them to watch them fail**

Run: `uv run pytest tests/unit/application tests/unit/infrastructure/test_pypdf_extractor.py -v`
Expected: FAIL — every `sample(...)` stays at its "before" value.

- [ ] **Step 3: Instrument UC-04**

`src/ecet/application/use_cases/run_deterministic_checks.py`:

```python
from ecet import metrics
from ecet.domain.evaluation import DeterministicResult, Verdict


class RunDeterministicChecks:
    async def execute(
        self, redacted: RedactedText, policies: Sequence[Policy]
    ) -> DeterministicResult:
        # `async` with nothing to await: the uniform `await use_case.execute(...)` call
        # shape in UC-01 is worth more than saving this frame.
        result = run_checks(redacted, policies)
        metrics.DETERMINISTIC_VERDICT_TOTAL.labels(verdict=result.verdict.value).inc()
        if result.verdict is Verdict.REJECT:
            # ADR-002's saving, counted: this claim goes to a human and the vendor is
            # never called.
            metrics.LLM_CALLS_AVOIDED_TOTAL.inc()
        return result
```

(and drop the docstring's "so Phase 6 has one place to increment…" clause — it happened.)

- [ ] **Step 4: Instrument UC-02**

`src/ecet/application/use_cases/redact_pii.py`:

```python
    async def execute(self, text: str) -> RedactedText:
        """`text` is raw and must not be logged, stored or returned — only the
        `RedactedText` leaves this call."""
        with metrics.PII_REDACTION_SECONDS.time():
            redacted = await self._redactor.redact(text)
        for entity, count in redacted.entity_counts.items():
            metrics.PII_ENTITIES_TOTAL.labels(entity=entity).inc(count)
        return redacted
```

- [ ] **Step 5: Instrument UC-01 and the extractor**

In `ingest_claim_document.py`, rename the existing `execute` to `_execute` (body unchanged) and add:

```python
    async def execute(self, command: IngestCommand) -> IngestResult:
        started = time.perf_counter()
        try:
            result = await self._execute(command)
        except BaseException:
            metrics.INGEST_SECONDS.labels(outcome="failed").observe(time.perf_counter() - started)
            raise
        outcome = "duplicate" if result.duplicate else "ingested"
        metrics.INGEST_SECONDS.labels(outcome=outcome).observe(time.perf_counter() - started)
        return result
```

with `import time` and `from ecet import metrics` at the top.

In `pypdf_extractor.py`:

```python
    async def extract(self, pdf: bytes) -> str:
        with metrics.PDF_EXTRACT_SECONDS.time():
            return await anyio.to_thread.run_sync(self._extract, pdf)
```

- [ ] **Step 6: Run them to watch them pass**

Run: `uv run pytest tests/unit -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/ecet/application/use_cases src/ecet/infrastructure/pdf/pypdf_extractor.py tests/unit
git commit -m "feat(metrics): instrument the ingestion path"
```

---

### Task 5: the evaluation path's metrics

**Files:**
- Modify: `src/ecet/infrastructure/llm/openai_gateway.py`
- Modify: `src/ecet/infrastructure/llm/fake_gateway.py`
- Modify: `src/ecet/application/use_cases/route_decision.py`
- Modify: `src/ecet/infrastructure/webhook/httpx_client.py`
- Test: `tests/unit/infrastructure/test_openai_gateway.py`, `tests/unit/infrastructure/test_fake_gateway.py`, `tests/unit/application/test_route_decision.py`, `tests/unit/infrastructure/test_httpx_webhook_client.py`

**Interfaces:**
- Consumes: `ecet.metrics`.
- Produces: no signature changes. Label vocabularies, fixed here and depended on by nothing else: `outcome ∈ {ok, invalid_output, permanent, transient}`, `direction ∈ {input, output}`, `status_class ∈ {2xx, 4xx, 5xx, error}`, `provider ∈ {openai, fake}`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/infrastructure/test_openai_gateway.py` (reuse that module's existing `MockTransport` builders):

```python
async def test_a_successful_call_counts_latency_and_tokens() -> None:
    before_calls = sample("ecet_llm_calls_total", provider="openai", model="test-model", outcome="ok")
    before_input = sample("ecet_llm_tokens_total", direction="input")

    await build_gateway(respond_with_tool_call()).evaluate(build_evaluation_request())

    assert (
        sample("ecet_llm_calls_total", provider="openai", model="test-model", outcome="ok")
        == before_calls + 1
    )
    assert sample("ecet_llm_tokens_total", direction="input") > before_input
    assert sample("ecet_llm_latency_seconds_count", provider="openai", model="test-model") >= 1


async def test_a_rate_limit_counts_a_transient_outcome() -> None:
    before = sample("ecet_llm_calls_total", provider="openai", model="test-model", outcome="transient")

    with pytest.raises(LLMTransientError):
        await build_gateway(respond_with_status(429)).evaluate(build_evaluation_request())

    assert (
        sample("ecet_llm_calls_total", provider="openai", model="test-model", outcome="transient")
        == before + 1
    )


async def test_an_unusable_body_counts_an_invalid_output() -> None:
    before = sample(
        "ecet_llm_calls_total", provider="openai", model="test-model", outcome="invalid_output"
    )

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(respond_without_tool_call()).evaluate(build_evaluation_request())

    assert (
        sample("ecet_llm_calls_total", provider="openai", model="test-model", outcome="invalid_output")
        == before + 1
    )
```

Append to `tests/unit/infrastructure/test_fake_gateway.py`:

```python
async def test_the_fake_gateway_counts_itself_as_a_call() -> None:
    """The compose default is the fake, and a demo with an empty `ecet_llm_calls_total`
    reads as a broken pipeline."""
    before = sample(
        "ecet_llm_calls_total", provider="fake", model="fake-deterministic", outcome="ok"
    )

    await FakeLlmGateway().evaluate(build_evaluation_request())

    assert (
        sample("ecet_llm_calls_total", provider="fake", model="fake-deterministic", outcome="ok")
        == before + 1
    )
```

Append to `tests/unit/application/test_route_decision.py`:

```python
async def test_the_route_is_counted() -> None:
    before = sample("ecet_triage_route_total", route="AUTO_NOTIFY")

    await build_route_decision().execute(build_evaluated_claim())

    assert sample("ecet_triage_route_total", route="AUTO_NOTIFY") == before + 1
```

Append to `tests/unit/infrastructure/test_httpx_webhook_client.py`:

```python
async def test_every_attempt_is_counted_by_response_class() -> None:
    before_2xx = sample("ecet_webhook_attempts_total", status_class="2xx")
    before_5xx = sample("ecet_webhook_attempts_total", status_class="5xx")
    responses = iter([httpx.Response(500), httpx.Response(200)])

    client = build_client(lambda request: next(responses), max_attempts=2, backoff_seconds=(0.0,))
    await client.deliver(build_tenant(), build_payload())

    assert sample("ecet_webhook_attempts_total", status_class="5xx") == before_5xx + 1
    assert sample("ecet_webhook_attempts_total", status_class="2xx") == before_2xx + 1


async def test_a_transport_failure_counts_as_error() -> None:
    before = sample("ecet_webhook_attempts_total", status_class="error")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(WebhookTransientError):
        await build_client(refuse, max_attempts=1).deliver(build_tenant(), build_payload())

    assert sample("ecet_webhook_attempts_total", status_class="error") == before + 1
```

(each module gets the same four-line `sample()` helper; `build_client`, `build_tenant`, `build_payload`, `build_route_decision`, `build_evaluated_claim` are the existing helpers in those modules — match whatever they are actually called.)

- [ ] **Step 2: Run them to watch them fail**

Run: `uv run pytest tests/unit -v -k "counts or counted"`
Expected: FAIL on every new test.

- [ ] **Step 3: Instrument the OpenAI gateway**

In `openai_gateway.py`, wrap the call and the parse. The outcome label is set on every exit:

```python
    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        messages = cast(list[ChatCompletionMessageParam], build_messages(request))
        started = time.perf_counter()
        try:
            completion = await self._client.chat.completions.create(...)  # unchanged
        except (APITimeoutError, APIConnectionError, RateLimitError, InternalServerError) as error:
            self._count(outcome="transient")
            raise LLMTransientError(f"{type(error).__name__}: {error}") from error
        except APIStatusError as error:
            if error.status_code in PERMANENT_STATUS:
                self._count(outcome="permanent")
                raise LLMPermanentError(f"status {error.status_code}") from error
            self._count(outcome="transient")
            raise LLMTransientError(f"status {error.status_code}") from error
        latency_ms = int((time.perf_counter() - started) * 1000)

        try:
            output = _parse(completion)
        except LLMInvalidOutput:
            self._count(outcome="invalid_output")
            raise
        ...
```

and after `evaluation = output.to_evaluation(...)`, before the log line:

```python
        self._count(outcome="ok", model=evaluation.model)
        metrics.LLM_LATENCY_SECONDS.labels(provider=PROVIDER, model=evaluation.model).observe(
            latency_ms / 1000
        )
        metrics.LLM_TOKENS_TOTAL.labels(direction="input").inc(evaluation.input_tokens)
        metrics.LLM_TOKENS_TOTAL.labels(direction="output").inc(evaluation.output_tokens)
```

with, at module level and on the class:

```python
PROVIDER = "openai"


    def _count(self, *, outcome: str, model: str | None = None) -> None:
        """The failure paths have no completion to read a model off, so they label the
        configured one."""
        metrics.LLM_CALLS_TOTAL.labels(
            provider=PROVIDER, model=model or self._model, outcome=outcome
        ).inc()
```

`self._model` must exist — it already does.

- [ ] **Step 4: Instrument the fake gateway, UC-07 and the webhook client**

In `fake_gateway.py`, at the end of `evaluate`, before the return:

```python
        metrics.LLM_CALLS_TOTAL.labels(provider="fake", model=self.MODEL, outcome="ok").inc()
        metrics.LLM_LATENCY_SECONDS.labels(provider="fake", model=self.MODEL).observe(0.0)
```

In `route_decision.py`, after `route = triage(evaluation, self._threshold)`:

```python
        metrics.TRIAGE_ROUTE_TOTAL.labels(route=route.value).inc()
```

In `httpx_client.py`, inside the attempt loop:

```python
            try:
                response = await self._client.post(url, content=body, headers=headers)
            except httpx.TransportError as error:
                metrics.WEBHOOK_ATTEMPTS_TOTAL.labels(status_class="error").inc()
                last = type(error).__name__
            else:
                status = response.status_code
                metrics.WEBHOOK_ATTEMPTS_TOTAL.labels(status_class=f"{status // 100}xx").inc()
```

- [ ] **Step 5: Run them to watch them pass**

Run: `uv run pytest tests/unit -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/ecet tests/unit
git commit -m "feat(metrics): instrument the vendor call, the route and every webhook attempt"
```

---

### Task 6: the worker's `/metrics` server, the pool gauge, and the worker healthcheck

**Files:**
- Modify: `src/ecet/interfaces/worker/container.py`
- Modify: `src/ecet/interfaces/api/container.py`
- Modify: `docker-compose.yml`
- Test: `tests/unit/test_compose.py`, `tests/unit/test_metrics.py`

**Interfaces:**
- Consumes: `metrics.start_metrics_server`, `metrics.DB_POOL_IN_USE`, `settings.metrics_port` (already 9100).
- Produces: the worker container binds `:9100` at build time; both containers feed `DB_POOL_IN_USE` from their own engine. This closes Phase 0 carry-over #5.

The server starts in `build_container`, not in `run()`: `run()` is called by `tests/unit/test_worker_main.py` with an injected container, and a unit test must not bind a port.

- [ ] **Step 1: Write the failing compose test**

Append to `tests/unit/test_compose.py`:

```python
def test_the_worker_has_a_healthcheck_against_its_metrics_port(compose: dict[str, Any]) -> None:
    """Phase 0 carry-over #5: the worker had no healthcheck because it had nothing to
    probe. `/metrics` on 9100 is that something."""
    probe = " ".join(compose["services"]["worker"]["healthcheck"]["test"])

    assert "9100" in probe
    assert "/metrics" in probe


def test_the_worker_metrics_port_is_not_published(compose: dict[str, Any]) -> None:
    """Scraped from inside the compose network; publishing it to the host is one more
    listening socket for nothing."""
    assert "ports" not in compose["services"]["worker"]
```

- [ ] **Step 2: Run it to watch it fail**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: FAIL — `KeyError: 'healthcheck'`.

- [ ] **Step 3: Add the healthcheck**

In `docker-compose.yml`, in the `worker` service, after `depends_on`:

```yaml
    healthcheck:
      # The worker serves `/metrics` on 9100 (ECET_METRICS_PORT). It is the only socket
      # the worker listens on, and a worker whose event loop is wedged stops answering
      # it — which is exactly what "unhealthy" should mean here.
      test:
        - CMD
        - python
        - -c
        - "import urllib.request; urllib.request.urlopen('http://localhost:9100/metrics')"
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
```

- [ ] **Step 4: Run it to watch it pass**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: PASS.

- [ ] **Step 5: Start the server and feed the pool gauge**

In `src/ecet/interfaces/worker/container.py`, inside `build_container`, right after `engine = create_engine(...)`:

```python
    # Started here rather than in `run()`: `run()` takes an injected container in tests,
    # and a unit test must not bind a port.
    start_metrics_server(settings.metrics_port)
    metrics.DB_POOL_IN_USE.set_function(engine.pool.checkedout)
```

with `from ecet import metrics` and `from ecet.metrics import start_metrics_server`.

In `src/ecet/interfaces/api/container.py`, after its own `engine = create_engine(...)`:

```python
    # The api's `/metrics` is mounted in `create_app`; this is the gauge behind it that
    # shows a connection held across an external call.
    metrics.DB_POOL_IN_USE.set_function(engine.pool.checkedout)
```

- [ ] **Step 6: Run the suite and commit**

```bash
uv run pytest
git add src/ecet/interfaces docker-compose.yml tests/unit/test_compose.py
git commit -m "feat(worker): serve /metrics on 9100 and give the container a healthcheck"
```

---

### Task 7: the gauges the worker refreshes

**Files:**
- Create: `src/ecet/interfaces/worker/gauges.py`
- Create: `tests/unit/interfaces/test_worker_gauges.py`
- Modify: `src/ecet/domain/ports/claim_repository.py`
- Modify: `src/ecet/domain/ports/review_task_repository.py`
- Modify: `src/ecet/infrastructure/postgres/repositories.py`
- Modify: `src/ecet/interfaces/worker/container.py`
- Modify: `src/ecet/interfaces/worker/main.py`
- Modify: `tests/fakes.py`
- Test: `tests/unit/domain/test_ports.py`, `tests/adapters/test_claim_repository.py`, `tests/adapters/test_review_task_repository.py`, `tests/unit/test_worker_main.py`

**Interfaces:**
- Consumes: `UnitOfWork`, `metrics.CLAIMS_BY_STATUS`, `metrics.REVIEW_OPEN`.
- Produces:
  - `ClaimRepository.count_by_status() -> dict[ClaimStatus, int]`
  - `ReviewTaskRepository.count_open_by_tenant() -> dict[TenantId, int]`
  - `ecet.interfaces.worker.gauges.refresh_once(uow_factory: Callable[[], UnitOfWork]) -> None`
  - `ecet.interfaces.worker.gauges.run_refresher(uow_factory, stop: asyncio.Event, *, interval: float = 30.0) -> None`
  - `WorkerContainer.uow_factory: Callable[[], UnitOfWork]` (new field, so `run()` can start the refresher).

- [ ] **Step 1: Write the failing port tests**

Append to `tests/unit/domain/test_ports.py`:

```python
async def test_claim_repository_counts_by_status() -> None:
    repo = FakeClaimRepository()
    await repo.add(build_claim(status=ClaimStatus.QUEUED))
    await repo.add(build_claim(status=ClaimStatus.QUEUED))
    await repo.add(build_claim(status=ClaimStatus.REVIEW_PENDING))

    counts = await repo.count_by_status()

    assert counts[ClaimStatus.QUEUED] == 2
    assert counts[ClaimStatus.REVIEW_PENDING] == 1
    assert ClaimStatus.APPROVED_AUTO not in counts  # zero rows means no key


async def test_review_task_repository_counts_open_tasks_per_tenant() -> None:
    repo = FakeReviewTaskRepository()
    await repo.add(build_task(tenant_id=TenantId("tenant-a")))
    await repo.add(build_task(tenant_id=TenantId("tenant-a")))
    await repo.add(build_task(tenant_id=TenantId("tenant-b")))
    resolved = build_task(tenant_id=TenantId("tenant-b"))
    resolved.resolve(
        resolution=Decision.MEETS_NECESSITY, reviewer="r", notes=None, now=NOW
    )
    await repo.add(resolved)

    counts = await repo.count_open_by_tenant()

    assert counts == {TenantId("tenant-a"): 2, TenantId("tenant-b"): 1}
```

(`build_claim` / `build_task` / `NOW`: use the helpers that module already defines.)

- [ ] **Step 2: Write the failing refresher test**

Create `tests/unit/interfaces/test_worker_gauges.py`:

```python
"""The gauges are the only metric nobody's request path produces, so the worker goes
and asks. A failed refresh must not take the worker down with it — a wrong gauge is
cheaper than a dead consumer."""

import asyncio

from prometheus_client import REGISTRY
from tests.fakes import FakeUnitOfWork

from ecet.domain.claim import ClaimStatus
from ecet.interfaces.worker.gauges import refresh_once, run_refresher


def sample(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return 0.0 if value is None else value


async def test_refresh_sets_a_gauge_per_status_and_tenant() -> None:
    uow = FakeUnitOfWork()
    await uow.claims.add(build_claim(status=ClaimStatus.QUEUED))
    await uow.review_tasks.add(build_task(tenant_id=TenantId("tenant-a")))

    await refresh_once(lambda: uow)

    assert sample("ecet_claims_by_status", status="QUEUED") == 1
    assert sample("ecet_claims_by_status", status="APPROVED_AUTO") == 0
    assert sample("ecet_review_open", tenant="tenant-a") == 1


async def test_a_refresh_failure_does_not_stop_the_loop() -> None:
    calls = 0

    def failing_factory() -> FakeUnitOfWork:
        nonlocal calls
        calls += 1
        raise ConnectionError("database is away")

    stop = asyncio.Event()
    task = asyncio.create_task(run_refresher(failing_factory, stop, interval=0.01))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert calls > 1  # it kept going after the first failure


async def test_the_refresher_returns_when_stop_is_set() -> None:
    uow = FakeUnitOfWork()
    stop = asyncio.Event()
    task = asyncio.create_task(run_refresher(lambda: uow, stop, interval=30.0))
    await asyncio.sleep(0)
    stop.set()

    # It must wake on the event, not sleep out the 30 s interval.
    await asyncio.wait_for(task, timeout=1)
```

(`build_claim` / `build_task` / `TenantId`: import the same helpers the other unit tests use; if none is importable, build the models inline — a `Claim` needs `id`, `tenant_id`, `source`, `created_at`, `updated_at`.)

- [ ] **Step 3: Run both to watch them fail**

Run: `uv run pytest tests/unit/domain/test_ports.py tests/unit/interfaces/test_worker_gauges.py -v`
Expected: FAIL — `AttributeError: 'FakeClaimRepository' object has no attribute 'count_by_status'`, then `ModuleNotFoundError: ecet.interfaces.worker.gauges`.

- [ ] **Step 4: Add the port methods and their three implementations**

In `src/ecet/domain/ports/claim_repository.py`:

```python
    async def count_by_status(self) -> dict[ClaimStatus, int]:
        """Rows per status, for the `ecet_claims_by_status` gauge. A status with no
        rows is absent from the mapping, not zero."""
        ...
```

In `src/ecet/domain/ports/review_task_repository.py`:

```python
    async def count_open_by_tenant(self) -> dict[TenantId, int]:
        """Open tasks per tenant, for the `ecet_review_open` gauge."""
        ...
```

In `src/ecet/infrastructure/postgres/repositories.py` (add `func` to the `sqlalchemy` import):

```python
    async def count_by_status(self) -> dict[ClaimStatus, int]:
        statement = select(ClaimRow.status, func.count()).group_by(ClaimRow.status)
        rows = (await self._session.execute(statement)).all()
        return {ClaimStatus(status): count for status, count in rows}
```

```python
    async def count_open_by_tenant(self) -> dict[TenantId, int]:
        statement = (
            select(ReviewTaskRow.tenant_id, func.count())
            .where(ReviewTaskRow.status == ReviewStatus.OPEN.value)
            .group_by(ReviewTaskRow.tenant_id)
        )
        rows = (await self._session.execute(statement)).all()
        return {TenantId(tenant_id): count for tenant_id, count in rows}
```

In `tests/fakes.py`:

```python
    async def count_by_status(self) -> dict[ClaimStatus, int]:
        counts: dict[ClaimStatus, int] = {}
        for claim in self.claims.values():
            counts[claim.status] = counts.get(claim.status, 0) + 1
        return counts
```

```python
    async def count_open_by_tenant(self) -> dict[TenantId, int]:
        counts: dict[TenantId, int] = {}
        for task in self.tasks.values():
            if task.status is ReviewStatus.OPEN:
                counts[task.tenant_id] = counts.get(task.tenant_id, 0) + 1
        return counts
```

(`self.tasks` — use whatever the fake's storage attribute is actually called.)

- [ ] **Step 5: Write the refresher**

Create `src/ecet/interfaces/worker/gauges.py`:

```python
"""`ecet_claims_by_status` and `ecet_review_open`: the two metrics no request path
produces, because they are statements about the whole table rather than about one
claim. The worker asks the database every 30 s, as the observability spec says.

The api does not do this. Two processes writing the same gauge would each publish
their own view, and the api's is the one that matters least.
"""

import asyncio
import contextlib
from collections.abc import Callable

import structlog

from ecet import metrics
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.domain.claim import ClaimStatus

log = structlog.get_logger(__name__)

REFRESH_SECONDS = 30.0


async def refresh_once(uow_factory: Callable[[], UnitOfWork]) -> None:
    async with uow_factory() as uow:
        claims = await uow.claims.count_by_status()
        reviews = await uow.review_tasks.count_open_by_tenant()

    for status in ClaimStatus:
        # Every status every time: a status that drops to zero rows must drop to zero
        # here too, and `count_by_status` omits it rather than reporting a zero.
        metrics.CLAIMS_BY_STATUS.labels(status=status.value).set(claims.get(status, 0))

    # A tenant whose queue empties would otherwise keep its last value forever.
    metrics.REVIEW_OPEN.clear()
    for tenant_id, count in reviews.items():
        metrics.REVIEW_OPEN.labels(tenant=str(tenant_id)).set(count)


async def run_refresher(
    uow_factory: Callable[[], UnitOfWork],
    stop: asyncio.Event,
    *,
    interval: float = REFRESH_SECONDS,
) -> None:
    """Refresh until `stop`. A failure is logged and the loop continues: a stale gauge
    is not worth killing the consumer for."""
    while not stop.is_set():
        try:
            await refresh_once(uow_factory)
        except Exception as error:
            log.warning("gauges.refresh_failed", error=type(error).__name__)
        with contextlib.suppress(TimeoutError):
            # Waiting on the event rather than sleeping: shutdown must not sit out a
            # 30 s interval before the worker can exit.
            await asyncio.wait_for(stop.wait(), timeout=interval)
```

- [ ] **Step 6: Run it to watch it pass**

Run: `uv run pytest tests/unit/domain/test_ports.py tests/unit/interfaces/test_worker_gauges.py -v`
Expected: PASS.

- [ ] **Step 7: Start the refresher in the worker**

In `src/ecet/interfaces/worker/container.py`, add the field to the dataclass and pass it:

```python
@dataclass
class WorkerContainer:
    settings: Settings
    uow_factory: Callable[[], UnitOfWork]
    evaluate: EvaluateClaim
    consumer: RabbitMqConsumer
    aclose: Callable[[], Awaitable[None]]
```

```python
    return WorkerContainer(
        settings=settings,
        uow_factory=uow_factory,
        evaluate=evaluate,
        consumer=consumer,
        aclose=aclose,
    )
```

In `src/ecet/interfaces/worker/main.py`, start it beside the consumer and let the `stop` event end it:

```python
    built = container if container is not None else await build_container(settings)
    await built.consumer.start()
    gauges_task = asyncio.create_task(run_refresher(built.uow_factory, stop))
    log.info("worker.started", env=settings.env, prefetch=settings.worker_prefetch)
    try:
        await stop.wait()
    finally:
        await gauges_task  # `stop` is set: it returns on the next loop check
        ...
```

`tests/unit/test_worker_main.py`'s `build_container` helper needs the new field — give it `uow_factory=lambda: FakeUnitOfWork()`, and assert the loop ran:

```python
async def test_the_gauge_refresher_runs_and_stops_with_the_worker(settings: Settings) -> None:
    uow = FakeUnitOfWork()
    stop = asyncio.Event()
    container = build_container(settings, _StubConsumer(), uow_factory=lambda: uow)
    task = asyncio.create_task(run(settings, stop, container))
    await asyncio.sleep(0)

    stop.set()

    await asyncio.wait_for(task, timeout=1)
```

- [ ] **Step 8: Write the failing adapter tests**

Append to `tests/adapters/test_claim_repository.py`:

```python
async def test_count_by_status_groups_in_the_database(uow: SqlAlchemyUnitOfWork) -> None:
    async with uow:
        await uow.claims.add(build_claim(status=ClaimStatus.QUEUED))
        await uow.claims.add(build_claim(status=ClaimStatus.QUEUED))
        await uow.claims.add(build_claim(status=ClaimStatus.REVIEW_PENDING))
        await uow.commit()

        counts = await uow.claims.count_by_status()

    assert counts[ClaimStatus.QUEUED] == 2
    assert counts[ClaimStatus.REVIEW_PENDING] == 1
```

Append to `tests/adapters/test_review_task_repository.py`:

```python
async def test_count_open_by_tenant_ignores_resolved_tasks(uow: SqlAlchemyUnitOfWork) -> None:
    async with uow:
        await uow.review_tasks.add(build_task(tenant_id=TenantId("tenant-a")))
        resolved = build_task(tenant_id=TenantId("tenant-a"))
        resolved.resolve(
            resolution=Decision.MEETS_NECESSITY, reviewer="r", notes=None, now=NOW
        )
        await uow.review_tasks.add(resolved)
        await uow.commit()

        counts = await uow.review_tasks.count_open_by_tenant()

    assert counts == {TenantId("tenant-a"): 1}
```

(match each module's existing fixture and helper names — they set up the container and the schema.)

- [ ] **Step 9: Run everything**

```bash
uv run pytest
uv run pytest -m slow tests/adapters/test_claim_repository.py tests/adapters/test_review_task_repository.py
```
Expected: PASS (the second needs Docker).

- [ ] **Step 10: Commit**

```bash
git add src/ecet tests
git commit -m "feat(metrics): worker-refreshed claims_by_status and review_open gauges"
```

---

### Task 8: `NotifyClient` takes a tenant and returns a `Delivery`

**Files:**
- Modify: `src/ecet/application/use_cases/notify_client.py`
- Modify: `src/ecet/application/use_cases/route_decision.py`
- Modify: `src/ecet/application/use_cases/human_review.py`
- Modify: `src/ecet/application/use_cases/retry_notify.py`
- Modify: `src/ecet/interfaces/api/container.py`, `src/ecet/interfaces/worker/container.py` (construction sites)
- Test: `tests/unit/application/test_notify_client.py`, `test_route_decision.py`, `test_resolve_review.py`, `test_retry_notify.py`

**Interfaces:**
- Consumes: `WebhookClient`, `Clock`, `Tenant`.
- Produces:
  - `NotifyClient(webhook: WebhookClient, clock: Clock)` — no repository.
  - `NotifyClient.execute(claim: Claim, tenant: Tenant, *, outcome: Decision, confidence: float, decided_by: DecidedBy) -> ClientNotification` — unchanged except for the `tenant` argument; it no longer touches the claim.
  - `NotifyClient.attempt(claim, tenant, *, outcome, confidence, decided_by) -> Delivery`.
  - `Delivery(failure_reason: str | None, error: str | None)` with `apply_to(claim: Claim) -> None`, and `Delivery.succeeded` (`failure_reason is None`).
  - `tenant_inactive(error: TenantNotFound) -> Delivery`.

This is the enabling change for Tasks 9–11: the claim that gets saved is re-read *after* the POST, so the POST must not be the thing that mutated the claim, and the tenant must already be in hand when the transaction closes. Behaviour is identical this task — every caller still calls it inside its unit of work.

- [ ] **Step 1: Write the failing test**

Rewrite the relevant cases in `tests/unit/application/test_notify_client.py` and add:

```python
async def test_attempt_reports_what_to_do_without_touching_the_claim() -> None:
    """The caller saves a claim it re-read after the POST; a NotifyClient that mutated
    the claim it was handed would write those fields onto a stale object."""
    claim = build_claim(status=ClaimStatus.EVALUATED)
    before = claim.notification_attempts

    delivery = await NotifyClient(FakeWebhookClient(), FixedClock(NOW)).attempt(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.9, decided_by="auto"
    )

    assert delivery.succeeded
    assert delivery.failure_reason is None
    assert claim.notification_attempts == before  # untouched

    delivery.apply_to(claim)
    assert claim.notification_attempts == before + 1
    assert claim.last_notify_error is None


async def test_a_rejected_delivery_carries_the_token_and_the_detail() -> None:
    claim = build_claim(status=ClaimStatus.EVALUATED)

    delivery = await NotifyClient(
        FakeWebhookClient(error=WebhookPermanentError("status 400")), FixedClock(NOW)
    ).attempt(
        claim, build_tenant(), outcome=Decision.MEETS_NECESSITY, confidence=0.9, decided_by="auto"
    )
    delivery.apply_to(claim)

    assert delivery.failure_reason == REJECTED
    assert claim.last_notify_error is not None
    assert claim.last_notify_error.startswith("WebhookPermanentError")
    assert claim.notification_attempts == 1


def test_a_deactivated_tenant_is_a_delivery_that_never_happened() -> None:
    delivery = tenant_inactive(TenantNotFound("tenant-a"))

    assert delivery.failure_reason == TENANT_INACTIVE
    assert delivery.error is not None
    assert not delivery.succeeded
```

- [ ] **Step 2: Run it to watch it fail**

Run: `uv run pytest tests/unit/application/test_notify_client.py -v`
Expected: FAIL — `NotifyClient.__init__` still wants a repository, `attempt` returns `str | None`.

- [ ] **Step 3: Rewrite `NotifyClient`**

`src/ecet/application/use_cases/notify_client.py`, replacing the class and adding the result type:

```python
@dataclass(frozen=True)
class Delivery:
    """What one attempt did, as a value.

    The claim is not mutated here: the caller delivers *outside* its unit of work and
    then saves a claim it re-read inside a second one, so the fields have to be
    applied to that object, not to the one the payload was built from.
    """

    failure_reason: str | None
    error: str | None

    @property
    def succeeded(self) -> bool:
        return self.failure_reason is None

    def apply_to(self, claim: Claim) -> None:
        claim.notification_attempts += 1
        claim.last_notify_error = self.error


def tenant_inactive(error: TenantNotFound) -> Delivery:
    """A tenant deactivated since the claim was ingested. Nothing was sent, and the
    claim parks under the same token as any other undelivered decision."""
    return Delivery(failure_reason=TENANT_INACTIVE, error=f"{type(error).__name__}: {error}")


class NotifyClient:
    def __init__(self, webhook: WebhookClient, clock: Clock) -> None:
        self._webhook = webhook
        self._clock = clock

    async def execute(
        self,
        claim: Claim,
        tenant: Tenant,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> ClientNotification:
        evaluation = claim.evaluation
        payload = ClientNotification(...)  # unchanged body
        await self._webhook.deliver(tenant, payload)
        log.info(
            "notify.delivered",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            outcome=payload.outcome,
            decided_by=decided_by,
        )
        return payload

    async def attempt(
        self,
        claim: Claim,
        tenant: Tenant,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> Delivery:
        """`execute` for a caller that parks the claim instead of propagating. UC-07,
        UC-09c and the operator retry all park a failed delivery the same way, so the
        mapping lives here once."""
        try:
            await self.execute(
                claim, tenant, outcome=outcome, confidence=confidence, decided_by=decided_by
            )
        except WebhookPermanentError as error:
            return self._failed(claim, REJECTED, error)
        except WebhookError as error:
            return self._failed(claim, UNREACHABLE, error)
        return Delivery(failure_reason=None, error=None)

    @staticmethod
    def _failed(claim: Claim, reason: str, error: Exception) -> Delivery:
        log.warning(
            "notify.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            error=type(error).__name__,
        )
        return Delivery(failure_reason=reason, error=f"{type(error).__name__}: {error}")
```

The `attempts=` field leaves the two log lines: the count now lives on the claim the caller saves, and reading it off a stale object would print the wrong number.

- [ ] **Step 4: Update the three callers, still inside their unit of work**

`route_decision.py` — `__init__` still takes `uow`, `webhook`, `clock` and `threshold` this task, but builds the client without a repository (`self._notify = NotifyClient(webhook, clock)`), reads the tenant from the uow, and keeps the call where it is for now:

```python
        else:
            try:
                tenant = await self._uow.tenants.get(claim.tenant_id)
            except TenantNotFound as error:
                delivery = tenant_inactive(error)
            else:
                delivery = await self._notify.attempt(
                    claim,
                    tenant,
                    outcome=evaluation.decision,
                    confidence=evaluation.confidence,
                    decided_by="auto",
                )
            delivery.apply_to(claim)
            if delivery.succeeded:
                self._advance(claim, ClaimStatus.APPROVED_AUTO)
            else:
                self._fail(claim, delivery.failure_reason or UNREACHABLE)
```

`human_review.py` (`ResolveReview`) and `retry_notify.py` take the same shape: read the tenant from the open uow inside a `try/except TenantNotFound`, `attempt(...)`, `delivery.apply_to(claim)`, then branch on `delivery.succeeded`.

Both container modules construct `NotifyClient` nowhere directly (the use cases do), so they need no change beyond staying green.

- [ ] **Step 5: Run the suite**

Run: `uv run pytest`
Expected: PASS. Update the assertions in `test_route_decision.py`, `test_resolve_review.py` and `test_retry_notify.py` that read `attempt`'s old `str | None` return.

- [ ] **Step 6: Commit**

```bash
git add src/ecet/application tests/unit/application
git commit -m "refactor(notify): NotifyClient takes a tenant and returns a Delivery"
```

---

### Task 9: UC-06 — the LLM call and the webhook leave the unit of work

**Files:**
- Modify: `src/ecet/application/use_cases/evaluate_claim.py`
- Modify: `src/ecet/application/use_cases/route_decision.py`
- Modify: `src/ecet/interfaces/worker/container.py`
- Modify: `tests/fakes.py`
- Test: `tests/unit/application/test_evaluate_claim.py`, `tests/unit/application/test_route_decision.py`

**Interfaces:**
- Consumes: `Delivery`, `tenant_inactive`, `NotifyClient` (Task 8).
- Produces:
  - `RouteDecision(*, clock: Clock, threshold: float)` — no `uow`, no `webhook`.
  - `RouteDecision.decide(claim: Claim, evaluation: Evaluation) -> Route` — pure; logs `claim.routed` and counts `ecet_triage_route_total`.
  - `RouteDecision.apply(uow: UnitOfWork, claim: Claim, *, route: Route, delivery: Delivery | None) -> None` — transitions and saves; opens a review task for `HUMAN_REVIEW`.
  - `FakeUnitOfWork.active: bool` and `FakeLLMGateway(..., watch: FakeUnitOfWork | None)` / `FakeWebhookClient(..., watch: FakeUnitOfWork | None)` recording `uow_open_during_call: list[bool]`.

This closes Phase 4 carry-over #11. Today `EvaluateClaim.execute` opens the transaction, SELECTs, and then holds that connection through `llm.evaluate` (up to 60 s) and up to three webhook attempts (1 s + 4 s backoff) before committing.

The new shape is **read, call, write**: a read-only unit of work gathers everything the external calls need, closes; the LLM call and the POST run with no connection held; a second unit of work does every write and commits once. A crash between them leaves the claim `QUEUED` and the message unacked, so RabbitMQ redelivers it — the same at-least-once contract Phase 4 already documents.

- [ ] **Step 1: Teach the fakes to notice an open unit of work**

In `tests/fakes.py`:

```python
class FakeUnitOfWork:
    def __init__(self, ...) -> None:
        ...
        self.commits = 0
        #: True between `__aenter__` and `__aexit__`. The external-call tests assert a
        #: vendor call happens with this False.
        self.active = False
        self.entries = 0

    async def __aenter__(self) -> "FakeUnitOfWork":
        self.active = True
        self.entries += 1
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.active = False
        return None
```

and give both outbound fakes a watcher:

```python
class FakeLLMGateway:
    def __init__(self, *, evaluation=None, error=None, watch: "FakeUnitOfWork | None" = None):
        ...
        self.watch = watch
        self.uow_open_during_call: list[bool] = []

    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        if self.watch is not None:
            self.uow_open_during_call.append(self.watch.active)
        ...
```

`FakeWebhookClient.deliver` gets the identical two lines.

- [ ] **Step 2: Write the failing tests**

Append to `tests/unit/application/test_evaluate_claim.py`:

```python
async def test_the_vendor_is_called_with_no_transaction_open() -> None:
    """Phase 4 carry-over #11: an `llm_timeout_s=60` call inside the unit of work is a
    Postgres connection idle-in-transaction for a minute, `worker_prefetch` of them
    per worker."""
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    llm = FakeLLMGateway(watch=uow)
    webhook = FakeWebhookClient(watch=uow)
    await seed_queued_claim(uow)

    await build_evaluate(uow, llm=llm, webhook=webhook).execute(build_message_for(uow))

    assert llm.uow_open_during_call == [False]
    assert webhook.uow_open_during_call == [False]


async def test_the_claim_is_saved_from_the_second_unit_of_work() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    claim = await seed_queued_claim(uow)

    await build_evaluate(uow).execute(build_message_for(uow))

    stored = await uow.claims.get(claim.id)
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.evaluation is not None
    assert stored.notification_attempts == 1
    assert uow.entries == 2  # one read-only, one write


async def test_a_claim_that_moved_on_while_the_vendor_was_answering_is_left_alone() -> None:
    """The status check is repeated in the second unit of work: two workers can hold
    the same message, and the one that finishes second must not overwrite the first."""
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    claim = await seed_queued_claim(uow)

    async def advance_it(request: EvaluationRequest) -> Evaluation:
        stolen = await uow.claims.get(claim.id)
        stolen.transition(ClaimStatus.EVALUATION_FAILED, reason="llm_permanent_error", now=NOW)
        await uow.claims.save(stolen)
        return build_evaluation()

    llm = FakeLLMGateway()
    llm.evaluate = advance_it  # type: ignore[method-assign]

    await build_evaluate(uow, llm=llm).execute(build_message_for(uow))

    assert (await uow.claims.get(claim.id)).status is ClaimStatus.EVALUATION_FAILED
```

(`build_evaluate`, `seed_queued_claim`, `build_message_for`, `build_evaluation`, `build_tenant`: use the helpers that module already has, extending them with the `llm=` / `webhook=` keywords if they do not take them yet.)

- [ ] **Step 3: Run them to watch them fail**

Run: `uv run pytest tests/unit/application/test_evaluate_claim.py -v`
Expected: FAIL — `llm.uow_open_during_call == [True]`, `uow.entries == 1`.

- [ ] **Step 4: Split `RouteDecision` into decide and apply**

`src/ecet/application/use_cases/route_decision.py`:

```python
class RouteDecision:
    """UC-07, in two halves, because the delivery between them runs outside the unit of
    work: `decide` is pure and `apply` writes."""

    def __init__(self, *, clock: Clock, threshold: float) -> None:
        self._clock = clock
        self._threshold = threshold

    def decide(self, claim: Claim, evaluation: Evaluation) -> Route:
        route = triage(evaluation, self._threshold)
        metrics.TRIAGE_ROUTE_TOTAL.labels(route=route.value).inc()
        log.info(
            "claim.routed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            route=route.value,
            decision=evaluation.decision.value,
            confidence=evaluation.confidence,
        )
        return route

    async def apply(
        self, uow: UnitOfWork, claim: Claim, *, route: Route, delivery: Delivery | None
    ) -> None:
        """`delivery` is `None` for `HUMAN_REVIEW`: nothing was sent, because nobody has
        decided anything yet."""
        if route is Route.HUMAN_REVIEW:
            await RequestHumanReview(uow.review_tasks, self._clock).execute(
                claim, self._reason_for(claim)
            )
            self._advance(claim, ClaimStatus.REVIEW_PENDING)
        elif delivery is not None and delivery.succeeded:
            self._advance(claim, ClaimStatus.APPROVED_AUTO)
        else:
            reason = delivery.failure_reason if delivery is not None else UNREACHABLE
            self._fail(claim, reason or UNREACHABLE)
        await uow.claims.save(claim)
```

`_reason_for`, `_advance` and `_fail` are unchanged.

- [ ] **Step 5: Reshape `EvaluateClaim`**

`src/ecet/application/use_cases/evaluate_claim.py` — the module docstring gains a paragraph and `execute` becomes:

```python
    async def execute(self, message: EvaluationMessage) -> None:
        async with self._uow_factory() as uow:  # read-only: nothing is written here
            claim = await self._load(uow, message)
            if claim is None:
                return
            tenant, tenant_error = await self._tenant(uow, claim)
            request = await self._build_request(uow, claim, message)

        # No connection is held for either of these. The vendor call can take
        # `llm_timeout_s` and the POST its own timeout; both used to run inside the
        # transaction above (Phase 4 carry-over #11).
        try:
            evaluation = await self._llm.evaluate(request)
        except LLMTransientError:
            # The only escape hatch: the consumer nacks with requeue and RabbitMQ's
            # x-delivery-limit eventually sends it to the DLQ. Nothing is saved, so
            # the claim is still QUEUED for the redelivery.
            raise
        except (LLMInvalidOutput, LLMPermanentError) as error:
            await self._fail(message, error)
            return

        route = self._router.decide(claim, evaluation)
        delivery: Delivery | None = None
        if route is Route.AUTO_NOTIFY:
            delivery = (
                tenant_inactive(tenant_error)
                if tenant is None
                else await NotifyClient(self._webhook, self._clock).attempt(
                    claim,
                    tenant,
                    outcome=evaluation.decision,
                    confidence=evaluation.confidence,
                    decided_by="auto",
                )
            )

        async with self._uow_factory() as uow:  # every write in this use case
            fresh = await self._load(uow, message)
            if fresh is None:
                # Someone else moved it while the vendor was answering. Their write
                # stands; ours is thrown away, and the message is still acked.
                return
            fresh.evaluation = evaluation
            self._advance(fresh, ClaimStatus.EVALUATED)
            await uow.claims.save(fresh)
            if delivery is not None:
                delivery.apply_to(fresh)
            await self._router.apply(uow, fresh, route=route, delivery=delivery)
            await uow.commit()
```

with the supporting pieces:

```python
    async def _tenant(
        self, uow: UnitOfWork, claim: Claim
    ) -> tuple[Tenant | None, TenantNotFound | None]:
        """Read before the transaction closes, because the delivery happens after it.
        A tenant deactivated since ingestion is not an error here — it is a delivery
        that will never happen, and the claim parks under `tenant_inactive`."""
        try:
            return await uow.tenants.get(claim.tenant_id), None
        except TenantNotFound as error:
            return None, error

    async def _fail(self, message: EvaluationMessage, error: Exception) -> None:
        """A vendor answer no retry can improve: park the claim and open a review."""
        reason = INVALID_OUTPUT if isinstance(error, LLMInvalidOutput) else PERMANENT_ERROR
        async with self._uow_factory() as uow:
            claim = await self._load(uow, message)
            if claim is None:
                return
            previous = claim.status
            claim.transition(ClaimStatus.EVALUATION_FAILED, reason=reason, now=self._clock.now())
            log.warning(
                "claim.failed",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                to=ClaimStatus.EVALUATION_FAILED.value,
                reason=reason,
                error=type(error).__name__,
                **{"from": previous.value},
            )
            await RequestHumanReview(uow.review_tasks, self._clock).execute(
                claim, ReviewReason.EVALUATION_FAILED
            )
            await uow.claims.save(claim)
            await uow.commit()
```

`__init__` builds the router once: `self._router = RouteDecision(clock=clock, threshold=threshold)`, and `_advance` is the same helper the other use cases have (transition + the `claim.transition` log line with `from`/`to`).

- [ ] **Step 6: Run them to watch them pass**

Run: `uv run pytest tests/unit/application -v`
Expected: PASS. `tests/unit/interfaces/test_worker_handler.py` must stay green — the handler's contract (raise = requeue) is unchanged.

- [ ] **Step 7: Commit**

```bash
git add src/ecet/application tests
git commit -m "perf(worker): run the LLM call and the webhook outside the unit of work"
```

---

### Task 10: UC-09c — resolve delivers outside the request's transaction

**Files:**
- Modify: `src/ecet/application/use_cases/human_review.py`
- Modify: `src/ecet/interfaces/api/routes/reviews.py` (docstring only)
- Test: `tests/unit/application/test_resolve_review.py`, `tests/api/test_reviews_routes.py`

**Interfaces:**
- Consumes: `Delivery`, `tenant_inactive`, `NotifyClient`.
- Produces: `ResolveReview.execute(command) -> ResolveReviewResult`, signature unchanged.

This is Phase 5 carry-over #2. The api's concurrency is unbounded — there is no prefetch equivalent — and `/readyz`'s database probe shares the pool, so a slow receiver could starve the readiness probe that keeps the container marked healthy.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/application/test_resolve_review.py`:

```python
async def test_the_webhook_is_delivered_with_no_transaction_open() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient(watch=uow)
    task = await seed_open_review(uow)

    await build_resolve(uow, webhook=webhook).execute(build_command(task))

    assert webhook.uow_open_during_call == [False]
    assert uow.entries == 2


async def test_a_failed_delivery_still_records_the_human_decision() -> None:
    """Unchanged behaviour, now across two transactions: the reviewer's work is done,
    only the delivery is outstanding."""
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient(error=WebhookTransientError("refused"))
    task = await seed_open_review(uow)

    result = await build_resolve(uow, webhook=webhook).execute(build_command(task))

    assert result.claim_status is ClaimStatus.NOTIFY_FAILED
    stored_task = await uow.review_tasks.get(task.id)
    assert stored_task.status is ReviewStatus.RESOLVED
    assert stored_task.reviewer == "demo.reviewer"


async def test_a_task_already_resolved_is_refused_before_anything_is_sent() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient()
    task = await seed_resolved_review(uow)

    with pytest.raises(ReviewAlreadyResolved):
        await build_resolve(uow, webhook=webhook).execute(build_command(task))

    assert webhook.deliveries == []
```

- [ ] **Step 2: Run them to watch them fail**

Run: `uv run pytest tests/unit/application/test_resolve_review.py -v`
Expected: FAIL — `webhook.uow_open_during_call == [True]`, `uow.entries == 1`.

- [ ] **Step 3: Reshape `ResolveReview.execute`**

```python
    async def execute(self, command: ResolveReviewCommand) -> ResolveReviewResult:
        async with self._uow_factory() as uow:  # read-only
            task = await uow.review_tasks.get(command.task_id)
            if task.tenant_id != command.tenant_id:
                # Same answer as "no such task": anything else confirms it exists.
                raise ReviewTaskNotFound(str(command.task_id))
            if task.status is ReviewStatus.RESOLVED:
                # Checked here so nothing is delivered for a decision already made;
                # `task.resolve` below raises the same error if it loses a race.
                raise ReviewAlreadyResolved(str(task.id))
            claim = await uow.claims.get(task.claim_id)
            if not claim.status.can_transition_to(ClaimStatus.REVIEW_RESOLVED):
                # Checked before the webhook: a delivery the transition then refused
                # would reach the client and be rolled back here.
                raise InvalidTransition(
                    f"claim {claim.id} is {claim.status}, which a review cannot resolve"
                )
            try:
                tenant = await uow.tenants.get(claim.tenant_id)
            except TenantNotFound as error:
                tenant, delivery = None, tenant_inactive(error)

        if tenant is not None:
            # Outside the transaction: one attempt, no in-request backoff, and no
            # pooled connection held for it (`retry-notify` is the retry).
            delivery = await NotifyClient(self._webhook, self._clock).attempt(
                claim, tenant, outcome=command.resolution, confidence=1.0, decided_by="human"
            )

        async with self._uow_factory() as uow:  # every write
            now = self._clock.now()
            task = await uow.review_tasks.get(command.task_id)
            # Raises `ReviewAlreadyResolved` if another request resolved it while the
            # POST was in flight. That request's decision stands; this one's does not.
            task.resolve(
                resolution=command.resolution,
                reviewer=command.reviewer,
                notes=command.notes,
                now=now,
            )
            await uow.review_tasks.save(task)

            claim = await uow.claims.get(task.claim_id)
            delivery.apply_to(claim)
            if delivery.succeeded:
                claim.transition(ClaimStatus.REVIEW_RESOLVED, now=now)
            else:
                claim.transition(
                    ClaimStatus.NOTIFY_FAILED, reason=delivery.failure_reason, now=now
                )
            await uow.claims.save(claim)
            await uow.commit()

        log.info(...)  # unchanged
        return ResolveReviewResult(task_id=task.id, claim_id=claim.id, claim_status=claim.status)
```

Update the module docstring: the delivery now happens between two transactions, and two concurrent resolves can both POST before one loses at `task.resolve` — the receiver's dedupe key is `claim_id` in the body, as Phase 4 deviation #3 already documents.

- [ ] **Step 4: Run them to watch them pass**

Run: `uv run pytest tests/unit/application/test_resolve_review.py tests/api/test_reviews_routes.py -v`
Expected: PASS.

- [ ] **Step 5: Update the route docstring and commit**

In `routes/reviews.py`, replace "The delivery — one attempt, no in-request backoff — runs inside the request" with: "The delivery — one attempt, no in-request backoff — runs inside the request but outside its transaction, so no pooled connection is held for it."

```bash
git add src/ecet tests
git commit -m "perf(api): deliver UC-09c's webhook outside the unit of work"
```

---

### Task 11: `retry-notify` delivers outside its transaction

**Files:**
- Modify: `src/ecet/application/use_cases/retry_notify.py`
- Test: `tests/unit/application/test_retry_notify.py`

**Interfaces:**
- Consumes: `Delivery`, `tenant_inactive`, `NotifyClient`.
- Produces: `RetryNotify.execute(claim_id: ClaimId) -> Claim`, signature unchanged.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/application/test_retry_notify.py`:

```python
async def test_the_retry_posts_with_no_transaction_open() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient(watch=uow)
    claim = await seed_notify_failed_claim(uow)

    await build_retry(uow, webhook=webhook).execute(claim.id)

    assert webhook.uow_open_during_call == [False]
    assert uow.entries == 2


async def test_a_second_failure_still_commits_the_attempt_count() -> None:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    webhook = FakeWebhookClient(error=WebhookTransientError("still refused"))
    claim = await seed_notify_failed_claim(uow)

    result = await build_retry(uow, webhook=webhook).execute(claim.id)

    assert result.status is ClaimStatus.NOTIFY_FAILED
    stored = await uow.claims.get(claim.id)
    assert stored.notification_attempts == claim.notification_attempts + 1
    assert stored.failure_reason == UNREACHABLE
```

- [ ] **Step 2: Run it to watch it fail**

Run: `uv run pytest tests/unit/application/test_retry_notify.py -v`
Expected: FAIL — `uow_open_during_call == [True]`.

- [ ] **Step 3: Reshape `RetryNotify.execute`**

```python
    async def execute(self, claim_id: ClaimId) -> Claim:
        async with self._uow_factory() as uow:  # read-only
            claim = await uow.claims.get(claim_id)
            if claim.status is not ClaimStatus.NOTIFY_FAILED:
                raise InvalidTransition(f"claim {claim.id} is {claim.status}, not NOTIFY_FAILED")
            task = await uow.review_tasks.find_by_claim(claim.id)
            outcome, confidence, decided_by, target = self._decision(claim, task)
            try:
                tenant = await uow.tenants.get(claim.tenant_id)
            except TenantNotFound as error:
                tenant, delivery = None, tenant_inactive(error)

        if tenant is not None:
            delivery = await NotifyClient(self._webhook, self._clock).attempt(
                claim, tenant, outcome=outcome, confidence=confidence, decided_by=decided_by
            )

        async with self._uow_factory() as uow:  # the write
            claim = await uow.claims.get(claim_id)
            now = self._clock.now()
            delivery.apply_to(claim)
            if delivery.succeeded:
                claim.transition(target, now=now)
            else:
                # There is no NOTIFY_FAILED -> NOTIFY_FAILED edge: the claim has not
                # moved, only the reason it is stuck may have.
                claim.failure_reason = delivery.failure_reason
                claim.updated_at = now
            await uow.claims.save(claim)
            await uow.commit()

        log.info(...)  # unchanged
        return claim
```

`_decision(claim, task)` is the existing if/elif/else block, lifted into a static method returning `tuple[Decision, float, DecidedBy, ClaimStatus]` and raising `InvalidTransition` for the unreachable case.

- [ ] **Step 4: Run it to watch it pass**

Run: `uv run pytest tests/unit/application/test_retry_notify.py tests/api/test_claims_routes.py -v`
Expected: PASS.

- [ ] **Step 5: Full check and commit**

```bash
uv run make check
git add src/ecet/application tests
git commit -m "perf(api): deliver retry-notify's webhook outside the unit of work"
```

---

### Task 12: record the phase in the docs

**Files:**
- Modify: `specs/06-roadmap.md`
- Modify: `docs/plans/2026-09-16-phase-6-observability.md` (this file: the deviations section)

**Interfaces:**
- Consumes: nothing.
- Produces: a roadmap whose Phase 6 matches what shipped and whose Phase 7 owns everything deferred.

- [ ] **Step 1: Rewrite Phase 6 in the roadmap**

Replace the `## Phase 6 — Observability & Polish` section with a Phase 6 that lists only what this plan shipped (structlog stdlib bridge, `request_id` end to end, `/metrics` on both processes, every `ecet_*` metric, the gauges, the worker healthcheck, the external-call move) and a link to this plan. Keep its "Done when": *a claim can be followed by one `request_id` from the api's response header into the worker's logs, and `/metrics` on both processes carries every series in the observability spec.*

- [ ] **Step 2: Add Phase 7 — Polish & E2E**

A new `## Phase 7 — Polish & E2E` section carrying everything this phase did not do: the E2E suite and the CI `e2e` job, the README rewrite (`/v1/reviews`, `retry-notify`, `make dlq-replay`, image size, `make spacy-model`, the static-API-key note, the mock client's missing freshness check, `make clean` after a topology change, quickstart verified under 2 minutes, cost-saving numbers now that the metrics exist), the CI spaCy cache and pinned model version, the optional Prometheus/Grafana profile (out of Phase 6 by the user's decision), and every small test/naming gap listed in this plan's "out of scope" block. Move the affected rows in the "Carried over from Phase 3 / 4 / 5" tables from "Phase 6" to "Phase 7", and mark as closed: Phase 0 #5 and #12, Phase 3 #3 and #4, Phase 4 #1 and #11, Phase 5 #1 and #2.

- [ ] **Step 3: Add a "Carried over from Phase 6" table**

One row per entry in this plan's *Deviations* section below, each naming the phase that closes it.

- [ ] **Step 4: Commit**

```bash
git add specs/06-roadmap.md docs/plans/2026-09-16-phase-6-observability.md
git commit -m "docs(phase-6): record what shipped and open Phase 7 for the polish"
```

---

## Phase exit criteria

- `make check` green: ruff, `mypy --strict` on domain + application, `mypy src/ecet`, `lint-imports` (four contracts), the full default test suite.
- `uv run pytest -m slow` green with Docker available (the two new repository counts are exercised against real Postgres).
- `make up` green, and `docker compose ps` shows **both** `api` and `worker` as `healthy`.
- `curl -s localhost:8000/metrics | grep ecet_` returns the api-side series; `docker compose exec worker python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9100/metrics').read().decode()[:200])"` returns the worker's.
- `make demo` still ends with a `decided_by: human` delivery in the mock client's list, and `docker compose logs api worker` is JSON on every line — uvicorn's included.
- A request id survives the whole trip: `curl -H "X-Request-Id: demo-1" ... /v1/claims/ingest` then `docker compose logs worker | grep demo-1` finds the worker's lines for that claim.
- `ecet_claims_by_status` in the worker's exposition is non-zero within 30 s of the demo.

## Deviations from spec (record in the PR description)

- The `observability` compose profile — Prometheus, Grafana, any dashboard JSON — was not built. The endpoints are the deliverable; scraping them is the user's decision to leave for later. Closed by: Phase 7 (if scheduled at all).
- `ecet_db_pool_in_use` is an addition to the observability spec's metric table, not one of its rows — the pool-idle-in-transaction risk Phase 4 and Phase 5 flagged had no series to point at until this phase added one. Closed by: n/a, permanent.
- The api and the worker each expose their own `prometheus_client` registry (the library's registry is process-global, one per process, and there is nothing to inject) — a scraper needs both targets, not one. Closed by: n/a, permanent.
- Two concurrent resolves of the same review can both send the webhook POST before either commits; only one wins at `ClaimRepository.save`'s optimistic check inside `ResolveReview`'s write unit of work. Flagged by Task 10's implementer as pre-existing in shape, not this task's to solve. Closed by: n/a, accepted.
- A crash between the read unit of work and the write unit of work (`EvaluateClaim`, `ResolveReview`, `RetryNotify`) leaves the claim recoverable — it is still in a retryable status — but with a delivery already sent that the system has no record of, since the write that would have recorded it never ran. Closed by: n/a, permanent — a transactional outbox is explicitly out of v1.
- `configure_logging` re-runs its root-handler and uvicorn-logger reset on every call rather than guarding for a second call. Harmless at today's one-call-per-process shape; Task 2's reviewer flagged it as worth a look only if it is ever called from two places. Closed by: n/a, accepted.
- The worker's shutdown (`interfaces/worker/main.py`) awaits an in-flight gauge refresh in its `finally` with no per-call timeout — a refresh against a hung database delays shutdown indefinitely. Task 7's reviewer confirmed this matches the codebase's existing no-DB-timeout posture (no timeout exists anywhere else in the DB path either), not a new gap this phase opened. Closed by: n/a, accepted unless operators see slow shutdowns.
- The `_tenant` helper is duplicated verbatim between `retry_notify.py` and `human_review.py`. Deliberate (Task 11's controller ruling): the duplication keeps the two use cases reading alike, and the only shared home would be `notify_client.py`, which knows nothing about a unit of work. Closed by: n/a, revisit if a third caller appears.
- `WEBHOOK_ATTEMPTS_TOTAL`'s `status_class` label can emit `1xx`/`3xx` for an unusual vendor response — the brief's own `f"{status // 100}xx"` snippet, not an implementer deviation, and realistic webhook responses stay in `{2xx, 4xx, 5xx}`. Closed by: n/a, permanent.
- `LLM_LATENCY_SECONDS` observes `latency_ms / 1000` rather than a raw `perf_counter()` delta, losing sub-millisecond precision — again the brief's own snippet. Closed by: n/a, permanent.
