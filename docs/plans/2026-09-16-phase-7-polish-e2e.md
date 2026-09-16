# Phase 7 — Polish & E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close v1. Every carry-over still scheduled against a phase is closed here — the small test and naming gaps from Phases 3–5, the consumer's untested health and drain paths, the pinned and cached spaCy model, the `observability` compose profile Phase 6 left out — and the system gets the proof it has been missing: an E2E suite that drives the real compose stack from outside (drop a PDF, receive the signed webhook, resolve a review as a human), runs in CI, and a README a stranger can follow from clone to demo.

**Architecture:** No new layers and no new runtime behaviour beyond three small fixes (two failure-reason tokens, `/readyz` with no probes, a leak-free `build_container`). The E2E suite lives in `tests/e2e/`, auto-marked `e2e` by its own `conftest.py` exactly as `tests/adapters/conftest.py` marks `slow`, so neither the default run nor the `slow` job ever needs a live stack. It talks to the stack only through the ports compose already publishes — the api on 8000, MinIO's S3 API on 9000 (a real `put_object` fires the real bucket notification), the mock client on 8081 and Postgres on 5432 for the one fact no API exposes (the claim row behind an object key) — plus `docker compose logs`/`exec` for the log and worker-metrics assertions. The `observability` profile is two stock images (Prometheus, Grafana) and three provisioning files under `deploy/observability/`; nothing in `src/` changes for it.

**Tech Stack:** Python 3.12, pytest + pytest-asyncio, httpx, aiobotocore (through the existing `S3ObjectStorage`), SQLAlchemy async (through the existing `create_engine`), Docker Compose v2 profiles, Prometheus v3, Grafana 12, GitHub Actions (`actions/cache@v6`).

**Spec:** [`specs/06-roadmap.md` §Phase 7](../../specs/06-roadmap.md#phase-7--polish--e2e) and every row marked "Phase 7" in its carry-over tables. It pulls in [testing](../../specs/05-platform/testing.md), [docker-compose](../../specs/05-platform/docker-compose.md), [observability](../../specs/05-platform/observability.md), [api](../../specs/04-interfaces/api.md), [UC-01](../../specs/02-use-cases/UC-01-ingest-claim-document.md), [UC-09](../../specs/02-use-cases/UC-09-human-review.md), [pdf-text-extractor](../../specs/03-infrastructure/pdf-text-extractor.md) and [queue-rabbitmq](../../specs/03-infrastructure/queue-rabbitmq.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- Git in this environment needs `export DEVELOPER_DIR=/Library/Developer/CommandLineTools` before any `git` command (Xcode license error otherwise).
- Work on branch `phase-7-polish-e2e` (already created from `main` at the Phase 6 merge, `323a0b4`).
- Layer rule (`import-linter`; `make imports` must stay green): `ecet.domain` imports stdlib + pydantic only; `ecet.application` never imports `openai`, `httpx`, `aio_pika`, `sqlalchemy`, `fastapi`, `typer` or `ecet.config`; the domain never imports `prometheus_client` or `ecet.metrics`. Do not weaken any contract.
- `mypy --strict` covers `src/ecet/domain` and `src/ecet/application`; `mypy src/ecet` covers the rest. Every function added under `src/` is annotated. Tests are not type-checked but are linted.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`. `ruff format --check` must pass.
- **ADR-001 still governs.** Every test that touches text, a payload, a log capture or a persisted claim ends with `assert_no_pii(...)` from `tests/pii.py`.
- **`failure_reason` values are short, fixed tokens** (`no_text`, `encrypted`, `too_many_pages`, `unreadable`, `object_unavailable`, `webhook_unreachable`, `webhook_rejected`, `tenant_inactive`, …) — never a client-supplied string or an exception message. Task 1 brings UC-01's last two stragglers into line.
- **Test file basenames must be unique across `tests/`** — the test directories have no `__init__.py`, so two `test_pipeline.py` files collide at collection. Every new file below has a name that is unique today.
- The seeded dev values are fixed and the E2E suite relies on them: api key `dev-api-key` (`.env.example`), MinIO `minioadmin`/`minioadmin`, Postgres `ecet`/`ecet`/`ecet`, tenant `tenant-a` → hook `northwind` (policies, MRI v2 covers `M54.5` and excludes `Z00.00`), tenant `tenant-empty` → no effective policy.
- The fake LLM gateway (`src/ecet/infrastructure/llm/fake_gateway.py`) is the E2E oracle: a redacted note containing `unclear` → `INSUFFICIENT_EVIDENCE` at 0.4 (human review); otherwise `MEETS_NECESSITY` at 0.91 (auto-approve). Never call a real vendor (user's decision, Phase 5).
- TDD: "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Where a task only adds a test for code that already exists (a coverage gap), "watch it fail" means **temporarily breaking the code under test** (the step says how), watching the test go red, and restoring it.
- Commit after every task with a conventional prefix (`feat`, `fix`, `test`, `ci`, `docs`, `build`, `refactor`). **No Claude attribution in commit messages** (the repository's history has none; keep it that way).
- `make check` (lint, typecheck, imports, tests) is green at the end of every task.

### Explicitly out of scope for Phase 7 (do not add)

- Everything in the roadmap's "Deferred (explicitly out of v1)" list: OCR, a transactional outbox, per-tenant keys/JWT, a policy management API, JSON-mode fallback, distributed tracing.
- A delayed-retry queue (Phase 5 #3, accepted for v1), a `POLICIES_ATTACHED` sweeper (Phase 5 #5), a live-vendor recording (Phase 5 #11).
- Freshness checking in the mock client (Phase 4 #6 is accepted; Task 9 documents it in the README instead).
- Docker layer caching for the CI `e2e` job's image build. The job builds uncached; revisit only if its runtime becomes a problem.
- New diagrams. The README reuses the rendered PNGs already in `specs/images/`.
- Any new setting in `config.py`.

---

### Task 1: UC-01's two failure-reason tokens

Closes Phase 3 #23 (`empty_text` vs `no_text`) and #24 (`NO_POLICIES` persists the bare tenant slug).

**Files:**
- Modify: `src/ecet/application/use_cases/ingest_claim_document.py` (`_run_pipeline`'s `NoPoliciesForTenant` branch; `_read_text`)
- Modify: `src/ecet/application/errors.py` (`ExtractionFailed` docstring)
- Test: `tests/unit/application/test_ingest_claim_document.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Claim.failure_reason == "no_text"` for an extractor that returns only whitespace (the same token pypdf raises for a scan), and `Claim.failure_reason == "no_policies"` for `NO_POLICIES`. Task 7's E2E test asserts `"no_policies"`.

- [ ] **Step 1: Change the two assertions to the tokens we want**

In `tests/unit/application/test_ingest_claim_document.py`:

In `test_a_tenant_with_no_policies_persists_no_policies_and_raises`, replace

```python
    assert stored.failure_reason == "tenant-a"
```

with

```python
    # A short token, like every other failure_reason — not the tenant slug that
    # `str(NoPoliciesForTenant)` happens to be.
    assert stored.failure_reason == "no_policies"
```

In `test_empty_extracted_text_is_an_extraction_failure`, replace

```python
    assert stored.failure_reason == "empty_text"
```

with

```python
    # Same condition as pypdf's "no_text" (a scan), so the same token: operators
    # should not have to know which layer noticed the page was blank.
    assert stored.failure_reason == "no_text"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py -k "no_policies or empty_extracted" -v`
Expected: 2 FAIL — `assert 'tenant-a' == 'no_policies'` and `assert 'empty_text' == 'no_text'`.

- [ ] **Step 3: Implement**

In `src/ecet/application/use_cases/ingest_claim_document.py`, in `_run_pipeline`, replace

```python
        except NoPoliciesForTenant as error:
            await self._fail(uow, claim, ClaimStatus.NO_POLICIES, str(error))
            raise
```

with

```python
        except NoPoliciesForTenant:
            await self._fail(uow, claim, ClaimStatus.NO_POLICIES, "no_policies")
            raise
```

In `_read_text`, replace

```python
            raise ExtractionFailed("empty_text")
```

with

```python
            raise ExtractionFailed("no_text")
```

In `src/ecet/application/errors.py`, the `ExtractionFailed` docstring already lists `no_text`; append one sentence so the next reader knows both layers raise it:

```python
class ExtractionFailed(DomainError):
    """No usable text. The message is a short token: `no_text`, `encrypted`,
    `too_many_pages`, `unreadable`, `object_unavailable`. It is persisted verbatim as
    `Claim.failure_reason`, so it must stay free of client-supplied strings. `no_text`
    comes from either the extractor (below its character floor) or UC-01 (only
    whitespace) — one condition, one token."""
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py -v`
Expected: all PASS.

Then: `grep -rn "empty_text" src tests` → no match other than the unrelated `non_empty_text` rule name and `test_empty_text_is_not_an_error`.

- [ ] **Step 5: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add src/ecet/application/use_cases/ingest_claim_document.py src/ecet/application/errors.py tests/unit/application/test_ingest_claim_document.py
git commit -m "fix(ingest): persist short tokens for NO_POLICIES and blank text"
```

---

### Task 2: UC-01's missing tests — every write is PII-free, duplicates in any status, the bare surname

Closes Phase 3 #15 (`Whitfield`), #16 (the `claims.save` argument assertion) and #17 (duplicate-status tests). Test-only, plus one recording list on the fake.

**Files:**
- Modify: `tests/fakes.py` (`FakeClaimRepository` records every write)
- Test: `tests/unit/application/test_ingest_claim_document.py`
- Test: `tests/unit/test_pii_helper.py`

**Interfaces:**
- Consumes: Task 1's tokens (the `EXTRACTION_FAILED` duplicate test asserts `no_text`).
- Produces: `FakeClaimRepository.writes: list[Claim]` — a copy of the argument of every `add` and every `save`, in call order. Existing `saved: list[ClaimId]` is unchanged.

- [ ] **Step 1: Record every write in the fake**

In `tests/fakes.py`, `FakeClaimRepository.__init__`, add after `self.saved: list[ClaimId] = []`:

```python
        #: A copy of every `add`/`save` argument, in call order. The final row can hide
        #: an intermediate write that held something it should not have (ADR-001).
        self.writes: list[Claim] = []
```

In `add`, add as the first line of the body:

```python
        self.writes.append(claim.model_copy())
```

In `save`, add immediately before `self.claims[claim.id] = claim.model_copy()`:

```python
        self.writes.append(claim.model_copy())
```

- [ ] **Step 2: Write the new UC-01 tests**

Append to `tests/unit/application/test_ingest_claim_document.py`:

```python
@pytest.mark.parametrize(
    ("note", "minimum_writes"),
    [
        ("meets", 3),  # add, save POLICIES_ATTACHED, save QUEUED
        ("unclear", 3),
        ("excluded_code", 2),  # add, save REVIEW_PENDING
    ],
)
async def test_no_claim_written_during_ingestion_carries_pii(note: str, minimum_writes: int) -> None:
    """UC-01's test list asks for the ADR-001 assertion on every `claims.save`
    argument, not only on the row that is left at the end."""
    harness = Harness(note=note)

    await harness.ingest()

    assert len(harness.uow.claims.writes) >= minimum_writes
    for written in harness.uow.claims.writes:
        assert_no_pii(written.model_dump_json())


async def test_a_failed_ingestion_writes_no_pii_either() -> None:
    harness = Harness(policies=[])

    with pytest.raises(NoPoliciesForTenant):
        await harness.ingest()

    assert harness.uow.claims.writes
    for written in harness.uow.claims.writes:
        assert_no_pii(written.model_dump_json())


async def test_a_duplicate_of_a_claim_under_review_is_returned_untouched() -> None:
    harness = Harness(note="excluded_code")
    first = await harness.ingest()
    assert first.status is ClaimStatus.REVIEW_PENDING
    writes_before = len(harness.uow.claims.writes)

    second = await harness.ingest()

    assert second.duplicate is True
    assert second.claim_id == first.claim_id
    assert second.status is ClaimStatus.REVIEW_PENDING
    assert harness.extractor.calls == 1
    assert harness.queue.published == []
    assert len(harness.uow.claims.writes) == writes_before
    assert len(harness.uow.review_tasks.tasks) == 1


async def test_a_duplicate_of_a_failed_claim_is_not_retried() -> None:
    """ADR-006: same object, same content, same answer. A blank page stays blank."""
    harness = Harness(extractor=FakeTextExtractor(error=ExtractionFailed("no_text")))
    with pytest.raises(ExtractionFailed):
        await harness.ingest()

    second = await harness.ingest()

    assert second.duplicate is True
    assert second.status is ClaimStatus.EXTRACTION_FAILED
    assert harness.extractor.calls == 1


async def test_the_duplicate_check_runs_before_the_tenant_lookup() -> None:
    """A tenant deactivated after its claim arrived must not turn a replayed event into
    a 404 — the claim exists, and the duplicate answer is the truthful one."""
    harness = Harness()
    first = await harness.ingest()
    harness.uow.tenants.tenants.clear()

    second = await harness.ingest()

    assert second.duplicate is True
    assert second.claim_id == first.claim_id
```

Add `NoPoliciesForTenant` to the existing `from ecet.domain.errors import ...` line at the top of the file (`InvalidObjectKey, NoPoliciesForTenant, PdfTooLarge, TenantNotFound`), and delete the now-redundant local `from ecet.domain.errors import NoPoliciesForTenant` inside `test_a_tenant_with_no_policies_persists_no_policies_and_raises`.

- [ ] **Step 3: Write the bare-surname test**

Append to `tests/unit/test_pii_helper.py`:

```python
async def test_the_fake_redactor_redacts_a_bare_surname() -> None:
    """`FAKE_REDACTOR_NAMES` lists "Whitfield" on its own so a note that drops the first
    name is still caught. No fixture does that, so this is the test that does."""
    result = await FakePiiRedactor().redact(
        "Whitfield tolerated the exam. Follow up with Whitfield in six weeks."
    )

    assert_no_pii(result.text)
    assert result.entity_counts == {"PERSON": 2}
```

- [ ] **Step 4: Run them, then prove each one can fail**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py tests/unit/test_pii_helper.py -v`
Expected: all PASS (these cover existing behaviour).

Prove they bite, one at a time, restoring after each:
1. In `tests/fakes.py`, change `FAKE_REDACTOR_NAMES` to `("Marcus Whitfield", "Alicia Ferreira")` → `test_the_fake_redactor_redacts_a_bare_surname` FAILS. Restore.
2. In `ingest_claim_document.py` `_run_pipeline`, replace the two lines `del text  # ADR-001: …` and `claim.redacted = redacted` with the single line `claim.redacted = redacted.model_copy(update={"text": text})` → `test_no_claim_written_during_ingestion_carries_pii[meets]` FAILS. Restore.
3. In `_execute`, move `await uow.tenants.get(tenant_id)` to just above `duplicate = await uow.claims.find_by_source(...)` → `test_the_duplicate_check_runs_before_the_tenant_lookup` FAILS with `TenantNotFound`. Restore.

Run the file once more: all PASS.

- [ ] **Step 5: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add tests/fakes.py tests/unit/application/test_ingest_claim_document.py tests/unit/test_pii_helper.py
git commit -m "test(ingest): assert every write is PII-free and cover duplicates in any status"
```

---

### Task 3: the api's three loose ends — unhandled-error `claim_id`, empty `/readyz`, `build_container`'s leak

Closes Phase 3 #21 and #26.

**Files:**
- Modify: `src/ecet/interfaces/api/errors.py` (`handle_unmapped`)
- Modify: `src/ecet/interfaces/api/routes/health.py` (`readyz`)
- Modify: `src/ecet/interfaces/api/container.py` (`build_container`)
- Test: `tests/api/test_errors.py`
- Test: `tests/api/test_readyz.py`
- Create: `tests/unit/interfaces/test_api_container.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `build_container(settings) -> ApiContainer` keeps its signature; on any exception it releases what it had already opened and re-raises. `ApiContainer.aclose` is now `contextlib.AsyncExitStack.aclose` of the popped stack (same type, `Callable[[], Awaitable[None]]`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_errors.py`:

```python
async def test_an_unhandled_error_logs_the_claim_id_from_the_path() -> None:
    """api spec: "Unhandled → 500, logged with `claim_id` if known". On a claim route
    it is known — it is in the path."""
    app = FastAPI()
    register_error_handlers(app)

    @app.post("/v1/claims/{claim_id}/boom")
    async def boom(claim_id: str) -> None:
        raise RuntimeError("a bug, not a domain error")

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    with structlog.testing.capture_logs() as captured:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/v1/claims/c-123/boom")

    assert response.status_code == 500
    (entry,) = [e for e in captured if e["event"] == "api.unhandled_error"]
    assert entry["claim_id"] == "c-123"
    assert "c-123" not in response.text
```

Append to `tests/api/test_readyz.py`:

```python
async def test_readyz_with_no_probes_is_not_ready(harness: ApiHarness) -> None:
    """`all([])` is True. A container that registered nothing has checked nothing, and
    "nothing checked" is not "ready"."""
    harness.container.probes.clear()

    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
```

Create `tests/unit/interfaces/test_api_container.py`:

```python
"""`build_container` opens an engine, a broker connection and an HTTP client before it
can fail on the next step (a schema behind head, a broker refusing, a missing spaCy
model). Whatever was already open must be released on the way out — the api process
exits on a startup failure, but a test or a supervisor retrying in-process would not."""

from types import SimpleNamespace
from typing import Any

import pytest

from ecet.config import Settings
from ecet.interfaces.api import container as api_container


class _Engine:
    def __init__(self) -> None:
        self.disposed = False
        self.pool = SimpleNamespace(checkedout=lambda: 0)

    async def dispose(self) -> None:
        self.disposed = True


class _Queue:
    built: list["_Queue"] = []

    def __init__(self, url: str) -> None:
        self.stopped = False
        _Queue.built.append(self)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True

    async def is_healthy(self) -> bool:
        return True


async def _at_head(engine: Any) -> None:
    return None


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch) -> _Engine:
    built = _Engine()
    monkeypatch.setattr(api_container, "create_engine", lambda url: built)
    return built


@pytest.fixture
def queues(monkeypatch: pytest.MonkeyPatch) -> list[_Queue]:
    _Queue.built = []
    monkeypatch.setattr(api_container, "RabbitMqEvaluationQueue", _Queue)
    return _Queue.built


async def test_a_schema_behind_head_disposes_the_engine(
    settings: Settings, engine: _Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def behind(engine: Any) -> None:
        raise RuntimeError("pending migration: database at None")

    monkeypatch.setattr(api_container, "assert_at_head", behind)

    with pytest.raises(RuntimeError, match="pending migration"):
        await api_container.build_container(settings)

    assert engine.disposed


async def test_a_broker_that_refuses_still_disposes_the_engine(
    settings: Settings, engine: _Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Refusing(_Queue):
        async def start(self) -> None:
            raise ConnectionError("connection refused")

    monkeypatch.setattr(api_container, "assert_at_head", _at_head)
    monkeypatch.setattr(api_container, "RabbitMqEvaluationQueue", Refusing)

    with pytest.raises(ConnectionError):
        await api_container.build_container(settings)

    assert engine.disposed


async def test_a_missing_spacy_model_stops_the_queue_and_disposes_the_engine(
    settings: Settings,
    engine: _Engine,
    queues: list[_Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_model(**kwargs: Any) -> None:
        raise OSError("[E050] Can't find model 'en_core_web_lg'")

    monkeypatch.setattr(api_container, "assert_at_head", _at_head)
    monkeypatch.setattr(api_container, "PresidioPiiRedactor", no_model)

    with pytest.raises(OSError, match="E050"):
        await api_container.build_container(settings)

    (queue,) = queues
    assert queue.stopped
    assert engine.disposed


async def test_a_built_container_releases_everything_on_aclose(
    settings: Settings,
    engine: _Engine,
    queues: list[_Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_container, "assert_at_head", _at_head)
    monkeypatch.setattr(api_container, "PresidioPiiRedactor", lambda **kwargs: object())

    built = await api_container.build_container(settings)
    assert not engine.disposed

    await built.aclose()

    (queue,) = queues
    assert queue.stopped
    assert engine.disposed
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/api/test_errors.py tests/api/test_readyz.py tests/unit/interfaces/test_api_container.py -v`
Expected: FAIL — `KeyError: 'claim_id'` (the unhandled log has no such key), `assert 200 == 503`, and the three failure-path container tests with `assert False` on `engine.disposed` / `queue.stopped`. `test_a_built_container_releases_everything_on_aclose` passes already (it pins today's behaviour so the refactor cannot lose it).

- [ ] **Step 3: Implement**

In `src/ecet/interfaces/api/errors.py`, replace the body of `handle_unmapped`'s log call:

```python
    async def handle_unmapped(request: Request, error: Exception) -> JSONResponse:
        # A generic-`Exception` handler still sits behind FastAPI's own HTTPException
        # handler in Starlette's mro-based lookup, so a 401/404 raised via
        # `HTTPException` is untouched by this — only a truly unhandled exception
        # (a bug) reaches here.
        # The router writes `path_params` into the shared scope before the endpoint
        # runs, so a claim route's id is still readable here. Logged, never returned.
        claim_id = getattr(error, "claim_id", None) or request.path_params.get("claim_id")
        log.error(
            "api.unhandled_error",
            error=type(error).__name__,
            path=request.url.path,
            claim_id=claim_id,
        )
```

(leave the `return JSONResponse(...)` below it unchanged).

In `src/ecet/interfaces/api/routes/health.py`, replace

```python
    status_code = 200 if all(results.values()) else 503
```

with

```python
    # `all([])` is True: no probes means nothing was checked, which is not ready.
    status_code = 200 if results and all(results.values()) else 503
```

In `src/ecet/interfaces/api/container.py`, add `import contextlib` at the top of the stdlib imports, then replace the whole `build_container` function (keep `_noop`: it is still `ApiContainer.aclose`'s default). The new function:

```python
async def build_container(settings: Settings) -> ApiContainer:
    # Everything that opens is registered on `cleanup` the moment it exists, so a
    # failure part-way — a schema behind head, a broker refusing, a missing spaCy
    # model — releases what was already open instead of leaking it. On success,
    # `pop_all()` hands the same callbacks to `aclose`, which runs them in reverse.
    async with contextlib.AsyncExitStack() as cleanup:
        engine = create_engine(settings.database_url.get_secret_value())
        cleanup.push_async_callback(engine.dispose)
        # The api's `/metrics` is mounted in `create_app`; this is the gauge behind it
        # that shows a connection held across an external call.
        metrics.DB_POOL_IN_USE.set_function(
            engine.pool.checkedout  # type: ignore[attr-defined]  # QueuePool, the engine default
        )

        if settings.auto_migrate:
            # Compose-only. Phase 2 shipped the migration and the seed but wired neither
            # into a process; this is where they run.
            await upgrade_to_head(settings.database_url.get_secret_value())
            if settings.env == "dev":
                statements = await load_seed(engine)
                log.info("db.seeded", statements=statements)
        else:
            await assert_at_head(engine)

        session_factory = create_session_factory(engine)
        queue = RabbitMqEvaluationQueue(settings.amqp_url.get_secret_value())
        # Registered before `start()`: a start that connects and then fails declaring
        # the topology leaves an open connection, and `stop()` is safe on a half-open
        # queue.
        cleanup.push_async_callback(queue.stop)
        await queue.start()

        redactor = PresidioPiiRedactor(
            replacements=ENTITY_REPLACEMENTS,
            custom_patterns=CUSTOM_PATTERNS,
            score_threshold=SCORE_THRESHOLD,
            spacy_model=settings.spacy_model,
            concurrency=settings.pii_concurrency,
        )
        storage = S3ObjectStorage(
            endpoint_url=settings.s3_endpoint,
            access_key=settings.s3_access_key.get_secret_value(),
            secret_key=settings.s3_secret_key.get_secret_value(),
        )
        clock = SystemClock()

        # UC-09c and the operator retry deliver from the api process, not the worker.
        # max_attempts=1 here, not settings.webhook_max_attempts: POST
        # /v1/claims/{id}/retry-notify *is* the retry, and both ResolveReview and
        # RetryNotify park cleanly on a first failure. In-request retries (up to
        # ~35s of backoff) would hold the request open while /readyz's database
        # probe shares the same pool.
        webhook = HttpxWebhookClient(timeout_s=settings.webhook_timeout_s, max_attempts=1)
        cleanup.push_async_callback(webhook.aclose)

        def uow_factory() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(session_factory)

        ingest = IngestClaimDocument(
            uow_factory=uow_factory,
            storage=storage,
            extractor=PypdfTextExtractor(max_pages=settings.max_pdf_pages),
            redact_pii=RedactPii(redactor),
            run_checks=RunDeterministicChecks(),
            enqueue=EnqueueEvaluation(queue, clock),
            clock=clock,
            max_pdf_bytes=settings.max_pdf_bytes,
        )

        async def database_ready() -> bool:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True

        async def queue_ready() -> bool:
            return await queue.is_healthy()

        async def redactor_ready() -> bool:
            return True  # constructing it loaded the model; reaching here means it is up

        return ApiContainer(
            settings=settings,
            uow_factory=uow_factory,
            storage=storage,
            ingest=ingest,
            list_reviews=ListOpenReviews(uow_factory=uow_factory),
            resolve_review=ResolveReview(uow_factory=uow_factory, webhook=webhook, clock=clock),
            retry_notify=RetryNotify(uow_factory=uow_factory, webhook=webhook, clock=clock),
            probes={
                "database": database_ready,
                "queue": queue_ready,
                "redactor": redactor_ready,
            },
            aclose=cleanup.pop_all().aclose,
        )
```

Note on order: `pop_all()` is evaluated while building the `ApiContainer` argument list, i.e. only after every constructor above has succeeded; if `ApiContainer(...)` itself raised afterwards the callbacks would already be popped — it cannot raise (a dataclass with no validation), so this is fine. The old nested `aclose` closure is gone; `AsyncExitStack.aclose` runs every callback even if an earlier one raises, which is what the nested `try/finally` did.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api tests/unit/interfaces/test_api_container.py -v`
Expected: all PASS.

- [ ] **Step 5: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add src/ecet/interfaces/api/errors.py src/ecet/interfaces/api/routes/health.py src/ecet/interfaces/api/container.py tests/api/test_errors.py tests/api/test_readyz.py tests/unit/interfaces/test_api_container.py
git commit -m "fix(api): log claim_id on unhandled errors, refuse an empty readyz, release a half-built container"
```

---

### Task 4: the queue's health check and the consumer's drain, under test

Closes Phase 4 #12. The roadmap names `RabbitMqConsumer.is_healthy`; that method no longer exists — the reconnect-aware health check lives on `RabbitMqEvaluationQueue.is_healthy` (`src/ecet/infrastructure/queue/rabbitmq.py`), which is what `/readyz` calls. That is the one tested here. Test-only.

**Files:**
- Create: `tests/unit/infrastructure/test_rabbitmq_queue_health.py`
- Modify: `tests/unit/infrastructure/test_rabbitmq_consumer.py` (two drain tests)

**Interfaces:**
- Consumes: `RabbitMqEvaluationQueue._connection`, `._exchange`; `RabbitMqConsumer._on_message`, `.stop()`, `drain_timeout`; the existing `_Delivery` and `build_message` helpers in the consumer test module.
- Produces: nothing for later tasks.

- [ ] **Step 1: Write the health tests**

Create `tests/unit/infrastructure/test_rabbitmq_queue_health.py`:

```python
"""`/readyz` trusts `RabbitMqEvaluationQueue.is_healthy`. aio-pika keeps a robust
connection "open" (`is_closed` False) while it retries, so every one of the four
conditions is load-bearing — each case below is the one that catches its removal."""

from typing import Any, cast

import pytest

from ecet.infrastructure.queue.rabbitmq import RabbitMqEvaluationQueue


class _Connection:
    def __init__(self, *, is_closed: bool = False, reconnecting: bool = False) -> None:
        self.is_closed = is_closed
        self.reconnecting = reconnecting


def queue_with(connection: _Connection | None, *, exchange: bool = True) -> RabbitMqEvaluationQueue:
    queue = RabbitMqEvaluationQueue("amqp://unused/")
    queue._connection = cast(Any, connection)
    queue._exchange = cast(Any, object() if exchange else None)
    return queue


@pytest.mark.parametrize(
    ("queue", "healthy"),
    [
        (queue_with(_Connection()), True),
        (queue_with(None), False),
        (queue_with(_Connection(is_closed=True)), False),
        (queue_with(_Connection(reconnecting=True)), False),
        (queue_with(_Connection(), exchange=False), False),
    ],
    ids=["open", "never-started", "closed", "reconnecting", "no-exchange"],
)
async def test_is_healthy_needs_an_open_settled_connection_and_an_exchange(
    queue: RabbitMqEvaluationQueue, healthy: bool
) -> None:
    assert await queue.is_healthy() is healthy
```

- [ ] **Step 2: Write the drain tests**

Append to `tests/unit/infrastructure/test_rabbitmq_consumer.py` (add `from structlog.testing import capture_logs` to the imports):

```python
async def test_stop_waits_for_an_in_flight_delivery_to_settle() -> None:
    """The SIGTERM story in compose: stop taking deliveries, let the one in hand finish
    and ack, then close — not close under it."""
    release = asyncio.Event()

    async def handle(message: EvaluationMessage) -> None:
        await release.wait()

    consumer = RabbitMqConsumer(
        "amqp://unused/",
        prefetch=1,
        handler=handle,
        should_requeue=lambda _: True,
        drain_timeout=5.0,
    )
    delivery = _Delivery(build_message().model_dump_json().encode("utf-8"))
    in_flight = asyncio.create_task(consumer._on_message(cast(Any, delivery)))
    await asyncio.sleep(0.01)  # into the handler

    stopping = asyncio.create_task(consumer.stop())
    await asyncio.sleep(0.05)
    assert not stopping.done(), "stop() returned with a delivery still in flight"

    release.set()
    await asyncio.wait_for(stopping, timeout=1.0)
    await in_flight

    assert delivery.settled == ["ack"]


async def test_stop_gives_up_on_a_stuck_delivery_after_the_drain_timeout() -> None:
    async def handle(message: EvaluationMessage) -> None:
        await asyncio.Event().wait()  # never finishes

    consumer = RabbitMqConsumer(
        "amqp://unused/",
        prefetch=1,
        handler=handle,
        should_requeue=lambda _: True,
        drain_timeout=0.05,
    )
    delivery = _Delivery(build_message().model_dump_json().encode("utf-8"))
    in_flight = asyncio.create_task(consumer._on_message(cast(Any, delivery)))
    await asyncio.sleep(0.01)

    with capture_logs() as captured:
        await asyncio.wait_for(consumer.stop(), timeout=1.0)

    (timeout_line,) = [e for e in captured if e["event"] == "consumer.drain_timeout"]
    assert timeout_line["in_flight"] == 1
    assert delivery.settled == []

    in_flight.cancel()
    with pytest.raises(asyncio.CancelledError):
        await in_flight
```

- [ ] **Step 3: Run them, then prove they bite**

Run: `uv run pytest tests/unit/infrastructure/test_rabbitmq_queue_health.py tests/unit/infrastructure/test_rabbitmq_consumer.py -v`
Expected: all PASS.

Prove they bite, restoring after each:
1. In `RabbitMqEvaluationQueue.is_healthy`, delete the line `and not connection.reconnecting` → the `reconnecting` case FAILS. Restore.
2. In `RabbitMqConsumer.stop`, replace `await asyncio.wait_for(self._idle.wait(), timeout=self._drain_timeout)` with `pass` → `test_stop_waits_for_an_in_flight_delivery_to_settle` FAILS. Restore.

- [ ] **Step 4: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add tests/unit/infrastructure/test_rabbitmq_queue_health.py tests/unit/infrastructure/test_rabbitmq_consumer.py
git commit -m "test(queue): cover the reconnect-aware health check and the consumer's drain"
```

---

### Task 5: storage, migrations and the pdf budget — the adapter-level gaps

Closes Phase 3 #18 (`head()` on a missing bucket), #19 (the 5-page <200 ms budget) and #20 (`test_migrations.py`'s hardcoded revision and fake behind-head state).

**Files:**
- Modify: `src/ecet/infrastructure/postgres/migrations.py` (extract `head_revision()`)
- Modify: `tests/adapters/test_migrations.py`
- Modify: `tests/adapters/test_s3_storage.py`
- Modify: `scripts/make_fixtures.py` (a 5-page fixture)
- Modify: `tests/unit/infrastructure/test_pypdf_extractor.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `ecet.infrastructure.postgres.migrations.head_revision() -> str | None` — the head revision id of the scripts in `<project_root>/migrations`. `build_all()` gains key `"note_five_pages"` → `tests/fixtures/pdfs/note_five_pages.pdf` (one note fixture per page, all five).

These tests need Docker (the `slow` ones) — run them locally with Docker running.

- [ ] **Step 1: Write the failing migration tests**

Replace the whole of `tests/adapters/test_migrations.py` with:

```python
import pytest
from sqlalchemy import text

from ecet.infrastructure.postgres.migrations import assert_at_head, head_revision, upgrade_to_head
from ecet.infrastructure.postgres.session import create_engine


async def test_a_migrated_database_is_at_head(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    try:
        await assert_at_head(engine)
    finally:
        await engine.dispose()


async def test_upgrade_to_head_is_idempotent(postgres_url: str) -> None:
    await upgrade_to_head(postgres_url)

    engine = create_engine(postgres_url)
    try:
        await assert_at_head(engine)
    finally:
        await engine.dispose()


def test_head_revision_names_a_revision() -> None:
    assert head_revision()


async def test_a_database_behind_head_is_rejected(postgres_url: str) -> None:
    """Genuinely behind, not stamped with garbage: with one migration, "behind head" is
    the base — an `alembic_version` table with no row, which Alembic reads as `None`."""
    head = head_revision()
    engine = create_engine(postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM alembic_version"))
        with pytest.raises(RuntimeError, match="database at None"):
            await assert_at_head(engine)
    finally:
        # `postgres_url` is session-scoped and later tests rely on it. Re-stamping head
        # directly is the undo; `upgrade_to_head` would re-run the initial migration
        # against tables that already exist.
        async with engine.begin() as connection:
            await connection.execute(
                text("INSERT INTO alembic_version (version_num) VALUES (:head)"), {"head": head}
            )
        await assert_at_head(engine)
        await engine.dispose()


async def test_a_database_alembic_never_touched_is_rejected(postgres_url: str) -> None:
    """No `alembic_version` table at all: `get_current_revision()` returns `None` here
    too, and the api must refuse to start rather than run against an empty schema."""
    admin = create_engine(postgres_url)
    try:
        async with admin.connect() as connection:
            # CREATE DATABASE cannot run inside a transaction block.
            autocommit = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await autocommit.execute(text("DROP DATABASE IF EXISTS unstamped"))
            await autocommit.execute(text("CREATE DATABASE unstamped"))
    finally:
        await admin.dispose()

    engine = create_engine(postgres_url.rsplit("/", 1)[0] + "/unstamped")
    try:
        with pytest.raises(RuntimeError, match="database at None"):
            await assert_at_head(engine)
    finally:
        await engine.dispose()
```

- [ ] **Step 2: Write the storage and pdf tests**

Append to `tests/adapters/test_s3_storage.py`:

```python
async def test_a_missing_bucket_raises_object_not_found_on_head(storage: S3ObjectStorage) -> None:
    """`POST /v1/claims/ingest` HEADs before anything else. A bucket that is not there
    must be the same 404 as a key that is not there, not a raw `ClientError` 500."""
    with pytest.raises(ObjectNotFound):
        await storage.head("no-such-bucket", KEY)
```

Append to `tests/unit/infrastructure/test_pypdf_extractor.py` (add `import time`, `from io import BytesIO` and `from pypdf import PdfReader` to the imports):

```python
async def test_a_five_page_note_extracts_within_the_soft_budget(pdfs: dict[str, Path]) -> None:
    """pdf-text-extractor spec: a 5-page fixture extracts in under 200 ms. Best of three,
    so a cold import or a noisy CI neighbour does not fail the build — a real regression
    is slow every time."""
    data = read(pdfs, "note_five_pages")
    assert len(PdfReader(BytesIO(data)).pages) == 5
    extractor = PypdfTextExtractor(max_pages=50)

    timings: list[float] = []
    for _ in range(3):
        started = time.perf_counter()
        await extractor.extract(data)
        timings.append(time.perf_counter() - started)

    assert min(timings) < 0.2, f"best of three took {min(timings) * 1000:.0f} ms"
```

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/unit/infrastructure/test_pypdf_extractor.py -v`
Expected: the budget test FAILS with `KeyError: 'note_five_pages'` (the fixture does not exist yet).

Run: `uv run pytest -m slow tests/adapters/test_migrations.py tests/adapters/test_s3_storage.py -v` (Docker running)
Expected: collection FAILS with `ImportError: cannot import name 'head_revision'`.

- [ ] **Step 4: Implement the fixture and `head_revision`**

In `scripts/make_fixtures.py`, inside `build_all`, add after the `multipage` block (before `scanned`):

```python
    # One note per page, all five — the pdf-text-extractor spec's "5-page fixture"
    # that its <200 ms soft budget is measured against.
    five_pages = destination / "note_five_pages.pdf"
    target = canvas.Canvas(str(five_pages), pagesize=LETTER)
    for note in ("meets", "does_not_meet", "unclear", "excluded_code", "no_codes"):
        _write_lines(target, _note_lines(note))
        target.showPage()
    target.save()
    built["note_five_pages"] = five_pages
```

In `src/ecet/infrastructure/postgres/migrations.py`, replace `_assert_at_head` with:

```python
def head_revision() -> str | None:
    """The newest revision in `migrations/` — what a database at head is stamped with."""
    root = project_root()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    return ScriptDirectory.from_config(config).get_current_head()


def _assert_at_head(connection: Connection) -> None:
    head = head_revision()
    current = MigrationContext.configure(connection).get_current_revision()
    if current != head:
        raise RuntimeError(f"pending migration: database at {current!r}, code expects {head!r}")
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest -m slow tests/adapters/test_migrations.py tests/adapters/test_s3_storage.py -v`
Expected: all PASS.

Run: `uv run pytest tests/unit/infrastructure/test_pypdf_extractor.py -v`
Expected: all PASS; the budget test's measured time is typically well under 100 ms.

Prove the S3 test bites: in `s3.py` temporarily remove `"404"` from `_MISSING_CODES` → the new head test FAILS with `ClientError`. Restore.

- [ ] **Step 6: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add src/ecet/infrastructure/postgres/migrations.py tests/adapters/test_migrations.py tests/adapters/test_s3_storage.py scripts/make_fixtures.py tests/unit/infrastructure/test_pypdf_extractor.py
git commit -m "test(adapters): genuine behind-head migrations, head() on a missing bucket, the pdf budget"
```

---

### Task 6: pin the spaCy model everywhere and cache it in CI

Closes Phase 3 #6. `uv.lock` resolves spaCy **3.8.16**; the matching `en_core_web_lg` release is **3.8.0** (spacy-models `en_core_web_lg-3.8.0` requires `spacy>=3.8.0,<3.9.0`). One version string, four places, and a test that keeps them agreeing.

**Files:**
- Modify: `Dockerfile`
- Modify: `Makefile` (`spacy-model`)
- Modify: `.github/workflows/ci.yml` (`slow` job)
- Create: `tests/unit/test_spacy_pin.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `SPACY_MODEL_VERSION=3.8.0` in `Dockerfile` (build arg), `Makefile` (variable) and `ci.yml` (job env). Task 7's `e2e` job builds the image through compose and inherits the Dockerfile default.

- [ ] **Step 1: Write the failing consistency test**

Create `tests/unit/test_spacy_pin.py`:

```python
"""`spacy download en_core_web_lg` resolves whatever release is newest for the
installed spaCy — unpinned, so two builds a week apart can redact differently. The
version is pinned in three files; this keeps them from drifting apart."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FILES = ("Dockerfile", "Makefile", ".github/workflows/ci.yml")
#: `ARG SPACY_MODEL_VERSION=3.8.0`, `SPACY_MODEL_VERSION ?= 3.8.0`, `SPACY_MODEL_VERSION: "3.8.0"`.
PIN = re.compile(r"SPACY_MODEL_VERSION\s*(?:\?=|=|:)\s*\"?([0-9]+\.[0-9]+\.[0-9]+)")


def pinned_in(relative: str) -> set[str]:
    return set(PIN.findall((ROOT / relative).read_text(encoding="utf-8")))


def test_the_model_version_is_pinned_and_agrees_everywhere() -> None:
    assert [pinned_in(relative) for relative in FILES] == [{"3.8.0"}] * len(FILES)


def test_no_unpinned_download_remains() -> None:
    for relative in FILES:
        content = (ROOT / relative).read_text(encoding="utf-8")
        assert "spacy download en_core_web_lg" not in content, relative
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_spacy_pin.py -v`
Expected: FAIL — `[set(), set(), set()] == [{'3.8.0'}, ...]`, and the unpinned `spacy download en_core_web_lg` in the Dockerfile trips the second test.

- [ ] **Step 3: Pin it**

`Dockerfile`, replace

```dockerfile
ARG SPACY_MODEL=en_core_web_lg
RUN VIRTUAL_ENV=/opt/venv /opt/venv/bin/python -m spacy download ${SPACY_MODEL}
```

with

```dockerfile
ARG SPACY_MODEL=en_core_web_lg
# Pinned: an unversioned `spacy download` takes the newest release compatible with the
# installed spaCy, so two builds could redact differently. 3.8.0 matches spaCy 3.8.x in
# uv.lock. Keep in step with the Makefile and ci.yml (tests/unit/test_spacy_pin.py).
ARG SPACY_MODEL_VERSION=3.8.0
RUN VIRTUAL_ENV=/opt/venv /opt/venv/bin/python -m spacy download \
    ${SPACY_MODEL}-${SPACY_MODEL_VERSION} --direct
```

and in the runtime stage replace

```dockerfile
RUN python -c "import en_core_web_lg"
```

with

```dockerfile
ARG SPACY_MODEL=en_core_web_lg
RUN python -c "import ${SPACY_MODEL}"
```

(so the documented `--build-arg SPACY_MODEL=en_core_web_md` swap no longer fails its own check).

`Makefile`, replace

```make
spacy-model:
	uv run python -m spacy download $(or $(SPACY_MODEL),en_core_web_lg)
```

with

```make
SPACY_MODEL ?= en_core_web_lg
# Keep in step with the Dockerfile and ci.yml (tests/unit/test_spacy_pin.py).
SPACY_MODEL_VERSION ?= 3.8.0

spacy-model:
	uv run python -m spacy download $(SPACY_MODEL)-$(SPACY_MODEL_VERSION) --direct
```

`.github/workflows/ci.yml`, in the `slow` job add under `runs-on: ubuntu-latest`:

```yaml
    env:
      # Keep in step with the Dockerfile and Makefile (tests/unit/test_spacy_pin.py).
      SPACY_MODEL_VERSION: "3.8.0"
```

and replace the step

```yaml
      - name: Download the spaCy model
        run: uv run python -m spacy download en_core_web_lg
```

with

```yaml
      # 590 MB. Cached as a wheel keyed on the pinned version, so a cache hit is a
      # local install and a version bump is a cache miss by construction.
      - name: Cache the spaCy model wheel
        uses: actions/cache@v6
        with:
          path: ~/.cache/spacy-models
          key: spacy-en_core_web_lg-${{ env.SPACY_MODEL_VERSION }}

      - name: Install the pinned spaCy model
        run: |
          WHEEL="en_core_web_lg-${SPACY_MODEL_VERSION}-py3-none-any.whl"
          mkdir -p ~/.cache/spacy-models
          if [ ! -f ~/.cache/spacy-models/"$WHEEL" ]; then
            curl -fsSL -o ~/.cache/spacy-models/"$WHEEL" \
              "https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-${SPACY_MODEL_VERSION}/$WHEEL"
          fi
          uv pip install ~/.cache/spacy-models/"$WHEEL"
```

Check `actions/cache` is still at v6 before committing: `gh api repos/actions/cache/releases/latest --jq .tag_name` (v6.1.0 when this plan was written).

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/unit/test_spacy_pin.py -v` → PASS.

Run: `make spacy-model` → installs `en_core_web_lg-3.8.0` (a no-op reinstall if already present); then `uv run python -c "import en_core_web_lg, spacy; print(spacy.load('en_core_web_lg').meta['version'])"` prints `3.8.0`.

Run: `docker compose build api` → succeeds, and the runtime `import en_core_web_lg` step passes.

If `spacy download <name>-<version> --direct` fails in a uv venv (it shells out to pip, which a uv venv may not have), install the same pinned wheel with uv instead — Makefile: `uv pip install https://github.com/explosion/spacy-models/releases/download/$(SPACY_MODEL)-$(SPACY_MODEL_VERSION)/$(SPACY_MODEL)-$(SPACY_MODEL_VERSION)-py3-none-any.whl`; Dockerfile builder stage (uv is already copied in): `uv pip install --python /opt/venv/bin/python <the same URL built from the two ARGs>`. Keep the `SPACY_MODEL_VERSION` variables so the test still holds, and record the switch under Deviations.

- [ ] **Step 5: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add Dockerfile Makefile .github/workflows/ci.yml tests/unit/test_spacy_pin.py
git commit -m "build(spacy): pin en_core_web_lg 3.8.0 and cache the wheel in CI"
```

---

### Task 7: the E2E suite, `make e2e`, and the CI `e2e` job

Closes the Phase 0 "CI `e2e` job" row, Phase 5 #10 (resolve → `decided_by=human` end to end) and the roadmap's "worker's end-to-end demo assertions". The suite drives the compose stack from outside, exactly as a tenant and a reviewer would.

**Files:**
- Modify: `scripts/make_fixtures.py` (a PDF of `excluded_code.txt`)
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/stack.py`
- Create: `tests/e2e/test_e2e_pipeline.py`
- Modify: `Makefile` (`e2e` target; the demo drops the deterministic-reject note too)
- Modify: `.github/workflows/ci.yml` (`e2e` job)

**Interfaces:**
- Consumes: Task 1's `failure_reason == "no_policies"`. The running stack's published ports (api 8000, MinIO 9000, mock client 8081, Postgres 5432). `S3ObjectStorage(endpoint_url=..., access_key=..., secret_key=...).client()`, `create_engine(url)`, `tests.pii.assert_no_pii`.
- Produces: `build_all()` key `"note_excluded_code"`; `make e2e`; the CI `e2e` job. Task 9's README documents both.

- [ ] **Step 1: The deterministic-reject fixture**

In `scripts/make_fixtures.py`, `build_all`, add after the `unclear` block:

```python
    # ADR-002's demo: Z00.00 is excluded by tenant-a's MRI policy, so the deterministic
    # checks reject it and the LLM is never called.
    excluded = destination / "note_excluded_code.pdf"
    target = canvas.Canvas(str(excluded), pagesize=LETTER)
    _write_lines(target, _note_lines("excluded_code"))
    target.save()
    built["note_excluded_code"] = excluded
```

Run `make fixtures` — it lists `note_excluded_code: tests/fixtures/pdfs/note_excluded_code.pdf`.

- [ ] **Step 2: The marker and readiness gate**

Create `tests/e2e/conftest.py`:

```python
"""E2E: the real compose stack, driven from outside like a tenant and a reviewer would.

Needs the stack up first — `make e2e` does both. Everything under `tests/e2e/` is
marked `e2e`, so the default run (`-m 'not slow and not e2e'`) and the CI `slow` job
(`-m 'not e2e'`) never collect a live-stack dependency.
"""

import subprocess
import time
from pathlib import Path

import httpx
import pytest
from tests.e2e.stack import API, REPO_ROOT

THIS_DIR = Path(__file__).resolve().parent

#: A cold stack builds presidio's engine before `/readyz` answers (see the api's
#: `start_period` in docker-compose.yml); this is that budget plus MinIO's restart.
READY_TIMEOUT_S = 300


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # This hook sees the whole session's items — filter, or every test gets marked.
    for item in items:
        if THIS_DIR in item.path.parents:
            item.add_marker(pytest.mark.e2e)


@pytest.fixture(scope="session", autouse=True)
def stack_ready() -> None:
    deadline = time.monotonic() + READY_TIMEOUT_S
    while True:
        try:
            if httpx.get(f"{API}/readyz", timeout=5).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if time.monotonic() > deadline:
            pytest.fail(f"api not ready after {READY_TIMEOUT_S}s — is the stack up? (make up)")
        time.sleep(2)
    # `minio-setup` restarts MinIO to register the webhook target; an object dropped
    # before it exits either hits a refused connection or fires no notification.
    subprocess.run(["docker", "compose", "wait", "minio-setup"], cwd=REPO_ROOT, check=True)
```

- [ ] **Step 3: The helpers**

Create `tests/e2e/stack.py`:

```python
"""Driving the compose stack from outside: drop an object, poll for what follows.

Only published ports and `docker compose` are used. Postgres is read for exactly one
fact no API exposes — which claim an object key became.
"""

import asyncio
import json
import os
import subprocess
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

import httpx
from sqlalchemy import text

from ecet.infrastructure.postgres.session import create_engine
from ecet.infrastructure.storage.s3 import S3ObjectStorage

REPO_ROOT = Path(__file__).resolve().parents[2]
PDFS = REPO_ROOT / "tests" / "fixtures" / "pdfs"

API = "http://localhost:8000"
MOCK_CLIENT = "http://localhost:8081"
MINIO = "http://localhost:9000"
DATABASE_URL = "postgresql+asyncpg://ecet:ecet@localhost:5432/ecet"
BUCKET = "claims"
#: `.env.example`'s key, which `make up` copies. Not `ECET_API_KEY`: the root conftest
#: strips every `ECET_*` variable from the test environment.
API_KEY = os.environ.get("E2E_API_KEY", "dev-api-key")

T = TypeVar("T")


async def drop(pdf: str, tenant: str) -> str:
    """Upload `tests/fixtures/pdfs/<pdf>.pdf` under a fresh key; MinIO's notification
    does the rest. Returns the key, which every later lookup is keyed on."""
    key = f"tenants/{tenant}/claims/e2e-{pdf}-{uuid4().hex}.pdf"
    storage = S3ObjectStorage(endpoint_url=MINIO, access_key="minioadmin", secret_key="minioadmin")
    async with storage.client() as client:
        await client.put_object(Bucket=BUCKET, Key=key, Body=(PDFS / f"{pdf}.pdf").read_bytes())
    return key


async def eventually(
    check: Callable[[], Awaitable[T | None]], *, what: str, timeout: float = 90.0
) -> T:
    """Poll `check` once a second until it returns something other than None."""
    deadline = time.monotonic() + timeout
    while True:
        result = await check()
        if result is not None:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"gave up after {timeout:.0f}s waiting for {what}")
        await asyncio.sleep(1)


async def deliveries_for(key: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(base_url=MOCK_CLIENT, timeout=10) as client:
        received: list[dict[str, Any]] = (await client.get("/received")).raise_for_status().json()
    return [d for d in received if d["payload"]["source_key"] == key]


async def delivery_for(key: str, *, decided_by: str) -> dict[str, Any] | None:
    return next((d for d in await deliveries_for(key) if d["payload"]["decided_by"] == decided_by), None)


async def claim_row(key: str, *, status: str | None = None) -> dict[str, Any] | None:
    """The claim an object key became, optionally only once it reaches `status`."""
    engine = create_engine(DATABASE_URL)
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT id, status, failure_reason FROM claims WHERE key = :key"),
                    {"key": key},
                )
            ).mappings().first()
    finally:
        await engine.dispose()
    if row is None or (status is not None and row["status"] != status):
        return None
    return dict(row)


async def api(method: str, path: str, *, tenant: str | None = None, **kwargs: Any) -> httpx.Response:
    headers = {"X-API-Key": API_KEY}
    if tenant is not None:
        headers["X-Tenant-Id"] = tenant
    async with httpx.AsyncClient(base_url=API, timeout=30, follow_redirects=True) as client:
        return await client.request(method, path, headers=headers, **kwargs)


def compose_logs(service: str) -> str:
    return subprocess.run(
        ["docker", "compose", "logs", "--no-color", "--no-log-prefix", service],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def log_records(service: str) -> list[dict[str, Any]]:
    """The service's structlog JSON lines. Anything else (a traceback, a banner) is
    skipped rather than failing the parse."""
    records: list[dict[str, Any]] = []
    for line in compose_logs(service).splitlines():
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            records.append(parsed)
    return records


def worker_metrics() -> str:
    """The worker's `:9100` is deliberately not published to the host; read it from
    inside the container, the same way its healthcheck does."""
    return subprocess.run(
        [
            "docker", "compose", "exec", "-T", "worker", "python", "-c",
            "import urllib.request;"
            "print(urllib.request.urlopen('http://localhost:9100/metrics').read().decode())",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
```

- [ ] **Step 4: The E2E tests**

Create `tests/e2e/test_e2e_pipeline.py`:

```python
"""The demo, as assertions. Each test drops its own object under a unique key, so the
tests are independent of each other and of whatever an earlier `make demo` left."""

import json

from tests.e2e.stack import (
    api,
    claim_row,
    compose_logs,
    deliveries_for,
    delivery_for,
    drop,
    eventually,
    log_records,
    worker_metrics,
)
from tests.pii import assert_no_pii


async def _claim_in(claim_id: str, tenant: str, status: str) -> dict[str, object] | None:
    body: dict[str, object] = (
        (await api("GET", f"/v1/claims/{claim_id}", tenant=tenant)).raise_for_status().json()
    )
    return body if body["status"] == status else None


async def test_a_note_that_meets_policy_is_approved_and_delivered_signed() -> None:
    key = await drop("note_simple", "tenant-a")

    delivery = await eventually(
        lambda: delivery_for(key, decided_by="auto"), what=f"the auto webhook for {key}"
    )
    payload = delivery["payload"]
    assert delivery["hook"] == "northwind"
    assert delivery["verified"] is True
    assert payload["outcome"] == "MEETS_NECESSITY"
    assert payload["confidence"] >= 0.85

    # The webhook is sent before the write that records it commits (Phase 6), so the
    # status is polled rather than read once.
    claim = await eventually(
        lambda: _claim_in(payload["claim_id"], "tenant-a", "APPROVED_AUTO"),
        what="the claim to reach APPROVED_AUTO",
    )
    assert_no_pii(json.dumps(delivery))
    assert_no_pii(json.dumps(claim))


async def test_an_unclear_note_waits_for_a_human_whose_decision_is_delivered() -> None:
    key = await drop("note_unclear", "tenant-a")

    row = await eventually(
        lambda: claim_row(key, status="REVIEW_PENDING"), what="the claim to reach REVIEW_PENDING"
    )
    claim_id = str(row["id"])
    assert await deliveries_for(key) == [], "nobody has decided anything yet"

    async def open_task() -> dict[str, object] | None:
        tasks = (await api("GET", "/v1/reviews?limit=200", tenant="tenant-a")).raise_for_status()
        return next((t for t in tasks.json() if t["claim_id"] == claim_id), None)

    task = await eventually(open_task, what="the review task")
    assert task["redacted_text"]
    assert_no_pii(json.dumps(task))

    resolved = await api(
        "POST",
        f"/v1/reviews/{task['task_id']}/resolve",
        tenant="tenant-a",
        json={
            "reviewer": "e2e.reviewer",
            "resolution": "MEETS_NECESSITY",
            "notes": "Therapy dates confirmed with the provider.",
        },
    )
    assert resolved.status_code == 200
    assert resolved.json()["claim_status"] == "REVIEW_RESOLVED"

    delivery = await eventually(
        lambda: delivery_for(key, decided_by="human"), what="the human webhook"
    )
    assert delivery["verified"] is True
    assert delivery["payload"]["outcome"] == "MEETS_NECESSITY"
    assert delivery["payload"]["claim_id"] == claim_id
    assert_no_pii(json.dumps(delivery))


async def test_an_excluded_code_is_rejected_without_calling_the_llm() -> None:
    """ADR-002: the deterministic checks reject it at ingestion, a human gets the task,
    and the worker never sees the claim."""
    key = await drop("note_excluded_code", "tenant-a")

    row = await eventually(
        lambda: claim_row(key, status="REVIEW_PENDING"), what="the deterministic reject"
    )
    claim_id = str(row["id"])

    tasks = (await api("GET", "/v1/reviews?limit=200", tenant="tenant-a")).raise_for_status()
    (task,) = [t for t in tasks.json() if t["claim_id"] == claim_id]
    assert task["reason"] == "DETERMINISTIC_REJECT"
    assert task["evaluation"] is None
    assert not [r for r in log_records("worker") if r.get("claim_id") == claim_id]


async def test_a_tenant_without_policies_records_no_policies_and_notifies_no_one() -> None:
    """ADR-005: no policy context, no evaluation."""
    key = await drop("note_simple", "tenant-empty")

    row = await eventually(
        lambda: claim_row(key, status="NO_POLICIES"), what="the claim to reach NO_POLICIES"
    )
    assert row["failure_reason"] == "no_policies"
    assert await deliveries_for(key) == []


async def test_one_request_id_follows_a_claim_from_the_api_into_the_worker() -> None:
    """Phase 6's done-when, end to end: grep one id across both processes."""
    key = await drop("note_simple", "tenant-a")
    delivery = await eventually(
        lambda: delivery_for(key, decided_by="auto"), what=f"the auto webhook for {key}"
    )
    claim_id = delivery["payload"]["claim_id"]

    def request_ids(service: str) -> set[str]:
        return {
            str(r["request_id"])
            for r in log_records(service)
            if r.get("claim_id") == claim_id and "request_id" in r
        }

    assert request_ids("api") & request_ids("worker")


async def test_both_processes_expose_the_pipeline_metrics() -> None:
    key = await drop("note_simple", "tenant-a")
    await eventually(lambda: delivery_for(key, decided_by="auto"), what="the auto webhook")

    api_metrics = (await api("GET", "/metrics")).raise_for_status().text
    assert 'ecet_ingest_seconds_count{outcome="ingested"}' in api_metrics
    assert "ecet_deterministic_verdict_total" in api_metrics

    worker = worker_metrics()
    assert 'ecet_triage_route_total{route="AUTO_NOTIFY"}' in worker
    assert "ecet_llm_calls_total" in worker
    assert 'ecet_webhook_attempts_total{status_class="2xx"}' in worker


async def test_no_service_log_carries_pii() -> None:
    """The testing spec's log-capture privacy assertion, on the real sinks. Every note
    fixture carries the full synthetic PII block, and this runs after a drop."""
    key = await drop("note_simple", "tenant-a")
    await eventually(lambda: delivery_for(key, decided_by="auto"), what="the auto webhook")

    for service in ("api", "worker", "mock-client"):
        assert_no_pii(compose_logs(service))
```

- [ ] **Step 5: `make e2e`, the demo's fourth drop**

In `Makefile`: add `e2e` to `.PHONY`, and add after the `drop:` target:

```make
# The E2E suite against the compose stack. `up` is a no-op when the stack is already
# running; the suite itself waits for /readyz and for minio-setup to finish.
e2e: fixtures up
	uv run pytest -m e2e
```

In the `demo:` target, add after the `note_unclear` drop line:

```make
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_excluded_code.pdf tenant-a
```

- [ ] **Step 6: Run the suite against a live stack**

```bash
make clean   # a fresh volume set, the same as CI
make e2e
```

Expected: 7 PASSED. If a test fails, `make logs` and the assertion message name the stage; fix the test's assumption only if the stack's behaviour is what the specs say — a genuine product bug gets a unit test first and a fix, recorded under Deviations.

Then confirm the default and slow runs still skip it: `uv run pytest --collect-only -q | grep -c e2e` → `0`.

- [ ] **Step 7: The CI job**

In `.github/workflows/ci.yml`, append a third job:

```yaml
  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - uses: astral-sh/setup-uv@v10.0.1
        with:
          enable-cache: true

      - name: Install Python 3.12
        run: uv python install 3.12

      - name: Sync dependencies
        run: uv sync --frozen

      # `make up` copies .env.example to .env and builds the image uncached; the
      # spaCy model download inside the build dominates this job's runtime.
      - name: Start the stack
        run: make fixtures up

      - name: E2E suite
        run: uv run pytest -m e2e

      - name: Service logs
        if: failure()
        run: docker compose logs --no-color

      - name: Stop the stack
        if: always()
        run: docker compose down -v
```

- [ ] **Step 8: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add scripts/make_fixtures.py tests/e2e Makefile .github/workflows/ci.yml
git commit -m "test(e2e): drive the compose stack end to end and run it in CI"
```

---

### Task 8: the `observability` compose profile

Closes Phase 6 #1 and the observability spec's "Optional compose profile". Two stock images, one scrape config, one provisioned datasource, one dashboard with the three panels the spec names (ingest latency, LLM avoided %, route split) plus the two gauges the worker already refreshes.

**Files:**
- Create: `deploy/observability/prometheus.yml`
- Create: `deploy/observability/grafana/provisioning/datasources/prometheus.yml`
- Create: `deploy/observability/grafana/provisioning/dashboards/ecet.yml`
- Create: `deploy/observability/grafana/dashboards/ecet.json`
- Modify: `docker-compose.yml` (`prometheus`, `grafana` services under `profiles: ["observability"]`)
- Modify: `Makefile` (`observability` target)
- Test: `tests/unit/test_compose.py`

**Interfaces:**
- Consumes: the api's `/metrics` on `api:8000` and the worker's on `worker:9100` (inside the compose network); every `ecet_*` series name in `src/ecet/metrics.py`.
- Produces: `make observability` → Prometheus on http://localhost:9090, Grafana on http://localhost:3000 (dashboard "ECET"). Task 9 documents both.

- [ ] **Step 1: Write the failing compose tests**

Append to `tests/unit/test_compose.py` (add `import json` to the imports):

```python
OBSERVABILITY = COMPOSE.parent / "deploy" / "observability"


def test_the_observability_services_only_start_under_their_profile(compose: dict[str, Any]) -> None:
    """`make up` and `make demo` must not pull Prometheus and Grafana."""
    for service in ("prometheus", "grafana"):
        assert compose["services"][service]["profiles"] == ["observability"]


def test_prometheus_scrapes_both_processes() -> None:
    """Phase 6 #3: one registry per process, so two targets, not one."""
    config = yaml.safe_load((OBSERVABILITY / "prometheus.yml").read_text(encoding="utf-8"))
    targets = {
        target
        for job in config["scrape_configs"]
        for static in job["static_configs"]
        for target in static["targets"]
    }

    assert targets == {"api:8000", "worker:9100"}


def test_the_dashboard_charts_what_the_spec_names() -> None:
    dashboard = json.loads(
        (OBSERVABILITY / "grafana" / "dashboards" / "ecet.json").read_text(encoding="utf-8")
    )
    datasource = yaml.safe_load(
        (OBSERVABILITY / "grafana" / "provisioning" / "datasources" / "prometheus.yml").read_text(
            encoding="utf-8"
        )
    )["datasources"][0]
    expressions = " ".join(
        target["expr"] for panel in dashboard["panels"] for target in panel["targets"]
    )

    assert "ecet_ingest_seconds_bucket" in expressions
    assert "ecet_llm_calls_avoided_total" in expressions
    assert "ecet_triage_route_total" in expressions
    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == datasource["uid"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: the three new tests FAIL (`KeyError: 'prometheus'`, `FileNotFoundError`).

- [ ] **Step 3: The provisioning files**

Create `deploy/observability/prometheus.yml`:

```yaml
# Scraped from inside the compose network. The worker's 9100 is deliberately not
# published to the host, and neither endpoint needs a key.
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: ecet-api
    # `/metrics` is a Starlette mount and 307s to `/metrics/`; Prometheus follows it.
    static_configs:
      - targets: ["api:8000"]
  - job_name: ecet-worker
    static_configs:
      - targets: ["worker:9100"]
```

Create `deploy/observability/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: ecet-prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

Create `deploy/observability/grafana/provisioning/dashboards/ecet.yml`:

```yaml
apiVersion: 1
providers:
  - name: ecet
    folder: ECET
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

Create `deploy/observability/grafana/dashboards/ecet.json`:

```json
{
  "title": "ECET",
  "uid": "ecet",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "10s",
  "time": { "from": "now-1h", "to": "now" },
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Ingest latency p95 (s)",
      "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "ecet-prometheus" },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum by (le, outcome) (rate(ecet_ingest_seconds_bucket[5m])))",
          "legendFormat": "{{outcome}}"
        }
      ]
    },
    {
      "id": 2,
      "type": "stat",
      "title": "LLM calls avoided (ADR-002)",
      "gridPos": { "x": 12, "y": 0, "w": 6, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "ecet-prometheus" },
      "fieldConfig": { "defaults": { "unit": "percentunit" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "expr": "sum(ecet_llm_calls_avoided_total) / clamp_min(sum(ecet_llm_calls_avoided_total) + sum(ecet_llm_calls_total), 1)"
        }
      ]
    },
    {
      "id": 3,
      "type": "piechart",
      "title": "Route split",
      "gridPos": { "x": 18, "y": 0, "w": 6, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "ecet-prometheus" },
      "targets": [
        {
          "refId": "A",
          "expr": "sum by (route) (ecet_triage_route_total)",
          "legendFormat": "{{route}}"
        }
      ]
    },
    {
      "id": 4,
      "type": "bargauge",
      "title": "Claims by status",
      "gridPos": { "x": 0, "y": 8, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "ecet-prometheus" },
      "targets": [
        {
          "refId": "A",
          "expr": "sum by (status) (ecet_claims_by_status)",
          "legendFormat": "{{status}}"
        }
      ]
    },
    {
      "id": 5,
      "type": "bargauge",
      "title": "Open reviews by tenant",
      "gridPos": { "x": 12, "y": 8, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "ecet-prometheus" },
      "targets": [
        {
          "refId": "A",
          "expr": "sum by (tenant) (ecet_review_open)",
          "legendFormat": "{{tenant}}"
        }
      ]
    }
  ]
}
```

- [ ] **Step 4: The compose services and the make target**

In `docker-compose.yml`, add before the top-level `volumes:` key:

```yaml
  prometheus:
    # Opt-in: `make observability` (`docker compose --profile observability up`).
    image: prom/prometheus:v3.5.0
    profiles: ["observability"]
    command: ["--config.file=/etc/prometheus/prometheus.yml"]
    volumes:
      - ./deploy/observability/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    depends_on:
      - api
      - worker

  grafana:
    image: grafana/grafana:12.1.1
    profiles: ["observability"]
    environment:
      # Dev only: anyone who can reach localhost:3000 can view the dashboard. The
      # admin login stays at Grafana's default (admin/admin) for editing.
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
    volumes:
      - ./deploy/observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/observability/grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
```

Before committing, confirm both tags pull: `docker pull prom/prometheus:v3.5.0 && docker pull grafana/grafana:12.1.1`. If either tag is gone, pin the newest release of the same major and update the compose file only.

In `Makefile`, add `observability` to `.PHONY` and:

```make
# The stack plus Prometheus (localhost:9090) and Grafana (localhost:3000, dashboard "ECET").
observability: .env
	docker compose --profile observability up --build -d
```

- [ ] **Step 5: Verify, including against the live stack**

Run: `uv run pytest tests/unit/test_compose.py -v` → all PASS.

Run: `docker compose config --quiet && docker compose --profile observability config --quiet` → both exit 0.

Then live (stack from Task 7 still up, or `make up`):

```bash
make observability
sleep 30
curl -s localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | "\(.labels.job) \(.health)"'
# → ecet-api up / ecet-worker up
curl -s localhost:3000/api/search?query=ECET | jq -r '.[].title'
# → ECET
curl -s 'localhost:9090/api/v1/query?query=sum(ecet_triage_route_total)' | jq '.data.result | length'
# → 1 (after at least one evaluated claim; run `make drop` first if it is 0)
```

Also confirm `make up` alone does not start them: `docker compose ps --services` lists `prometheus`/`grafana` only while the profile is up; after `docker compose --profile observability down && make up`, it does not.

- [ ] **Step 6: `make check` and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add deploy docker-compose.yml Makefile tests/unit/test_compose.py
git commit -m "feat(observability): opt-in Prometheus and Grafana profile with the ECET dashboard"
```

---

### Task 9: the README

Closes Phase 3 #5, Phase 5 #9 and the README bullet of the roadmap's Phase 7, including the accepted notes that were told "belongs in the README" (Phase 4 #6, the `make clean`-after-topology note). Measured numbers come from real runs in Step 1, never from estimates.

**Files:**
- Modify: `README.md` (full rewrite)

**Interfaces:**
- Consumes: Tasks 6–8 (`make spacy-model`, `make e2e`, `make observability`), the existing images in `specs/images/`.
- Produces: nothing later tasks import.

- [ ] **Step 1: Measure**

The quickstart claim must be true. With the image already built once (`make up` in the main checkout), stop the main stack so its ports are free, then time a fresh clone in a scratch directory:

```bash
make clean   # in the main checkout: frees 8000/9000/5432/… for the clone's stack
cd "$(mktemp -d)"
git clone --branch phase-7-polish-e2e /Users/harielgiacomuzzi/Development/FDE-Study/HealthcareTriageEngine ecet
cd ecet
time (uv sync && make demo)
make clean
```

Compose names this project after the directory (`ecet`), so it starts from empty volumes while BuildKit reuses the cached image layers. Record the wall-clock `real` time. **If it exceeds 2 minutes**, find what dominates (almost always presidio's model load in the api's start period) and state the measured time honestly in the README instead of "under 2 minutes" — record that under Deviations.

Then, back in the original checkout, run `make demo` and read the ADR-002 numbers:

```bash
curl -sL localhost:8000/metrics | grep -E '^ecet_llm_calls_avoided_total|^ecet_deterministic_verdict_total'
docker compose exec -T worker python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:9100/metrics').read().decode())" | grep -E '^ecet_llm_calls_total'
```

Record: avoided count, LLM call count, and the ratio `avoided / (avoided + calls)` for that run, plus how many drops `make demo` made.

Finally `du`-check the image size: `docker image ls --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -i ecet` — record it.

- [ ] **Step 2: Write the README**

Replace `README.md` with the following, substituting the four measured values from Step 1 where marked `⟨measured: …⟩` (and nothing else):

````markdown
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

which the Grafana dashboard shows as "LLM calls avoided". In one `make demo` run (⟨measured: number of drops⟩ drops) that was ⟨measured: avoided⟩ avoided against ⟨measured: calls⟩ made. The demo mix is chosen to show every route, not to be representative — on real traffic the ratio is whatever share of notes cite an excluded code or no code at all, and each avoided call is one vendor request's latency and price not paid.

## 4. Quickstart

Prerequisites: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker with Compose v2, `jq`.

```bash
git clone https://github.com/HarielGiacomuzzi/HealthcareTriageEngine.git
cd HealthcareTriageEngine
uv sync
make demo
```

With the image already built, `make demo` took ⟨measured: wall-clock time⟩ from a fresh clone. The first build takes longer: the image is ⟨measured: image size⟩, most of it the spaCy `en_core_web_lg` model presidio needs (see [Image size](#image-size)).

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
H='-H X-API-Key:dev-api-key -H X-Tenant-Id:tenant-a'

# The review queue (redacted note included — the only response that carries it)
curl -s $H localhost:8000/v1/reviews | jq '[.[] | {task_id, claim_id, reason}]'

# Resolve one
curl -s $H -H 'Content-Type: application/json' \
  -d '{"reviewer":"jane.doe","resolution":"MEETS_NECESSITY","notes":"Dates confirmed."}' \
  localhost:8000/v1/reviews/<task_id>/resolve | jq

# A claim
curl -s $H localhost:8000/v1/claims/<claim_id> | jq '{status, failure_reason, evaluation}'

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

CI runs three jobs: `check` (every commit), `slow` (adapters, 85 % coverage gate, the model wheel cached by version) and `e2e` (the compose stack, fake LLM). Layer rules are enforced by import-linter; the domain and application layers are `mypy --strict`. Test strategy: [testing spec](specs/05-platform/testing.md).

### Image size

The single image is ⟨measured: image size⟩, dominated by `en_core_web_lg`. For a smaller demo image build with the medium model and tell the api to load it:

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
````

- [ ] **Step 3: Verify every claim in it**

- Every relative link resolves: `grep -oE '\]\((specs|docs)/[^)#]+' README.md | sed 's/](//' | xargs -I{} test -e {} && echo links ok`.
- Every `make` target named exists: `for t in up down logs demo drop dlq-replay observability clean install spacy-model check e2e; do grep -q "^$t:" Makefile || echo "missing $t"; done` prints nothing.
- The API examples run: with the stack up, run the `/v1/reviews` and `/v1/claims/<id>` curls with a real id and confirm they answer 200.
- No `⟨measured` marker remains: `grep -c '⟨measured' README.md` → `0`.

- [ ] **Step 4: Commit**

```bash
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add README.md
git commit -m "docs(readme): quickstart, API and ops guide, ADRs, measured ADR-002 savings"
```

---

### Task 10: close the roadmap and the specs

Records what Phase 7 shipped and marks every carry-over it closed, so the roadmap ends with no row still pointing at a future phase.

**Files:**
- Modify: `specs/06-roadmap.md`
- Modify: `specs/05-platform/testing.md`
- Modify: `specs/05-platform/docker-compose.md`
- Modify: `specs/05-platform/observability.md`
- Modify: `docs/plans/2026-09-16-phase-7-polish-e2e.md` (this file: the Deviations section)

**Interfaces:** none.

- [ ] **Step 1: The Phase 7 section**

In `specs/06-roadmap.md`, replace the whole `## Phase 7 — Polish & E2E` section (heading through its `Specs:` line) with:

```markdown
## Phase 7 — Polish & E2E
Plan: [`docs/plans/2026-09-16-phase-7-polish-e2e.md`](../docs/plans/2026-09-16-phase-7-polish-e2e.md).
- `tests/e2e/` drives the compose stack from outside — a real `put_object` into MinIO, the signed webhook read back from the mock client, a review resolved through `/v1/reviews/{id}/resolve` ending in `decided_by=human`, ADR-002's deterministic reject with no worker activity, ADR-005's `NO_POLICIES`, one `request_id` across both processes' logs, both `/metrics` endpoints, and `assert_no_pii` over every service's logs. `make e2e` runs it; the CI `e2e` job runs it on every push.
- The `observability` compose profile: Prometheus scraping `api:8000` and `worker:9100`, Grafana with a provisioned "ECET" dashboard (ingest p95, LLM calls avoided, route split, claims by status, open reviews). `make observability`.
- `en_core_web_lg` pinned to 3.8.0 in the Dockerfile, Makefile and CI (a test keeps them agreeing); the CI `slow` job caches the wheel by version. The runtime image check follows the `SPACY_MODEL` build arg.
- README rewrite: measured quickstart, workflow and integration diagrams, the ADR table, measured ADR-002 savings, API and ops guide, and the v1 limitations (static key, mock-client freshness, `make clean` after a topology change, image size, `make spacy-model`).
- `failure_reason` is `no_text` for blank extracted text and `no_policies` for `NO_POLICIES`; `handle_unmapped` logs the path's `claim_id`; `/readyz` with no probes is 503; `build_container` releases what it opened when a later step fails.
- The test gaps from Phases 3–5: every UC-01 write asserted PII-free, duplicates in non-`QUEUED` statuses and before the tenant lookup, the bare-surname redaction, `head()` on a missing bucket, the 5-page <200 ms extraction budget, a genuinely behind-head and an unstamped database, the queue's reconnect-aware health check, and the consumer's drain and drain timeout.
- Deviations from the plan: [Carried over from Phase 7](#carried-over-from-phase-7).
Done when: README quickstart reproduces the demo from a clean clone (measured, see README §4); `pytest -m e2e` is green in CI.
Specs: [testing](05-platform/testing.md), [docker-compose](05-platform/docker-compose.md), [observability](05-platform/observability.md).
```

- [ ] **Step 2: Mark the closed rows**

In the carry-over tables, change the "Closed by" cell of each row below to the text shown:

| Table | Row | New "Closed by" |
|---|---|---|
| Phase 0 | `— \| CI e2e job` | `Phase 7 — closed` |
| Phase 3 | #5, #6, #15, #16, #17, #18, #19, #20, #21, #23, #24, #26 | `Phase 7 — closed` |
| Phase 4 | #6 | `accepted — documented in the README (Phase 7)` |
| Phase 4 | #12 | `Phase 7 — closed (the health check lives on RabbitMqEvaluationQueue; the consumer's was removed)` |
| Phase 5 | #9, #10 | `Phase 7 — closed` |
| Phase 6 | #1 | `Phase 7 — closed` |

Then append after the Phase 6 table:

```markdown
## Carried over from Phase 7

Every deviation recorded in [`docs/plans/2026-09-16-phase-7-polish-e2e.md`](../docs/plans/2026-09-16-phase-7-polish-e2e.md#deviations-from-spec-record-in-the-pr-description). Phase 7 is the last v1 phase: nothing here is scheduled, each row is accepted or belongs to the deferred list below.

| # | Deviation in Phase 7 | Status |
|---|----------------------|--------|
```

and add one row per bullet of this plan's final Deviations section (Step 4).

Verify nothing still points forward: `grep -nE '\| Phase 7 *\|$' specs/06-roadmap.md` prints nothing (every Phase 7 row now says "closed" or "accepted"), and `grep -rn "Phase 8" specs docs README.md` prints nothing.

- [ ] **Step 3: The platform specs**

`specs/05-platform/testing.md`, in the Layers table replace the E2E row's last cell `manual / release` with `` `make e2e`; CI `e2e` job on every push ``, and append a section:

```markdown
## E2E (`tests/e2e/`)
Drives the compose stack only through published ports (api, MinIO S3, mock client, Postgres for the key → claim lookup) and `docker compose logs`/`exec`. Each test drops its own object under a unique key, so tests are order-independent and survive a previous `make demo`. The fake LLM gateway is the oracle: `unclear` in the note → human review, otherwise auto-approve.
```

`specs/05-platform/docker-compose.md`: in the services table add rows

```markdown
| prometheus    | prom/prometheus (profile `observability`) | 9090 | — |
| grafana       | grafana/grafana (profile `observability`) | 3000 | — |
```

and in "Demo flow", after the third drop sentence, add: `A fourth drop, note_excluded_code.pdf for tenant-a, is rejected by the deterministic checks (ADR-002) and opens a review without an LLM call.`

`specs/05-platform/observability.md`: replace the last line of "Optional compose profile `observability`" with

```markdown
Prometheus + Grafana with one dashboard JSON (ingest latency, LLM avoided %, route split, claims by status, open reviews). `make observability`; files under `deploy/observability/`. Shipped in [Phase 7](../06-roadmap.md#phase-7--polish--e2e).
```

- [ ] **Step 4: Deviations**

Fill this plan's `## Deviations from spec (record in the PR description)` section with every deviation that actually happened during Tasks 1–9 (each implementer's report lists theirs), keeping the two recorded at planning time.

- [ ] **Step 5: Verify and commit**

```bash
make check
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
git add specs docs/plans/2026-09-16-phase-7-polish-e2e.md
git commit -m "docs(phase-7): record what shipped and close every carried-over row"
```

---

## Phase exit criteria

1. `make check` green.
2. `uv run pytest -m 'not e2e' --cov=ecet --cov-fail-under=85` green with Docker running and `make spacy-model` done.
3. `make clean && make e2e` → 7 passed.
4. The README's quickstart time is a measured number from a fresh clone with the image cached, and every link and `make` target in it exists.
5. `make observability` shows both scrape targets `up` and the "ECET" dashboard in Grafana.
6. No row in `specs/06-roadmap.md`'s carry-over tables names a phase that has not happened.
7. CI (`check`, `slow`, `e2e`) green on the pull request.

## Deviations from spec (record in the PR description)

- The roadmap's Phase 4 #12 names `RabbitMqConsumer.is_healthy`; that method no longer exists. The reconnect-aware check it described lives on `RabbitMqEvaluationQueue.is_healthy`, which `/readyz` calls, and that is what Task 4 tests. Closed by: n/a, the roadmap row is corrected in Task 10.
- The CI `e2e` job builds the image with no Docker layer cache, so each run downloads the spaCy model inside the build. Closed by: n/a, accepted unless the job's runtime becomes a problem.
- Task 3: the brief's `_Queue.built` class attribute needed a `ClassVar` annotation to satisfy ruff RUF012. Type-only. Closed by: n/a.
- Task 7: `docker compose wait minio-setup` exits 1 once the one-shot has already exited (Compose v5.5), which is always the case by the time `/readyz` answers. The E2E conftest polls `docker compose ps -a` for `exited 0` instead, and `make demo` got the same bounded poll. Closed by: n/a, permanent.
- Task 7: `tests/e2e/stack.py`'s `eventually` uses PEP 695 `[T]` type parameters and names its deadline `within_s` (the brief's `TypeVar`/`timeout` failed ruff). Closed by: n/a.
- Task 7 (product bug found by the E2E suite): UC-01 publishes the evaluation before it commits `QUEUED`, so a fast worker could read the claim still `POLICIES_ATTACHED` and ack the message as a duplicate, stranding the claim `QUEUED` with no message. UC-06 now holds 0.5 s outside any unit of work and raises `ClaimNotYetQueued`, which the worker requeues; the UC-06 and worker specs say so. A commit slower than ~2.5 s (five immediate deliveries) dead-letters the message, recoverable with `make dlq-replay` because the claim is then `QUEUED`; a claim truly stuck `POLICIES_ATTACHED` now dead-letters its message instead of acking it (DLQ noise, not loss), and under a failed commit plus a MinIO re-send inside that window two messages can both evaluate (at-least-once, ADR-006). Closed by: n/a, accepted — the transactional outbox stays out of v1.
- Task 7: `minio-setup` failed (exit 1) whenever it re-ran against an existing `miniodata` volume, because `mc event add` rejects an overlapping rule; it now skips the add when the rule exists. Closed by: n/a.
- Task 7: the E2E review lists use `?limit=200`; on a long-lived stack that is never `make clean`ed, open tenant-a tasks can accumulate past that and a new task falls outside the page. CI always starts from empty volumes. Closed by: n/a, accepted.
- Task 9: the fresh-clone quickstart (`uv sync && make demo`, image cached) measured 2 min 16 s, not under 2 minutes; the README states the measured time. Presidio's model load inside the api's start period dominates. Closed by: n/a, accepted.
