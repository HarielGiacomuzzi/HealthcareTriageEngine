# Phase 5 — Human Review & Ops Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A claim parked for a human can be listed, read and decided over HTTP, and resolving it delivers a signed `decided_by: "human"` webhook. Every other place a claim gets stuck gets a way out too: `NOTIFY_FAILED` through `POST /v1/claims/{id}/retry-notify`, `POLICIES_ATTACHED` through re-ingesting the same object, and a dead-lettered message through `ecet dlq-replay`. The vendor adapter gets checked against one real response, recorded once.

**Architecture:** No new ports, no new adapters, and one new repository method. UC-09b (`ListOpenReviews`) and UC-09c (`ResolveReview`) join UC-09a in `application/use_cases/human_review.py`. The operator retry is its own use case, `RetryNotify`. All three take a `uow_factory`, the same way `IngestClaimDocument` and `EvaluateClaim` do. Mapping a delivery failure to a short `failure_reason` token currently lives inline in `RouteDecision`. It moves into `NotifyClient.attempt`, so UC-07, UC-09c and the retry all park a failed webhook the same way. `ReviewTaskRepository.find_by_claim` is how the retry tells a human decision from an automatic one. A claim that has a review task was decided by a human, and `review_tasks.claim_id` is unique. The `POLICIES_ATTACHED` retry is not a new endpoint. UC-01's duplicate check re-publishes a claim it finds still `POLICIES_ATTACHED`. So MinIO re-sending an event it got a 503 for, or an operator re-posting `/v1/claims/ingest`, is the retry. `ecet dlq-replay` lives beside the consumer in `infrastructure/queue/rabbitmq.py` and reuses `declare_topology` and the queue constants. The api container now builds an `HttpxWebhookClient`, because resolve and retry deliver from the api process.

**Tech Stack:** Python 3.12, FastAPI 0.141 (new `routes/reviews.py`), aio-pika 9.6 (`Queue.get` + publisher confirms for the replay), Typer (the `dlq-replay` command), openai SDK + httpx event hooks (the one-off live recording), pytest with the `tests/fakes.py` fakes, `httpx.MockTransport` and testcontainers.

**Spec:** [`specs/06-roadmap.md` §Phase 5](../../specs/06-roadmap.md#phase-5--human-review--ops-endpoints) and its "Carried over from Phase 3 / Phase 4" rows closed by Phase 5. It pulls in [UC-09](../../specs/02-use-cases/UC-09-human-review.md), [UC-08](../../specs/02-use-cases/UC-08-notify-client.md), [UC-01](../../specs/02-use-cases/UC-01-ingest-claim-document.md), [UC-05](../../specs/02-use-cases/UC-05-enqueue-evaluation.md), [api](../../specs/04-interfaces/api.md), [worker](../../specs/04-interfaces/worker.md), [queue-rabbitmq](../../specs/03-infrastructure/queue-rabbitmq.md), [llm-gateway](../../specs/03-infrastructure/llm-gateway.md), [webhook-client](../../specs/03-infrastructure/webhook-client.md), [postgres](../../specs/03-infrastructure/postgres.md), [claim](../../specs/01-domain/claim.md), [evaluation](../../specs/01-domain/evaluation.md), [config](../../specs/05-platform/config.md), [docker-compose](../../specs/05-platform/docker-compose.md), [testing](../../specs/05-platform/testing.md) and [project-layout](../../specs/05-platform/project-layout.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- Layer rule (`import-linter`; `make imports` must stay green): `ecet.domain` imports stdlib + pydantic only. `ecet.application` imports domain + stdlib + pydantic, and **never** `openai`, `httpx`, `aio_pika`, `sqlalchemy`, `fastapi`, `typer` or `ecet.config`. Only `ecet.infrastructure`, `ecet.interfaces` and `ecet/cli.py` may import those. The `forbidden` contract already lists all of them; do not weaken it.
- `mypy --strict` covers `src/ecet/domain` and `src/ecet/application`. `mypy src/ecet` (repo-wide `disallow_untyped_defs = true`) covers infrastructure and interfaces. Every function added in this phase is annotated.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`. `scripts/` is linted too.
- **ADR-001 still governs, and this phase ships the first API route that returns note text.** `ReviewTaskView.redacted_text` (UC-09b) is the only place the API returns claim text, and it is the redacted text. `GET /v1/claims/{id}` keeps omitting it. Nothing may log `redacted_text`, a reviewer's `notes` (free text a human typed), a prompt body, a webhook secret or an HMAC signature. `ClientNotification` is unchanged and carries no text. Every test that touches text or a payload ends with `assert_no_pii(...)`.
- **Log through `structlog` only.** Never use `logging.getLogger`; Phase 6 carry-over #12 still stands.
- **`EvaluationMessage` is a frozen wire contract.** `ecet dlq-replay` re-publishes the body byte for byte and does not re-validate or re-shape it.
- **`declare_topology` and the queue constants are shared.** The replay imports `DLQ`, `ROUTING_KEY` and `declare_topology` from `ecet/infrastructure/queue/rabbitmq.py`. It must not spell any queue or exchange name as a literal.
- **`SqlAlchemyUnitOfWork` is single-use.** Every new use case takes a `uow_factory: Callable[[], UnitOfWork]` and opens one unit of work per call.
- **`PostgresClaimRepository.save` refuses a claim it never read.** Every new use case `get()`s (or `find_by_source()`s) the claim in the same unit of work before saving it.
- **`failure_reason` is a short token, never an exception message.** This phase adds no token. A failed resolve or retry reuses `webhook_rejected`, `webhook_unreachable` and `tenant_inactive`, which now live in `application/use_cases/notify_client.py`. Exception detail goes on `Claim.last_notify_error`.
- **Tenant isolation is the use case's job, and a miss is a 404, never a 403.** `ReviewTaskRepository.get(task_id)` has no tenant parameter, so `ResolveReview` compares `task.tenant_id` against the caller's tenant before it touches anything. A task belonging to another tenant raises `ReviewTaskNotFound`, exactly like one that does not exist.
- **Nothing in CI, compose or pytest calls a real LLM vendor — nor does anything else, ever.** `ECET_LLM_PROVIDER=fake` stays the default in `.env.example`, and `docker-compose.yml` never overrides it. Task 8 (a script that would have called a live endpoint once, by hand) was cancelled at the user's direction; the OpenAI-compatible adapter is verified against `httpx.MockTransport` only.
- Adapter tests that need a container live in `tests/adapters/`, where that directory's `conftest.py` auto-marks them `slow`. Run them with `-m slow`, which overrides the default `addopts` marker filter.
- TDD: every step pair is "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Commit after every task with a conventional prefix. **No Claude attribution in commit messages.**
- Git in this environment needs `export DEVELOPER_DIR=/Library/Developer/CommandLineTools` before any `git` command (Xcode license error otherwise).

### Explicitly out of scope for Phase 5 (do not add)

- `/metrics`, `prometheus_client`, the worker healthcheck and every `ecet_*` metric (Phase 6).
- A delayed-retry queue, a per-message requeue backoff, or turning on the OpenAI SDK's own retries. Requeue keeps having no delay; `ecet dlq-replay` is the recovery for a claim that burned its delivery budget (deviation 11).
- A transactional outbox or a background sweeper for `POLICIES_ATTACHED` claims. The retry is re-ingesting the same object (deviation 4).
- Redacted text on `GET /v1/claims/{id}` (deviation 3).
- Moving the LLM call or the webhook POST out of the open unit of work (Phase 4 carry-over #11, Phase 6).
- Review assignment, claiming, cursors or any pagination beyond `limit`.
- Per-tenant API keys or JWT, and a tenant header on `retry-notify` (the api spec gives it the api key only; deviation 7).
- The README rewrite, the E2E suite and `tests/e2e/` (Phase 6).
- Any pytest test, CI job or compose default that calls a real vendor.

---

### Task 1: The `EVALUATION_FAILED → NOTIFY_FAILED` edge and `ReviewTaskRepository.find_by_claim`

**Files:**
- Modify: `src/ecet/domain/claim.py`
- Modify: `src/ecet/domain/ports/review_task_repository.py`
- Modify: `src/ecet/infrastructure/postgres/repositories.py`
- Modify: `tests/fakes.py`
- Test: `tests/unit/domain/test_claim.py`, `tests/unit/domain/test_ports.py`, `tests/adapters/test_review_task_repository.py`

**Interfaces:**
- Consumes: `ClaimStatus`, `_ALLOWED`, `ReviewTask`, `ReviewTaskRow`, `review_task_from_row` (all existing).
- Produces:
  - `ClaimStatus.EVALUATION_FAILED.can_transition_to(ClaimStatus.NOTIFY_FAILED) is True`.
  - `ReviewTaskRepository.find_by_claim(claim_id: ClaimId) -> ReviewTask | None`, which returns the claim's task in **any** status. It is implemented by `PostgresReviewTaskRepository` and `tests.fakes.FakeReviewTaskRepository`.

Both are prerequisites for later tasks. UC-09c (Task 2) resolves claims that UC-06 parked in `EVALUATION_FAILED`, and that resolution's webhook can fail like any other, but the state machine has no edge for it today. `RetryNotify` (Task 4) needs to find a *resolved* task, and `find_open_by_claim` deliberately cannot.

- [x] **Step 1: Write the failing domain test**

Append to `tests/unit/domain/test_claim.py`:

```python
def test_a_resolved_evaluation_failure_can_still_fail_delivery() -> None:
    """UC-09c resolves claims UC-06 parked in EVALUATION_FAILED (it opens a review task
    for them), and the human decision's webhook can fail like any other."""
    claim = build_claim(status=ClaimStatus.EVALUATION_FAILED, failure_reason="llm_invalid_output")

    claim.transition(ClaimStatus.NOTIFY_FAILED, reason="webhook_unreachable", now=LATER)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_unreachable"
```

- [x] **Step 2: Write the failing fake-repository test**

Append to `tests/unit/domain/test_ports.py`:

```python
async def test_review_task_repository_finds_a_claims_task_in_any_status() -> None:
    """`RetryNotify` needs the task after it is resolved; `find_open_by_claim` cannot
    see it by design."""
    repo = FakeReviewTaskRepository()
    claim_id = ClaimId(uuid4())
    task = ReviewTask(
        id=uuid4(),
        claim_id=claim_id,
        tenant_id=TENANT,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )
    await repo.add(task)
    task.status = ReviewStatus.RESOLVED
    await repo.save(task)

    assert await repo.find_by_claim(claim_id) == task
    assert await repo.find_by_claim(ClaimId(uuid4())) is None
```

- [x] **Step 3: Run both and watch them fail**

Run: `uv run pytest tests/unit/domain/test_claim.py tests/unit/domain/test_ports.py -v`
Expected: FAIL. `test_a_resolved_evaluation_failure_can_still_fail_delivery` fails with `InvalidTransition: EVALUATION_FAILED -> NOTIFY_FAILED is not an allowed transition`, and the port test fails with `AttributeError: 'FakeReviewTaskRepository' object has no attribute 'find_by_claim'`.

- [x] **Step 4: Add the edge**

In `src/ecet/domain/claim.py`, replace the `EVALUATION_FAILED` entry of `_ALLOWED`:

```python
    # UC-06 opens a review task on invalid LLM output; a human still resolves it, and
    # that resolution's webhook can fail like any other (UC-09c step 4).
    ClaimStatus.EVALUATION_FAILED: frozenset(
        {ClaimStatus.REVIEW_RESOLVED, ClaimStatus.NOTIFY_FAILED}
    ),
```

`test_every_allowed_edge_survives_a_real_transition` already walks `_ALLOWED`, so it covers the new edge without change.

- [x] **Step 5: Add the port method**

In `src/ecet/domain/ports/review_task_repository.py`, add below `find_open_by_claim`:

```python
    async def find_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        """The claim's task in any status. `review_tasks.claim_id` is unique, so a claim
        has at most one task, ever — which is how the operator retry tells a human
        decision (UC-09c) from an automatic one (UC-07)."""
        ...
```

- [x] **Step 6: Implement it in the fake**

In `tests/fakes.py`, add to `FakeReviewTaskRepository` below `find_open_by_claim`:

```python
    async def find_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        return next((task for task in self.tasks.values() if task.claim_id == claim_id), None)
```

- [x] **Step 7: Run the unit tests and watch them pass**

Run: `uv run pytest tests/unit/domain -v`
Expected: PASS, including both new tests and `test_fakes_satisfy_their_ports`.

- [x] **Step 8: Write the failing adapter test**

Append to `tests/adapters/test_review_task_repository.py`:

```python
async def test_find_by_claim_returns_a_resolved_task_too(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert await repository.find_by_claim(claim.id) is None
        await repository.add(task)
        task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="nurse", notes=None, now=NOW)
        await repository.save(task)
        await session.commit()

    async with session_factory() as session:
        found = await PostgresReviewTaskRepository(session).find_by_claim(claim.id)

    assert found is not None
    assert found.id == task.id
    assert found.status is ReviewStatus.RESOLVED
    assert found.resolution is Decision.DOES_NOT_MEET
```

- [x] **Step 9: Run it and watch it fail**

Run: `uv run pytest tests/adapters/test_review_task_repository.py -m slow -v`
Expected: FAIL with `AttributeError: 'PostgresReviewTaskRepository' object has no attribute 'find_by_claim'`.

- [x] **Step 10: Implement it in the Postgres repository**

In `src/ecet/infrastructure/postgres/repositories.py`, add to `PostgresReviewTaskRepository` below `find_open_by_claim`:

```python
    async def find_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        """Any status. `review_tasks.claim_id` is unique, so this is at most one row."""
        statement = (
            select(ReviewTaskRow)
            .where(ReviewTaskRow.claim_id == claim_id)
            .execution_options(populate_existing=True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return None if row is None else review_task_from_row(row)
```

- [x] **Step 11: Run the adapter test and watch it pass**

Run: `uv run pytest tests/adapters/test_review_task_repository.py -m slow -v`
Expected: PASS (every test in the file).

- [x] **Step 12: Check the layers, the types and the default suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 13: Commit**

```bash
git add src/ecet/domain/claim.py src/ecet/domain/ports/review_task_repository.py \
  src/ecet/infrastructure/postgres/repositories.py tests/fakes.py \
  tests/unit/domain/test_claim.py tests/unit/domain/test_ports.py \
  tests/adapters/test_review_task_repository.py
git commit -m "feat(review): EVALUATION_FAILED -> NOTIFY_FAILED edge and ReviewTaskRepository.find_by_claim"
```

---

### Task 2: `NotifyClient.attempt` and UC-09c ResolveReview

**Files:**
- Modify: `src/ecet/application/use_cases/notify_client.py`
- Modify: `src/ecet/application/use_cases/route_decision.py`
- Modify: `src/ecet/application/use_cases/human_review.py`
- Test: `tests/unit/application/test_notify_client.py`, `tests/unit/application/test_resolve_review.py` (create)

**Interfaces:**
- Consumes: `NotifyClient.execute` (Phase 4); `ReviewTask.resolve`, `HumanResolution` (Phase 1); `ReviewTaskRepository.get` / `save`; the `EVALUATION_FAILED → NOTIFY_FAILED` edge (Task 1).
- Produces:
  - `ecet.application.use_cases.notify_client`: the constants `REJECTED = "webhook_rejected"`, `UNREACHABLE = "webhook_unreachable"` and `TENANT_INACTIVE = "tenant_inactive"` (moved from `route_decision.py`), plus `NotifyClient.attempt(claim, *, outcome: Decision, confidence: float, decided_by: DecidedBy) -> str | None`. It returns `None` when the webhook is delivered, and otherwise the `failure_reason` token.
  - `ecet.application.use_cases.human_review`:
    - `ResolveReviewCommand` (fields `task_id: UUID`, `tenant_id: TenantId`, `reviewer: str`, `resolution: HumanResolution`, `notes: str | None = None`).
    - `ResolveReviewResult` (fields `task_id: UUID`, `claim_id: ClaimId`, `claim_status: ClaimStatus`).
    - `ResolveReview`, with `__init__(*, uow_factory: Callable[[], UnitOfWork], webhook: WebhookClient, clock: Clock)` and `async execute(command: ResolveReviewCommand) -> ResolveReviewResult`.

`RouteDecision` today maps a delivery failure to a token inline: `TenantNotFound` becomes `tenant_inactive`, `WebhookPermanentError` becomes `webhook_rejected`, and any other `WebhookError` becomes `webhook_unreachable`. UC-09c and the retry in Task 4 need the same mapping, so this task moves it into `NotifyClient.attempt` once and points `RouteDecision` at it. The existing `test_route_decision.py` is the regression guard for that move.

- [x] **Step 1: Write the failing `attempt` tests**

Append to `tests/unit/application/test_notify_client.py`:

```python
@pytest.mark.parametrize(
    ("error", "token"),
    [
        (WebhookPermanentError("400"), "webhook_rejected"),
        (WebhookTransientError("3 attempts failed"), "webhook_unreachable"),
    ],
)
async def test_attempt_folds_a_delivery_failure_into_its_token(
    error: Exception, token: str
) -> None:
    claim = build_claim()

    reason = await build_use_case(FakeWebhookClient(error=error)).attempt(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert reason == token
    assert claim.notification_attempts == 1
    assert claim.last_notify_error is not None


async def test_attempt_parks_an_inactive_tenant_instead_of_raising() -> None:
    claim = build_claim()
    inactive = build_tenant().model_copy(update={"active": False})
    use_case = NotifyClient(FakeTenantRepository([inactive]), FakeWebhookClient(), FixedClock(NOW))

    reason = await use_case.attempt(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert reason == "tenant_inactive"
    assert claim.last_notify_error is not None


async def test_attempt_returns_none_when_the_webhook_is_delivered() -> None:
    webhook = FakeWebhookClient()

    reason = await build_use_case(webhook).attempt(
        build_claim(), outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert reason is None
    assert len(webhook.deliveries) == 1
```

- [x] **Step 2: Run them and watch them fail**

Run: `uv run pytest tests/unit/application/test_notify_client.py -v`
Expected: FAIL with `AttributeError: 'NotifyClient' object has no attribute 'attempt'`.

- [x] **Step 3: Add `attempt` and the tokens to `NotifyClient`**

In `src/ecet/application/use_cases/notify_client.py`, change the imports to:

```python
import structlog

from ecet.application.errors import WebhookError, WebhookPermanentError
from ecet.application.notifications import ClientNotification, DecidedBy, Outcome
from ecet.application.ports.clock import Clock
from ecet.application.ports.webhook_client import WebhookClient
from ecet.domain.claim import Claim
from ecet.domain.errors import TenantNotFound
from ecet.domain.evaluation import Decision
from ecet.domain.ports.tenant_repository import TenantRepository
```

add below `log = structlog.get_logger(__name__)`:

```python
#: `failure_reason` tokens for a delivery that did not happen. Short tokens, matching the
#: `EXTRACTION_FAILED` convention; the exception detail lives on `Claim.last_notify_error`.
REJECTED = "webhook_rejected"
UNREACHABLE = "webhook_unreachable"
TENANT_INACTIVE = "tenant_inactive"
```

and add this method to `NotifyClient`, below `execute`:

```python
    async def attempt(
        self,
        claim: Claim,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> str | None:
        """`execute` for a caller that parks the claim instead of propagating: `None`
        when the webhook was delivered, otherwise the `failure_reason` token to park it
        under. UC-07, UC-09c and the operator retry all park a failed delivery the same
        way, so the mapping lives here once."""
        try:
            await self.execute(
                claim, outcome=outcome, confidence=confidence, decided_by=decided_by
            )
        except TenantNotFound as error:
            # The tenant is read before any delivery is attempted, and the repository
            # refuses one deactivated since the claim was ingested.
            claim.last_notify_error = f"{type(error).__name__}: {error}"
            return TENANT_INACTIVE
        except WebhookPermanentError:
            return REJECTED
        except WebhookError:
            return UNREACHABLE
        return None
```

In the module docstring, change "the caller (UC-07, and UC-09c in Phase 5) owns the unit of work" to "the caller (UC-07, UC-09c, or the operator retry) owns the unit of work".

- [x] **Step 4: Point `RouteDecision` at it**

In `src/ecet/application/use_cases/route_decision.py`, delete the three token constants and their comment. Also delete the imports `from ecet.application.errors import WebhookError, WebhookPermanentError` and `from ecet.domain.errors import TenantNotFound`. Then replace the whole `else:` branch of `execute` with:

```python
        else:
            reason = await self._notify.attempt(
                claim,
                outcome=evaluation.decision,
                confidence=evaluation.confidence,
                decided_by="auto",
            )
            if reason is None:
                self._advance(claim, ClaimStatus.APPROVED_AUTO)
            else:
                self._fail(claim, reason)
```

- [x] **Step 5: Run the notify and routing tests and watch them pass**

Run: `uv run pytest tests/unit/application/test_notify_client.py tests/unit/application/test_route_decision.py tests/unit/application/test_evaluate_claim.py -v`
Expected: PASS. The three new `attempt` tests pass, and every existing UC-07 and UC-06 test is unchanged, including `test_a_tenant_deactivated_before_delivery_parks_the_claim` and both token tests.

- [x] **Step 6: Write the failing UC-09c test**

Create `tests/unit/application/test_resolve_review.py`:

```python
"""UC-09c. A human decision is recorded even when its delivery fails — the review is
done, only the webhook is outstanding — and a task is invisible to every other tenant."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookTransientError
from ecet.application.use_cases.human_review import ResolveReview, ResolveReviewCommand
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import InvalidTransition, ReviewAlreadyResolved, ReviewTaskNotFound
from ecet.domain.evaluation import (
    Decision,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
)
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim(status: ClaimStatus) -> Claim:
    evaluation = None
    if status is not ClaimStatus.EVALUATION_FAILED:
        evaluation = Evaluation(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            confidence=0.4,
            rationale="The note leaves the required criteria undocumented.",
            model="fake-deterministic",
            prompt_version="v1",
        )
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=status,
        redacted=RedactedText(
            text="Patient <PERSON> with M54.5, duration unclear.", redactor="fake"
        ),
        evaluation=evaluation,
        created_at=NOW,
        updated_at=NOW,
    )


class Case:
    """A claim waiting on its review task, wired to UC-09c over fakes."""

    def __init__(self, claim: Claim, webhook: FakeWebhookClient) -> None:
        self.claim = claim
        self.webhook = webhook
        self.uow = FakeUnitOfWork(tenants=[build_tenant()])
        self.task = ReviewTask(
            id=uuid4(),
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            reason=ReviewReason.LOW_CONFIDENCE,
            created_at=NOW,
        )
        self.use_case = ResolveReview(
            uow_factory=lambda: self.uow, webhook=webhook, clock=FixedClock(NOW)
        )

    def command(self, **overrides: Any) -> ResolveReviewCommand:
        fields: dict[str, Any] = {
            "task_id": self.task.id,
            "tenant_id": "tenant-a",
            "reviewer": "nurse.okafor",
            "resolution": "DOES_NOT_MEET",
            "notes": "No prior imaging report attached.",
        }
        fields.update(overrides)
        return ResolveReviewCommand.model_validate(fields)

    def stored_claim(self) -> Claim:
        return self.uow.claims.claims[self.claim.id]

    def stored_task(self) -> ReviewTask:
        return self.uow.review_tasks.tasks[self.task.id]


async def build_case(
    *,
    status: ClaimStatus = ClaimStatus.REVIEW_PENDING,
    webhook: FakeWebhookClient | None = None,
) -> Case:
    case = Case(build_claim(status), webhook or FakeWebhookClient())
    await case.uow.claims.add(case.claim)
    await case.uow.review_tasks.add(case.task)
    return case


async def test_resolving_records_the_decision_and_notifies_as_human() -> None:
    case = await build_case()

    result = await case.use_case.execute(case.command())

    task = case.stored_task()
    assert task.status is ReviewStatus.RESOLVED
    assert task.resolution is Decision.DOES_NOT_MEET
    assert task.reviewer == "nurse.okafor"
    assert task.resolved_at == NOW
    ((tenant, payload),) = case.webhook.deliveries
    assert tenant.id == "tenant-a"
    assert payload.decided_by == "human"
    assert payload.outcome == "DOES_NOT_MEET"
    assert payload.confidence == 1.0
    assert payload.claim_id == case.claim.id
    assert case.stored_claim().status is ClaimStatus.REVIEW_RESOLVED
    assert result.task_id == case.task.id
    assert result.claim_id == case.claim.id
    assert result.claim_status is ClaimStatus.REVIEW_RESOLVED
    assert case.uow.commits == 1
    assert_no_pii(payload.model_dump_json())


async def test_the_reviewers_notes_never_reach_the_webhook() -> None:
    case = await build_case()

    await case.use_case.execute(case.command(notes="Spoke to Dr. Ferreira's office."))

    body = case.webhook.deliveries[0][1].model_dump_json()
    assert "Ferreira" not in body
    assert "notes" not in body


async def test_resolving_twice_is_an_error_and_notifies_once() -> None:
    case = await build_case()
    await case.use_case.execute(case.command())

    with pytest.raises(ReviewAlreadyResolved):
        await case.use_case.execute(case.command(resolution="MEETS_NECESSITY"))

    assert len(case.webhook.deliveries) == 1
    assert case.stored_task().resolution is Decision.DOES_NOT_MEET


async def test_a_cross_tenant_resolve_is_not_found() -> None:
    case = await build_case()

    with pytest.raises(ReviewTaskNotFound):
        await case.use_case.execute(case.command(tenant_id="tenant-b"))

    assert case.stored_task().status is ReviewStatus.OPEN
    assert case.webhook.deliveries == []
    assert case.uow.commits == 0


async def test_an_unknown_task_is_not_found() -> None:
    case = await build_case()

    with pytest.raises(ReviewTaskNotFound):
        await case.use_case.execute(case.command(task_id=uuid4()))


async def test_a_failed_delivery_still_records_the_resolution() -> None:
    case = await build_case(webhook=FakeWebhookClient(error=WebhookTransientError("3 attempts")))

    result = await case.use_case.execute(case.command())

    assert case.stored_task().status is ReviewStatus.RESOLVED
    claim = case.stored_claim()
    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_unreachable"
    assert claim.notification_attempts == 1
    assert claim.last_notify_error is not None
    assert result.claim_status is ClaimStatus.NOTIFY_FAILED
    assert case.uow.commits == 1


async def test_a_claim_that_failed_evaluation_can_be_resolved() -> None:
    # UC-06 opens a review task for invalid LLM output; the claim has no evaluation.
    case = await build_case(status=ClaimStatus.EVALUATION_FAILED)

    await case.use_case.execute(case.command())

    assert case.stored_claim().status is ClaimStatus.REVIEW_RESOLVED
    payload = case.webhook.deliveries[0][1]
    assert payload.rationale == ""
    assert payload.cited_codes == []


async def test_a_claim_that_failed_evaluation_can_park_in_notify_failed() -> None:
    case = await build_case(
        status=ClaimStatus.EVALUATION_FAILED,
        webhook=FakeWebhookClient(error=WebhookTransientError("3 attempts")),
    )

    await case.use_case.execute(case.command())

    assert case.stored_claim().status is ClaimStatus.NOTIFY_FAILED


async def test_a_claim_not_awaiting_review_is_refused_before_any_delivery() -> None:
    # A webhook the transition then refused would reach the client and be rolled back.
    case = await build_case(status=ClaimStatus.APPROVED_AUTO)

    with pytest.raises(InvalidTransition):
        await case.use_case.execute(case.command())

    assert case.webhook.deliveries == []
    assert case.uow.commits == 0
```

- [x] **Step 7: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_resolve_review.py -v`
Expected: FAIL with `ImportError: cannot import name 'ResolveReview' from 'ecet.application.use_cases.human_review'`.

- [x] **Step 8: Write UC-09c**

Replace the module docstring and imports of `src/ecet/application/use_cases/human_review.py` with:

```python
"""UC-09 human review: request (09a), list (09b), resolve (09c).

UC-09a is idempotent by claim: `review_tasks.claim_id` is unique in the schema, so a
second request for a claim that already has an open task returns that task rather than
racing the constraint. The caller — not this use case — transitions the claim to
`REVIEW_PENDING`, because only the caller knows whether the claim is also being saved
in the same unit of work.

UC-09c records the decision even when its webhook fails. The reviewer's work is done;
only the delivery is outstanding, so the claim parks in `NOTIFY_FAILED` for the
operator retry rather than rolling the resolution back. A reviewer's `notes` are
free text a person typed: they are stored on the task and never logged or sent.
"""

from collections.abc import Callable
from uuid import UUID, uuid4

import structlog
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import InvalidTransition, ReviewTaskNotFound
from ecet.domain.evaluation import HumanResolution, ReviewReason, ReviewTask
from ecet.domain.ids import ClaimId, TenantId
from ecet.domain.ports.review_task_repository import ReviewTaskRepository

log = structlog.get_logger(__name__)
```

Keep `RequestHumanReview` exactly as it is, then append:

```python
class ResolveReviewCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_id: UUID
    #: Plain `TenantId`, not the validated field: the value comes from a header, and a
    #: malformed one should miss (404) rather than fail validation with a different
    #: status than a well-formed wrong tenant.
    tenant_id: TenantId
    reviewer: str = Field(min_length=1, max_length=200)
    resolution: HumanResolution
    notes: str | None = Field(default=None, max_length=2000)


class ResolveReviewResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_id: UUID
    claim_id: ClaimId
    claim_status: ClaimStatus


class ResolveReview:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        webhook: WebhookClient,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._webhook = webhook
        self._clock = clock

    async def execute(self, command: ResolveReviewCommand) -> ResolveReviewResult:
        async with self._uow_factory() as uow:
            task = await uow.review_tasks.get(command.task_id)
            if task.tenant_id != command.tenant_id:
                # Same answer as "no such task": anything else confirms it exists.
                raise ReviewTaskNotFound(str(command.task_id))

            now = self._clock.now()
            # Raises `ReviewAlreadyResolved` before anything has been sent.
            task.resolve(
                resolution=command.resolution,
                reviewer=command.reviewer,
                notes=command.notes,
                now=now,
            )
            claim = await uow.claims.get(task.claim_id)
            if not claim.status.can_transition_to(ClaimStatus.REVIEW_RESOLVED):
                # Checked before the webhook: a delivery the transition then refused
                # would reach the client and be rolled back here.
                raise InvalidTransition(
                    f"claim {claim.id} is {claim.status}, which a review cannot resolve"
                )
            await uow.review_tasks.save(task)

            reason = await NotifyClient(uow.tenants, self._webhook, self._clock).attempt(
                claim, outcome=command.resolution, confidence=1.0, decided_by="human"
            )
            if reason is None:
                claim.transition(ClaimStatus.REVIEW_RESOLVED, now=now)
            else:
                claim.transition(ClaimStatus.NOTIFY_FAILED, reason=reason, now=now)
            await uow.claims.save(claim)
            await uow.commit()

        log.info(
            "review.resolved",
            task_id=str(task.id),
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            resolution=command.resolution.value,
            claim_status=claim.status.value,
        )
        return ResolveReviewResult(task_id=task.id, claim_id=claim.id, claim_status=claim.status)
```

Every import above is used after this step, either by `RequestHumanReview` (`Claim`, `ReviewReason`, `ReviewTask`, `ReviewTaskRepository`, `uuid4`) or by the new code. If `ruff check` reports an unused import, you have dropped a line of `RequestHumanReview`.

- [x] **Step 9: Run UC-09c and watch it pass**

Run: `uv run pytest tests/unit/application/test_resolve_review.py tests/unit/application/test_request_human_review.py -v`
Expected: PASS (9 new tests, plus the two existing UC-09a tests).

- [x] **Step 10: Check types and layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest
```
Expected: all green. `mypy --strict` is the real check that `command.resolution` (a `Literal` of two `Decision` members) is accepted where `NotifyClient` wants a `Decision`.

- [x] **Step 11: Commit**

```bash
git add src/ecet/application/use_cases/notify_client.py \
  src/ecet/application/use_cases/route_decision.py \
  src/ecet/application/use_cases/human_review.py \
  tests/unit/application/test_notify_client.py tests/unit/application/test_resolve_review.py
git commit -m "feat(review): UC-09c ResolveReview and NotifyClient.attempt"
```

---

### Task 3: UC-09b ListOpenReviews

**Files:**
- Modify: `src/ecet/application/use_cases/human_review.py`
- Test: `tests/unit/application/test_list_open_reviews.py` (create)

**Interfaces:**
- Consumes: `ReviewTaskRepository.list_open(tenant_id, limit)`, `ClaimRepository.get` (Phase 2); `DeterministicResult`, `Evaluation`, `ReviewTask` (Phase 1).
- Produces: in `ecet.application.use_cases.human_review`:
  - `ReviewTaskView`, with fields `task_id: UUID`, `claim_id: ClaimId`, `tenant_id: TenantIdField`, `reason: ReviewReason`, `created_at: datetime`, `claim_status: ClaimStatus`, `redacted_text: str | None`, `deterministic: DeterministicResult | None` and `evaluation: Evaluation | None`, plus `classmethod of(task, claim)`.
  - `ListOpenReviews`, with `__init__(*, uow_factory: Callable[[], UnitOfWork])` and `async execute(tenant_id: TenantId, limit: int = 50) -> list[ReviewTaskView]`.

This closes Phase 3 carry-over #2 ("`ClaimView` omits the redacted text that UC-09b needs"). The review view carries the text, and `ClaimView` does not grow it (deviation 3).

- [x] **Step 1: Write the failing UC-09b test**

Create `tests/unit/application/test_list_open_reviews.py`:

```python
"""UC-09b. The review queue is the one place the API returns a claim's note, so the
tests that matter most here prove it is the redacted note and that it is tenant-scoped."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from tests.fakes import FakePiiRedactor, FakeUnitOfWork
from tests.pii import assert_no_pii, read_note

from ecet.application.use_cases.human_review import ListOpenReviews
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewTask,
    Verdict,
)
from ecet.domain.ids import ClaimId, TenantId

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
TENANT = TenantId("tenant-a")


async def seed_review(
    uow: FakeUnitOfWork, *, tenant_id: str = "tenant-a", name: str = "unclear", minutes: int = 0
) -> ReviewTask:
    created = NOW + timedelta(minutes=minutes)
    claim = Claim(
        id=ClaimId(uuid4()),
        tenant_id=tenant_id,
        source=SourceObject(
            bucket="claims",
            key=f"tenants/{tenant_id}/claims/{name}.pdf",
            etag=f"etag-{name}",
            size=2048,
        ),
        status=ClaimStatus.REVIEW_PENDING,
        redacted=await FakePiiRedactor().redact(read_note("unclear")),
        deterministic=DeterministicResult(
            verdict=Verdict.UNCERTAIN,
            checks=[
                CheckOutcome(name="covered_code_hit", passed=False, detail="no covered code")
            ],
        ),
        evaluation=Evaluation(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            confidence=0.4,
            rationale="The note leaves the required criteria undocumented.",
            model="fake-deterministic",
            prompt_version="v1",
        ),
        created_at=created,
        updated_at=created,
    )
    await uow.claims.add(claim)
    task = ReviewTask(
        id=uuid4(),
        claim_id=claim.id,
        tenant_id=claim.tenant_id,
        reason=ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW,
        created_at=created,
    )
    await uow.review_tasks.add(task)
    return task


def build_use_case(uow: FakeUnitOfWork) -> ListOpenReviews:
    return ListOpenReviews(uow_factory=lambda: uow)


async def test_a_view_carries_what_a_reviewer_reads_to_decide() -> None:
    uow = FakeUnitOfWork()
    task = await seed_review(uow)

    (view,) = await build_use_case(uow).execute(TENANT)

    claim = uow.claims.claims[task.claim_id]
    assert claim.redacted is not None
    assert view.task_id == task.id
    assert view.claim_id == task.claim_id
    assert view.tenant_id == "tenant-a"
    assert view.reason is ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW
    assert view.created_at == task.created_at
    assert view.claim_status is ClaimStatus.REVIEW_PENDING
    assert view.redacted_text == claim.redacted.text
    assert view.deterministic == claim.deterministic
    assert view.evaluation == claim.evaluation


async def test_the_note_in_the_view_is_redacted() -> None:
    uow = FakeUnitOfWork()
    await seed_review(uow)

    (view,) = await build_use_case(uow).execute(TENANT)

    assert view.redacted_text is not None
    assert "<PERSON>" in view.redacted_text
    assert_no_pii(view.model_dump_json())


async def test_another_tenants_reviews_are_never_listed() -> None:
    uow = FakeUnitOfWork()
    mine = await seed_review(uow, tenant_id="tenant-a", name="mine")
    await seed_review(uow, tenant_id="tenant-b", name="theirs")

    views = await build_use_case(uow).execute(TENANT)

    assert [view.task_id for view in views] == [mine.id]


async def test_a_resolved_review_is_not_listed() -> None:
    uow = FakeUnitOfWork()
    task = await seed_review(uow)
    task.resolve(resolution=Decision.MEETS_NECESSITY, reviewer="nurse", notes=None, now=NOW)

    assert await build_use_case(uow).execute(TENANT) == []


async def test_the_limit_is_honoured() -> None:
    uow = FakeUnitOfWork()
    for index in range(3):
        await seed_review(uow, name=f"note-{index}", minutes=index)

    views = await build_use_case(uow).execute(TENANT, limit=2)

    assert len(views) == 2
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_list_open_reviews.py -v`
Expected: FAIL with `ImportError: cannot import name 'ListOpenReviews'`.

- [x] **Step 3: Write UC-09b**

In `src/ecet/application/use_cases/human_review.py`, extend the imports:

```python
from datetime import datetime

from ecet.domain.evaluation import (
    DeterministicResult,
    Evaluation,
    HumanResolution,
    ReviewReason,
    ReviewTask,
)
from ecet.domain.ids import ClaimId, TenantId, TenantIdField
```

(merge these into the existing `ecet.domain.evaluation` and `ecet.domain.ids` import lines; do not add second ones). Then add, between `RequestHumanReview` and `ResolveReviewCommand`:

```python
class ReviewTaskView(BaseModel):
    """One open review, with what a reviewer reads to decide it (UC-09b).

    The redacted note is here and on no other API response: it is clinical content even
    after redaction, and the review queue is the one caller that needs it."""

    model_config = ConfigDict(frozen=True)

    task_id: UUID
    claim_id: ClaimId
    tenant_id: TenantIdField
    reason: ReviewReason
    created_at: datetime
    claim_status: ClaimStatus
    redacted_text: str | None
    deterministic: DeterministicResult | None
    evaluation: Evaluation | None

    @classmethod
    def of(cls, task: ReviewTask, claim: Claim) -> "ReviewTaskView":
        return cls(
            task_id=task.id,
            claim_id=task.claim_id,
            tenant_id=task.tenant_id,
            reason=task.reason,
            created_at=task.created_at,
            claim_status=claim.status,
            redacted_text=claim.redacted.text if claim.redacted else None,
            deterministic=claim.deterministic,
            evaluation=claim.evaluation,
        )


class ListOpenReviews:
    def __init__(self, *, uow_factory: Callable[[], UnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def execute(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTaskView]:
        """Tenant-scoped by the repository query itself; there is no cross-tenant list."""
        async with self._uow_factory() as uow:
            tasks = await uow.review_tasks.list_open(tenant_id, limit)
            # ponytail: one claim read per task, bounded by `limit` (the route caps it at
            # 200); a join in the repository if the queue ever gets long enough to notice.
            return [ReviewTaskView.of(task, await uow.claims.get(task.claim_id)) for task in tasks]
```

Update the module docstring's first paragraph to mention UC-09b: "UC-09b lists a tenant's open tasks with the redacted note, the deterministic checks and the LLM evaluation beside each."

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/application/test_list_open_reviews.py -v`
Expected: PASS (5 tests).

- [x] **Step 5: Check types and layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/human_review.py \
  tests/unit/application/test_list_open_reviews.py
git commit -m "feat(review): UC-09b ListOpenReviews with the redacted note beside each task"
```

---

### Task 4: RetryNotify, the operator exit from `NOTIFY_FAILED`

**Files:**
- Create: `src/ecet/application/use_cases/retry_notify.py`
- Test: `tests/unit/application/test_retry_notify.py` (create)

**Interfaces:**
- Consumes: `NotifyClient.attempt` (Task 2); `ReviewTaskRepository.find_by_claim` (Task 1); `NOTIFY_FAILED → APPROVED_AUTO | REVIEW_RESOLVED` (Phase 1 state machine).
- Produces: `ecet.application.use_cases.retry_notify.RetryNotify`, with `__init__(*, uow_factory: Callable[[], UnitOfWork], webhook: WebhookClient, clock: Clock)` and `async execute(claim_id: ClaimId) -> Claim`. It raises `ClaimNotFound`, or `InvalidTransition` when the claim is not `NOTIFY_FAILED` or has no decision to deliver. A delivery that fails again is **not** raised; the claim comes back still `NOTIFY_FAILED`, with the attempt committed.

- [x] **Step 1: Write the failing test**

Create `tests/unit/application/test_retry_notify.py`:

```python
"""The operator retry out of NOTIFY_FAILED. It re-sends a decision that already exists,
as whoever made it — it never re-evaluates and never re-opens a review."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookPermanentError
from ecet.application.use_cases.retry_notify import RetryNotify
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import ClaimNotFound, InvalidTransition
from ecet.domain.evaluation import Decision, Evaluation, ReviewReason, ReviewTask
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=30)
KEY = "tenants/tenant-a/claims/note-1.pdf"


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        "status": ClaimStatus.NOTIFY_FAILED,
        "redacted": RedactedText(text="Patient <PERSON> with M54.5.", redactor="fake"),
        "evaluation": Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            rationale="Conservative therapy documented.",
            model="fake-deterministic",
            prompt_version="v1",
        ),
        "failure_reason": "webhook_unreachable",
        "notification_attempts": 1,
        "last_notify_error": "WebhookTransientError: 3 attempts failed, last: ConnectError",
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def build_case(
    claim: Claim, webhook: FakeWebhookClient | None = None
) -> tuple[RetryNotify, FakeUnitOfWork, FakeWebhookClient]:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    await uow.claims.add(claim)
    webhook = webhook or FakeWebhookClient()
    retry = RetryNotify(uow_factory=lambda: uow, webhook=webhook, clock=FixedClock(LATER))
    return retry, uow, webhook


async def test_an_automatic_decision_is_re_sent_as_auto_and_approves() -> None:
    claim = build_claim()
    retry, uow, webhook = await build_case(claim)

    result = await retry.execute(claim.id)

    ((_, payload),) = webhook.deliveries
    assert payload.decided_by == "auto"
    assert payload.outcome == "MEETS_NECESSITY"
    assert payload.confidence == 0.91
    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.failure_reason is None
    assert stored.last_notify_error is None
    assert stored.notification_attempts == 2
    assert result.status is ClaimStatus.APPROVED_AUTO
    assert uow.commits == 1
    assert_no_pii(payload.model_dump_json())


async def test_a_human_decision_is_re_sent_as_human_and_resolves() -> None:
    claim = build_claim()
    retry, uow, webhook = await build_case(claim)
    task = ReviewTask(
        id=uuid4(),
        claim_id=claim.id,
        tenant_id=claim.tenant_id,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )
    task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="nurse.okafor", notes=None, now=NOW)
    await uow.review_tasks.add(task)

    await retry.execute(claim.id)

    ((_, payload),) = webhook.deliveries
    assert payload.decided_by == "human"
    assert payload.outcome == "DOES_NOT_MEET"
    assert payload.confidence == 1.0
    assert uow.claims.claims[claim.id].status is ClaimStatus.REVIEW_RESOLVED


async def test_a_retry_that_fails_again_is_still_recorded() -> None:
    claim = build_claim()
    retry, uow, _ = await build_case(
        claim, FakeWebhookClient(error=WebhookPermanentError("status 410"))
    )

    result = await retry.execute(claim.id)

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.NOTIFY_FAILED
    assert stored.failure_reason == "webhook_rejected"
    assert stored.notification_attempts == 2
    assert stored.last_notify_error == "WebhookPermanentError: status 410"
    assert stored.updated_at == LATER
    assert result.status is ClaimStatus.NOTIFY_FAILED
    assert uow.commits == 1


@pytest.mark.parametrize(
    "status", [ClaimStatus.QUEUED, ClaimStatus.APPROVED_AUTO, ClaimStatus.REVIEW_PENDING]
)
async def test_only_a_notify_failed_claim_can_be_retried(status: ClaimStatus) -> None:
    claim = build_claim(status=status, failure_reason=None)
    retry, uow, webhook = await build_case(claim)

    with pytest.raises(InvalidTransition):
        await retry.execute(claim.id)

    assert webhook.deliveries == []
    assert uow.commits == 0


async def test_an_unknown_claim_is_not_found() -> None:
    retry, _, _ = await build_case(build_claim())

    with pytest.raises(ClaimNotFound):
        await retry.execute(ClaimId(uuid4()))


async def test_a_claim_with_no_decision_is_refused_rather_than_invented() -> None:
    claim = build_claim(evaluation=None)
    retry, _, webhook = await build_case(claim)

    with pytest.raises(InvalidTransition, match="no decision"):
        await retry.execute(claim.id)

    assert webhook.deliveries == []
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_retry_notify.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ecet.application.use_cases.retry_notify'`.

- [x] **Step 3: Write the use case**

Create `src/ecet/application/use_cases/retry_notify.py`:

```python
"""`POST /v1/claims/{id}/retry-notify` — the only way out of `NOTIFY_FAILED`.

The decision already exists; only its delivery failed. So nothing is re-evaluated and
nobody re-reviews: the retry re-sends what was decided, as whoever decided it. A claim
with a review task was decided by a human (UC-09c) — `review_tasks.claim_id` is
unique, so there is never more than one — and a claim without one was decided
automatically (UC-07).

A retry that fails again is not an error from this use case's point of view. It
leaves the claim `NOTIFY_FAILED` and still commits, because the attempt count and the
latest error are exactly what the operator reads next.
"""

from collections.abc import Callable

import structlog

from ecet.application.notifications import DecidedBy
from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import InvalidTransition
from ecet.domain.evaluation import Decision
from ecet.domain.ids import ClaimId

log = structlog.get_logger(__name__)


class RetryNotify:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        webhook: WebhookClient,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._webhook = webhook
        self._clock = clock

    async def execute(self, claim_id: ClaimId) -> Claim:
        async with self._uow_factory() as uow:
            claim = await uow.claims.get(claim_id)
            if claim.status is not ClaimStatus.NOTIFY_FAILED:
                raise InvalidTransition(f"claim {claim.id} is {claim.status}, not NOTIFY_FAILED")

            task = await uow.review_tasks.find_by_claim(claim.id)
            evaluation = claim.evaluation
            outcome: Decision
            confidence: float
            decided_by: DecidedBy
            target: ClaimStatus
            if task is not None and task.resolution is not None:
                outcome = task.resolution
                confidence = 1.0
                decided_by = "human"
                target = ClaimStatus.REVIEW_RESOLVED
            elif task is None and evaluation is not None:
                outcome = evaluation.decision
                confidence = evaluation.confidence
                decided_by = "auto"
                target = ClaimStatus.APPROVED_AUTO
            else:
                # Not reachable through the use cases — NOTIFY_FAILED is only ever
                # entered once a decision exists. Refusing beats inventing an outcome.
                raise InvalidTransition(f"claim {claim.id} has no decision to deliver")

            reason = await NotifyClient(uow.tenants, self._webhook, self._clock).attempt(
                claim, outcome=outcome, confidence=confidence, decided_by=decided_by
            )
            now = self._clock.now()
            if reason is None:
                claim.transition(target, now=now)
            else:
                # There is no NOTIFY_FAILED -> NOTIFY_FAILED edge: the claim has not
                # moved, only the reason it is stuck may have.
                claim.failure_reason = reason
                claim.updated_at = now
            await uow.claims.save(claim)
            await uow.commit()

        log.info(
            "notify.retried",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            decided_by=decided_by,
            status=claim.status.value,
            attempts=claim.notification_attempts,
        )
        return claim
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/application/test_retry_notify.py -v`
Expected: PASS (8 tests including the three parametrised statuses).

- [x] **Step 5: Check types and layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/retry_notify.py tests/unit/application/test_retry_notify.py
git commit -m "feat(notify): RetryNotify, the operator exit from NOTIFY_FAILED"
```

---

### Task 5: Re-publish a claim left `POLICIES_ATTACHED` when its object arrives again

**Files:**
- Modify: `src/ecet/application/use_cases/ingest_claim_document.py`
- Test: `tests/unit/application/test_ingest_claim_document.py`

**Interfaces:**
- Consumes: `PolicyRepository.get_many(ids)` (Phase 2); `EnqueueEvaluation.execute(claim, policies)` (Phase 3).
- Produces: no new name. `IngestClaimDocument.execute` on a duplicate whose status is `POLICIES_ATTACHED` now re-publishes it and returns `IngestResult(duplicate=True, status=QUEUED)`. A duplicate in any other status is still a no-op.

This closes Phase 3 carry-over #1. UC-01 commits `POLICIES_ATTACHED` before publishing, so a failed publish leaves a durable claim, and the api answers 503. The compose MinIO webhook target has a persistent `queue_dir` (Phase 4), so MinIO spools that event and re-sends it. Today the re-sent event hits the duplicate check and returns the stuck claim untouched. After this task the same event is the retry, and so is an operator re-posting `POST /v1/claims/ingest` for the same object.

- [x] **Step 1: Write the failing tests**

Append to `tests/unit/application/test_ingest_claim_document.py`:

```python
async def test_the_same_object_arriving_again_re_publishes_a_claim_stuck_before_publish() -> None:
    # Phase 3 carry-over: a publish failure leaves a durable POLICIES_ATTACHED claim.
    # MinIO re-sending the event it got a 503 for — or an operator re-posting
    # /v1/claims/ingest — is the retry.
    harness = Harness(queue=FakeEvaluationQueue(error=QueuePublishError("no confirm")))
    with pytest.raises(QueuePublishError):
        await harness.ingest()
    harness.queue.error = None

    result = await harness.ingest()

    assert result.duplicate is True
    assert result.status is ClaimStatus.QUEUED
    stored = harness.uow.claims.claims[result.claim_id]
    assert stored.status is ClaimStatus.QUEUED
    (message,) = harness.queue.published
    assert message.claim_id == result.claim_id
    assert [policy.id for policy in message.policies] == stored.policy_ids
    assert harness.extractor.calls == 1  # nothing before the publish is redone
    assert_no_pii(message.model_dump_json())


async def test_a_re_publish_that_fails_again_leaves_the_claim_policies_attached() -> None:
    harness = Harness(queue=FakeEvaluationQueue(error=QueuePublishError("no confirm")))
    with pytest.raises(QueuePublishError):
        await harness.ingest()

    with pytest.raises(QueuePublishError):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.POLICIES_ATTACHED
    assert harness.queue.published == []
```

`test_a_duplicate_source_object_is_a_no_op` already covers a `QUEUED` duplicate, which must still publish exactly once.

- [x] **Step 2: Run them and watch them fail**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py -v`
Expected: FAIL. The first test gets `status is POLICIES_ATTACHED` and an empty `published`. The second raises nothing on the second ingest (`DID NOT RAISE`).

- [x] **Step 3: Re-publish from the duplicate branch**

In `src/ecet/application/use_cases/ingest_claim_document.py`, replace the duplicate branch in `execute` with:

```python
            duplicate = await uow.claims.find_by_source(source.bucket, source.key, source.etag)
            if duplicate is not None:
                if duplicate.status is ClaimStatus.POLICIES_ATTACHED:
                    await self._republish(uow, duplicate)
                # ADR-006: same object, same content — nothing else to redo.
                log.info(
                    "claim.duplicate",
                    claim_id=str(duplicate.id),
                    tenant_id=str(tenant_id),
                    status=duplicate.status.value,
                )
                return IngestResult(claim_id=duplicate.id, status=duplicate.status, duplicate=True)
```

and add this method below `_run_pipeline`:

```python
    async def _republish(self, uow: UnitOfWork, claim: Claim) -> None:
        """The retry for a failed publish (step 10). The claim already holds the redacted
        text, the deterministic result and its policy ids, so only the publish repeats —
        against the policy versions attached the first time, not whatever is active
        today. Raises `QueuePublishError` again if the broker is still refusing, leaving
        the claim exactly as it was."""
        policies = await uow.policies.get_many(claim.policy_ids)
        await self._enqueue.execute(claim, policies)
        self._advance(claim, ClaimStatus.QUEUED)
        await uow.claims.save(claim)
        await uow.commit()
```

Extend the third paragraph of the module docstring:

```
The publish (step 10) happens *after* the `POLICIES_ATTACHED` commit. A publish that
fails therefore leaves a durable `POLICIES_ATTACHED` claim that a retry can re-send;
the transactional outbox that would make this atomic is deferred out of v1. The retry
is the same object arriving again — MinIO re-sending the event it got a 503 for, or an
operator re-posting `/v1/claims/ingest`: the duplicate check re-publishes a claim it
finds still `POLICIES_ATTACHED` instead of returning it untouched.
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py tests/api -v`
Expected: PASS. Both new tests pass, and every existing UC-01 and API test still passes, including `test_the_happy_path_queues_the_claim`'s `commits == 3`.

- [x] **Step 5: Check types and layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/ingest_claim_document.py \
  tests/unit/application/test_ingest_claim_document.py
git commit -m "fix(ingest): re-publish a claim left POLICIES_ATTACHED when its object arrives again"
```

---

### Task 6: The `/v1/reviews` routes, `retry-notify`, and the api container

**Files:**
- Create: `src/ecet/interfaces/api/routes/reviews.py`
- Modify: `src/ecet/interfaces/api/routes/claims.py`
- Modify: `src/ecet/interfaces/api/app.py`
- Modify: `src/ecet/interfaces/api/container.py`
- Modify: `tests/api/conftest.py`
- Test: `tests/api/test_reviews_routes.py` (create), `tests/api/test_claims_routes.py`

**Interfaces:**
- Consumes: `ListOpenReviews`, `ResolveReview`, `ResolveReviewCommand`, `ResolveReviewResult`, `ReviewTaskView` (Tasks 2–3); `RetryNotify` (Task 4); `HttpxWebhookClient(timeout_s, max_attempts)` (Phase 4); `ContainerDep`, `TenantDep`, `require_api_key` (Phase 3); `ClaimView` (Phase 3).
- Produces:
  - `ApiContainer` gains the fields `list_reviews: ListOpenReviews`, `resolve_review: ResolveReview` and `retry_notify: RetryNotify`, placed after `ingest` and before `probes`.
  - `GET /v1/reviews?limit=` (1..200, default 50) returns `list[ReviewTaskView]`.
  - `POST /v1/reviews/{task_id}/resolve` with body `{reviewer, resolution, notes?}` returns `ResolveReviewResult` with status 200.
  - `POST /v1/claims/{claim_id}/retry-notify` returns a `ClaimView` body: status 200 when the webhook was delivered, 502 when it failed again.
  - `tests.api.conftest.ApiHarness.webhook: FakeWebhookClient`.

- [x] **Step 1: Wire the harness to the new container fields**

In `tests/api/conftest.py`, add `FakeWebhookClient` to the `tests.fakes` import and add these imports:

```python
from ecet.application.use_cases.human_review import ListOpenReviews, ResolveReview
from ecet.application.use_cases.retry_notify import RetryNotify
```

In `ApiHarness.__init__`, add `self.webhook = FakeWebhookClient()` directly below `self.queue = FakeEvaluationQueue()`. Then replace the `ApiContainer(...)` call with:

```python
        self.container = ApiContainer(
            settings=self.settings,
            uow_factory=uow_factory,
            storage=self.storage,
            ingest=self.ingest,
            list_reviews=ListOpenReviews(uow_factory=uow_factory),
            resolve_review=ResolveReview(
                uow_factory=uow_factory, webhook=self.webhook, clock=self.clock
            ),
            retry_notify=RetryNotify(
                uow_factory=uow_factory, webhook=self.webhook, clock=self.clock
            ),
            probes={name: self._probe(name) for name in self.probe_results},
            aclose=self._aclose,
        )
```

- [x] **Step 2: Write the failing reviews-route test**

Create `tests/api/test_reviews_routes.py`:

```python
"""`/v1/reviews`. UC-09b and UC-09c are covered with fakes in `tests/unit/application`;
this file pins the HTTP contract — the status codes, the tenant header, and that the one
route returning note text returns only redacted text."""

from uuid import uuid4

from tests.api.conftest import API_KEY, BUCKET, NOW, ApiHarness
from tests.fakes import FakePiiRedactor
from tests.pii import assert_no_pii, read_note

from ecet.application.errors import WebhookTransientError
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import ReviewReason, ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId

RESOLUTION = {
    "reviewer": "nurse.okafor",
    "resolution": "MEETS_NECESSITY",
    "notes": "Therapy dates confirmed with the provider's office.",
}


async def seed_review(harness: ApiHarness, *, tenant_id: str = "tenant-a") -> ReviewTask:
    claim = Claim(
        id=ClaimId(uuid4()),
        tenant_id=tenant_id,
        source=SourceObject(
            bucket=BUCKET,
            key=f"tenants/{tenant_id}/claims/unclear.pdf",
            etag=f"etag-{tenant_id}",
            size=2048,
        ),
        status=ClaimStatus.REVIEW_PENDING,
        redacted=await FakePiiRedactor().redact(read_note("unclear")),
        created_at=NOW,
        updated_at=NOW,
    )
    await harness.uow.claims.add(claim)
    task = ReviewTask(
        id=uuid4(),
        claim_id=claim.id,
        tenant_id=claim.tenant_id,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )
    await harness.uow.review_tasks.add(task)
    return task


async def test_the_queue_lists_open_reviews_with_the_redacted_note(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.get("/v1/reviews", headers=api_headers)

    assert response.status_code == 200
    (body,) = response.json()
    assert body["task_id"] == str(task.id)
    assert body["claim_id"] == str(task.claim_id)
    assert body["claim_status"] == "REVIEW_PENDING"
    assert "<PERSON>" in body["redacted_text"]
    assert_no_pii(response.text)


async def test_the_queue_never_lists_another_tenants_reviews(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    await seed_review(harness, tenant_id="tenant-b")

    async with harness.client() as client:
        response = await client.get("/v1/reviews", headers=api_headers)

    assert response.status_code == 200
    assert response.json() == []


async def test_the_limit_is_bounded(harness: ApiHarness, api_headers: dict[str, str]) -> None:
    async with harness.client() as client:
        too_small = await client.get("/v1/reviews?limit=0", headers=api_headers)
        too_large = await client.get("/v1/reviews?limit=201", headers=api_headers)

    assert too_small.status_code == 422
    assert too_large.status_code == 422


async def test_the_queue_needs_the_api_key_and_the_tenant_header(harness: ApiHarness) -> None:
    async with harness.client() as client:
        no_key = await client.get("/v1/reviews", headers={"X-Tenant-Id": "tenant-a"})
        no_tenant = await client.get("/v1/reviews", headers={"X-API-Key": API_KEY})

    assert no_key.status_code == 401
    assert no_tenant.status_code == 422


async def test_resolving_a_review_delivers_the_decision_as_human(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert response.status_code == 200
    assert response.json() == {
        "task_id": str(task.id),
        "claim_id": str(task.claim_id),
        "claim_status": "REVIEW_RESOLVED",
    }
    ((_, payload),) = harness.webhook.deliveries
    assert payload.decided_by == "human"
    assert payload.outcome == "MEETS_NECESSITY"
    assert harness.uow.review_tasks.tasks[task.id].status is ReviewStatus.RESOLVED
    assert_no_pii(payload.model_dump_json())


async def test_resolving_under_another_tenants_header_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve",
            json=RESOLUTION,
            headers={**api_headers, "X-Tenant-Id": "tenant-b"},
        )

    # 404, not 403: a 403 would confirm the task exists.
    assert response.status_code == 404
    assert harness.webhook.deliveries == []


async def test_resolving_an_unknown_task_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{uuid4()}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert response.status_code == 404


async def test_resolving_twice_is_409(harness: ApiHarness, api_headers: dict[str, str]) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        first = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )
        second = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert first.status_code == 200
    assert second.status_code == 409
    assert len(harness.webhook.deliveries) == 1


async def test_insufficient_evidence_is_not_a_human_resolution(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    task = await seed_review(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve",
            json={**RESOLUTION, "resolution": "INSUFFICIENT_EVIDENCE"},
            headers=api_headers,
        )

    assert response.status_code == 422
    assert harness.webhook.deliveries == []


async def test_a_failed_delivery_still_answers_200_with_the_parked_status(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    # The reviewer's decision is recorded; the delivery is an operator's problem now.
    task = await seed_review(harness)
    harness.webhook.error = WebhookTransientError("3 attempts failed")

    async with harness.client() as client:
        response = await client.post(
            f"/v1/reviews/{task.id}/resolve", json=RESOLUTION, headers=api_headers
        )

    assert response.status_code == 200
    assert response.json()["claim_status"] == "NOTIFY_FAILED"
```

- [x] **Step 3: Write the failing retry-notify route tests**

In `tests/api/test_claims_routes.py`, change the first two import lines to:

```python
from uuid import uuid4

from tests.api.conftest import API_KEY, BUCKET, KEY, NOW, ApiHarness
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookTransientError
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import ClaimId
```

(replacing the existing `from ecet.domain.claim import ClaimStatus`), and append:

```python
async def seed_notify_failed(harness: ApiHarness) -> ClaimId:
    claim = Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(
            bucket=BUCKET, key="tenants/tenant-a/claims/stuck.pdf", etag="etag-stuck", size=2048
        ),
        status=ClaimStatus.NOTIFY_FAILED,
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            model="fake-deterministic",
            prompt_version="v1",
        ),
        failure_reason="webhook_unreachable",
        notification_attempts=1,
        created_at=NOW,
        updated_at=NOW,
    )
    await harness.uow.claims.add(claim)
    return claim.id


async def test_retry_notify_delivers_and_approves(harness: ApiHarness) -> None:
    claim_id = await seed_notify_failed(harness)

    async with harness.client() as client:
        response = await client.post(
            f"/v1/claims/{claim_id}/retry-notify", headers={"X-API-Key": API_KEY}
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "APPROVED_AUTO"
    assert body["failure_reason"] is None
    ((_, payload),) = harness.webhook.deliveries
    assert payload.decided_by == "auto"


async def test_a_retry_that_fails_again_answers_502_with_the_claim(harness: ApiHarness) -> None:
    claim_id = await seed_notify_failed(harness)
    harness.webhook.error = WebhookTransientError("3 attempts failed")

    async with harness.client() as client:
        response = await client.post(
            f"/v1/claims/{claim_id}/retry-notify", headers={"X-API-Key": API_KEY}
        )

    # 502 so a `curl -f` in a script notices; the body is still the claim.
    assert response.status_code == 502
    body = response.json()
    assert body["status"] == "NOTIFY_FAILED"
    assert body["failure_reason"] == "webhook_unreachable"
    assert harness.uow.claims.claims[claim_id].notification_attempts == 2


async def test_retry_notify_on_a_claim_that_did_not_fail_is_409(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        response = await client.post(
            f"/v1/claims/{created.json()['claim_id']}/retry-notify",
            headers={"X-API-Key": API_KEY},
        )

    assert response.status_code == 409
    assert harness.webhook.deliveries == []


async def test_retry_notify_on_an_unknown_claim_is_404(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            f"/v1/claims/{uuid4()}/retry-notify", headers={"X-API-Key": API_KEY}
        )

    assert response.status_code == 404


async def test_retry_notify_needs_the_api_key(harness: ApiHarness) -> None:
    claim_id = await seed_notify_failed(harness)

    async with harness.client() as client:
        response = await client.post(f"/v1/claims/{claim_id}/retry-notify")

    assert response.status_code == 401
    assert harness.webhook.deliveries == []
```

- [x] **Step 4: Run the API suite and watch it fail**

Run: `uv run pytest tests/api -v`
Expected: FAIL. At first every test errors with `TypeError: ApiContainer.__init__() got an unexpected keyword argument 'list_reviews'`. After the container change in Step 5 and before the routes exist, the new tests fail with 404 or 405.

- [x] **Step 5: Add the container fields and build them**

In `src/ecet/interfaces/api/container.py`, add these imports:

```python
from ecet.application.use_cases.human_review import ListOpenReviews, ResolveReview
from ecet.application.use_cases.retry_notify import RetryNotify
from ecet.infrastructure.webhook.httpx_client import HttpxWebhookClient
```

add the three fields to `ApiContainer` directly below `ingest: IngestClaimDocument`:

```python
    list_reviews: ListOpenReviews
    resolve_review: ResolveReview
    retry_notify: RetryNotify
```

In `build_container`, below `clock = SystemClock()`, build the webhook client:

```python
    # UC-09c and the operator retry deliver from the api process, not the worker.
    webhook = HttpxWebhookClient(
        timeout_s=settings.webhook_timeout_s, max_attempts=settings.webhook_max_attempts
    )
```

replace `aclose` with:

```python
    async def aclose() -> None:
        try:
            await queue.stop()
        finally:
            try:
                await webhook.aclose()
            finally:
                await engine.dispose()
```

and pass the new fields in the `ApiContainer(...)` return, after `ingest=ingest,`:

```python
        list_reviews=ListOpenReviews(uow_factory=uow_factory),
        resolve_review=ResolveReview(uow_factory=uow_factory, webhook=webhook, clock=clock),
        retry_notify=RetryNotify(uow_factory=uow_factory, webhook=webhook, clock=clock),
```

- [x] **Step 6: Write the reviews router**

Create `src/ecet/interfaces/api/routes/reviews.py`:

```python
"""`/v1/reviews` — the human-review queue (UC-09b) and its decisions (UC-09c).

Both routes are tenant-scoped by `X-Tenant-Id`, and a task belonging to another tenant
answers 404 exactly like one that does not exist.

`GET /v1/reviews` is the one API response that carries claim text, and it is the
redacted text: a reviewer cannot decide without reading the note.

`POST /v1/reviews/{id}/resolve` answers 200 once the decision is recorded, even when
its webhook then fails. The reviewer's work is done, and `claim_status: NOTIFY_FAILED`
in the body is the cue for `POST /v1/claims/{id}/retry-notify`. The delivery — up to
`ECET_WEBHOOK_MAX_ATTEMPTS` attempts with backoff — runs inside the request.
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.use_cases.human_review import (
    ResolveReviewCommand,
    ResolveReviewResult,
    ReviewTaskView,
)
from ecet.domain.evaluation import HumanResolution
from ecet.interfaces.api.dependencies import ContainerDep, TenantDep, require_api_key

router = APIRouter(tags=["reviews"], dependencies=[Depends(require_api_key)])


class ResolveReviewRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    reviewer: str = Field(min_length=1, max_length=200)
    resolution: HumanResolution
    notes: str | None = Field(default=None, max_length=2000)


@router.get("/v1/reviews")
async def list_reviews(
    tenant_id: TenantDep,
    container: ContainerDep,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[ReviewTaskView]:
    return await container.list_reviews.execute(tenant_id, limit)


@router.post("/v1/reviews/{task_id}/resolve")
async def resolve_review(
    task_id: UUID,
    body: ResolveReviewRequest,
    tenant_id: TenantDep,
    container: ContainerDep,
) -> ResolveReviewResult:
    return await container.resolve_review.execute(
        ResolveReviewCommand(
            task_id=task_id,
            tenant_id=tenant_id,
            reviewer=body.reviewer,
            resolution=body.resolution,
            notes=body.notes,
        )
    )
```

- [x] **Step 7: Add the retry-notify route**

In `src/ecet/interfaces/api/routes/claims.py`, add `from fastapi.responses import JSONResponse` to the imports, and append:

```python
@router.post("/v1/claims/{claim_id}/retry-notify")
async def retry_notify(claim_id: UUID, container: ContainerDep) -> JSONResponse:
    """Re-send a `NOTIFY_FAILED` claim's decision. 200 with the claim when it landed;
    502 with the same body when it failed again, so a scripted `curl -f` notices. A
    claim in any other state is 409 (`InvalidTransition`)."""
    claim = await container.retry_notify.execute(ClaimId(claim_id))
    status_code = 502 if claim.status is ClaimStatus.NOTIFY_FAILED else 200
    return JSONResponse(ClaimView.of(claim).model_dump(mode="json"), status_code=status_code)
```

Extend the module docstring with:

```
`POST /v1/claims/{id}/retry-notify` takes the api key only, as the api spec lists it:
it is an operator action, like manual ingest, and v1's single global key already
reaches every tenant. It re-sends a decision that exists; it never decides anything.
```

- [x] **Step 8: Register the router**

In `src/ecet/interfaces/api/app.py`, change the routes import to `from ecet.interfaces.api.routes import claims, events, health, reviews` and add `app.include_router(reviews.router)` below `app.include_router(claims.router)`.

- [x] **Step 9: Run the API suite and watch it pass**

Run: `uv run pytest tests/api -v`
Expected: PASS. The 10 new reviews-route tests and the 5 new retry-notify tests pass, and every existing API test is unchanged.

- [x] **Step 10: Check the whole default suite and the types**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 11: Commit**

```bash
git add src/ecet/interfaces/api/routes/reviews.py src/ecet/interfaces/api/routes/claims.py \
  src/ecet/interfaces/api/app.py src/ecet/interfaces/api/container.py tests/api/conftest.py \
  tests/api/test_reviews_routes.py tests/api/test_claims_routes.py
git commit -m "feat(api): /v1/reviews, resolve and retry-notify routes"
```

---

### Task 7: `ecet dlq-replay`

**Files:**
- Modify: `src/ecet/infrastructure/queue/rabbitmq.py`
- Modify: `src/ecet/cli.py`
- Modify: `Makefile`
- Test: `tests/unit/test_cli.py`, `tests/adapters/test_rabbitmq_consumer.py`

**Interfaces:**
- Consumes: `declare_topology`, `DLQ`, `ROUTING_KEY` (Phase 3/4); `Settings.amqp_url`.
- Produces:
  - `ecet.infrastructure.queue.rabbitmq.REPLAYED_HEADERS: tuple[str, ...]`.
  - `ecet.infrastructure.queue.rabbitmq.replay_dead_letters(url: str, *, limit: int) -> int` (async; returns how many messages moved).
  - The CLI command `ecet dlq-replay --limit N` (default 100, minimum 1).
  - The Makefile target `make dlq-replay LIMIT=N`.

This closes Phase 4 carry-over #10's recovery path. Requeue has no delay, so a provider 429 burns all five deliveries in milliseconds and dead-letters the claim. Nothing is saved on that path (UC-06 re-raises `LLMTransientError` before touching the claim), so the claim is still `QUEUED`, and putting its message back on `claims.evaluate` is a complete recovery.

- [x] **Step 1: Write the failing CLI tests**

Append to `tests/unit/test_cli.py`:

```python
def test_dlq_replay_moves_messages_and_reports_the_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "tok")
    monkeypatch.setenv("ECET_API_KEY", "key")
    monkeypatch.setenv("ECET_AMQP_URL", "amqp://guest:guest@broker:5672/")
    calls: list[tuple[str, int]] = []

    async def fake_replay(url: str, *, limit: int) -> int:
        calls.append((url, limit))
        return 3

    monkeypatch.setattr("ecet.infrastructure.queue.rabbitmq.replay_dead_letters", fake_replay)

    result = runner.invoke(app, ["dlq-replay", "--limit", "7"])

    assert result.exit_code == 0
    assert calls == [("amqp://guest:guest@broker:5672/", 7)]
    assert "replayed 3" in result.stdout


def test_dlq_replay_refuses_a_non_positive_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "tok")
    monkeypatch.setenv("ECET_API_KEY", "key")

    result = runner.invoke(app, ["dlq-replay", "--limit", "0"])

    assert result.exit_code == 2
```

- [x] **Step 2: Run them and watch them fail**

Run: `uv run pytest tests/unit/test_cli.py -v`
Expected: FAIL. The first test gets exit code 2 with `No such command 'dlq-replay'`. The second passes for the wrong reason (a missing command also exits 2); it becomes meaningful once the command exists.

- [x] **Step 3: Write the failing adapter test**

In `tests/adapters/test_rabbitmq_consumer.py`, extend the `ecet.infrastructure.queue.rabbitmq` import to:

```python
from ecet.infrastructure.queue.rabbitmq import (
    DLQ,
    DLX,
    QUEUE,
    ROUTING_KEY,
    RabbitMqConsumer,
    RabbitMqEvaluationQueue,
    replay_dead_letters,
)
```

and append:

```python
async def test_dlq_replay_moves_dead_letters_back_within_the_limit(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    # `publisher` has declared the topology. Start from empty queues so leftovers from
    # the other tests in this module cannot be mistaken for a replayed message.
    sent = [build_message(), build_message()]
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await (await channel.get_queue(QUEUE)).purge()
        await (await channel.get_queue(DLQ)).purge()
        dlx = await channel.get_exchange(DLX)
        for message in sent:
            await dlx.publish(
                aio_pika.Message(
                    body=message.model_dump_json().encode("utf-8"),
                    content_type="application/json",
                    message_id=str(message.message_id),
                    headers={"x-tenant-id": "tenant-a", "x-schema-version": 1},
                ),
                routing_key=ROUTING_KEY,
            )

    assert await replay_dead_letters(amqp_url, limit=1) == 1
    assert await replay_dead_letters(amqp_url, limit=10) == 1
    assert await replay_dead_letters(amqp_url, limit=10) == 0

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        work = await channel.get_queue(QUEUE)
        replayed = []
        for _ in sent:
            delivered = await work.get(timeout=10)
            assert delivered is not None
            await delivered.ack()
            replayed.append(delivered)
        assert await (await channel.get_queue(DLQ)).get(fail=False) is None

    claim_ids = {EvaluationMessage.model_validate_json(m.body).claim_id for m in replayed}
    assert claim_ids == {message.claim_id for message in sent}
    assert all(m.headers["x-tenant-id"] == "tenant-a" for m in replayed)
    assert all(m.delivery_mode == 2 for m in replayed)
    for m in replayed:
        assert_no_pii(m.body.decode("utf-8"))
```

- [x] **Step 4: Run it and watch it fail**

Run: `uv run pytest tests/adapters/test_rabbitmq_consumer.py -m slow -v`
Expected: FAIL with `ImportError: cannot import name 'replay_dead_letters' from 'ecet.infrastructure.queue.rabbitmq'`.

- [x] **Step 5: Write the replay**

In `src/ecet/infrastructure/queue/rabbitmq.py`, append below `declare_topology`:

```python
#: The headers the publisher sets. The broker's own bookkeeping (`x-death`,
#: `x-delivery-count`, `x-first-death-*`) is dropped so a replayed message starts a
#: fresh delivery budget.
REPLAYED_HEADERS: tuple[str, ...] = ("x-tenant-id", "x-schema-version")


async def replay_dead_letters(url: str, *, limit: int) -> int:
    """Move up to `limit` messages from `claims.evaluate.dlq` back onto `claims.evaluate`
    (`ecet dlq-replay`). Returns how many moved.

    The recovery path for a message the delivery limit gave up on — above all a vendor
    429, which burns all five immediate redeliveries in milliseconds while the claim
    stays `QUEUED`. Each message is re-published as a new message and acked off the DLQ
    only after the broker confirms the publish: a crash mid-replay duplicates a message
    rather than losing it, and the worker already skips a claim that is no longer
    `QUEUED`.

    Nothing is re-validated, so a body that did not parse the first time dead-letters
    again. `limit` also bounds a message that bounces straight back while this runs.
    """
    moved = 0
    connection = await connect_robust(url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = (await declare_topology(channel)).exchange
        dead_letters = await channel.get_queue(DLQ)
        while moved < limit:
            dead = await dead_letters.get(fail=False)
            if dead is None:
                break
            headers = dead.headers or {}
            try:
                await exchange.publish(
                    Message(
                        body=dead.body,
                        content_type=dead.content_type,
                        delivery_mode=DeliveryMode.PERSISTENT,
                        message_id=dead.message_id,
                        headers={k: v for k, v in headers.items() if k in REPLAYED_HEADERS},
                    ),
                    routing_key=ROUTING_KEY,
                )
            except Exception:
                await dead.nack(requeue=True)  # back onto the DLQ, untouched
                raise
            await dead.ack()
            moved += 1
            log.info(
                "dlq.replayed",
                message_id=dead.message_id,
                tenant_id=headers.get("x-tenant-id"),
            )
    return moved
```

Extend the module docstring's last paragraph: "`ecet dlq-replay` (`replay_dead_letters`) is the operator's way back out of the DLQ."

- [x] **Step 6: Add the CLI command**

In `src/ecet/cli.py`, add `from typing import Annotated` to the imports, and append:

```python
@app.command("dlq-replay")
def dlq_replay(
    limit: Annotated[int, typer.Option(min=1, help="Most messages to move.")] = 100,
) -> None:
    """Move dead-lettered evaluations back onto claims.evaluate."""
    from ecet.infrastructure.queue.rabbitmq import replay_dead_letters

    settings = _load_settings()
    configure_logging(settings.log_level)
    moved = asyncio.run(replay_dead_letters(settings.amqp_url.get_secret_value(), limit=limit))
    typer.echo(f"replayed {moved} message(s) from claims.evaluate.dlq")
```

- [x] **Step 7: Add the Makefile target**

In the `Makefile`, add `dlq-replay` to the `.PHONY` line and append:

```makefile
# Move dead-lettered evaluations back onto claims.evaluate — the recovery for a claim a
# vendor 429 dead-lettered (requeue has no delay, so five deliveries go in milliseconds).
dlq-replay: .env
	docker compose exec worker ecet dlq-replay --limit $(or $(LIMIT),100)
```

- [x] **Step 8: Run the CLI and adapter tests and watch them pass**

```bash
uv run pytest tests/unit/test_cli.py -v
uv run pytest tests/adapters/test_rabbitmq_consumer.py -m slow -v
```
Expected: PASS (7 CLI tests; 5 consumer adapter tests including the replay).

- [x] **Step 9: Check types and layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 10: Commit**

```bash
git add src/ecet/infrastructure/queue/rabbitmq.py src/ecet/cli.py Makefile \
  tests/unit/test_cli.py tests/adapters/test_rabbitmq_consumer.py
git commit -m "feat(queue): ecet dlq-replay moves dead letters back onto claims.evaluate"
```

---

### Task 8: The real vendor run and the recorded fixture

> **Cancelled.** The user decided not to call a real vendor at all ("use mocks for
> testing it, we won't use a real API"). `scripts/record_vendor_fixture.py`, the
> `make record-vendor` target and the recorded-fixture replay test were removed from
> the tree; there is no `tests/fixtures/llm/openai_meets.json` and there never will be.
> Only the compose/CI no-vendor guard (`test_the_default_stack_never_calls_a_real_vendor`
> in `tests/unit/test_compose.py`) was kept. The OpenAI-compatible adapter stays
> verified against `httpx.MockTransport` only, as it was after Phase 4.

**Files:**
- Create: `scripts/record_vendor_fixture.py`
- Create: `tests/fixtures/llm/openai_meets.json` (**recorded by the script against a live endpoint, never written by hand**)
- Modify: `tests/unit/infrastructure/test_openai_gateway.py`
- Modify: `tests/unit/test_compose.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: `OpenAiLlmGateway(base_url, api_key, model, timeout_s, http_client)` (Phase 4); `FakePiiRedactor`, `build_evaluation_request(*, redacted_text, policy_id, found_codes, prompt_version)` (`tests/fakes.py`); `read_note`, `assert_no_pii` (`tests/pii.py`); `Settings`, `LlmProvider`.
- Produces:
  - The fixture `tests/fixtures/llm/openai_meets.json`, with the keys `model`, `prompt_version`, `policy_id`, `found_codes`, `response` (the raw chat-completions body) and `evaluation` (the parsed `Evaluation` dumped as JSON, minus `latency_ms`).
  - The Makefile target `make record-vendor`.

The "env flag" is the setting that already exists: `ECET_LLM_PROVIDER=openai`. Phase 4 shipped the adapter tested only against a mock transport. This task sends one real request, keeps the raw response, and replays it through the adapter in the default test run forever after, so a regression in how the adapter reads a *real* server's answer fails CI without CI ever calling a vendor.

> **This task needs a real API key, which an implementer subagent does not have.** Do Steps 1–5 and 7, then stop at Step 6 and ask the human partner to run the recording. Do not fabricate the fixture, and do not commit without it.

- [ ] **Step 1: Write the failing replay test**

In `tests/unit/infrastructure/test_openai_gateway.py`, add `from pathlib import Path`, `from uuid import UUID` and `from ecet.domain.ids import PolicyId` to the imports, then append:

```python
RECORDED = Path(__file__).resolve().parents[2] / "fixtures" / "llm" / "openai_meets.json"


async def test_a_recorded_live_response_still_parses_to_the_evaluation_it_produced() -> None:
    # Recorded once against a real endpoint by `make record-vendor`; nothing in CI calls a
    # vendor. If this fails, the adapter no longer reads a response a real server sent.
    recorded = json.loads(RECORDED.read_text(encoding="utf-8"))
    request = build_evaluation_request(
        policy_id=PolicyId(UUID(recorded["policy_id"])),
        found_codes=recorded["found_codes"],
        prompt_version=recorded["prompt_version"],
    )
    gateway = build_gateway(responder(200, recorded["response"]), model=recorded["model"])

    evaluation = await gateway.evaluate(request)

    assert evaluation.model_dump(mode="json", exclude={"latency_ms"}) == recorded["evaluation"]
    assert_no_pii(json.dumps(recorded))
```

- [ ] **Step 2: Write the failing no-vendor guard**

Append to `tests/unit/test_compose.py`:

```python
def test_the_default_stack_never_calls_a_real_vendor(compose: dict[str, Any]) -> None:
    # `make up` copies `.env.example` to `.env`, and compose takes the provider from it.
    # A real vendor is opt-in per machine, never a default and never a compose override.
    example = (COMPOSE.parent / ".env.example").read_text(encoding="utf-8").splitlines()

    assert "ECET_LLM_PROVIDER=fake" in example
    for service in ("api", "worker"):
        assert "ECET_LLM_PROVIDER" not in compose["services"][service].get("environment", {})
```

- [ ] **Step 3: Run them**

Run: `uv run pytest tests/unit/infrastructure/test_openai_gateway.py tests/unit/test_compose.py -v`
Expected: the replay test FAILS with `FileNotFoundError: ... tests/fixtures/llm/openai_meets.json`. The compose guard PASSES straight away: it pins a property that already holds, so a later edit cannot quietly break it.

- [ ] **Step 4: Write the recording script**

Create `scripts/record_vendor_fixture.py`:

```python
"""Record one real evaluation from a live OpenAI-compatible endpoint.

    ECET_LLM_PROVIDER=openai ECET_LLM_API_KEY=... make record-vendor

The only code in the repository that calls a real LLM vendor, and nothing runs it but
a person: CI and compose default to `ECET_LLM_PROVIDER=fake`, and this script refuses
to start with any other provider. It sends the `meets` fixture note through the same
`OpenAiLlmGateway` the worker uses — after `FakePiiRedactor` and an `assert_no_pii`
gate, because ADR-001 governs a one-off script too — and writes the raw HTTP response,
plus the evaluation the adapter parsed from it, to `tests/fixtures/llm/openai_meets.json`.
`tests/unit/infrastructure/test_openai_gateway.py` replays that response through a mock
transport, so later adapter changes are checked against a shape a real server produced.

Run from the repository root as a module (`python -m scripts.record_vendor_fixture`):
it imports `tests.fakes` and `tests.pii`. Never prints the key or the prompt.
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import httpx

from ecet.config import LlmProvider, Settings
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway
from tests.fakes import FakePiiRedactor, build_evaluation_request
from tests.pii import assert_no_pii, read_note

FIXTURE = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "llm" / "openai_meets.json"


async def record(settings: Settings) -> dict[str, Any]:
    key = settings.llm_api_key
    if settings.llm_provider is not LlmProvider.OPENAI or key is None:
        raise SystemExit(
            "refusing to call a vendor: set ECET_LLM_PROVIDER=openai and ECET_LLM_API_KEY"
        )

    redacted = await FakePiiRedactor().redact(read_note("meets"))
    request = build_evaluation_request(
        redacted_text=redacted.text, prompt_version=settings.prompt_version
    )
    assert_no_pii(request.model_dump_json())

    responses: list[dict[str, Any]] = []

    async def capture(response: httpx.Response) -> None:
        await response.aread()
        if response.is_success:
            responses.append(response.json())
        else:
            # The error body names the rejected field; it never echoes the key.
            print(
                f"vendor answered {response.status_code}: {response.text[:500]}",
                file=sys.stderr,
            )

    gateway = OpenAiLlmGateway(
        base_url=settings.llm_base_url,
        api_key=key.get_secret_value(),
        model=settings.llm_model,
        timeout_s=settings.llm_timeout_s,
        http_client=httpx.AsyncClient(event_hooks={"response": [capture]}),
    )
    try:
        evaluation = await gateway.evaluate(request)
    finally:
        await gateway.aclose()

    recorded: dict[str, Any] = {
        "model": settings.llm_model,
        "prompt_version": request.prompt_version,
        "policy_id": str(request.policies[0].id),
        "found_codes": request.found_codes,
        "response": responses[-1],
        "evaluation": evaluation.model_dump(mode="json", exclude={"latency_ms"}),
    }
    assert_no_pii(json.dumps(recorded))
    return recorded


def main() -> None:
    settings = Settings()  # type: ignore[call-arg]  # required fields come from env / `.env`
    recorded = asyncio.run(record(settings))
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(recorded, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    evaluation = recorded["evaluation"]
    print(
        f"recorded {evaluation['decision']} ({evaluation['confidence']}) "
        f"from {evaluation['model']} -> {FIXTURE}"
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Add the Makefile target and check the refusal path**

In the `Makefile`, add `record-vendor` to the `.PHONY` line and append:

```makefile
# The one target that calls a real LLM vendor. Needs ECET_LLM_PROVIDER=openai and
# ECET_LLM_API_KEY (plus ECET_LLM_BASE_URL / ECET_LLM_MODEL for a non-default server)
# in the environment or `.env`. Nothing else — not CI, not compose — runs it.
record-vendor:
	uv run python -m scripts.record_vendor_fixture
```

Then prove it refuses without the flag (the `.env` default is `fake`):

```bash
uv run ruff format scripts && uv run ruff check --fix scripts
ECET_LLM_PROVIDER=fake make record-vendor; echo "exit=$?"
```
Expected: `refusing to call a vendor: set ECET_LLM_PROVIDER=openai and ECET_LLM_API_KEY` and a non-zero exit, with no network call.

- [ ] **Step 6: Record against a live endpoint (human partner, real key)**

Stop and ask the human partner to run this, with their own key, from the repository root:

```bash
ECET_LLM_PROVIDER=openai ECET_LLM_API_KEY=<key> make record-vendor
```

The defaults target Anthropic's OpenAI-compatible endpoint (`ECET_LLM_BASE_URL=https://api.anthropic.com/v1/`, `ECET_LLM_MODEL=claude-sonnet-5`). Override both for another server.

Expected: `recorded MEETS_NECESSITY (0.9…) from claude-sonnet-5 -> …/tests/fixtures/llm/openai_meets.json`. The decision a real model returns may differ, and any decision is a valid recording.

If the vendor refuses the request (the stderr line shows a 400 naming a field of the tool schema or of `tool_choice`), **do not guess-fix the adapter or the prompt**. Record the status and the field in "Deviations from spec" and stop. A schema the vendor rejects is a Phase 4 adapter defect and needs its own reviewed change. A 429 here is a rate limit: wait and re-run.

Open the written file and read it before committing. It must contain no key, no `Authorization` header and no name, phone number, email or SSN from the note. The script already asserts the last of these.

- [ ] **Step 7: Run the replay test and watch it pass**

Run: `uv run pytest tests/unit/infrastructure/test_openai_gateway.py tests/unit/test_compose.py -v`
Expected: PASS, including `test_a_recorded_live_response_still_parses_to_the_evaluation_it_produced`.

- [ ] **Step 8: Check the default suite and that nothing else calls a vendor**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy src/ecet
uv run pytest
grep -rn "ECET_LLM_PROVIDER" docker-compose.yml .github/ || echo "no provider override in compose or CI"
```
Expected: all green, and the grep prints `no provider override in compose or CI`.

- [ ] **Step 9: Commit**

```bash
git add scripts/record_vendor_fixture.py tests/fixtures/llm/openai_meets.json \
  tests/unit/infrastructure/test_openai_gateway.py tests/unit/test_compose.py Makefile
git commit -m "feat(llm): record a live vendor evaluation and replay it in the adapter tests"
```

---

### Task 9: Compose and the demo — resolving a review delivers `decided_by=human`

**Files:**
- Modify: `docker-compose.yml`
- Modify: `Makefile`
- Modify: `tests/unit/test_compose.py`

**Interfaces:**
- Consumes: `GET /v1/reviews`, `POST /v1/reviews/{id}/resolve`, `POST /v1/claims/{id}/retry-notify` (Task 6); the mock client's `GET /received` (Phase 4).
- Produces: `make demo` now ends by resolving the `note_unclear` review as a human and printing the two deliveries. The api service waits for a healthy `mock-client`.

This is the phase's "Done when": resolving a review triggers a webhook with `decided_by=human`.

- [x] **Step 1: Write the failing compose test**

Append to `tests/unit/test_compose.py`:

```python
def test_the_api_waits_for_the_mock_client_it_now_delivers_to(compose: dict[str, Any]) -> None:
    # UC-09c and the operator retry send webhooks from the api process.
    assert compose["services"]["api"]["depends_on"]["mock-client"]["condition"] == (
        "service_healthy"
    )
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: FAIL with `KeyError: 'mock-client'`.

- [x] **Step 3: Add the dependency**

In `docker-compose.yml`, add to the `api` service's `depends_on` block:

```yaml
      # UC-09c and POST /v1/claims/{id}/retry-notify deliver webhooks from the api.
      mock-client:
        condition: service_healthy
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: PASS.

- [x] **Step 5: Resolve the review in `make demo`**

In the `Makefile`, add near the top (below `DB_URL ?= …`):

```makefile
# Matches `.env.example`. Override when your `.env` uses another key.
API_KEY ?= dev-api-key
```

and replace the last four lines of the `demo` recipe (from `@sleep 5` to the final `curl`) with:

```makefile
	@sleep 5
	docker compose logs --since 60s worker
	@echo "--- resolving the oldest open review for tenant-a as a human ---"
	@# Polls: the worker may still be evaluating note_unclear when the sleep ends.
	@for i in $$(seq 1 30); do \
		TASK=$$(curl -sf -H "X-API-Key: $(API_KEY)" -H "X-Tenant-Id: tenant-a" \
			localhost:8000/v1/reviews | jq -r '.[0].task_id // empty'); \
		[ -n "$$TASK" ] && break; \
		sleep 1; \
	done; \
	[ -n "$$TASK" ] || { echo "no open review for tenant-a after 30s; check 'make logs'"; exit 1; }; \
	curl -sf -H "X-API-Key: $(API_KEY)" -H "X-Tenant-Id: tenant-a" localhost:8000/v1/reviews \
		| jq '[.[] | {task_id, claim_status, reason}]'; \
	curl -sf -X POST -H "X-API-Key: $(API_KEY)" -H "X-Tenant-Id: tenant-a" \
		-H "Content-Type: application/json" \
		-d '{"reviewer":"demo.reviewer","resolution":"MEETS_NECESSITY","notes":"Therapy dates confirmed."}' \
		localhost:8000/v1/reviews/$$TASK/resolve | jq .
	@echo "--- webhooks received by the mock client ---"
	curl -s localhost:8081/received | jq '[.[] | {hook, verified, outcome: .payload.outcome, decided_by: .payload.decided_by}]'
```

- [x] **Step 6: Run the checks**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 7: Run the stack for real**

```bash
make clean
make demo
```
Expected, in order:
1. The Phase 4 output as before: `APPROVED_AUTO` for `note_simple` and `REVIEW_PENDING` for `note_unclear`, plus the `tenant-empty` 422.
2. The review listing printed before the resolve shows exactly one task, with `claim_status: "REVIEW_PENDING"`.
3. The resolve answers `{"task_id": …, "claim_id": …, "claim_status": "REVIEW_RESOLVED"}`.
4. `webhooks received` lists two deliveries, both `verified: true`: one `decided_by: "auto"` with `outcome: "MEETS_NECESSITY"`, and one `decided_by: "human"` with `outcome: "MEETS_NECESSITY"`.
5. `docker compose logs api | grep review.resolved` shows one line with `claim_status=REVIEW_RESOLVED` and no `notes` field.

- [x] **Step 8: Exercise the `NOTIFY_FAILED` exit by hand**

```bash
docker compose stop mock-client
./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-a
sleep 12   # three attempts with a 1 s + 4 s backoff, then the claim parks
CLAIM=$(docker compose exec -T postgres psql -U ecet -d ecet -tAc \
  "select id from claims where status='NOTIFY_FAILED' order by updated_at desc limit 1")
curl -s -o /dev/null -w "%{http_code}\n" -X POST -H "X-API-Key: dev-api-key" \
  localhost:8000/v1/claims/$CLAIM/retry-notify          # 502: still unreachable
docker compose start mock-client && sleep 5
curl -s -X POST -H "X-API-Key: dev-api-key" localhost:8000/v1/claims/$CLAIM/retry-notify \
  | jq '{status, failure_reason}'
```
Expected: `502`, then `{"status": "APPROVED_AUTO", "failure_reason": null}`. The mock client keeps deliveries in memory, so the restart emptied `/received`, and `curl -s localhost:8081/received | jq length` is now `1`: the retried delivery, with `decided_by: "auto"`.

- [x] **Step 9: Confirm `dlq-replay` runs in the stack**

```bash
make dlq-replay LIMIT=10
```
Expected: `replayed 0 message(s) from claims.evaluate.dlq`. The fake provider dead-letters nothing, so this proves the command reaches the broker from the worker container. The replay itself is proven by the adapter test in Task 7.

- [ ] **Step 10 (optional, needs a key): the real vendor through the whole stack** — not run. Task 8 was cancelled at the user's direction and no real vendor is ever called (see Task 8's cancellation note); this optional step is moot.

Set `ECET_LLM_PROVIDER=openai` and `ECET_LLM_API_KEY=<key>` in your local `.env` (never in `.env.example`), then run `make clean && make demo`. The worker log shows `llm.evaluated provider=openai` with real token counts. A 429 dead-letters the claim within a second (deviation 11); `make dlq-replay` puts it back once the rate limit clears. Set `.env` back to `fake` afterwards.

- [x] **Step 11: Commit**

```bash
git add docker-compose.yml Makefile tests/unit/test_compose.py
git commit -m "feat(compose): the demo resolves its open review as a human"
```

---

### Task 10: Record the phase in the docs

**Files:**
- Modify: `specs/06-roadmap.md`
- Modify: `docs/plans/2026-09-15-phase-5-human-review-ops.md` (this file: tick the boxes)

- [x] **Step 1: Confirm the plan link**

`## Phase 5 — Human Review & Ops Endpoints` in `specs/06-roadmap.md` already starts with this line, which was added with the plan. Check that it is still there:

```markdown
Plan: [`docs/plans/2026-09-15-phase-5-human-review-ops.md`](../docs/plans/2026-09-15-phase-5-human-review-ops.md).
```

- [x] **Step 2: Add the Phase 5 carry-over table**

Append a `## Carried over from Phase 5` section to `specs/06-roadmap.md`, after the Phase 4 table and before `## Deferred (explicitly out of v1)`. Fill it in from the "Deviations from spec" section at the bottom of this plan **as it actually ends up**. Do not copy the list below verbatim without checking what changed during execution:

```markdown
## Carried over from Phase 5

Every deferral recorded in [`docs/plans/2026-09-15-phase-5-human-review-ops.md`](../docs/plans/2026-09-15-phase-5-human-review-ops.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 5 | Closed by |
|---|---------------------|-----------|
| 1 | No metric on the review or retry paths either (reviews resolved, webhook attempts from the api), and still no `/metrics` server | Phase 6 |
| 2 | UC-09c's and `retry-notify`'s webhook POST runs inside the HTTP request and inside the open unit of work — up to `ECET_WEBHOOK_MAX_ATTEMPTS` attempts with backoff while a connection sits idle-in-transaction; the api-side twin of Phase 4 #11 | Phase 6 |
| 3 | Requeue still has no delay: a vendor 429 dead-letters a claim in milliseconds, and `ecet dlq-replay` is a manual recovery, not a retry policy | accepted for v1 — a delayed-retry queue is a topology change |
| 4 | `GET /v1/claims/{id}` still omits the redacted text; `GET /v1/reviews` is the only response that carries it | accepted, deliberate |
| 5 | A claim stuck `POLICIES_ATTACHED` is retried only when its object arrives again (MinIO's spooled re-send, or an operator re-posting `/v1/claims/ingest`); there is no sweeper, and `GET /v1/claims/{id}` does not show the object key an operator would re-post | accepted — the outbox stays out of v1 |
| 6 | `ListOpenReviews` reads one claim per task (bounded by `limit` ≤ 200) | accepted unless the queue gets long |
| 7 | `POST /v1/claims/{id}/retry-notify` takes the api key only, as the api spec lists it, so it is not tenant-scoped | accepted — v1 has one global key |
| 8 | A re-publish racing an in-flight ingestion of the same object can publish twice; one save loses with `ConcurrentModification` and the worker skips the second message | accepted, permanent |
| 9 | README: `/v1/reviews`, `retry-notify`, `make dlq-replay`, `make record-vendor`, and the note that only `record-vendor` ever calls a vendor | Phase 6 |
| 10 | No E2E test of resolve → `decided_by=human`; `make demo` is the only end-to-end proof | Phase 6 |
```

- [x] **Step 3: Add the inline Phase 6 carry-over line**

In `## Phase 6`, append after the `- Carried from Phase 4:` block:

```markdown
- Carried from Phase 5: no metric on the review or retry paths either, and the webhook POST behind `POST /v1/reviews/{id}/resolve` and `POST /v1/claims/{id}/retry-notify` runs inside the HTTP request and the open unit of work — the api-side twin of the Phase 4 item above, wanting the same pool-usage metric. The README needs `/v1/reviews`, `retry-notify`, `make dlq-replay` and `make record-vendor` (the only thing that ever calls a vendor). The E2E suite should cover resolve → `decided_by=human`, which only `make demo` proves today.
```

- [x] **Step 4: Verify every roadmap link resolves**

```bash
uv run python - <<'PY'
import re
from pathlib import Path

root = Path("specs")
bad = []
for source in root.rglob("*.md"):
    for target in re.findall(r"\]\((?!https?:)([^)#]+)", source.read_text()):
        if not (source.parent / target).exists():
            bad.append(f"{source}: {target}")
print("\n".join(bad) or "all links resolve")
PY
```

Expected: only the known false positive (`specs/01-domain/policy.md` matches the ICD-10 regex `\.[0-9A-Z]{1,4}` as if it were a link).

- [x] **Step 5: Commit**

```bash
git add specs/06-roadmap.md docs/plans/2026-09-15-phase-5-human-review-ops.md
git commit -m "docs(phase-5): record the deviations and the Phase 5 carry-overs"
```

---

## Phase exit criteria

Everything below must be true before the phase is called done:

1. `make check` is green (lint, `mypy --strict` on domain + application, `mypy src/ecet`, import contracts, default test suite).
2. `uv run pytest -m 'not e2e' --cov=ecet --cov-fail-under=85` is green with Docker and `en_core_web_lg` available.
3. `make clean && make demo` ends with two verified deliveries in `curl -s localhost:8081/received`. Exactly one has `"decided_by": "human"`: `curl -s localhost:8081/received | jq '[.[] | select(.payload.decided_by=="human")] | length'` prints `1`.
4. After the demo, `GET /v1/reviews` with `X-Tenant-Id: tenant-a` returns `[]`, and `select status from review_tasks` shows one `RESOLVED` row.
5. Resolving the same task a second time answers 409. Resolving it under `X-Tenant-Id: tenant-b` answers 404 and sends no webhook.
6. The Task 9 Step 8 sequence turns a `NOTIFY_FAILED` claim into `APPROVED_AUTO` through `POST /v1/claims/{id}/retry-notify`: 502 while the receiver is down, 200 once it is back.
7. `make dlq-replay` runs in the stack. `tests/adapters/test_rabbitmq_consumer.py::test_dlq_replay_moves_dead_letters_back_within_the_limit` is green.
8. Cancelled at the user's direction: no real vendor is ever called, so there is no `tests/fixtures/llm/openai_meets.json` and no `make record-vendor` target. The OpenAI-compatible adapter stays verified against `httpx.MockTransport` only. `docker-compose.yml`, `.github/` and `.env.example` contain no `ECET_LLM_PROVIDER=openai`.
9. A publish failure followed by the same object arriving again leaves the claim `QUEUED` with one message on the queue (the Task 5 unit test).
10. No API response other than `GET /v1/reviews` contains claim text, no webhook payload or log line contains a reviewer's `notes`, and nothing contains a string from `tests/pii.py::PII_STRINGS` (asserted in the unit and API tests, and eyeballed once in `docker compose logs api worker`).

## Deviations from spec (record in the PR description)

Fill this in during execution. The list below is what the plan *expects* to deviate on. Add anything else that comes up, and drop anything that turns out not to be needed.

1. **`ReviewTaskRepository` gains `find_by_claim`**, which [evaluation.md §4](../../specs/01-domain/evaluation.md#4-reviewtask-human-in-the-loop) does not list (the spec lists `add`, `get`, `list_open`, `save`, and Phase 2 already added `find_open_by_claim`). The operator retry has to tell a human decision from an automatic one, and a *resolved* task is the only record of which it was. `review_tasks.claim_id` is unique, so the lookup is exact.
2. **`EVALUATION_FAILED → NOTIFY_FAILED` is a new state-machine edge.** [UC-09c](../../specs/02-use-cases/UC-09-human-review.md#uc-09c-resolvereview) step 4 sends a resolved claim to `NOTIFY_FAILED` on a delivery failure. But UC-06 parks invalid LLM output in `EVALUATION_FAILED` and opens a review task for it, and that state had no way into `NOTIFY_FAILED`, so resolving such a claim with a down receiver would have raised `InvalidTransition` after the webhook attempt.
3. **`ClaimView` still omits the redacted text.** The Phase 3 carry-over read "`ClaimView` omits the redacted text that UC-09b needs". UC-09b's own `ReviewTaskView` carries it instead, so the review queue is the one API response that returns clinical content, and `GET /v1/claims/{id}` keeps the narrower shape the [api spec](../../specs/04-interfaces/api.md#endpoints) describes ("status, deterministic, evaluation, no secrets").
4. **The `POLICIES_ATTACHED` retry is re-ingesting the same object, not a new endpoint or a sweeper.** The Phase 3 plan anticipated "`/v1/claims/{id}/retry-notify` and a sweeper". UC-01's duplicate check now re-publishes a claim it finds still `POLICIES_ATTACHED`. With MinIO's persistent `queue_dir`, the event the api answered 503 is re-sent and becomes the retry on its own. An operator re-posting `/v1/claims/ingest` is the manual path.
5. **`POST /v1/claims/{id}/retry-notify` answers 502 (with the `ClaimView` body) when the delivery fails again.** The api spec gives no status. A 200 would tell a scripted `curl -f` that the claim was delivered when it was not. The attempt is still committed.
6. **`POST /v1/reviews/{id}/resolve` answers 200 even when the webhook then fails**, with `claim_status: "NOTIFY_FAILED"` in the body. The decision is recorded, and rolling back a human's resolution because a receiver was down would lose work nobody can recover automatically.
7. **`retry-notify` is not tenant-scoped.** The api spec gives it the api key only (unlike `GET /v1/claims/{id}` and the review routes), and v1's single global key already reaches every tenant. The route is an operator action that re-sends an existing decision.
8. **The delivery-failure → token mapping moved from `RouteDecision` into `NotifyClient.attempt`**, and the tokens `webhook_rejected`, `webhook_unreachable` and `tenant_inactive` moved with it. UC-07, UC-09c and the retry park a failed delivery identically, and three copies of the same `except` ladder would drift.
9. **`ecet dlq-replay` re-publishes each body verbatim and strips the broker's headers**, keeping only `x-tenant-id` and `x-schema-version`, so a replayed message starts a fresh delivery budget. It acks off the DLQ only after the publish is confirmed (at-least-once) and does not re-validate: an undecodable body dead-letters again.
10. **The roadmap's "real vendor run behind env flag; record one real evaluation output as fixture" line for Phase 5 was dropped, at the user's direction: no real vendor is ever called.** Task 8 (`scripts/record_vendor_fixture.py`, `make record-vendor`, `tests/fixtures/llm/openai_meets.json`, and the recorded-fixture replay test) was cancelled and removed from the tree. Only the compose/CI no-vendor guard test survives (`test_the_default_stack_never_calls_a_real_vendor` in `tests/unit/test_compose.py`). The OpenAI-compatible adapter stays verified against `httpx.MockTransport` only, exactly as it was after Phase 4.
11. **Requeue still has no delay.** The roadmap names `ecet dlq-replay` as this phase's recovery for the 429 case (Phase 4 carry-over #10), and that is what ships. A delayed-retry queue, or per-message backoff before `nack(requeue=True)`, is a topology change and a different retry policy, so it is recorded as accepted for v1 rather than half-built.
12. **The api now depends on `mock-client` in compose.** It delivers webhooks for UC-09c and the retry. The [docker-compose spec](../../specs/05-platform/docker-compose.md#docker-composeyml-services) lists only postgres and rabbitmq for it.
13. **The webhook POST behind resolve and retry runs inside the HTTP request and the open unit of work.** This is the api-side twin of Phase 4 deviation 17, and is recorded, not restructured, for the same reason. Closed by Phase 6.
14. **`ResolveReviewCommand.tenant_id` is a plain `TenantId`, not the pattern-validated `TenantIdField`.** It comes from a header, and a malformed tenant should miss with a 404 like any wrong tenant, not fail pydantic validation with a different status.
