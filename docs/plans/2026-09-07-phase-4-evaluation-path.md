# Phase 4 — Evaluation Path (Worker side) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A message on `claims.evaluate` becomes an LLM evaluation, a routed claim and a signed webhook delivered to a running mock client — so `make up` plus one PDF drop walks the whole pipeline from bucket to client callback with no external API key.

**Architecture:** The worker is the mirror image of the Phase 3 API: one `LLMGateway` port and one `WebhookClient` port in `ecet/application/ports/`, three use cases (`EvaluateClaim` → `RouteDecision` → `NotifyClient`) that depend only on ports plus the Phase 2 `UnitOfWork`, and adapters wired once at startup into a `WorkerContainer`. `EvaluateClaim` owns the transaction boundary through a `uow_factory`, exactly as `IngestClaimDocument` does, because `SqlAlchemyUnitOfWork` is single-use. The RabbitMQ consumer lives beside the publisher in `infrastructure/queue/rabbitmq.py` and reuses `declare_topology`; the ack/nack decision is a pure function in `interfaces/worker/handler.py`, so the delivery policy is unit-testable without a broker. The prompt is application-owned and versioned; the vendor adapter only transports it and fills `model`, `latency_ms` and token counts.

**Tech Stack:** Python 3.12, aio-pika 9.x (consumer), openai SDK (any OpenAI-compatible server), httpx 0.27 (webhook delivery, now a runtime dependency), FastAPI 0.141 (the `services/mock-client` receiver), SQLAlchemy 2 async + the Phase 2 repositories, pytest with `httpx.MockTransport` and testcontainers.

**Spec:** [`specs/06-roadmap.md` §Phase 4](../../specs/06-roadmap.md#phase-4--evaluation-path-worker-side), which pulls in [UC-06](../../specs/02-use-cases/UC-06-evaluate-claim.md), [UC-07](../../specs/02-use-cases/UC-07-route-decision.md), [UC-08](../../specs/02-use-cases/UC-08-notify-client.md), [UC-09a](../../specs/02-use-cases/UC-09-human-review.md#uc-09a-requesthumanreview), [llm-gateway](../../specs/03-infrastructure/llm-gateway.md), [webhook-client](../../specs/03-infrastructure/webhook-client.md), [queue-rabbitmq](../../specs/03-infrastructure/queue-rabbitmq.md), [worker](../../specs/04-interfaces/worker.md), [evaluation](../../specs/01-domain/evaluation.md), [claim](../../specs/01-domain/claim.md), [tenant](../../specs/01-domain/tenant.md), [config](../../specs/05-platform/config.md), [docker-compose](../../specs/05-platform/docker-compose.md), [testing](../../specs/05-platform/testing.md), [project-layout](../../specs/05-platform/project-layout.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- Layer rule (`import-linter`, `make imports` must stay green): `ecet.domain` imports stdlib + pydantic only; `ecet.application` imports domain + stdlib + pydantic — **never** `openai`, `httpx`, `aio_pika`, `sqlalchemy`, `fastapi`, or `ecet.config`. The `forbidden` contract already lists all of them; do not weaken it. Only `ecet.infrastructure` and `ecet.interfaces` may import those.
- `mypy --strict` covers `src/ecet/domain` and `src/ecet/application`; `mypy src/ecet` (repo-wide `disallow_untyped_defs = true`) covers infrastructure and interfaces. Every function added in this phase is annotated.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`.
- **ADR-001 still governs.** The worker never sees raw text — the message carries `redacted_text` only. Nothing in this phase may log a text field, and `ClientNotification` carries no claim text at all. Every test that touches text or a payload ends with `assert_no_pii(...)`.
- **Log through `structlog` only** — never `logging.getLogger`. Phase 6 carry-over #12: stdlib records bypass the `drop_sensitive_fields` guard. Never log a prompt body, a webhook secret, or an HMAC signature.
- **`EvaluationMessage` is a frozen wire contract.** Do not add, rename or retype a field. If this phase seems to need one, it does not: `schema_version` is a `Literal[1]` and changing the shape is a version bump, which is out of scope.
- **`declare_topology` and the queue constants are shared.** The consumer imports them from `ecet/infrastructure/queue/rabbitmq.py`; it must not redeclare the exchange, the queue or `QUEUE_ARGUMENTS` with its own literals.
- **`SqlAlchemyUnitOfWork` is single-use.** Re-entering an instance does not create a fresh session. Every worker message gets a new one from a `uow_factory: Callable[[], UnitOfWork]`, never a shared instance.
- **`PostgresClaimRepository.save` refuses a claim it never read.** `get()` registers the optimistic baseline; a `save()` without one raises `ConcurrentModification`. The worker always `get()`s the claim first, so this is satisfied — but do not construct a `Claim` and save it.
- **`failure_reason` is a short token, never an exception message.** `EXTRACTION_FAILED` set the convention in Phase 3 (`no_text`, `object_unavailable`). This phase adds `llm_invalid_output`, `llm_permanent_error`, `webhook_rejected`, `webhook_unreachable`. Exception detail belongs in `Claim.last_notify_error` and in the structlog record, not in `failure_reason`.
- **Tenant isolation is the use case's job.** Repositories take no tenant parameter. `EvaluateClaim` compares the loaded claim's `tenant_id` against the message's before doing anything else.
- Adapter tests that need a container live in `tests/adapters/` and are auto-marked `slow` by that directory's `conftest.py`. Tests that only need a mock HTTP transport live in `tests/unit/infrastructure/` so they stay in the default run.
- TDD: every step-pair is "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Commit after every task with a conventional prefix. **No Claude attribution in commit messages.**

### Explicitly out of scope for Phase 4 (do not add)

- `/metrics`, `prometheus_client`, the worker container healthcheck, and every `ecet_*` metric name including `ecet_llm_calls_total`, `ecet_triage_route_total` and `ecet_webhook_attempts_total` (Phase 6).
- Binding `request_id` to the structlog context and propagating `x-request-id` through the queue (Phase 6).
- UC-09b / UC-09c, `/v1/reviews*`, `POST /v1/claims/{id}/retry-notify`, `ecet dlq-replay`, and any automatic retry of a `NOTIFY_FAILED` claim (Phase 5).
- A real vendor run against a live endpoint, and recording a real evaluation as a fixture (Phase 5). The OpenAI-compatible adapter ships and is tested against a mock transport; nothing in CI or compose calls a real API.
- A JSON-mode / `response_format` fallback for servers without tool calling — the `ponytail:` comment in the [llm-gateway spec](../../specs/03-infrastructure/llm-gateway.md) defers it until a target server actually fails the tool path.
- `infrastructure/webhook/fake_client.py` and `infrastructure/queue/in_memory.py` from the [project layout](../../specs/05-platform/project-layout.md): `tests/fakes.py` covers both needs and no compose service wants either. See deviation 2.
- The README rewrite, the E2E suite and the `tests/e2e/` directory (Phase 6).

---

### Task 1: The LLM gateway port, prompt v1 and the fake gateway

**Files:**
- Modify: `src/ecet/application/errors.py`
- Create: `src/ecet/application/ports/llm_gateway.py`
- Create: `src/ecet/application/prompts/__init__.py`, `src/ecet/application/prompts/evaluate_v1.py`
- Create: `src/ecet/infrastructure/llm/__init__.py`, `src/ecet/infrastructure/llm/fake_gateway.py`
- Modify: `tests/fakes.py`
- Test: `tests/unit/application/test_llm_port.py`, `tests/unit/application/test_prompt_v1.py`, `tests/unit/infrastructure/test_fake_gateway.py`

**Interfaces:**
- Consumes: `ecet.application.messages.PolicySnapshot`, `ecet.domain.evaluation.Decision`, `ecet.domain.evaluation.Evaluation`, `ecet.domain.policy.Icd10Code`, `ecet.domain.ids.ClaimId`, `ecet.application.errors.DomainError` tree (all existing).
- Produces:
  - `ecet.application.errors`: `LLMError(DomainError)`, `LLMTransientError(LLMError)`, `LLMPermanentError(LLMError)`, `LLMInvalidOutput(LLMError)`.
  - `ecet.application.ports.llm_gateway`: `EvaluationRequest` (fields `claim_id: ClaimId`, `redacted_text: str`, `policies: list[PolicySnapshot]`, `found_codes: list[str]`, `prompt_version: str`), `EvaluationOutput` (the vendor-facing subset, with `to_evaluation(...) -> Evaluation`), `LLMGateway` Protocol (`async evaluate(request: EvaluationRequest) -> Evaluation`).
  - `ecet.application.prompts.evaluate_v1`: `PROMPT_VERSION: str`, `TOOL_NAME: str`, `SYSTEM_PROMPT: str`, `TOOL: dict[str, Any]`, `render_user_message(request) -> str`, `build_messages(request) -> list[dict[str, str]]`.
  - `ecet.infrastructure.llm.fake_gateway.FakeLlmGateway` (`MODEL: str`, `async evaluate(request) -> Evaluation`).
  - `tests.fakes.FakeLLMGateway` (records requests, returns a canned `Evaluation` or raises a canned error).

- [x] **Step 1: Write the failing port and output-model test**

Create `tests/unit/application/test_llm_port.py`:

```python
"""The gateway contract, tested at the boundary the adapters have to honour:
`EvaluationOutput` is what a vendor is allowed to return, `Evaluation` is what the
domain stores, and `to_evaluation` is the only bridge between them."""

from uuid import uuid4

import pytest
from tests.fakes import FakeLLMGateway

from ecet.application.ports.llm_gateway import (
    EvaluationOutput,
    EvaluationRequest,
    LLMGateway,
)
from ecet.domain.evaluation import Decision
from ecet.domain.ids import ClaimId, PolicyId


def build_output(**overrides: object) -> EvaluationOutput:
    fields: dict[str, object] = {
        "decision": Decision.MEETS_NECESSITY,
        "confidence": 0.91,
        "rationale": "Conservative therapy documented for nine weeks.",
        "cited_codes": ["M54.5"],
    }
    fields.update(overrides)
    return EvaluationOutput.model_validate(fields)


def test_the_fake_gateway_satisfies_the_port() -> None:
    assert isinstance(FakeLLMGateway(), LLMGateway)


def test_to_evaluation_carries_the_vendor_metadata() -> None:
    evaluation = build_output().to_evaluation(
        model="claude-sonnet-5",
        prompt_version="v1",
        latency_ms=412,
        input_tokens=900,
        output_tokens=120,
    )

    assert evaluation.decision is Decision.MEETS_NECESSITY
    assert evaluation.confidence == 0.91
    assert [code.code for code in evaluation.cited_codes] == ["M54.5"]
    assert evaluation.model == "claude-sonnet-5"
    assert evaluation.prompt_version == "v1"
    assert evaluation.latency_ms == 412
    assert evaluation.input_tokens == 900
    assert evaluation.output_tokens == 120


def test_a_missing_confidence_becomes_insufficient_evidence_at_zero() -> None:
    # evaluation.md: "vendor must supply; missing -> INSUFFICIENT_EVIDENCE, 0.0".
    evaluation = build_output(confidence=None).to_evaluation(
        model="m", prompt_version="v1", latency_ms=0, input_tokens=0, output_tokens=0
    )

    assert evaluation.decision is Decision.INSUFFICIENT_EVIDENCE
    assert evaluation.confidence == 0.0


def test_a_malformed_cited_code_is_dropped_not_fatal() -> None:
    evaluation = build_output(cited_codes=["M54.5", "not-a-code", ""]).to_evaluation(
        model="m", prompt_version="v1", latency_ms=0, input_tokens=0, output_tokens=0
    )

    assert [code.code for code in evaluation.cited_codes] == ["M54.5"]


def test_a_matched_policy_id_outside_the_request_is_discarded() -> None:
    known = PolicyId(uuid4())
    hallucinated = uuid4()  # a policy id that was never in the request

    evaluation = build_output(matched_policy_id=hallucinated).to_evaluation(
        model="m",
        prompt_version="v1",
        latency_ms=0,
        input_tokens=0,
        output_tokens=0,
        known_policy_ids={known},
    )

    assert evaluation.matched_policy_id is None


def test_an_evaluation_request_is_frozen() -> None:
    request = EvaluationRequest(
        claim_id=ClaimId(uuid4()),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[],
        found_codes=["M54.5"],
        prompt_version="v1",
    )

    with pytest.raises(Exception):
        request.redacted_text = "mutated"  # type: ignore[misc]
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_llm_port.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.ports.llm_gateway'`.

- [x] **Step 3: Add the LLM errors**

Append to `src/ecet/application/errors.py`, and extend `__all__` to
`["ExtractionFailed", "LLMError", "LLMInvalidOutput", "LLMPermanentError", "LLMTransientError", "ObjectNotFound", "QueuePublishError"]`:

```python
class LLMError(DomainError):
    """Base for every `LLMGateway` failure, so the worker's ack policy can branch on
    one tree rather than on a list of unrelated types."""


class LLMTransientError(LLMError):
    """429, 5xx, timeout or connection failure. The message is nacked with requeue;
    RabbitMQ's `x-delivery-limit` moves it to the DLQ after five deliveries."""


class LLMPermanentError(LLMError):
    """400, 401, 403 or 404 — a request or credential the retry would repeat verbatim.
    The claim goes `EVALUATION_FAILED` and a human picks it up."""


class LLMInvalidOutput(LLMError):
    """The vendor answered, but not with something `EvaluationOutput` accepts: no tool
    call, unparseable arguments, or a schema violation."""
```

- [x] **Step 4: Write the port module**

Create `src/ecet/application/ports/llm_gateway.py`:

```python
"""The vendor boundary (ADR-004).

Two models, deliberately not one. `EvaluationOutput` is what a vendor is permitted to
return — the subset a language model can actually author. `Evaluation` is what the
domain stores, and it additionally carries the facts only the adapter knows: which
model answered, how long it took, how many tokens it cost. `to_evaluation` is the one
bridge, so every adapter fills those fields the same way.
"""

from collections.abc import Collection
from typing import Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ecet.application.messages import PolicySnapshot
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code

__all__ = ["EvaluationOutput", "EvaluationRequest", "LLMGateway"]


class EvaluationRequest(BaseModel):
    """Everything the model is allowed to see. No tenant, no webhook, no raw text."""

    model_config = ConfigDict(frozen=True)

    claim_id: ClaimId
    redacted_text: str
    policies: list[PolicySnapshot] = Field(default_factory=list)
    found_codes: list[str] = Field(default_factory=list)
    prompt_version: str


class EvaluationOutput(BaseModel):
    """The tool-call schema. `extra="forbid"` makes an invented field a validation
    error rather than silently ignored output."""

    model_config = ConfigDict(extra="forbid")

    decision: Decision
    #: Optional on purpose: the prompt demands it, but a server that omits it must
    #: degrade to INSUFFICIENT_EVIDENCE (evaluation.md §2) rather than fail the claim.
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    matched_policy_id: UUID | None = None
    cited_codes: list[str] = Field(default_factory=list)
    rationale: str = Field(default="", max_length=2000)
    evidence_found: list[str] = Field(default_factory=list)
    evidence_missing: list[str] = Field(default_factory=list)

    def to_evaluation(
        self,
        *,
        model: str,
        prompt_version: str,
        latency_ms: int,
        input_tokens: int,
        output_tokens: int,
        known_policy_ids: Collection[PolicyId] = (),
    ) -> Evaluation:
        """Build the domain model. Two defensive conversions happen here rather than in
        each adapter: a code the model invented that is not ICD-10-shaped is dropped,
        and a `matched_policy_id` that was not in the request is discarded — neither is
        worth failing an otherwise usable evaluation over, and both would otherwise be
        persisted as fact."""
        decision = self.decision
        confidence = self.confidence
        if confidence is None:
            decision, confidence = Decision.INSUFFICIENT_EVIDENCE, 0.0

        matched = self.matched_policy_id
        if matched is not None and known_policy_ids and matched not in set(known_policy_ids):
            matched = None

        return Evaluation(
            decision=decision,
            confidence=confidence,
            matched_policy_id=PolicyId(matched) if matched is not None else None,
            cited_codes=_parse_codes(self.cited_codes),
            rationale=self.rationale,
            evidence_found=list(self.evidence_found),
            evidence_missing=list(self.evidence_missing),
            model=model,
            prompt_version=prompt_version,
            latency_ms=latency_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )


def _parse_codes(codes: list[str]) -> list[Icd10Code]:
    parsed: list[Icd10Code] = []
    for code in codes:
        try:
            parsed.append(Icd10Code(code=code))
        except ValidationError:
            continue
    return parsed


@runtime_checkable
class LLMGateway(Protocol):
    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        """Raises `LLMTransientError`, `LLMPermanentError` or `LLMInvalidOutput`."""
        ...
```

- [x] **Step 5: Add `FakeLLMGateway` to `tests/fakes.py`**

Add the imports `from ecet.application.ports.llm_gateway import EvaluationRequest` and
`from ecet.domain.evaluation import Decision, Evaluation, ReviewStatus, ReviewTask` (extend the
existing `ecet.domain.evaluation` import rather than adding a second one), then append:

```python
class FakeLLMGateway:
    """Records every request. Returns the canned evaluation, or raises the canned
    error — the two branches UC-06 has to tell apart."""

    def __init__(
        self,
        *,
        evaluation: Evaluation | None = None,
        error: Exception | None = None,
    ) -> None:
        self.evaluation = evaluation or Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            rationale="canned",
            model="fake",
            prompt_version="v1",
        )
        self.error = error
        self.requests: list[EvaluationRequest] = []

    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.evaluation
```

- [x] **Step 6: Run the port test and watch it pass**

Run: `uv run pytest tests/unit/application/test_llm_port.py -v`
Expected: PASS (6 tests).

- [x] **Step 7: Write the failing prompt test**

Create `tests/unit/application/test_prompt_v1.py`:

```python
"""The prompt is application-owned, so it is testable without a vendor. What matters
here is that every fact the model needs is in the message and nothing it must not see
is."""

from uuid import uuid4

from tests.pii import assert_no_pii

from ecet.application.messages import PolicySnapshot
from ecet.application.ports.llm_gateway import EvaluationOutput, EvaluationRequest
from ecet.application.prompts.evaluate_v1 import (
    PROMPT_VERSION,
    SYSTEM_PROMPT,
    TOOL,
    TOOL_NAME,
    build_messages,
    render_user_message,
)
from ecet.domain.ids import ClaimId, PolicyId

POLICY_ID = PolicyId(uuid4())


def build_request() -> EvaluationRequest:
    return EvaluationRequest(
        claim_id=ClaimId(uuid4()),
        redacted_text="Patient <PERSON> with M54.5 after nine weeks of therapy.",
        policies=[
            PolicySnapshot(
                id=POLICY_ID,
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M51.26", "M54.5"],
                excluded_codes=["Z00.00"],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=["conservative therapy >= 6 weeks"],
            )
        ],
        found_codes=["M54.5"],
        prompt_version=PROMPT_VERSION,
    )


def test_the_user_message_carries_every_policy_field_the_model_needs() -> None:
    rendered = render_user_message(build_request())

    assert str(POLICY_ID) in rendered
    assert "MRI lumbar spine" in rendered
    assert "M51.26" in rendered
    assert "Z00.00" in rendered
    assert "six weeks of conservative therapy" in rendered
    assert "conservative therapy >= 6 weeks" in rendered
    assert "M54.5" in rendered
    assert "Patient <PERSON>" in rendered


def test_the_rendered_prompt_carries_no_pii() -> None:
    assert_no_pii(render_user_message(build_request()))
    assert_no_pii(SYSTEM_PROMPT)


def test_build_messages_is_a_system_then_user_pair() -> None:
    messages = build_messages(build_request())

    assert [message["role"] for message in messages] == ["system", "user"]
    assert messages[0]["content"] == SYSTEM_PROMPT


def test_the_tool_schema_matches_the_output_model() -> None:
    assert TOOL["type"] == "function"
    assert TOOL["function"]["name"] == TOOL_NAME
    parameters = TOOL["function"]["parameters"]
    assert parameters == EvaluationOutput.model_json_schema()
    assert set(parameters["properties"]) == set(EvaluationOutput.model_fields)


def test_an_empty_policy_list_still_renders() -> None:
    request = build_request().model_copy(update={"policies": [], "found_codes": []})

    rendered = render_user_message(request)

    assert "none" in rendered.lower()
```

- [x] **Step 8: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_prompt_v1.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.prompts'`.

- [x] **Step 9: Write the prompt module**

Create an empty `src/ecet/application/prompts/__init__.py`, then create
`src/ecet/application/prompts/evaluate_v1.py`:

```python
"""Prompt v1 for UC-06.

The prompt lives in the application layer, not in the adapter, because it is part of
what the system decides — the adapter only transports it. `PROMPT_VERSION` is stored
on every `Evaluation`, so an output can always be traced back to the text that
produced it: change the wording, bump the version.

Structured output is a forced tool call rather than "reply in JSON", so there is no
prose to parse and no fenced-code stripping anywhere in the codebase.
"""

from typing import Any

from ecet.application.messages import PolicySnapshot
from ecet.application.ports.llm_gateway import EvaluationOutput, EvaluationRequest

__all__ = [
    "PROMPT_VERSION",
    "SYSTEM_PROMPT",
    "TOOL",
    "TOOL_NAME",
    "build_messages",
    "render_user_message",
]

PROMPT_VERSION = "v1"
TOOL_NAME = "submit_evaluation"

SYSTEM_PROMPT = """You are a utilisation review assistant for a health insurer.

You are given one de-identified clinical note and the payer policies that apply to \
the requested service. Decide whether the note establishes medical necessity under \
those policies.

Rules:
- Judge only against the policies given. Do not apply outside clinical knowledge as \
if it were policy.
- MEETS_NECESSITY: the note documents every criterion of at least one policy.
- DOES_NOT_MEET: the note contradicts a criterion, or the service is excluded.
- INSUFFICIENT_EVIDENCE: the note neither establishes nor contradicts the criteria.
- confidence is your calibrated probability that the decision is correct, from 0.0 to \
1.0. Always supply it. If you cannot tell, choose INSUFFICIENT_EVIDENCE with a low \
confidence rather than guessing a decision with a high one.
- matched_policy_id must be one of the policy ids given, or null.
- evidence_found and evidence_missing are drawn from the policies' required evidence.
- rationale is at most a short paragraph and cites what the note says.

The note has already been de-identified. Do not speculate about the identity of the \
patient or the provider, and do not ask for the removed values.

Answer only by calling the submit_evaluation tool."""

TOOL: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": TOOL_NAME,
        "description": "Submit the medical-necessity evaluation for this claim.",
        "parameters": EvaluationOutput.model_json_schema(),
    },
}


def _render_policy(index: int, policy: PolicySnapshot) -> str:
    covered = ", ".join(policy.covered_codes) or "none listed"
    excluded = ", ".join(policy.excluded_codes) or "none listed"
    evidence = "\n".join(f"    - {item}" for item in policy.required_evidence) or "    - none"
    return (
        f"Policy {index}\n"
        f"  id: {policy.id}\n"
        f"  name: {policy.name} (version {policy.version})\n"
        f"  covered codes: {covered}\n"
        f"  excluded codes: {excluded}\n"
        f"  criteria: {policy.criteria_text}\n"
        f"  required evidence:\n{evidence}"
    )


def render_user_message(request: EvaluationRequest) -> str:
    policies = "\n\n".join(
        _render_policy(index, policy) for index, policy in enumerate(request.policies, start=1)
    )
    codes = ", ".join(request.found_codes)
    return (
        "APPLICABLE POLICIES\n"
        f"{policies or 'none'}\n\n"
        "DIAGNOSIS CODES FOUND IN THE NOTE\n"
        f"{codes or 'none'}\n\n"
        "DE-IDENTIFIED CLINICAL NOTE\n"
        f"{request.redacted_text}"
    )


def build_messages(request: EvaluationRequest) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": render_user_message(request)},
    ]
```

- [x] **Step 10: Run the prompt test and watch it pass**

Run: `uv run pytest tests/unit/application/test_prompt_v1.py -v`
Expected: PASS (5 tests).

- [x] **Step 11: Write the failing fake-gateway test**

Create `tests/unit/infrastructure/test_fake_gateway.py`:

```python
"""The fake gateway is what `docker compose up` runs by default, so it is production
code for the demo, not a test double — hence a real test of its rule table."""

from uuid import uuid4

import pytest
from tests.fakes import build_evaluation_request

from ecet.domain.evaluation import Decision
from ecet.domain.ids import PolicyId
from ecet.infrastructure.llm.fake_gateway import FakeLlmGateway


@pytest.mark.parametrize(
    ("text", "decision", "confidence"),
    [
        ("The request was denied by the reviewer.", Decision.DOES_NOT_MEET, 0.92),
        ("Duration unclear and not documented.", Decision.INSUFFICIENT_EVIDENCE, 0.4),
        ("Nine weeks of conservative therapy documented.", Decision.MEETS_NECESSITY, 0.91),
    ],
)
async def test_the_rule_table(text: str, decision: Decision, confidence: float) -> None:
    evaluation = await FakeLlmGateway().evaluate(build_evaluation_request(redacted_text=text))

    assert evaluation.decision is decision
    assert evaluation.confidence == confidence


async def test_the_rules_are_case_insensitive() -> None:
    evaluation = await FakeLlmGateway().evaluate(build_evaluation_request(redacted_text="DENIED."))

    assert evaluation.decision is Decision.DOES_NOT_MEET


async def test_a_decisive_answer_matches_the_first_policy() -> None:
    policy_id = PolicyId(uuid4())
    request = build_evaluation_request(policy_id=policy_id, redacted_text="therapy documented")

    evaluation = await FakeLlmGateway().evaluate(request)

    assert evaluation.matched_policy_id == policy_id


async def test_an_insufficient_answer_matches_no_policy() -> None:
    request = build_evaluation_request(redacted_text="duration unclear")

    evaluation = await FakeLlmGateway().evaluate(request)

    assert evaluation.matched_policy_id is None


async def test_the_vendor_metadata_is_filled_in() -> None:
    request = build_evaluation_request(redacted_text="therapy documented")

    evaluation = await FakeLlmGateway().evaluate(request)

    assert evaluation.model == FakeLlmGateway.MODEL
    assert evaluation.prompt_version == request.prompt_version
    assert [code.code for code in evaluation.cited_codes] == request.found_codes
```

- [x] **Step 12: Add the shared request builder to `tests/fakes.py`**

Append to `tests/fakes.py` (it is imported by four test modules in this phase, so it
lives with the fakes rather than being copied into each):

```python
def build_evaluation_request(
    *,
    redacted_text: str = "Patient <PERSON> with M54.5.",
    policy_id: PolicyId | None = None,
    found_codes: Sequence[str] = ("M54.5",),
    prompt_version: str = "v1",
) -> EvaluationRequest:
    """A minimal, valid `EvaluationRequest` with exactly one policy."""
    from uuid import uuid4

    return EvaluationRequest(
        claim_id=ClaimId(uuid4()),
        redacted_text=redacted_text,
        policies=[
            PolicySnapshot(
                id=policy_id if policy_id is not None else PolicyId(uuid4()),
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M54.5"],
                excluded_codes=["Z00.00"],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=["conservative therapy >= 6 weeks"],
            )
        ],
        found_codes=list(found_codes),
        prompt_version=prompt_version,
    )
```

Move the `uuid4` import to the module's import block (`from uuid import UUID, uuid4`) rather
than leaving it inside the function, and add `from ecet.application.messages import
EvaluationMessage, PolicySnapshot` to the existing messages import.

- [x] **Step 13: Run it and watch it fail**

Run: `uv run pytest tests/unit/infrastructure/test_fake_gateway.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.llm'`.

- [x] **Step 14: Write the fake gateway**

Create an empty `src/ecet/infrastructure/llm/__init__.py`, then create
`src/ecet/infrastructure/llm/fake_gateway.py`:

```python
"""The default gateway (`ECET_LLM_PROVIDER=fake`).

Not a test double: this is what `docker compose up` runs, so the demo needs no API key
and no network. The rules are substring matches on the redacted note, chosen so the
committed fixtures land on three different routes — `meets` auto-approves, `unclear`
goes to human review, and a note containing "denied" is refused.
"""

import structlog

from ecet.application.ports.llm_gateway import EvaluationRequest
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code

log = structlog.get_logger(__name__)

#: (needle, decision, confidence, rationale). First match wins.
RULES: tuple[tuple[str, Decision, float, str], ...] = (
    (
        "denied",
        Decision.DOES_NOT_MEET,
        0.92,
        "The note records a denial, which contradicts the policy criteria.",
    ),
    (
        "unclear",
        Decision.INSUFFICIENT_EVIDENCE,
        0.4,
        "The note leaves the required criteria undocumented.",
    ),
)

DEFAULT = (
    Decision.MEETS_NECESSITY,
    0.91,
    "The note documents the criteria required by the matched policy.",
)


class FakeLlmGateway:
    MODEL = "fake-deterministic"

    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        text = request.redacted_text.lower()
        decision, confidence, rationale = DEFAULT
        for needle, ruled_decision, ruled_confidence, ruled_rationale in RULES:
            if needle in text:
                decision, confidence, rationale = ruled_decision, ruled_confidence, ruled_rationale
                break

        matched: PolicyId | None = None
        if decision is not Decision.INSUFFICIENT_EVIDENCE and request.policies:
            matched = request.policies[0].id

        log.info(
            "llm.evaluated",
            claim_id=str(request.claim_id),
            provider="fake",
            model=self.MODEL,
            decision=decision.value,
            confidence=confidence,
        )
        return Evaluation(
            decision=decision,
            confidence=confidence,
            matched_policy_id=matched,
            cited_codes=[Icd10Code(code=code) for code in request.found_codes],
            rationale=rationale,
            model=self.MODEL,
            prompt_version=request.prompt_version,
        )
```

- [x] **Step 15: Run the fake-gateway test and watch it pass**

Run: `uv run pytest tests/unit/infrastructure/test_fake_gateway.py tests/unit/application -v`
Expected: PASS, no regressions in the existing application tests.

- [x] **Step 16: Check the layers, the types and the whole default suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green. `lint-imports` matters here — `application/prompts` must not have
pulled anything forbidden in.

- [x] **Step 17: Commit**

```bash
git add src/ecet/application/errors.py src/ecet/application/ports/llm_gateway.py \
  src/ecet/application/prompts src/ecet/infrastructure/llm tests/fakes.py \
  tests/unit/application/test_llm_port.py tests/unit/application/test_prompt_v1.py \
  tests/unit/infrastructure/test_fake_gateway.py
git commit -m "feat(llm): gateway port, prompt v1 and the deterministic fake gateway"
```

---

### Task 2: The webhook port, the client notification payload and UC-08 NotifyClient

**Files:**
- Modify: `src/ecet/application/errors.py`
- Create: `src/ecet/application/ports/webhook_client.py`
- Create: `src/ecet/application/notifications.py`
- Create: `src/ecet/application/use_cases/notify_client.py`
- Modify: `tests/fakes.py`
- Test: `tests/unit/application/test_notify_client.py`

**Interfaces:**
- Consumes: `ecet.domain.claim.Claim`, `ecet.domain.tenant.Tenant`, `ecet.domain.ports.tenant_repository.TenantRepository`, `ecet.application.ports.clock.Clock`, `ecet.domain.evaluation.Decision` (all existing).
- Produces:
  - `ecet.application.errors`: `WebhookError(DomainError)`, `WebhookTransientError(WebhookError)`, `WebhookPermanentError(WebhookError)`.
  - `ecet.application.notifications.ClientNotification` — the exact payload from [UC-08](../../specs/02-use-cases/UC-08-notify-client.md), plus `DecidedBy = Literal["auto", "human"]`.
  - `ecet.application.ports.webhook_client.WebhookClient` Protocol: `async deliver(tenant: Tenant, payload: ClientNotification) -> None`.
  - `ecet.application.use_cases.notify_client.NotifyClient` — `__init__(tenants, webhook, clock)`, `async execute(claim, *, outcome: Decision, confidence: float, decided_by: DecidedBy) -> ClientNotification`.
  - `tests.fakes.FakeWebhookClient` — records `(tenant, payload)` pairs, optional canned error.

- [x] **Step 1: Write the failing UC-08 test**

Create `tests/unit/application/test_notify_client.py`:

```python
"""UC-08. Two things are load-bearing: the payload never carries claim text
(ADR-001), and every attempt is recorded on the claim so an operator can see why a
delivery is stuck."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import FakeTenantRepository, FakeWebhookClient, FixedClock

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import Decision, Evaluation
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
POLICY_ID = PolicyId(uuid4())


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim() -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=ClaimStatus.EVALUATED,
        redacted=RedactedText(
            text="Patient <PERSON> with M54.5 after nine weeks.", redactor="fake"
        ),
        policy_ids=[POLICY_ID],
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            matched_policy_id=POLICY_ID,
            cited_codes=[Icd10Code(code="M54.5")],
            rationale="Conservative therapy documented for nine weeks.",
            evidence_found=["conservative therapy >= 6 weeks"],
            evidence_missing=[],
            model="fake-deterministic",
            prompt_version="v1",
        ),
        created_at=NOW,
        updated_at=NOW,
    )


def build_use_case(webhook: FakeWebhookClient) -> NotifyClient:
    return NotifyClient(FakeTenantRepository([build_tenant()]), webhook, FixedClock(NOW))


async def test_a_successful_delivery_sends_the_full_payload() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    payload = await build_use_case(webhook).execute(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert len(webhook.deliveries) == 1
    tenant, delivered = webhook.deliveries[0]
    assert tenant.id == "tenant-a"
    assert delivered == payload
    assert payload.event == "claim.triaged"
    assert payload.claim_id == claim.id
    assert payload.source_key == KEY
    assert payload.outcome == "MEETS_NECESSITY"
    assert payload.decided_by == "auto"
    assert payload.confidence == 0.91
    assert payload.matched_policy_id == POLICY_ID
    assert payload.cited_codes == ["M54.5"]
    assert payload.decided_at == NOW


async def test_the_payload_carries_no_claim_text() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    await build_use_case(webhook).execute(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    body = webhook.deliveries[0][1].model_dump_json()
    assert "redacted_text" not in body
    assert "Patient <PERSON>" not in body
    assert "dev-hmac-tenant-a" not in body


async def test_a_successful_delivery_records_the_attempt_and_clears_the_error() -> None:
    claim = build_claim()
    claim.notification_attempts = 2
    claim.last_notify_error = "500"

    await build_use_case(FakeWebhookClient()).execute(
        claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
    )

    assert claim.notification_attempts == 3
    assert claim.last_notify_error is None


@pytest.mark.parametrize(
    "error", [WebhookPermanentError("400"), WebhookTransientError("3 attempts failed")]
)
async def test_a_failed_delivery_records_the_error_and_re_raises(error: Exception) -> None:
    webhook = FakeWebhookClient(error=error)
    claim = build_claim()

    with pytest.raises(type(error)):
        await build_use_case(webhook).execute(
            claim, outcome=Decision.MEETS_NECESSITY, confidence=0.91, decided_by="auto"
        )

    assert claim.notification_attempts == 1
    assert claim.last_notify_error is not None
    assert type(error).__name__ in claim.last_notify_error


async def test_a_human_decision_reports_decided_by_human() -> None:
    webhook = FakeWebhookClient()
    claim = build_claim()

    payload = await build_use_case(webhook).execute(
        claim, outcome=Decision.DOES_NOT_MEET, confidence=1.0, decided_by="human"
    )

    assert payload.decided_by == "human"
    assert payload.outcome == "DOES_NOT_MEET"
    assert payload.confidence == 1.0


async def test_a_claim_with_no_evaluation_still_notifies() -> None:
    # UC-09c (Phase 5) resolves a claim that failed evaluation, so `evaluation` may be
    # None at delivery time. The payload degrades rather than crashing.
    webhook = FakeWebhookClient()
    claim = build_claim()
    claim.evaluation = None

    payload = await build_use_case(webhook).execute(
        claim, outcome=Decision.DOES_NOT_MEET, confidence=1.0, decided_by="human"
    )

    assert payload.matched_policy_id is None
    assert payload.cited_codes == []
    assert payload.rationale == ""
    assert isinstance(payload, ClientNotification)
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_notify_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.notifications'`.

- [x] **Step 3: Add the webhook errors**

Append to `src/ecet/application/errors.py`, and add the three names to `__all__`:

```python
class WebhookError(DomainError):
    """Base for every `WebhookClient` failure."""


class WebhookTransientError(WebhookError):
    """The tenant's endpoint was unreachable or answered 408/429/5xx on every attempt.
    The claim goes `NOTIFY_FAILED`; an operator retries it (Phase 5)."""


class WebhookPermanentError(WebhookError):
    """The endpoint answered a 4xx that a retry would repeat verbatim."""
```

- [x] **Step 4: Write the notification payload**

Create `src/ecet/application/notifications.py`:

```python
"""The outbound contract: what a tenant's system receives when a claim is decided.

No claim text, deliberately (UC-08): the client already owns the document, so sending
its de-identified body back would put clinical content on a wire that does not need
it. The signature and delivery headers are the adapter's job, not this model's.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ecet.domain.ids import ClaimId, PolicyId, TenantIdField

__all__ = ["ClientNotification", "DecidedBy", "Outcome"]

DecidedBy = Literal["auto", "human"]
Outcome = Literal["MEETS_NECESSITY", "DOES_NOT_MEET", "INSUFFICIENT_EVIDENCE"]


class ClientNotification(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: Literal["claim.triaged"] = "claim.triaged"
    claim_id: ClaimId
    tenant_id: TenantIdField
    source_key: str
    outcome: Outcome
    confidence: float = Field(ge=0.0, le=1.0)
    decided_by: DecidedBy
    matched_policy_id: PolicyId | None = None
    cited_codes: list[str] = Field(default_factory=list)
    rationale: str = ""
    evidence_missing: list[str] = Field(default_factory=list)
    decided_at: datetime
```

- [x] **Step 5: Write the webhook port**

Create `src/ecet/application/ports/webhook_client.py`:

```python
"""Delivery of a decided claim to the tenant's system.

The port takes the whole `Tenant` rather than a URL and a secret, because signing is
the adapter's responsibility and splitting the tenant apart at the call site is how a
secret ends up in a log line or a use-case signature.
"""

from typing import Protocol, runtime_checkable

from ecet.application.notifications import ClientNotification
from ecet.domain.tenant import Tenant


@runtime_checkable
class WebhookClient(Protocol):
    async def deliver(self, tenant: Tenant, payload: ClientNotification) -> None:
        """Raises `WebhookPermanentError` on a non-retryable 4xx and
        `WebhookTransientError` once the retry budget is exhausted."""
        ...
```

- [x] **Step 6: Write UC-08**

Create `src/ecet/application/use_cases/notify_client.py`:

```python
"""UC-08 NotifyClient.

One `execute` is one delivery attempt from the claim's point of view — the adapter's
internal retries are invisible here, so `notification_attempts` counts the times the
system tried to tell the client, not the number of HTTP requests. The claim is
mutated but not saved: the caller (UC-07, and UC-09c in Phase 5) owns the unit of
work and decides what the failure means for the claim's status.
"""

import structlog

from ecet.application.errors import WebhookError
from ecet.application.notifications import ClientNotification, DecidedBy, Outcome
from ecet.application.ports.clock import Clock
from ecet.application.ports.webhook_client import WebhookClient
from ecet.domain.claim import Claim
from ecet.domain.evaluation import Decision
from ecet.domain.ports.tenant_repository import TenantRepository

log = structlog.get_logger(__name__)


class NotifyClient:
    def __init__(
        self, tenants: TenantRepository, webhook: WebhookClient, clock: Clock
    ) -> None:
        self._tenants = tenants
        self._webhook = webhook
        self._clock = clock

    async def execute(
        self,
        claim: Claim,
        *,
        outcome: Decision,
        confidence: float,
        decided_by: DecidedBy,
    ) -> ClientNotification:
        tenant = await self._tenants.get(claim.tenant_id)
        evaluation = claim.evaluation
        payload = ClientNotification(
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            source_key=claim.source.key,
            # `Decision` is a StrEnum, so its value is the literal the payload declares.
            outcome=cast_outcome(outcome),
            confidence=confidence,
            decided_by=decided_by,
            matched_policy_id=evaluation.matched_policy_id if evaluation else None,
            cited_codes=[code.code for code in evaluation.cited_codes] if evaluation else [],
            rationale=evaluation.rationale if evaluation else "",
            evidence_missing=list(evaluation.evidence_missing) if evaluation else [],
            decided_at=self._clock.now(),
        )

        claim.notification_attempts += 1
        try:
            await self._webhook.deliver(tenant, payload)
        except WebhookError as error:
            # The detail goes on the claim for an operator to read; `failure_reason`
            # stays a short token, set by the caller.
            claim.last_notify_error = f"{type(error).__name__}: {error}"
            log.warning(
                "notify.failed",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                attempts=claim.notification_attempts,
                error=type(error).__name__,
            )
            raise
        claim.last_notify_error = None
        log.info(
            "notify.delivered",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            outcome=payload.outcome,
            decided_by=decided_by,
            attempts=claim.notification_attempts,
        )
        return payload


def cast_outcome(decision: Decision) -> Outcome:
    """`Decision` and `Outcome` list the same three names; this is the one place the
    static types are joined, so a new `Decision` member fails here rather than in a
    payload."""
    outcome: Outcome
    match decision:
        case Decision.MEETS_NECESSITY:
            outcome = "MEETS_NECESSITY"
        case Decision.DOES_NOT_MEET:
            outcome = "DOES_NOT_MEET"
        case Decision.INSUFFICIENT_EVIDENCE:
            outcome = "INSUFFICIENT_EVIDENCE"
    return outcome
```

- [x] **Step 7: Add `FakeWebhookClient` to `tests/fakes.py`**

Append:

```python
class FakeWebhookClient:
    """Records `(tenant, payload)` per delivery. `error` makes every delivery fail,
    which is how the `NOTIFY_FAILED` branch is exercised."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.deliveries: list[tuple[Tenant, ClientNotification]] = []
        self.error = error

    async def deliver(self, tenant: Tenant, payload: ClientNotification) -> None:
        if self.error is not None:
            raise self.error
        self.deliveries.append((tenant, payload))
```

with `from ecet.application.notifications import ClientNotification` added to the imports.

- [x] **Step 8: Run it and watch it pass**

Run: `uv run pytest tests/unit/application/test_notify_client.py -v`
Expected: PASS (7 tests including both parametrised errors).

- [x] **Step 9: Check types and layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest
```
Expected: all green. `mypy --strict` is the real check on `cast_outcome`: a `match`
without a fallthrough over a StrEnum is only exhaustive if every member is listed.

- [x] **Step 10: Commit**

```bash
git add src/ecet/application/errors.py src/ecet/application/notifications.py \
  src/ecet/application/ports/webhook_client.py \
  src/ecet/application/use_cases/notify_client.py tests/fakes.py \
  tests/unit/application/test_notify_client.py
git commit -m "feat(notify): UC-08 NotifyClient, the client notification payload and the webhook port"
```

---

### Task 3: UC-07 RouteDecision

**Files:**
- Create: `src/ecet/application/use_cases/route_decision.py`
- Test: `tests/unit/application/test_route_decision.py`

**Interfaces:**
- Consumes: `ecet.domain.evaluation.triage`, `Route`, `ReviewReason`, `Verdict`; `ecet.application.use_cases.notify_client.NotifyClient` (Task 2); `ecet.application.use_cases.human_review.RequestHumanReview` (Phase 3); `ecet.application.ports.unit_of_work.UnitOfWork` (Phase 2).
- Produces: `ecet.application.use_cases.route_decision.RouteDecision` — `__init__(*, uow: UnitOfWork, webhook: WebhookClient, clock: Clock, threshold: float)`, `async execute(claim: Claim) -> None`. It saves the claim through `uow.claims.save`; it does **not** commit.

- [x] **Step 1: Write the failing UC-07 test**

Create `tests/unit/application/test_route_decision.py`:

```python
"""UC-07. The confidence gate (ADR-003) and the two ways a decided claim can end:
delivered, or waiting for a human."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import FakeUnitOfWork, FakeWebhookClient, FixedClock

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.use_cases.route_decision import RouteDecision
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
THRESHOLD = 0.85


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_evaluation(
    *, confidence: float, decision: Decision = Decision.MEETS_NECESSITY
) -> Evaluation:
    return Evaluation(
        decision=decision,
        confidence=confidence,
        matched_policy_id=PolicyId(uuid4()),
        rationale="Conservative therapy documented.",
        model="fake-deterministic",
        prompt_version="v1",
    )


def build_claim(*, confidence: float, verdict: Verdict = Verdict.PASS) -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=ClaimStatus.EVALUATED,
        redacted=RedactedText(text="Patient <PERSON> with M54.5.", redactor="fake"),
        deterministic=DeterministicResult(
            verdict=verdict,
            checks=[CheckOutcome(name="icd10_present", passed=True, detail="1 code")],
        ),
        evaluation=build_evaluation(confidence=confidence),
        created_at=NOW,
        updated_at=NOW,
    )


async def build_case(
    claim: Claim, webhook: FakeWebhookClient
) -> tuple[RouteDecision, FakeUnitOfWork]:
    uow = FakeUnitOfWork(tenants=[build_tenant()])
    await uow.claims.add(claim)
    route = RouteDecision(
        uow=uow, webhook=webhook, clock=FixedClock(NOW), threshold=THRESHOLD
    )
    return route, uow


async def test_a_confident_decision_notifies_and_approves() -> None:
    claim = build_claim(confidence=0.90)
    webhook = FakeWebhookClient()
    route, uow = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.APPROVED_AUTO
    assert len(webhook.deliveries) == 1
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_the_threshold_boundary_is_inclusive() -> None:
    claim = build_claim(confidence=THRESHOLD)
    webhook = FakeWebhookClient()
    route, _ = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.APPROVED_AUTO


async def test_a_low_confidence_decision_opens_a_review_and_does_not_notify() -> None:
    claim = build_claim(confidence=0.80)
    webhook = FakeWebhookClient()
    route, uow = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.REVIEW_PENDING
    assert webhook.deliveries == []
    tasks = list(uow.review_tasks.tasks.values())
    assert len(tasks) == 1
    assert tasks[0].reason is ReviewReason.LOW_CONFIDENCE
    assert tasks[0].claim_id == claim.id


async def test_an_uncertain_deterministic_verdict_names_the_combined_reason() -> None:
    claim = build_claim(confidence=0.80, verdict=Verdict.UNCERTAIN)
    route, uow = await build_case(claim, FakeWebhookClient())

    await route.execute(claim)

    tasks = list(uow.review_tasks.tasks.values())
    assert tasks[0].reason is ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW


async def test_insufficient_evidence_never_auto_notifies_however_confident() -> None:
    claim = build_claim(confidence=0.99)
    claim.evaluation = build_evaluation(
        confidence=0.99, decision=Decision.INSUFFICIENT_EVIDENCE
    )
    webhook = FakeWebhookClient()
    route, _ = await build_case(claim, webhook)

    await route.execute(claim)

    assert claim.status is ClaimStatus.REVIEW_PENDING
    assert webhook.deliveries == []


async def test_a_permanent_delivery_failure_sets_notify_failed_with_a_token() -> None:
    claim = build_claim(confidence=0.90)
    route, uow = await build_case(claim, FakeWebhookClient(error=WebhookPermanentError("400")))

    await route.execute(claim)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_rejected"
    assert claim.last_notify_error is not None
    assert uow.review_tasks.tasks == {}
    assert uow.claims.saved == [claim.id]


async def test_a_transient_delivery_failure_sets_notify_failed_with_its_own_token() -> None:
    claim = build_claim(confidence=0.90)
    route, _ = await build_case(claim, FakeWebhookClient(error=WebhookTransientError("timeout")))

    await route.execute(claim)

    assert claim.status is ClaimStatus.NOTIFY_FAILED
    assert claim.failure_reason == "webhook_unreachable"


async def test_routing_a_claim_with_no_evaluation_is_a_programming_error() -> None:
    claim = build_claim(confidence=0.90)
    claim.evaluation = None
    route, _ = await build_case(claim, FakeWebhookClient())

    with pytest.raises(ValueError, match="no evaluation"):
        await route.execute(claim)
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_route_decision.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.route_decision'`.

- [x] **Step 3: Write UC-07**

Create `src/ecet/application/use_cases/route_decision.py`:

```python
"""UC-07 RouteDecision (ADR-003).

The gate is one pure function — `triage(evaluation, threshold)` in the domain — and
everything here is the consequence: notify and approve, or open a review task and
wait. The threshold is injected, never read from config inside the domain.

`NotifyClient` and `RequestHumanReview` are built here rather than injected, for the
same reason UC-01 builds `AttachTenantPolicies`: they depend on repositories that
belong to one unit of work, and a unit of work outlives one message, not a process.

A delivery failure does **not** open a review task. Nobody needs to read the note
again — the decision stands, only the delivery failed — so the claim parks in
`NOTIFY_FAILED` for an operator retry (Phase 5).
"""

import structlog

from ecet.application.errors import WebhookError, WebhookPermanentError
from ecet.application.ports.clock import Clock
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.notify_client import NotifyClient
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.evaluation import ReviewReason, Route, Verdict, triage

log = structlog.get_logger(__name__)

#: Short tokens, matching the `EXTRACTION_FAILED` convention. The exception detail
#: lives on `Claim.last_notify_error`, not here.
REJECTED = "webhook_rejected"
UNREACHABLE = "webhook_unreachable"


class RouteDecision:
    def __init__(
        self,
        *,
        uow: UnitOfWork,
        webhook: WebhookClient,
        clock: Clock,
        threshold: float,
    ) -> None:
        self._uow = uow
        self._clock = clock
        self._threshold = threshold
        self._notify = NotifyClient(uow.tenants, webhook, clock)
        self._request_review = RequestHumanReview(uow.review_tasks, clock)

    async def execute(self, claim: Claim) -> None:
        evaluation = claim.evaluation
        if evaluation is None:
            raise ValueError(f"claim {claim.id} has no evaluation to route")

        route = triage(evaluation, self._threshold)
        log.info(
            "claim.routed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            route=route.value,
            decision=evaluation.decision.value,
            confidence=evaluation.confidence,
        )

        if route is Route.HUMAN_REVIEW:
            await self._request_review.execute(claim, self._reason_for(claim))
            self._advance(claim, ClaimStatus.REVIEW_PENDING)
        else:
            try:
                await self._notify.execute(
                    claim,
                    outcome=evaluation.decision,
                    confidence=evaluation.confidence,
                    decided_by="auto",
                )
            except WebhookError as error:
                reason = REJECTED if isinstance(error, WebhookPermanentError) else UNREACHABLE
                self._fail(claim, reason)
            else:
                self._advance(claim, ClaimStatus.APPROVED_AUTO)

        await self._uow.claims.save(claim)

    @staticmethod
    def _reason_for(claim: Claim) -> ReviewReason:
        deterministic = claim.deterministic
        if deterministic is not None and deterministic.verdict is Verdict.UNCERTAIN:
            return ReviewReason.DETERMINISTIC_UNCERTAIN_LLM_LOW
        return ReviewReason.LOW_CONFIDENCE

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
        )

    def _fail(self, claim: Claim, reason: str) -> None:
        claim.transition(ClaimStatus.NOTIFY_FAILED, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=ClaimStatus.NOTIFY_FAILED.value,
            reason=reason,
        )
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/application/test_route_decision.py -v`
Expected: PASS (8 tests).

- [x] **Step 5: Check types, layers and the whole default suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/route_decision.py \
  tests/unit/application/test_route_decision.py
git commit -m "feat(routing): UC-07 RouteDecision with the confidence gate and the notify-failure path"
```

---

### Task 4: UC-06 EvaluateClaim

**Files:**
- Create: `src/ecet/application/use_cases/evaluate_claim.py`
- Test: `tests/unit/application/test_evaluate_claim.py`

**Interfaces:**
- Consumes: `ecet.application.messages.EvaluationMessage` (Phase 3, frozen), `ecet.application.ports.llm_gateway.LLMGateway` / `EvaluationRequest` (Task 1), `RouteDecision` (Task 3), `RequestHumanReview` (Phase 3), `ecet.domain.ports.icd10_repository.Icd10CodeRepository` (Phase 2).
- Produces: `ecet.application.use_cases.evaluate_claim.EvaluateClaim` — `__init__(*, uow_factory: Callable[[], UnitOfWork], llm: LLMGateway, webhook: WebhookClient, clock: Clock, threshold: float, prompt_version: str)`, `async execute(message: EvaluationMessage) -> None`. Raising means "requeue"; returning means "ack".

- [x] **Step 1: Write the failing UC-06 test**

Create `tests/unit/application/test_evaluate_claim.py`:

```python
"""UC-06. The worker's whole contract lives in this class: what it does with a good
answer, what it does with a bad one, and — the part at-least-once delivery makes
non-negotiable — what it does with a message it has already handled."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tests.fakes import (
    FakeLLMGateway,
    FakeUnitOfWork,
    FakeWebhookClient,
    FixedClock,
)

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.application.use_cases.evaluate_claim import EvaluateClaim
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code
from ecet.domain.tenant import Tenant

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/note-1.pdf"
POLICY_ID = PolicyId(uuid4())
CATALOGUE = [Icd10Code(code="M54.5"), Icd10Code(code="T12")]


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_claim(*, status: ClaimStatus = ClaimStatus.QUEUED) -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source=SourceObject(bucket="claims", key=KEY, etag="etag-1", size=2048),
        status=status,
        redacted=RedactedText(text="Patient <PERSON> with M54.5.", redactor="fake"),
        policy_ids=[POLICY_ID],
        deterministic=DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="icd10_present", passed=True, detail="1 code")],
        ),
        created_at=NOW,
        updated_at=NOW,
    )


def build_message(claim: Claim, *, found_codes: list[str] | None = None) -> EvaluationMessage:
    return EvaluationMessage(
        message_id=uuid4(),
        claim_id=claim.id,
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        entity_counts={"PERSON": 1},
        policies=[
            PolicySnapshot(
                id=POLICY_ID,
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M54.5"],
                excluded_codes=["Z00.00"],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=["conservative therapy >= 6 weeks"],
            )
        ],
        deterministic_verdict="PASS",
        found_codes=["M54.5"] if found_codes is None else found_codes,
        enqueued_at=NOW,
    )


def build_case(
    uow: FakeUnitOfWork, llm: FakeLLMGateway, webhook: FakeWebhookClient
) -> EvaluateClaim:
    return EvaluateClaim(
        uow_factory=lambda: uow,
        llm=llm,
        webhook=webhook,
        clock=FixedClock(NOW),
        threshold=0.85,
        prompt_version="v1",
    )


async def build_world(
    *,
    claim: Claim | None = None,
    llm: FakeLLMGateway | None = None,
    webhook: FakeWebhookClient | None = None,
) -> tuple[EvaluateClaim, FakeUnitOfWork, Claim, FakeLLMGateway, FakeWebhookClient]:
    claim = claim if claim is not None else build_claim()
    llm = llm if llm is not None else FakeLLMGateway()
    webhook = webhook if webhook is not None else FakeWebhookClient()
    uow = FakeUnitOfWork(tenants=[build_tenant()], known_codes=CATALOGUE)
    await uow.claims.add(claim)
    return build_case(uow, llm, webhook), uow, claim, llm, webhook


async def test_a_confident_evaluation_reaches_approved_auto() -> None:
    case, uow, claim, llm, webhook = await build_world()

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.APPROVED_AUTO
    assert stored.evaluation is not None
    assert stored.evaluation.confidence == 0.91
    assert len(llm.requests) == 1
    assert len(webhook.deliveries) == 1
    assert uow.commits >= 1


async def test_the_request_carries_the_message_not_a_fresh_read() -> None:
    case, _, claim, llm, _ = await build_world()

    await case.execute(build_message(claim))

    request = llm.requests[0]
    assert request.claim_id == claim.id
    assert request.redacted_text == "Patient <PERSON> with M54.5."
    assert [policy.id for policy in request.policies] == [POLICY_ID]
    assert request.prompt_version == "v1"


async def test_found_codes_are_filtered_against_the_seeded_catalogue() -> None:
    # Phase 1 carry-over: the regex matches "B12" in "Vitamin B12". The catalogue is
    # what stops it reaching the prompt as a diagnosis.
    case, _, claim, llm, _ = await build_world()

    await case.execute(build_message(claim, found_codes=["M54.5", "B12", "T12"]))

    assert llm.requests[0].found_codes == ["M54.5", "T12"]


async def test_a_low_confidence_evaluation_reaches_review_pending() -> None:
    llm = FakeLLMGateway(
        evaluation=Evaluation(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            confidence=0.4,
            model="fake",
            prompt_version="v1",
        )
    )
    case, uow, claim, _, webhook = await build_world(llm=llm)

    await case.execute(build_message(claim))

    assert uow.claims.claims[claim.id].status is ClaimStatus.REVIEW_PENDING
    assert webhook.deliveries == []
    assert len(uow.review_tasks.tasks) == 1


async def test_a_transient_llm_error_propagates_and_leaves_the_claim_queued() -> None:
    llm = FakeLLMGateway(error=LLMTransientError("429"))
    case, uow, claim, _, _ = await build_world(llm=llm)

    with pytest.raises(LLMTransientError):
        await case.execute(build_message(claim))

    assert uow.claims.claims[claim.id].status is ClaimStatus.QUEUED
    assert uow.claims.saved == []


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (LLMInvalidOutput("no tool_calls"), "llm_invalid_output"),
        (LLMPermanentError("401"), "llm_permanent_error"),
    ],
)
async def test_a_permanent_llm_failure_fails_the_claim_and_opens_a_review(
    error: Exception, reason: str
) -> None:
    case, uow, claim, _, webhook = await build_world(llm=FakeLLMGateway(error=error))

    await case.execute(build_message(claim))

    stored = uow.claims.claims[claim.id]
    assert stored.status is ClaimStatus.EVALUATION_FAILED
    assert stored.failure_reason == reason
    tasks = list(uow.review_tasks.tasks.values())
    assert len(tasks) == 1
    assert tasks[0].reason is ReviewReason.EVALUATION_FAILED
    assert webhook.deliveries == []


async def test_a_redelivered_message_for_an_evaluated_claim_calls_no_gateway() -> None:
    claim = build_claim(status=ClaimStatus.EVALUATED)
    case, uow, claim, llm, webhook = await build_world(claim=claim)

    await case.execute(build_message(claim))

    assert llm.requests == []
    assert webhook.deliveries == []
    assert uow.claims.saved == []


async def test_a_message_for_an_unknown_claim_is_swallowed() -> None:
    case, _, claim, llm, _ = await build_world()
    orphan = build_message(claim).model_copy(update={"claim_id": ClaimId(uuid4())})

    await case.execute(orphan)  # no exception: the consumer must ack a poison message

    assert llm.requests == []


async def test_a_tenant_mismatch_between_message_and_claim_is_refused() -> None:
    case, uow, claim, llm, _ = await build_world()
    forged = build_message(claim).model_copy(update={"tenant_id": TenantId("tenant-b")})

    await case.execute(forged)

    assert llm.requests == []
    assert uow.claims.claims[claim.id].status is ClaimStatus.QUEUED
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/application/test_evaluate_claim.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.evaluate_claim'`.

- [x] **Step 3: Write UC-06**

Create `src/ecet/application/use_cases/evaluate_claim.py`:

```python
"""UC-06 EvaluateClaim — the worker-side orchestrator.

The contract with the consumer is expressed by control flow, not by a return value:
**raising means requeue, returning means ack.** Every branch that a retry could not
improve — an unknown claim, a claim already past `QUEUED`, a vendor answer that will
be identical next time — returns. Only a transient vendor failure escapes.

The message is the source of truth for what the model sees. Policies travel as a
snapshot taken at enqueue time (UC-05), so a policy edited while the message sat in
the queue cannot silently change the basis of a decision. The claim row is still read,
because the status check, the tenant check and the write all need it.

The one thing re-read from the database is the ICD-10 catalogue: `domain/rules.py`
extracts codes by pattern, which false-positives on clinical prose ("Vitamin B12"),
and the seeded catalogue is what keeps those out of the prompt.
"""

from collections.abc import Callable

import structlog

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.messages import EvaluationMessage
from ecet.application.ports.clock import Clock
from ecet.application.ports.llm_gateway import EvaluationRequest, LLMGateway
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.ports.webhook_client import WebhookClient
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.route_decision import RouteDecision
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound
from ecet.domain.evaluation import ReviewReason

log = structlog.get_logger(__name__)

INVALID_OUTPUT = "llm_invalid_output"
PERMANENT_ERROR = "llm_permanent_error"


class EvaluateClaim:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        llm: LLMGateway,
        webhook: WebhookClient,
        clock: Clock,
        threshold: float,
        prompt_version: str,
    ) -> None:
        self._uow_factory = uow_factory
        self._llm = llm
        self._webhook = webhook
        self._clock = clock
        self._threshold = threshold
        self._prompt_version = prompt_version

    async def execute(self, message: EvaluationMessage) -> None:
        async with self._uow_factory() as uow:
            claim = await self._load(uow, message)
            if claim is None:
                return

            request = await self._build_request(uow, claim, message)
            try:
                evaluation = await self._llm.evaluate(request)
            except LLMTransientError:
                # The only escape hatch: the consumer nacks with requeue and RabbitMQ's
                # x-delivery-limit eventually sends it to the DLQ. Nothing is saved, so
                # the claim is still QUEUED for the redelivery.
                raise
            except (LLMInvalidOutput, LLMPermanentError) as error:
                await self._fail(uow, claim, error)
                return

            claim.evaluation = evaluation
            claim.transition(ClaimStatus.EVALUATED, now=self._clock.now())
            log.info(
                "claim.transition",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                to=ClaimStatus.EVALUATED.value,
            )
            await uow.claims.save(claim)

            route = RouteDecision(
                uow=uow,
                webhook=self._webhook,
                clock=self._clock,
                threshold=self._threshold,
            )
            await route.execute(claim)
            await uow.commit()

    async def _load(self, uow: UnitOfWork, message: EvaluationMessage) -> Claim | None:
        """The three ack-and-forget cases, in the order they can be checked cheaply."""
        try:
            claim = await uow.claims.get(message.claim_id)
        except ClaimNotFound:
            log.error("evaluate.claim_missing", claim_id=str(message.claim_id))
            return None

        if claim.tenant_id != message.tenant_id:
            # Nothing legitimate produces this. Refusing it keeps a forged or corrupted
            # message from evaluating one tenant's claim against another's policies.
            log.error(
                "evaluate.tenant_mismatch",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                message_tenant_id=str(message.tenant_id),
            )
            return None

        if claim.status is not ClaimStatus.QUEUED:
            log.info(
                "evaluate.skipped_duplicate",
                claim_id=str(claim.id),
                tenant_id=str(claim.tenant_id),
                status=claim.status.value,
            )
            return None
        return claim

    async def _build_request(
        self, uow: UnitOfWork, claim: Claim, message: EvaluationMessage
    ) -> EvaluationRequest:
        known = {code.code for code in await uow.icd10_codes.known_codes()}
        return EvaluationRequest(
            claim_id=claim.id,
            redacted_text=message.redacted_text,
            policies=list(message.policies),
            found_codes=[code for code in message.found_codes if code in known],
            prompt_version=self._prompt_version,
        )

    async def _fail(self, uow: UnitOfWork, claim: Claim, error: Exception) -> None:
        reason = INVALID_OUTPUT if isinstance(error, LLMInvalidOutput) else PERMANENT_ERROR
        claim.transition(ClaimStatus.EVALUATION_FAILED, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=ClaimStatus.EVALUATION_FAILED.value,
            reason=reason,
            error=type(error).__name__,
        )
        await RequestHumanReview(uow.review_tasks, self._clock).execute(
            claim, ReviewReason.EVALUATION_FAILED
        )
        await uow.claims.save(claim)
        await uow.commit()
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/application/test_evaluate_claim.py -v`
Expected: PASS (10 tests including both parametrised failures).

- [x] **Step 5: Check types, layers and the whole default suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run lint-imports
uv run pytest --cov=ecet --cov-report=term-missing
```
Expected: all green; `src/ecet/application/use_cases/evaluate_claim.py` shows no
uncovered branch other than the `raise` re-raise line's else path.

- [x] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/evaluate_claim.py \
  tests/unit/application/test_evaluate_claim.py
git commit -m "feat(worker): UC-06 EvaluateClaim with duplicate, tenant and vendor-failure handling"
```

---

### Task 5: The OpenAI-compatible gateway adapter and the provider factory

**Files:**
- Modify: `pyproject.toml` (add `openai`, promote `httpx` to a runtime dependency)
- Create: `src/ecet/infrastructure/llm/openai_gateway.py`, `src/ecet/infrastructure/llm/factory.py`
- Test: `tests/unit/infrastructure/test_openai_gateway.py`, `tests/unit/infrastructure/test_llm_factory.py`

**Interfaces:**
- Consumes: `EvaluationRequest`, `EvaluationOutput`, `LLMGateway` (Task 1); `TOOL`, `TOOL_NAME`, `build_messages` (Task 1); `ecet.config.Settings`, `ecet.config.LlmProvider` (Phase 0).
- Produces:
  - `ecet.infrastructure.llm.openai_gateway.OpenAiLlmGateway` — `__init__(*, base_url: str, api_key: str, model: str, timeout_s: int, http_client: httpx.AsyncClient | None = None)`, `async evaluate(request) -> Evaluation`, `async aclose() -> None`.
  - `ecet.infrastructure.llm.openai_gateway.PERMANENT_STATUS: frozenset[int]`.
  - `ecet.infrastructure.llm.factory.build_gateway(settings: Settings) -> LLMGateway`.

- [x] **Step 1: Add the dependencies**

In `pyproject.toml`, add to `[project] dependencies` (keep the list alphabetical where it
already is, otherwise append):

```toml
    "httpx>=0.27,<0.28",
    "openai>=1.60,<2",
```

and **remove** `"httpx>=0.27,<0.28"` from `[dependency-groups] dev` — the webhook adapter
(Task 6) makes it a runtime dependency, and leaving it in both places lets a `--no-dev`
image resolve a different version than the tests ran against.

Then:

```bash
uv sync
uv run python -c "import openai, httpx; print(openai.__version__, httpx.__version__)"
```
Expected: a 1.x openai version and 0.27.x httpx. If `uv` cannot resolve `openai<2`
against the locked `httpx<0.28`, widen to `"openai>=1.60,<3"` and re-run — the adapter
only uses `chat.completions.create`, the exception classes and `usage`, which are
stable across both majors.

- [x] **Step 2: Write the failing adapter test**

Create `tests/unit/infrastructure/test_openai_gateway.py`:

```python
"""The vendor adapter, driven through `httpx.MockTransport` — no network, no key, and
in the default (non-slow) test run.

`respx` would do the same job; the spec names it, but a MockTransport handler is a
plain function and saves a dependency (see deviation 4).
"""

import json
from typing import Any

import httpx
import pytest
from tests.fakes import build_evaluation_request
from tests.pii import assert_no_pii

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.ports.llm_gateway import LLMGateway
from ecet.domain.evaluation import Decision
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway

ARGUMENTS = json.dumps(
    {
        "decision": "MEETS_NECESSITY",
        "confidence": 0.91,
        "cited_codes": ["M54.5"],
        "rationale": "Nine weeks of conservative therapy documented.",
        "evidence_found": ["conservative therapy >= 6 weeks"],
        "evidence_missing": [],
    }
)


def completion_body(
    *, arguments: str = ARGUMENTS, tool_calls: bool = True, usage: bool = True
) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": None}
    if tool_calls:
        message["tool_calls"] = [
            {
                "id": "call_1",
                "type": "function",
                "function": {"name": "submit_evaluation", "arguments": arguments},
            }
        ]
    body: dict[str, Any] = {
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "created": 1,
        "model": "claude-sonnet-5",
        "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls"}],
    }
    if usage:
        body["usage"] = {"prompt_tokens": 900, "completion_tokens": 120, "total_tokens": 1020}
    return body


def build_gateway(
    handler: Any, *, model: str = "claude-sonnet-5"
) -> OpenAiLlmGateway:
    return OpenAiLlmGateway(
        base_url="http://vendor.invalid/v1/",
        api_key="test-key",
        model=model,
        timeout_s=5,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def responder(status: int, body: dict[str, Any] | None = None) -> Any:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=body if body is not None else {"error": "x"})

    return handle


async def test_the_adapter_satisfies_the_port() -> None:
    assert isinstance(build_gateway(responder(200, completion_body())), LLMGateway)


async def test_a_tool_call_becomes_an_evaluation() -> None:
    gateway = build_gateway(responder(200, completion_body()))

    evaluation = await gateway.evaluate(build_evaluation_request())

    assert evaluation.decision is Decision.MEETS_NECESSITY
    assert evaluation.confidence == 0.91
    assert [code.code for code in evaluation.cited_codes] == ["M54.5"]
    assert evaluation.model == "claude-sonnet-5"
    assert evaluation.prompt_version == "v1"
    assert evaluation.input_tokens == 900
    assert evaluation.output_tokens == 120
    assert evaluation.latency_ms >= 0
    await gateway.aclose()


async def test_the_request_forces_the_tool_and_carries_no_pii() -> None:
    seen: list[dict[str, Any]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=completion_body())

    await build_gateway(handle).evaluate(build_evaluation_request())

    sent = seen[0]
    assert sent["model"] == "claude-sonnet-5"
    assert sent["temperature"] == 0
    assert sent["max_tokens"] == 1024
    assert sent["tool_choice"]["function"]["name"] == "submit_evaluation"
    assert sent["tools"][0]["function"]["name"] == "submit_evaluation"
    assert [message["role"] for message in sent["messages"]] == ["system", "user"]
    assert_no_pii(json.dumps(sent))


@pytest.mark.parametrize("status", [429, 500, 502, 503])
async def test_retryable_statuses_are_transient(status: int) -> None:
    with pytest.raises(LLMTransientError):
        await build_gateway(responder(status)).evaluate(build_evaluation_request())


@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_client_errors_are_permanent(status: int) -> None:
    with pytest.raises(LLMPermanentError):
        await build_gateway(responder(status)).evaluate(build_evaluation_request())


async def test_a_connection_failure_is_transient() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(LLMTransientError):
        await build_gateway(handle).evaluate(build_evaluation_request())


async def test_a_response_with_no_tool_call_is_invalid_output() -> None:
    body = completion_body(tool_calls=False)

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(responder(200, body)).evaluate(build_evaluation_request())


async def test_unparseable_arguments_are_invalid_output() -> None:
    body = completion_body(arguments="{not json")

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(responder(200, body)).evaluate(build_evaluation_request())


async def test_arguments_that_break_the_schema_are_invalid_output() -> None:
    body = completion_body(arguments=json.dumps({"decision": "MAYBE", "confidence": 0.9}))

    with pytest.raises(LLMInvalidOutput):
        await build_gateway(responder(200, body)).evaluate(build_evaluation_request())


async def test_a_missing_usage_block_defaults_the_token_counts() -> None:
    # Local servers (Ollama, LM Studio) often omit `usage` entirely.
    body = completion_body(usage=False)

    evaluation = await build_gateway(responder(200, body)).evaluate(build_evaluation_request())

    assert evaluation.input_tokens == 0
    assert evaluation.output_tokens == 0


async def test_a_hallucinated_policy_id_is_discarded() -> None:
    arguments = json.dumps(
        {
            "decision": "MEETS_NECESSITY",
            "confidence": 0.9,
            "matched_policy_id": "11111111-1111-4111-8111-111111111111",
        }
    )

    evaluation = await build_gateway(responder(200, completion_body(arguments=arguments))).evaluate(
        build_evaluation_request()
    )

    assert evaluation.matched_policy_id is None
```

- [x] **Step 3: Run it and watch it fail**

Run: `uv run pytest tests/unit/infrastructure/test_openai_gateway.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.llm.openai_gateway'`.

- [x] **Step 4: Write the adapter**

Create `src/ecet/infrastructure/llm/openai_gateway.py`:

```python
"""One adapter for every OpenAI-compatible server (ADR-004).

There is no vendor branch in here on purpose: Anthropic's compatibility endpoint,
OpenAI, Azure, vLLM, Ollama, OpenRouter and LM Studio all speak
`/v1/chat/completions` with function calling, so the only difference between them is
`ECET_LLM_BASE_URL`, `ECET_LLM_API_KEY` and `ECET_LLM_MODEL`.

`max_retries=0` is deliberate. The SDK will happily retry a 429 for you, inside the
message handler, while the message's own delivery budget ticks — two retry policies
stacked. RabbitMQ owns retries in this system, so the SDK does none.

Nothing here redacts: the input was redacted at the trust boundary (UC-02, ADR-001)
and re-checking it at the wire would be theatre. Nothing here logs the prompt either.
"""

import time
from typing import Any, cast

import httpx
import structlog
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    InternalServerError,
    RateLimitError,
)
from openai.types.chat import ChatCompletionMessageParam
from pydantic import ValidationError

from ecet.application.errors import LLMInvalidOutput, LLMPermanentError, LLMTransientError
from ecet.application.ports.llm_gateway import EvaluationOutput, EvaluationRequest
from ecet.application.prompts.evaluate_v1 import TOOL, TOOL_NAME, build_messages
from ecet.domain.evaluation import Evaluation

log = structlog.get_logger(__name__)

MAX_TOKENS = 1024

#: A retry would send the identical request and get the identical answer.
PERMANENT_STATUS = frozenset({400, 401, 403, 404})


class OpenAiLlmGateway:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_s: int,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._model = model
        self._client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            timeout=float(timeout_s),
            max_retries=0,
            http_client=http_client,
        )

    async def aclose(self) -> None:
        await self._client.close()

    async def evaluate(self, request: EvaluationRequest) -> Evaluation:
        messages = cast(list[ChatCompletionMessageParam], build_messages(request))
        started = time.perf_counter()
        try:
            completion = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                tools=[cast(Any, TOOL)],
                tool_choice=cast(
                    Any, {"type": "function", "function": {"name": TOOL_NAME}}
                ),
                max_tokens=MAX_TOKENS,
                temperature=0,
            )
        except (APITimeoutError, APIConnectionError, RateLimitError, InternalServerError) as error:
            raise LLMTransientError(f"{type(error).__name__}: {error}") from error
        except APIStatusError as error:
            if error.status_code in PERMANENT_STATUS:
                raise LLMPermanentError(f"status {error.status_code}") from error
            raise LLMTransientError(f"status {error.status_code}") from error
        latency_ms = int((time.perf_counter() - started) * 1000)

        output = _parse(completion)
        usage = completion.usage
        evaluation = output.to_evaluation(
            model=completion.model or self._model,
            prompt_version=request.prompt_version,
            latency_ms=latency_ms,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            known_policy_ids=[policy.id for policy in request.policies],
        )
        log.info(
            "llm.evaluated",
            claim_id=str(request.claim_id),
            provider="openai",
            model=evaluation.model,
            latency_ms=evaluation.latency_ms,
            input_tokens=evaluation.input_tokens,
            output_tokens=evaluation.output_tokens,
            decision=evaluation.decision.value,
            confidence=evaluation.confidence,
        )
        return evaluation


def _parse(completion: Any) -> EvaluationOutput:
    """Every "the server answered, but not usefully" case collapses to one error type,
    because UC-06 treats them identically: the claim fails and a human looks at it."""
    choices = completion.choices or []
    tool_calls = getattr(choices[0].message, "tool_calls", None) if choices else None
    if not tool_calls:
        raise LLMInvalidOutput("the response contained no tool call")
    try:
        return EvaluationOutput.model_validate_json(tool_calls[0].function.arguments)
    except ValidationError as error:
        # The arguments may echo note content; only the error class is logged.
        raise LLMInvalidOutput(f"tool arguments rejected: {error.error_count()} problem(s)") from error
```

- [x] **Step 5: Run it and watch it pass**

Run: `uv run pytest tests/unit/infrastructure/test_openai_gateway.py -v`
Expected: PASS (all cases, including the eight parametrised status codes).

- [x] **Step 6: Write the failing factory test**

Create `tests/unit/infrastructure/test_llm_factory.py`:

```python
"""One switch, so nothing else in the codebase branches on the provider."""

import pytest
from pydantic import ValidationError

from ecet.config import LlmProvider, Settings
from ecet.infrastructure.llm.factory import build_gateway
from ecet.infrastructure.llm.fake_gateway import FakeLlmGateway
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway


def build_settings(**overrides: object) -> Settings:
    fields: dict[str, object] = {
        "_env_file": None,
        "s3_event_token": "t",
        "api_key": "k",
    }
    fields.update(overrides)
    return Settings(**fields)  # type: ignore[arg-type]


def test_the_default_provider_is_the_fake_one() -> None:
    assert isinstance(build_gateway(build_settings()), FakeLlmGateway)


def test_the_openai_provider_builds_the_vendor_adapter() -> None:
    settings = build_settings(llm_provider=LlmProvider.OPENAI, llm_api_key="sk-test")

    assert isinstance(build_gateway(settings), OpenAiLlmGateway)


def test_the_openai_provider_without_a_key_fails_at_settings_time() -> None:
    # The guard lives in `Settings`, so a misconfigured worker exits at startup with a
    # field name rather than at the first message with a 401.
    with pytest.raises(ValidationError):
        build_settings(llm_provider=LlmProvider.OPENAI)
```

- [x] **Step 7: Run it and watch it fail**

Run: `uv run pytest tests/unit/infrastructure/test_llm_factory.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.llm.factory'`.

- [x] **Step 8: Write the factory**

Create `src/ecet/infrastructure/llm/factory.py`:

```python
"""Provider selection. The only place in the codebase that reads `llm_provider`."""

from ecet.application.ports.llm_gateway import LLMGateway
from ecet.config import LlmProvider, Settings
from ecet.infrastructure.llm.fake_gateway import FakeLlmGateway
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway


def build_gateway(settings: Settings) -> LLMGateway:
    match settings.llm_provider:
        case LlmProvider.FAKE:
            return FakeLlmGateway()
        case LlmProvider.OPENAI:
            key = settings.llm_api_key
            if key is None:  # pragma: no cover - Settings refuses this combination
                raise ValueError("ECET_LLM_API_KEY is required when ECET_LLM_PROVIDER=openai")
            return OpenAiLlmGateway(
                base_url=settings.llm_base_url,
                api_key=key.get_secret_value(),
                model=settings.llm_model,
                timeout_s=settings.llm_timeout_s,
            )
```

- [x] **Step 9: Run everything and check the layers**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green. `lint-imports` is the one that matters: `openai` and `httpx` must
appear only under `ecet.infrastructure`.

- [x] **Step 10: Commit**

```bash
git add pyproject.toml uv.lock src/ecet/infrastructure/llm/openai_gateway.py \
  src/ecet/infrastructure/llm/factory.py tests/unit/infrastructure/test_openai_gateway.py \
  tests/unit/infrastructure/test_llm_factory.py
git commit -m "feat(llm): OpenAI-compatible gateway adapter and the provider factory"
```

---

### Task 6: The httpx webhook client

**Files:**
- Create: `src/ecet/infrastructure/webhook/__init__.py`, `src/ecet/infrastructure/webhook/httpx_client.py`
- Test: `tests/unit/infrastructure/test_httpx_webhook_client.py`

**Interfaces:**
- Consumes: `ClientNotification` and `WebhookClient` (Task 2), `ecet.domain.tenant.Tenant`.
- Produces: `ecet.infrastructure.webhook.httpx_client.HttpxWebhookClient` — `__init__(*, timeout_s: int, max_attempts: int = 3, backoff_seconds: Sequence[float] = (1.0, 4.0, 16.0), http_client: httpx.AsyncClient | None = None)`, `async deliver(tenant, payload) -> None`, `async aclose() -> None`; plus `sign(secret: bytes, timestamp: str, body: bytes) -> str` and `RETRY_STATUS: frozenset[int]`.

- [x] **Step 1: Write the failing adapter test**

Create `tests/unit/infrastructure/test_httpx_webhook_client.py`:

```python
"""Delivery, signing and the retry policy. `backoff_seconds=(0, 0, 0)` keeps the retry
test instant — the schedule is a constructor argument precisely so a test never has to
sleep 21 seconds to prove three attempts happened."""

import hashlib
import hmac
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.application.ports.webhook_client import WebhookClient
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant
from ecet.infrastructure.webhook.httpx_client import HttpxWebhookClient

SECRET = "dev-hmac-tenant-a"
NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": SECRET,
        }
    )


def build_payload() -> ClientNotification:
    return ClientNotification(
        claim_id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source_key="tenants/tenant-a/claims/note-1.pdf",
        outcome="MEETS_NECESSITY",
        confidence=0.91,
        decided_by="auto",
        cited_codes=["M54.5"],
        rationale="Conservative therapy documented.",
        decided_at=NOW,
    )


def build_client(handler: Any, **overrides: Any) -> HttpxWebhookClient:
    return HttpxWebhookClient(
        timeout_s=5,
        max_attempts=3,
        backoff_seconds=(0.0, 0.0, 0.0),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        **overrides,
    )


async def test_the_adapter_satisfies_the_port() -> None:
    assert isinstance(build_client(lambda request: httpx.Response(200)), WebhookClient)


async def test_a_2xx_is_one_attempt() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 1
    assert str(seen[0].url) == "http://mock-client:8081/hooks/northwind"


async def test_the_signature_verifies_with_the_shared_secret() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    await build_client(handle).deliver(build_tenant(), build_payload())

    request = seen[0]
    timestamp = request.headers["X-ECET-Timestamp"]
    expected = hmac.new(
        SECRET.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + request.content,
        hashlib.sha256,
    ).hexdigest()
    assert request.headers["X-ECET-Signature"] == f"sha256={expected}"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["X-ECET-Delivery"]
    assert SECRET not in request.content.decode("utf-8")


async def test_two_server_errors_then_success_is_three_attempts_one_delivery_id() -> None:
    statuses = iter([500, 500, 200])
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(next(statuses))

    await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 3
    assert len({request.headers["X-ECET-Delivery"] for request in seen}) == 1


@pytest.mark.parametrize("status", [408, 429, 500, 503])
async def test_retryable_statuses_exhaust_into_a_transient_error(status: int) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)

    with pytest.raises(WebhookTransientError):
        await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 3


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
async def test_other_client_errors_are_permanent_after_one_attempt(status: int) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)

    with pytest.raises(WebhookPermanentError):
        await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 1


async def test_a_transport_error_is_retried_then_transient() -> None:
    attempts = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(WebhookTransientError):
        await build_client(handle).deliver(build_tenant(), build_payload())

    assert attempts == 3
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/infrastructure/test_httpx_webhook_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.webhook'`.

- [x] **Step 3: Write the adapter**

Create an empty `src/ecet/infrastructure/webhook/__init__.py`, then create
`src/ecet/infrastructure/webhook/httpx_client.py`:

```python
"""Signed webhook delivery with a bounded retry.

The signature covers `"{timestamp}.{body}"`, not the body alone: without the timestamp
in the signed material a captured request stays replayable forever, and the receiver
has nothing to compare a freshness window against.

`X-ECET-Delivery` is generated once per `deliver`, not once per attempt, so a receiver
can deduplicate retries of the same delivery.

The retry schedule is a constructor argument rather than a constant because a test
that proves "three attempts happened" should not take 21 seconds to do it.
"""

import asyncio
import hashlib
import hmac
import time
from collections.abc import Sequence
from uuid import uuid4

import httpx
import structlog

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.domain.tenant import Tenant

log = structlog.get_logger(__name__)

#: Retryable 4xx. Everything else in the 4xx range means the request itself is wrong.
RETRY_STATUS = frozenset({408, 429})


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret, timestamp.encode("utf-8") + b"." + body, hashlib.sha256)
    return f"sha256={digest.hexdigest()}"


class HttpxWebhookClient:
    def __init__(
        self,
        *,
        timeout_s: int,
        max_attempts: int = 3,
        backoff_seconds: Sequence[float] = (1.0, 4.0, 16.0),
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._max_attempts = max(1, max_attempts)
        self._backoff = tuple(backoff_seconds) or (0.0,)
        # One client for the process: the worker delivers to the same few hosts over
        # and over, so connection reuse is the whole point of not building it per call.
        # `http_client` is the test seam, matching `OpenAiLlmGateway`.
        self._client = http_client or httpx.AsyncClient(timeout=float(timeout_s))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def deliver(self, tenant: Tenant, payload: ClientNotification) -> None:
        body = payload.model_dump_json().encode("utf-8")
        secret = tenant.webhook_secret.get_secret_value().encode("utf-8")
        url = str(tenant.webhook_url)
        delivery_id = str(uuid4())
        last = "no attempt"

        for attempt in range(1, self._max_attempts + 1):
            timestamp = str(int(time.time()))
            headers = {
                "Content-Type": "application/json",
                "X-ECET-Delivery": delivery_id,
                "X-ECET-Timestamp": timestamp,
                "X-ECET-Signature": sign(secret, timestamp, body),
            }
            try:
                response = await self._client.post(url, content=body, headers=headers)
            except httpx.TransportError as error:
                last = type(error).__name__
            else:
                status = response.status_code
                if 200 <= status < 300:
                    log.info(
                        "webhook.delivered",
                        tenant_id=str(tenant.id),
                        claim_id=str(payload.claim_id),
                        status=status,
                        attempt=attempt,
                        delivery=delivery_id,
                    )
                    return
                if status not in RETRY_STATUS and status < 500:
                    log.warning(
                        "webhook.rejected",
                        tenant_id=str(tenant.id),
                        claim_id=str(payload.claim_id),
                        status=status,
                        delivery=delivery_id,
                    )
                    raise WebhookPermanentError(f"status {status}")
                last = f"status {status}"

            log.warning(
                "webhook.attempt_failed",
                tenant_id=str(tenant.id),
                claim_id=str(payload.claim_id),
                attempt=attempt,
                reason=last,
                delivery=delivery_id,
            )
            if attempt < self._max_attempts:
                await asyncio.sleep(self._backoff[min(attempt - 1, len(self._backoff) - 1)])

        raise WebhookTransientError(f"{self._max_attempts} attempts failed, last: {last}")
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/infrastructure/test_httpx_webhook_client.py -v`
Expected: PASS (all cases including the nine parametrised statuses).

- [x] **Step 5: Check types, layers and the full default suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 6: Commit**

```bash
git add src/ecet/infrastructure/webhook tests/unit/infrastructure/test_httpx_webhook_client.py
git commit -m "feat(webhook): signed httpx delivery client with a bounded retry schedule"
```

---

### Task 7: The mock client webhook receiver

**Files:**
- Create: `services/mock-client/app.py`, `services/mock-client/Dockerfile`, `services/mock-client/requirements.txt`
- Test: `tests/unit/test_mock_client.py`

**Interfaces:**
- Consumes: nothing from `ecet` — it is a separate service with its own dependencies, and it must stay that way so the demo receiver cannot accidentally share the sender's signing code and prove nothing.
- Produces: a FastAPI app on port 8081 with `POST /hooks/{name}` (verifies the signature, records the payload), `GET /received` (everything recorded), `DELETE /received` (reset, for repeat demos) and `GET /healthz`.

- [x] **Step 1: Write the failing receiver test**

Create `tests/unit/test_mock_client.py`:

```python
"""The mock client is demo infrastructure, but the signature check is the only
independent proof that `HttpxWebhookClient` signs correctly — it re-derives the HMAC
from the shared secret rather than calling the sender's own `sign`.

The service directory is hyphenated (`services/mock-client/`) and therefore not
importable as a package, so the module is loaded by path.
"""

import hashlib
import hmac
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

APP_PATH = Path(__file__).resolve().parents[1] / "services" / "mock-client" / "app.py"
SECRET = "dev-hmac-tenant-a"

PAYLOAD: dict[str, Any] = {
    "event": "claim.triaged",
    "claim_id": "11111111-1111-4111-8111-111111111111",
    "tenant_id": "tenant-a",
    "source_key": "tenants/tenant-a/claims/note-1.pdf",
    "outcome": "MEETS_NECESSITY",
    "confidence": 0.91,
    "decided_by": "auto",
    "cited_codes": ["M54.5"],
    "rationale": "Conservative therapy documented.",
    "evidence_missing": [],
    "decided_at": "2026-09-07T12:00:00Z",
}


def load_app(monkeypatch: pytest.MonkeyPatch, **env: str) -> Any:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("mock_client_app", APP_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["mock_client_app"] = module
    spec.loader.exec_module(module)
    return module


def signed_headers(body: bytes, secret: str = SECRET) -> dict[str, str]:
    timestamp = str(int(time.time()))
    digest = hmac.new(
        secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + body, hashlib.sha256
    ).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-ECET-Delivery": "delivery-1",
        "X-ECET-Timestamp": timestamp,
        "X-ECET-Signature": f"sha256={digest}",
    }


def client(module: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=module.app), base_url="http://mock"
    )


async def test_a_correctly_signed_delivery_is_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS=json.dumps({"northwind": SECRET}))
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        posted = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        listed = await http.get("/received")

    assert posted.status_code == 200
    received = listed.json()
    assert len(received) == 1
    assert received[0]["hook"] == "northwind"
    assert received[0]["verified"] is True
    assert received[0]["payload"]["outcome"] == "MEETS_NECESSITY"


async def test_a_wrongly_signed_delivery_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS=json.dumps({"northwind": SECRET}))
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        posted = await http.post(
            "/hooks/northwind", content=body, headers=signed_headers(body, "wrong-secret")
        )
        listed = await http.get("/received")

    assert posted.status_code == 401
    assert listed.json() == []


async def test_an_unknown_hook_is_recorded_unverified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No secret configured for this hook: still useful for a demo, but honest about
    # not having checked anything.
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS="{}")
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        posted = await http.post("/hooks/anything", content=body, headers=signed_headers(body))
        listed = await http.get("/received")

    assert posted.status_code == 200
    assert listed.json()[0]["verified"] is False


async def test_fail_first_n_rejects_then_accepts(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS="{}", FAIL_FIRST_N="2")
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        first = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        second = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        third = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        listed = await http.get("/received")

    assert [first.status_code, second.status_code, third.status_code] == [500, 500, 200]
    assert len(listed.json()) == 1


async def test_received_can_be_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS="{}")
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        await http.delete("/received")
        listed = await http.get("/received")

    assert listed.json() == []
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_mock_client.py -v`
Expected: FAIL — the app path does not exist, so `spec_from_file_location` returns
`None` and the `assert spec is not None` trips.

- [x] **Step 3: Write the receiver**

Create `services/mock-client/app.py`:

```python
"""A stand-in for a tenant's claims system.

It exists so `docker compose up` demonstrates the whole loop with no external system:
the worker signs a delivery, this receives it, checks the HMAC and keeps it for
`curl localhost:8081/received | jq`.

Deliberately independent of the `ecet` package. If it imported the sender's signing
helper, a bug in that helper would verify against itself and prove nothing.
"""

import hashlib
import hmac
import json
import os
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Request

#: {"<hook name>": "<shared secret>"}. A hook with no secret is accepted and recorded
#: with `verified: false` rather than refused — an unverifiable demo is more useful
#: than a 401 nobody can debug.
SECRETS: dict[str, str] = json.loads(os.environ.get("MOCK_CLIENT_SECRETS", "{}"))

#: Reject this many deliveries per hook with a 500 before accepting, to demo the
#: sender's retry schedule.
FAIL_FIRST_N = int(os.environ.get("FAIL_FIRST_N", "0"))

app = FastAPI(title="ECET mock client")

received: list[dict[str, Any]] = []
_rejections: dict[str, int] = {}


def _verify(name: str, body: bytes, timestamp: str, signature: str) -> bool:
    secret = SECRETS.get(name)
    if secret is None:
        return False
    expected = hmac.new(
        secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, f"sha256={expected}")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/hooks/{name}")
async def receive(
    name: str,
    request: Request,
    x_ecet_signature: Annotated[str, Header()] = "",
    x_ecet_timestamp: Annotated[str, Header()] = "",
    x_ecet_delivery: Annotated[str, Header()] = "",
) -> dict[str, Any]:
    seen = _rejections.get(name, 0)
    if seen < FAIL_FIRST_N:
        _rejections[name] = seen + 1
        raise HTTPException(500, "induced failure (FAIL_FIRST_N)")

    body = await request.body()
    verified = _verify(name, body, x_ecet_timestamp, x_ecet_signature)
    if name in SECRETS and not verified:
        raise HTTPException(401, "bad signature")

    received.append(
        {
            "hook": name,
            "delivery": x_ecet_delivery,
            "verified": verified,
            "payload": json.loads(body),
        }
    )
    return {"ok": True, "verified": verified}


@app.get("/received")
async def list_received() -> list[dict[str, Any]]:
    return received


@app.delete("/received")
async def reset() -> dict[str, int]:
    count = len(received)
    received.clear()
    _rejections.clear()
    return {"cleared": count}
```

Create `services/mock-client/requirements.txt`:

```
fastapi>=0.141,<0.142
uvicorn[standard]>=0.34,<1
```

Create `services/mock-client/Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./
RUN useradd --create-home --uid 1000 mock
USER mock
EXPOSE 8081
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8081"]
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_mock_client.py -v`
Expected: PASS (5 tests).

- [x] **Step 5: Check the whole default suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run pytest
```
Expected: green. `services/` is not part of the `ecet` package, so mypy and
import-linter do not cover it; ruff does, and the file must satisfy it.

- [x] **Step 6: Commit**

```bash
git add services/mock-client tests/unit/test_mock_client.py
git commit -m "feat(mock-client): webhook receiver with signature verification and induced failures"
```

---

### Task 8: The RabbitMQ consumer and the worker process

**Files:**
- Modify: `src/ecet/infrastructure/queue/rabbitmq.py`
- Create: `src/ecet/interfaces/worker/handler.py`, `src/ecet/interfaces/worker/container.py`
- Modify: `src/ecet/interfaces/worker/main.py`
- Create: `tests/unit/interfaces/__init__.py`, `tests/unit/interfaces/test_worker_handler.py`
- Create: `tests/adapters/test_rabbitmq_consumer.py`
- Modify: `tests/adapters/test_rabbitmq_queue.py` (the `declare_topology` return type changes)

**Interfaces:**
- Consumes: `declare_topology`, `QUEUE`, `QUEUE_ARGUMENTS`, `ROUTING_KEY` (Phase 3); `EvaluateClaim` (Task 4); `build_gateway` (Task 5); `HttpxWebhookClient` (Task 6); `SqlAlchemyUnitOfWork`, `create_engine`, `create_session_factory`, `assert_at_head` (Phase 2/3).
- Produces:
  - `ecet.infrastructure.queue.rabbitmq.Topology` (NamedTuple `exchange`, `queue`) — `declare_topology` now returns this instead of the bare exchange.
  - `ecet.infrastructure.queue.rabbitmq.RabbitMqConsumer` — `__init__(url, *, prefetch, handler, should_requeue, drain_timeout=30.0)`, `async start()`, `async stop()`.
  - `ecet.interfaces.worker.handler.AckAction` (`ACK`/`REQUEUE`/`DLQ`), `classify(error) -> AckAction`, `should_requeue(error) -> bool`, `WorkerMessageHandler` (`async handle(message) -> None`).
  - `ecet.interfaces.worker.container.WorkerContainer` + `async build_container(settings) -> WorkerContainer`.

- [x] **Step 1: Write the failing handler test**

Create an empty `tests/unit/interfaces/__init__.py`, then create
`tests/unit/interfaces/test_worker_handler.py`:

```python
"""The ack/nack policy is a pure function so that "does a 429 requeue?" is answerable
without a broker, a database or a vendor."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError
from tests.fakes import FakeLLMGateway

from ecet.application.errors import (
    LLMInvalidOutput,
    LLMPermanentError,
    LLMTransientError,
    QueuePublishError,
    WebhookTransientError,
)
from ecet.application.messages import EvaluationMessage
from ecet.domain.errors import ClaimNotFound, InvalidTransition
from ecet.domain.ids import ClaimId, TenantId
from ecet.interfaces.worker.handler import (
    AckAction,
    WorkerMessageHandler,
    classify,
    should_requeue,
)

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)


def build_message() -> EvaluationMessage:
    return EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[],
        deterministic_verdict="PASS",
        found_codes=["M54.5"],
        enqueued_at=NOW,
    )


def test_no_error_acks() -> None:
    assert classify(None) is AckAction.ACK


@pytest.mark.parametrize(
    "error",
    [
        LLMTransientError("429"),
        QueuePublishError("no confirm"),
        OperationalError("SELECT 1", {}, Exception("connection reset")),
        ConnectionError("reset"),
        TimeoutError("slow"),
    ],
)
def test_recoverable_failures_requeue(error: BaseException) -> None:
    assert classify(error) is AckAction.REQUEUE
    assert should_requeue(error) is True


@pytest.mark.parametrize(
    "error",
    [
        LLMPermanentError("401"),
        LLMInvalidOutput("no tool call"),
        WebhookTransientError("exhausted"),
        InvalidTransition("QUEUED -> APPROVED_AUTO"),
        ClaimNotFound("nope"),
        IntegrityError("INSERT", {}, Exception("duplicate key")),
        ValueError("bug"),
    ],
)
def test_everything_else_goes_to_the_dead_letter_queue(error: BaseException) -> None:
    # WebhookTransientError is in this list on purpose: UC-07 catches it and parks the
    # claim in NOTIFY_FAILED, so if one ever escapes to here it is a bug, not a retry.
    assert classify(error) is AckAction.DLQ
    assert should_requeue(error) is False


class _Evaluate:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[EvaluationMessage] = []
        self.error = error

    async def execute(self, message: EvaluationMessage) -> None:
        self.calls.append(message)
        if self.error is not None:
            raise self.error


async def test_the_handler_delegates_to_the_use_case() -> None:
    evaluate = _Evaluate()
    message = build_message()

    await WorkerMessageHandler(evaluate).handle(message)  # type: ignore[arg-type]

    assert evaluate.calls == [message]


async def test_the_handler_re_raises_so_the_consumer_can_classify() -> None:
    evaluate = _Evaluate(LLMTransientError("429"))

    with pytest.raises(LLMTransientError):
        await WorkerMessageHandler(evaluate).handle(build_message())  # type: ignore[arg-type]


def test_the_fake_gateway_import_stays_available_to_the_worker_tests() -> None:
    # Guards against the fakes module drifting out from under the worker suite.
    assert FakeLLMGateway() is not None
```

- [x] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/interfaces/test_worker_handler.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.interfaces.worker.handler'`.

- [x] **Step 3: Write the handler**

Create `src/ecet/interfaces/worker/handler.py`:

```python
"""The delivery policy, as a pure function.

Everything the worker does with a message reduces to one of three answers, and the
mapping is a policy decision worth reading in one place: **requeue only what a
retry could plausibly fix.** A vendor 429 or a dropped database connection, yes. A
401, a schema violation, a bug — no: those return identically forever and would spin
against the delivery limit before landing in the DLQ anyway.

`WebhookTransientError` is deliberately *not* requeued. UC-07 already catches it and
parks the claim in `NOTIFY_FAILED`; one escaping to here means a code path forgot to,
and requeueing would re-run a whole LLM evaluation to retry an HTTP POST.
"""

import time
from enum import StrEnum
from typing import Protocol

import structlog
from sqlalchemy.exc import InterfaceError, OperationalError

from ecet.application.errors import LLMTransientError, QueuePublishError
from ecet.application.messages import EvaluationMessage

log = structlog.get_logger(__name__)


class AckAction(StrEnum):
    ACK = "ACK"
    REQUEUE = "REQUEUE"
    DLQ = "DLQ"


#: `OperationalError`/`InterfaceError` are SQLAlchemy's connection-level failures; a
#: statement-level failure (IntegrityError, ProgrammingError) is not in the list.
REQUEUE_ERRORS: tuple[type[BaseException], ...] = (
    LLMTransientError,
    QueuePublishError,
    OperationalError,
    InterfaceError,
    ConnectionError,
    TimeoutError,
)


def classify(error: BaseException | None) -> AckAction:
    if error is None:
        return AckAction.ACK
    if isinstance(error, REQUEUE_ERRORS):
        return AckAction.REQUEUE
    return AckAction.DLQ


def should_requeue(error: BaseException) -> bool:
    return classify(error) is AckAction.REQUEUE


class _Evaluator(Protocol):
    async def execute(self, message: EvaluationMessage) -> None: ...


class WorkerMessageHandler:
    """Runs the use case and emits the per-message log line the worker spec asks for.
    It re-raises: the consumer, not the handler, owns the ack."""

    def __init__(self, evaluate: _Evaluator) -> None:
        self._evaluate = evaluate

    async def handle(self, message: EvaluationMessage) -> None:
        started = time.perf_counter()
        try:
            await self._evaluate.execute(message)
        except BaseException as error:
            log.warning(
                "worker.message_failed",
                claim_id=str(message.claim_id),
                tenant_id=str(message.tenant_id),
                outcome=classify(error).value,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error=type(error).__name__,
            )
            raise
        log.info(
            "worker.message_handled",
            claim_id=str(message.claim_id),
            tenant_id=str(message.tenant_id),
            outcome=AckAction.ACK.value,
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
```

- [x] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/interfaces/test_worker_handler.py -v`
Expected: PASS (all cases including the twelve parametrised errors).

- [x] **Step 5: Write the failing consumer test**

Create `tests/adapters/test_rabbitmq_consumer.py`:

```python
"""The consumer against a real broker. The three outcomes that matter are the ones
the DLQ makes visible: acked and gone, requeued and redelivered, dead-lettered."""

import asyncio
from collections.abc import AsyncIterator, Iterator

import aio_pika
import pytest
from testcontainers.core.container import DockerContainer
from tests.adapters.containers import wait_until
from tests.adapters.test_rabbitmq_queue import build_message

from ecet.application.errors import LLMTransientError
from ecet.application.messages import EvaluationMessage
from ecet.infrastructure.queue.rabbitmq import (
    DLQ,
    RabbitMqConsumer,
    RabbitMqEvaluationQueue,
)
from ecet.interfaces.worker.handler import should_requeue


@pytest.fixture(scope="session")
def amqp_url() -> Iterator[str]:
    container = DockerContainer("rabbitmq:3.13-management").with_exposed_ports(5672)
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
async def publisher(amqp_url: str) -> AsyncIterator[RabbitMqEvaluationQueue]:
    adapter = RabbitMqEvaluationQueue(amqp_url)
    await wait_until(adapter.start)
    yield adapter
    await adapter.stop()


async def drain(predicate: object, timeout: float = 15.0) -> None:
    """Poll until `predicate()` is true or the deadline passes."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():  # type: ignore[operator]
            return
        await asyncio.sleep(0.1)
    raise AssertionError("condition not reached before the deadline")


async def test_a_published_message_reaches_the_handler(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    handled: list[EvaluationMessage] = []

    async def handle(message: EvaluationMessage) -> None:
        handled.append(message)

    consumer = RabbitMqConsumer(
        amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue
    )
    await consumer.start()
    try:
        sent = build_message()
        await publisher.publish(sent)
        await drain(lambda: len(handled) == 1)
    finally:
        await consumer.stop()

    assert handled[0].claim_id == sent.claim_id


async def test_a_dlq_classified_failure_dead_letters_the_message(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    attempts: list[int] = []

    async def handle(message: EvaluationMessage) -> None:
        attempts.append(1)
        raise ValueError("a bug, not a blip")

    consumer = RabbitMqConsumer(
        amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue
    )
    await consumer.start()
    try:
        await publisher.publish(build_message())
        await drain(lambda: len(attempts) >= 1)
        await asyncio.sleep(1.0)  # let the broker route it to the DLX
    finally:
        await consumer.stop()

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        dead_letters = await channel.get_queue(DLQ)
        dead = await dead_letters.get(timeout=10)
        assert dead is not None
        await dead.ack()

    assert len(attempts) == 1


async def test_an_undecodable_body_dead_letters_without_reaching_the_handler(
    amqp_url: str
) -> None:
    handled: list[EvaluationMessage] = []

    async def handle(message: EvaluationMessage) -> None:
        handled.append(message)

    consumer = RabbitMqConsumer(
        amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue
    )
    await consumer.start()
    try:
        connection = await aio_pika.connect_robust(amqp_url)
        async with connection:
            channel = await connection.channel()
            exchange = await channel.get_exchange("ecet")
            await exchange.publish(
                aio_pika.Message(body=b"{not json}"), routing_key="claims.evaluate"
            )
        await asyncio.sleep(2.0)
    finally:
        await consumer.stop()

    assert handled == []

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        dead_letters = await channel.get_queue(DLQ)
        dead = await dead_letters.get(timeout=10)
        assert dead is not None
        await dead.ack()


async def test_a_requeue_classified_failure_is_redelivered(
    publisher: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    attempts: list[int] = []

    async def handle(message: EvaluationMessage) -> None:
        attempts.append(1)
        if len(attempts) < 2:
            raise LLMTransientError("429")

    consumer = RabbitMqConsumer(
        amqp_url, prefetch=4, handler=handle, should_requeue=should_requeue
    )
    await consumer.start()
    try:
        await publisher.publish(build_message())
        await drain(lambda: len(attempts) >= 2, timeout=20.0)
    finally:
        await consumer.stop()

    assert len(attempts) >= 2
```

- [x] **Step 6: Run it and watch it fail**

Run: `uv run pytest tests/adapters/test_rabbitmq_consumer.py -m slow -v`
Expected: FAIL — `ImportError: cannot import name 'RabbitMqConsumer'`.

- [x] **Step 7: Extend the queue module**

In `src/ecet/infrastructure/queue/rabbitmq.py`, change `declare_topology` to return both
objects and add the consumer. Replace the existing `declare_topology` with:

```python
class Topology(NamedTuple):
    exchange: AbstractExchange
    queue: AbstractQueue


async def declare_topology(channel: AbstractChannel) -> Topology:
    """Declare exchanges, queues and bindings. Idempotent. Both processes call it."""
    exchange = await channel.declare_exchange(EXCHANGE, "topic", durable=True)
    dlx = await channel.declare_exchange(DLX, "topic", durable=True)

    dead_letters = await channel.declare_queue(DLQ, durable=True)
    await dead_letters.bind(dlx, routing_key=ROUTING_KEY)

    evaluations = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGUMENTS)
    await evaluations.bind(exchange, routing_key=ROUTING_KEY)

    return Topology(exchange=exchange, queue=evaluations)
```

Update the imports at the top of the module to
`from typing import NamedTuple` and
`from aio_pika.abc import (AbstractChannel, AbstractExchange, AbstractIncomingMessage, AbstractQueue, AbstractRobustConnection)`,
and in `RabbitMqEvaluationQueue.start`, change the assignment to:

```python
        self._exchange = (await declare_topology(channel)).exchange
```

Then fix `is_healthy` (Phase 3 carry-over #25) — a robust connection reports
`is_closed == False` for the whole duration of a reconnect, so `/readyz` could call the
queue healthy while every publish would fail:

```python
    async def is_healthy(self) -> bool:
        """Used by `/readyz`; never raises. `reconnecting` is the part `is_closed`
        misses: aio-pika keeps a robust connection "open" while it retries, so without
        this check the api reports a healthy queue that cannot accept a publish."""
        connection = self._connection
        return (
            connection is not None
            and not connection.is_closed
            and not connection.reconnecting
            and self._exchange is not None
        )
```

Finally append the consumer:

```python
class RabbitMqConsumer:
    """The worker's side of `claims.evaluate`.

    The ack decision is delegated to `should_requeue`, which lives in the worker
    interface: the broker adapter knows how to nack, not which failures deserve
    another go.

    A body that does not validate is dead-lettered without reaching the handler —
    there is no version of "retry" that makes malformed JSON parse.
    """

    def __init__(
        self,
        url: str,
        *,
        prefetch: int,
        handler: Callable[[EvaluationMessage], Awaitable[None]],
        should_requeue: Callable[[BaseException], bool],
        drain_timeout: float = 30.0,
    ) -> None:
        self._url = url
        self._prefetch = prefetch
        self._handler = handler
        self._should_requeue = should_requeue
        self._drain_timeout = drain_timeout
        self._connection: AbstractRobustConnection | None = None
        self._queue: AbstractQueue | None = None
        self._tag: str | None = None
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self) -> None:
        self._connection = await connect_robust(self._url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch)
        self._queue = (await declare_topology(channel)).queue
        self._tag = await self._queue.consume(self._on_message)
        log.info("consumer.started", queue=QUEUE, prefetch=self._prefetch)

    async def stop(self) -> None:
        """Graceful: stop taking new deliveries, let the in-flight ones finish, close."""
        if self._queue is not None and self._tag is not None:
            with contextlib.suppress(Exception):
                await self._queue.cancel(self._tag)
            self._tag = None
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=self._drain_timeout)
        except TimeoutError:
            log.warning("consumer.drain_timeout", in_flight=self._in_flight)
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        self._queue = None
        log.info("consumer.stopped")

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        self._in_flight += 1
        self._idle.clear()
        try:
            try:
                parsed = EvaluationMessage.model_validate_json(message.body)
            except ValidationError as error:
                log.error(
                    "queue.undecodable",
                    message_id=message.message_id,
                    problems=error.error_count(),
                )
                await message.nack(requeue=False)
                return

            try:
                await self._handler(parsed)
            except BaseException as error:
                requeue = self._should_requeue(error)
                log.warning(
                    "queue.nacked",
                    claim_id=str(parsed.claim_id),
                    tenant_id=str(parsed.tenant_id),
                    requeue=requeue,
                    delivery_count=(message.headers or {}).get("x-delivery-count"),
                    error=type(error).__name__,
                )
                await message.nack(requeue=requeue)
            else:
                await message.ack()
        finally:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._idle.set()
```

with these imports added at the top of the module: `asyncio`, `contextlib`,
`from collections.abc import Awaitable, Callable`, and `from pydantic import ValidationError`.

- [x] **Step 8: Fix the one existing caller of `declare_topology`**

`tests/adapters/test_rabbitmq_queue.py` does not call it directly, so nothing there
changes — but run it to be sure the publisher still declares the same topology:

```bash
uv run pytest tests/adapters/test_rabbitmq_queue.py -m slow -v
```
Expected: PASS (4 tests). If `test_the_topology_is_declared_idempotently` fails with
`PRECONDITION_FAILED`, a stale classic `claims.evaluate` is sitting in a reused
`rabbitdata` volume — `make clean` and retry (Task 9 documents this).

- [x] **Step 9: Run the consumer test and watch it pass**

Run: `uv run pytest tests/adapters/test_rabbitmq_consumer.py -m slow -v`
Expected: PASS (4 tests). These need Docker.

- [x] **Step 10: Write the worker container and main**

Create `src/ecet/interfaces/worker/container.py`:

```python
"""Worker wiring. Same shape as the api container: ports in the fields, adapters built
once, everything closed in `aclose`.

The worker never migrates. `ECET_AUTO_MIGRATE` is an api-only setting (see the config
spec), and two processes racing `alembic upgrade head` against one database is a
worse failure than starting a moment later — compose gates the worker on the api being
healthy, which is exactly "migrations have run".
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog

from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.evaluate_claim import EvaluateClaim
from ecet.config import Settings
from ecet.infrastructure.clock import SystemClock
from ecet.infrastructure.llm.factory import build_gateway
from ecet.infrastructure.postgres.migrations import assert_at_head
from ecet.infrastructure.postgres.session import create_engine, create_session_factory
from ecet.infrastructure.postgres.unit_of_work import SqlAlchemyUnitOfWork
from ecet.infrastructure.queue.rabbitmq import RabbitMqConsumer
from ecet.infrastructure.webhook.httpx_client import HttpxWebhookClient
from ecet.interfaces.worker.handler import WorkerMessageHandler, should_requeue

log = structlog.get_logger(__name__)


@dataclass
class WorkerContainer:
    settings: Settings
    evaluate: EvaluateClaim
    consumer: RabbitMqConsumer
    aclose: Callable[[], Awaitable[None]]


async def build_container(settings: Settings) -> WorkerContainer:
    engine = create_engine(settings.database_url.get_secret_value())
    await assert_at_head(engine)
    session_factory = create_session_factory(engine)

    llm = build_gateway(settings)
    webhook = HttpxWebhookClient(
        timeout_s=settings.webhook_timeout_s, max_attempts=settings.webhook_max_attempts
    )

    def uow_factory() -> UnitOfWork:
        # One per message: `SqlAlchemyUnitOfWork` does not reopen its session.
        return SqlAlchemyUnitOfWork(session_factory)

    evaluate = EvaluateClaim(
        uow_factory=uow_factory,
        llm=llm,
        webhook=webhook,
        clock=SystemClock(),
        threshold=settings.confidence_threshold,
        prompt_version=settings.prompt_version,
    )
    consumer = RabbitMqConsumer(
        settings.amqp_url.get_secret_value(),
        prefetch=settings.worker_prefetch,
        handler=WorkerMessageHandler(evaluate).handle,
        should_requeue=should_requeue,
    )

    async def aclose() -> None:
        try:
            await consumer.stop()
        finally:
            try:
                await webhook.aclose()
            finally:
                await engine.dispose()

    log.info(
        "worker.container_built",
        provider=settings.llm_provider.value,
        model=settings.llm_model,
        threshold=settings.confidence_threshold,
        prefetch=settings.worker_prefetch,
    )
    return WorkerContainer(
        settings=settings, evaluate=evaluate, consumer=consumer, aclose=aclose
    )
```

Replace the body of `src/ecet/interfaces/worker/main.py` with:

```python
"""Worker entrypoint: consume `claims.evaluate` until SIGTERM, then drain and exit 0."""

import asyncio
import contextlib
import signal

import structlog

from ecet.config import Settings
from ecet.interfaces.worker.container import WorkerContainer, build_container

log = structlog.get_logger(__name__)


async def run(
    settings: Settings,
    stop: asyncio.Event | None = None,
    container: WorkerContainer | None = None,
) -> None:
    """`container` is injected by tests, mirroring `create_app(settings, container=...)`."""
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not implemented on Windows event loops; the compose stack is Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    built = container if container is not None else await build_container(settings)
    await built.consumer.start()
    log.info("worker.started", env=settings.env, prefetch=settings.worker_prefetch)
    try:
        await stop.wait()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(sig)
        if container is None:
            await built.aclose()
        log.info("worker.stopped")
```

- [x] **Step 11: Run everything, including the slow suite**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run mypy src/ecet
uv run lint-imports
uv run pytest
uv run pytest -m 'not e2e'   # needs Docker and `make spacy-model`
```
Expected: all green.

- [x] **Step 12: Commit**

```bash
git add src/ecet/infrastructure/queue/rabbitmq.py src/ecet/interfaces/worker \
  tests/unit/interfaces tests/adapters/test_rabbitmq_consumer.py
git commit -m "feat(worker): RabbitMQ consumer, ack classification and the worker container"
```

---

### Task 9: Compose wiring, the bucket guard and the demo loop

**Files:**
- Modify: `src/ecet/config.py`, `.env.example`
- Modify: `src/ecet/interfaces/api/routes/claims.py`, `src/ecet/interfaces/api/routes/events.py`
- Modify: `docker-compose.yml`, `Makefile`, `scripts/make_fixtures.py`
- Modify: `tests/unit/test_compose.py`, `tests/api/test_claims_routes.py`, `tests/api/test_events_route.py`

**Interfaces:**
- Consumes: `ecet.application.errors.ObjectNotFound` (Phase 3), `ApiContainer.settings` (Phase 3).
- Produces: `Settings.s3_bucket: str` (default `"claims"`), and a `make demo` target that runs the whole loop end to end.

This task closes the two Phase 3 carry-overs the roadmap assigns to Phase 4 — the missing
bucket check (#22) and `is_healthy` during a reconnect (#25, done in Task 8) — and makes
`docker compose up` demonstrate the full path.

- [x] **Step 1: Write the failing bucket-guard tests**

Append to `tests/api/test_claims_routes.py`:

```python
async def test_manual_ingest_refuses_a_bucket_the_deployment_does_not_own(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    # Without this the endpoint HEADs any bucket the caller names — a size/existence
    # oracle over the whole MinIO instance, since v1 has one global API key.
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": "someone-elses-bucket", "key": KEY},
            headers=api_headers,
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "No object at that bucket and key."
    assert harness.storage.reads == []
```

Append to `tests/api/test_events_route.py`:

```python
async def test_an_event_for_another_bucket_is_ignored(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    event = put_event()
    event["Records"][0]["s3"]["bucket"]["name"] = "not-claims"

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=event, headers=event_headers)

    assert response.status_code == 200
    assert response.json() == {"ignored": 1}
    assert harness.uow.claims.claims == {}
```

- [x] **Step 2: Run them and watch them fail**

Run: `uv run pytest tests/api/test_claims_routes.py tests/api/test_events_route.py -v`
Expected: FAIL — the manual-ingest test gets a 200 (it HEADs the fake storage), the
event test gets a 200 with an `IngestResult` body rather than `{"ignored": 1}`.

- [x] **Step 3: Add the setting**

In `src/ecet/config.py`, add below `s3_endpoint`:

```python
    #: The one bucket this deployment ingests from. The api refuses any other bucket
    #: rather than trusting the name in a client-supplied event.
    s3_bucket: str = "claims"
```

In `.env.example`, add below `ECET_S3_ENDPOINT`:

```
ECET_S3_BUCKET=claims
```

- [x] **Step 4: Guard the manual ingest route**

In `src/ecet/interfaces/api/routes/claims.py`, import `ObjectNotFound`
(`from ecet.application.errors import ObjectNotFound`) and add the check before the HEAD:

```python
    if body.bucket != container.settings.s3_bucket:
        # Same answer as a missing object, on purpose: naming a bucket that exists but
        # is not ours must not be distinguishable from naming one that does not exist.
        raise ObjectNotFound(f"{body.bucket}/{body.key}")
    head = await container.storage.head(body.bucket, body.key)
```

Extend the module docstring with a sentence:

```
`POST /v1/claims/ingest` and `POST /v1/events/s3` both refuse a bucket other than
`ECET_S3_BUCKET`. Nothing else constrains which bucket a caller can name, and both
paths otherwise read whatever they are told to.
```

- [x] **Step 5: Guard the event route**

In `src/ecet/interfaces/api/routes/events.py`, change the `actionable` filter so a
record for another bucket is ignored rather than processed:

```python
    bucket = container.settings.s3_bucket
    actionable = [
        (i, r)
        for i, r in enumerate(envelope.records)
        if r.is_claim_pdf() and r.s3.bucket.name == bucket
    ]
```

and extend the module docstring:

```
A record naming a bucket other than `ECET_S3_BUCKET` is counted as ignored, exactly
like a non-`.pdf` key: the notification target is registered on one bucket, so anything
else is either a misconfiguration or a forged payload, and neither deserves a claim.
```

- [x] **Step 6: Run the API suite and watch it pass**

Run: `uv run pytest tests/api -v`
Expected: PASS, including the two new tests and every existing one (the fixtures use
bucket `claims`, which is the default).

- [x] **Step 7: Write the failing compose test**

Append to `tests/unit/test_compose.py`:

```python
def test_the_mock_client_is_built_and_exposed(compose: dict[str, Any]) -> None:
    mock = compose["services"]["mock-client"]

    assert mock["build"] == "services/mock-client"
    assert "8081:8081" in mock["ports"]
    assert "MOCK_CLIENT_SECRETS" in mock["environment"]


def test_the_worker_waits_for_the_api_so_migrations_have_run(compose: dict[str, Any]) -> None:
    # The worker never migrates; a healthy api is the signal that the schema is at head.
    worker = compose["services"]["worker"]

    assert worker["depends_on"]["api"]["condition"] == "service_healthy"
    assert worker["depends_on"]["rabbitmq"]["condition"] == "service_healthy"


def test_the_worker_can_reach_the_mock_client(compose: dict[str, Any]) -> None:
    # The seeded tenants' webhook URLs point at http://mock-client:8081/hooks/...
    assert "mock-client" in compose["services"]["worker"]["depends_on"]


def test_the_minio_webhook_target_is_persistent(compose: dict[str, Any]) -> None:
    # Phase 3 carry-over: without `queue_dir` MinIO's webhook target is fire-and-forget,
    # so an event the api answered 4xx is gone — no claim row, no retry, no signal.
    script = " ".join(compose["services"]["minio-setup"]["entrypoint"])

    assert "queue_dir=" in script
    assert "queue_limit=" in script
```

- [x] **Step 8: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: FAIL — `KeyError: 'mock-client'`.

- [x] **Step 9: Wire compose**

In `docker-compose.yml`, add the `mock-client` service and extend `worker`:

```yaml
  mock-client:
    build: services/mock-client
    ports:
      - "8081:8081"
    environment:
      # Matches the seeded tenants (see src/ecet/infrastructure/postgres/seed/tenants.sql).
      # Dev HMAC keys, worthless outside compose.
      MOCK_CLIENT_SECRETS: >-
        {"northwind":"dev-hmac-tenant-a",
         "cascade":"dev-hmac-tenant-b",
         "harbor":"dev-hmac-tenant-empty",
         "meridian":"dev-hmac-tenant-legacy"}
      FAIL_FIRST_N: "0"
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - "import urllib.request; urllib.request.urlopen('http://localhost:8081/healthz')"
      interval: 10s
      timeout: 5s
      retries: 5
```

Make the MinIO webhook target persistent, closing the Phase 3 carry-over that a
non-2xx response silently loses the event. In the `minio-setup` entrypoint, extend the
`mc admin config set` line and create the spool directory:

```sh
        mc mb -p local/claims
        mc admin config set local notify_webhook:ecet \
          endpoint=http://api:8000/v1/events/s3 \
          auth_token="$$ECET_S3_EVENT_TOKEN" \
          queue_dir=/data/.ecet-notify \
          queue_limit=10000
```

`queue_dir` must be on the same filesystem MinIO serves, which is the `miniodata`
volume mounted at `/data`; MinIO creates the directory itself on restart.

Then replace the `worker` service's `depends_on` block with:

```yaml
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
      # The api owns migrations (ECET_AUTO_MIGRATE); a healthy api means the schema is
      # at head, which is what `assert_at_head` in the worker container requires.
      api:
        condition: service_healthy
      mock-client:
        condition: service_healthy
```

- [x] **Step 10: Run the compose test and watch it pass**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: PASS (7 tests).

- [x] **Step 11: Add the unclear fixture and the demo targets**

In `scripts/make_fixtures.py`, add a single-note "unclear" PDF next to `note_simple`,
inside `build_all` and before the multipage block:

```python
    unclear = destination / "note_unclear.pdf"
    target = canvas.Canvas(str(unclear), pagesize=LETTER)
    _write_lines(target, _note_lines("unclear"))
    target.save()
    built["note_unclear"] = unclear
```

In the `Makefile`, add `demo` to the `.PHONY` line and append:

```makefile
# `clean` removes the named volumes, including `rabbitdata`. That matters after a
# topology change: `claims.evaluate` is declared with `x-queue-type: quorum`, and a
# classic queue of the same name left in an old volume makes both the api and the
# worker fail to start with PRECONDITION_FAILED.

demo: fixtures up
	@echo "waiting for the api to become ready..."
	@until curl -sf localhost:8000/readyz >/dev/null; do sleep 2; done
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-a
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_unclear.pdf tenant-a
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-empty || true
	@sleep 5
	docker compose logs --since 60s worker
	@echo "--- webhooks received by the mock client ---"
	curl -s localhost:8081/received | python -m json.tool
```

- [x] **Step 12: Run the whole default suite and the checks**

```bash
uv run ruff format . && uv run ruff check --fix .
uv run mypy src/ecet
uv run lint-imports
uv run pytest
```
Expected: all green.

- [x] **Step 13: Run the stack for real**

```bash
make clean          # drops rabbitdata; see the note above
make demo
```
Expected, in order:
1. `api` log: `claim.transition to=QUEUED` for the `tenant-a` drop.
2. `worker` log: `llm.evaluated provider=fake decision=MEETS_NECESSITY confidence=0.91`,
   then `claim.routed route=AUTO_NOTIFY`, then `webhook.delivered status=200`,
   then `claim.transition to=APPROVED_AUTO`.
3. `worker` log for the `note_unclear` drop: `decision=INSUFFICIENT_EVIDENCE confidence=0.4`,
   `claim.routed route=HUMAN_REVIEW`, `claim.transition to=REVIEW_PENDING`, no webhook.
4. `api` log for the `tenant-empty` drop: `NO_POLICIES`, the webhook answered 422.
5. `curl localhost:8081/received` lists exactly one delivery, with `"verified": true`
   and `"outcome": "MEETS_NECESSITY"`.

If the worker exits at startup with an Alembic error, the api had not finished
migrating — check `docker compose ps` shows `api` as healthy.

- [x] **Step 14: Commit**

```bash
git add src/ecet/config.py .env.example src/ecet/interfaces/api/routes/claims.py \
  src/ecet/interfaces/api/routes/events.py docker-compose.yml Makefile \
  scripts/make_fixtures.py tests/unit/test_compose.py tests/api/test_claims_routes.py \
  tests/api/test_events_route.py
git commit -m "feat(compose): mock-client service, bucket scoping and the end-to-end demo target"
```

---

### Task 10: Record the phase in the docs

**Files:**
- Modify: `specs/06-roadmap.md`
- Modify: `docs/plans/2026-09-07-phase-4-evaluation-path.md` (this file — tick the boxes)

- [x] **Step 1: Link the plan from the roadmap**

Under `## Phase 4 — Evaluation Path (Worker side)`, add the plan link as the first line
of the section, matching Phases 2 and 3:

```markdown
Plan: [`docs/plans/2026-09-07-phase-4-evaluation-path.md`](../docs/plans/2026-09-07-phase-4-evaluation-path.md).
```

- [x] **Step 2: Add the Phase 4 carry-over table**

Append a `## Carried over from Phase 4` section to `specs/06-roadmap.md`, after the
Phase 3 table, filled in from the "Deviations from spec" section at the bottom of this
plan **as it actually ends up** — do not copy the list below verbatim without checking
what changed during execution:

```markdown
## Carried over from Phase 4

Every deferral recorded in [`docs/plans/2026-09-07-phase-4-evaluation-path.md`](../docs/plans/2026-09-07-phase-4-evaluation-path.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 4 | Closed by |
|---|---------------------|-----------|
| 1 | No metric is emitted anywhere on the worker path: `ecet_llm_calls_total`, `ecet_llm_latency_seconds`, `ecet_llm_tokens_total`, `ecet_triage_route_total`, `ecet_webhook_attempts_total` are all unimplemented, and the worker has no `/metrics` server and therefore still no container healthcheck | Phase 6 |
| 2 | `infrastructure/webhook/fake_client.py` and `infrastructure/queue/in_memory.py` from the layout spec are still not built; `tests/fakes.py` covers both | accepted, permanent |
| 3 | UC-06 commits once, after routing: a webhook delivered but not committed is re-delivered on redelivery. At-least-once is the queue's contract and the payload is idempotent by `claim_id` | accepted, permanent |
| 4 | The adapter tests use `httpx.MockTransport` rather than `respx` | accepted, permanent |
| 5 | The vendor adapter is never exercised against a live endpoint; `ECET_LLM_PROVIDER=openai` is untested outside a mock transport | Phase 5 (real vendor run behind an env flag) |
| 6 | The mock client verifies a signature but not the timestamp's freshness, so a captured delivery replays forever | accepted — it is a demo receiver, and the note belongs in the README |
| 7 | `EvaluationOutput` accepts a missing `confidence` and degrades to `INSUFFICIENT_EVIDENCE`, but the tool schema still marks it required — a server that omits it is silently downgraded rather than reported | accepted, deliberate |
| 8 | A `NOTIFY_FAILED` claim has no retry path: `POST /v1/claims/{id}/retry-notify` is Phase 5 | Phase 5 |
| 9 | The worker depends on a healthy api in compose so migrations have run, rather than waiting for head itself; a worker restarted alone against a behind-head database exits | accepted, deliberate |
```

- [x] **Step 3: Add the inline Phase 5 and Phase 6 carry-over lines**

In `## Phase 5`, append to the existing `- Carried from Phase 3:` block:

```markdown
- Carried from Phase 4: `POST /v1/claims/{id}/retry-notify` is the only exit from `NOTIFY_FAILED`, and `NOTIFY_FAILED -> APPROVED_AUTO | REVIEW_RESOLVED` is already in the state machine. `NotifyClient.execute(claim, outcome=..., confidence=1.0, decided_by="human")` is what UC-09c calls — it exists and is tested. The real-vendor run is the first time `ECET_LLM_PROVIDER=openai` talks to a live endpoint.
```

In `## Phase 6`, append:

```markdown
- Carried from Phase 4: no metric is emitted on the worker path either (`ecet_llm_calls_total`, `ecet_llm_latency_seconds`, `ecet_llm_tokens_total`, `ecet_triage_route_total`, `ecet_webhook_attempts_total`), and the worker still has no `/metrics` server, so its compose healthcheck is still missing. The README needs the note that the mock client verifies the HMAC but not the timestamp's freshness, and that `make clean` is required after a queue-topology change.
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

Expected: only the known false positive (`specs/01-domain/policy.md` matches the ICD-10
regex `\.[0-9A-Z]{1,4}` as if it were a link).

- [x] **Step 5: Commit**

```bash
git add specs/06-roadmap.md docs/plans/2026-09-07-phase-4-evaluation-path.md
git commit -m "docs(phase-4): record the deviations and the Phase 4 carry-overs"
```

---

## Phase exit criteria

Everything below must be true before the phase is called done:

1. `make check` green (lint, `mypy --strict` on domain + application, `mypy src/ecet`, import contracts, default test suite).
2. `uv run pytest -m 'not e2e' --cov=ecet --cov-fail-under=85` green with Docker and `en_core_web_lg` available.
3. `make clean && make demo` shows, in order: a `QUEUED` claim, `llm.evaluated`, `claim.routed route=AUTO_NOTIFY`, `webhook.delivered`, `APPROVED_AUTO`.
4. `curl -s localhost:8081/received | jq '.[0].verified'` is `true` — the HMAC the worker produced verifies against the seeded secret, checked by code that does not share the signing helper.
5. The `note_unclear` drop reaches `REVIEW_PENDING` with an open `review_tasks` row and **no** webhook delivery.
6. `docker compose up --scale worker=3` starts three workers, and one drop produces exactly one webhook delivery.
7. `select claim_id, status, failure_reason from claims` shows no `EVALUATION_FAILED` row for the fake provider, and `select * from review_tasks` shows one open row for the unclear drop.
8. No webhook payload, no queue message and no log line contains a string from `tests/pii.py::PII_STRINGS` (the ADR-001 check, asserted in the unit tests and eyeballed once in `docker compose logs worker`).
9. `POST /v1/claims/ingest` with a bucket other than `claims` answers 404 and performs no HEAD.

## Deviations from spec (record in the PR description)

Fill this in during execution — the list below is what the plan *expects* to deviate on.
Add anything else that comes up, and drop anything that turns out not to be needed.

1. **`RouteDecision` saves the claim but does not commit**; [UC-07](../../specs/02-use-cases/UC-07-route-decision.md) step 4 says "Save, commit". UC-06 owns the unit of work and commits once at the end, so routing and the `EVALUATED` transition land in one transaction rather than two.
2. **`infrastructure/webhook/fake_client.py` and `infrastructure/queue/in_memory.py` are still not built**, though the [project layout](../../specs/05-platform/project-layout.md) lists them. `tests/fakes.py` provides `FakeWebhookClient`, and every compose service uses a real broker and a real HTTP client. Same reasoning as Phase 3 deviation 6.
3. **`EvaluationRequest` and `EvaluationOutput` live in `application/ports/llm_gateway.py`**, not in the use-case module where [UC-06](../../specs/02-use-cases/UC-06-evaluate-claim.md) sketches `EvaluationRequest`. They are the port's vocabulary — the adapters import them and the use case does too; putting them in the use case would make `infrastructure` import a use case.
4. **The adapter tests use `httpx.MockTransport`, not `respx`.** Both specs name respx. A MockTransport handler is a plain function that returns a response, both the openai SDK and `HttpxWebhookClient` accept an injected `http_client`, and this way the test suite gains no dependency.
5. **`EvaluationOutput.confidence` is optional.** [evaluation.md §2](../../specs/01-domain/evaluation.md#2-evaluation-llm-output-vendor-neutral) requires the degradation "missing → INSUFFICIENT_EVIDENCE, 0.0", which is impossible if a missing value is a schema error. The prompt still demands it.
6. **An ICD-10 code the model invents is dropped rather than failing the evaluation**, and a `matched_policy_id` that was not in the request is nulled. Neither is worth sending an otherwise usable evaluation to a human, and both would otherwise be persisted as fact.
7. **`retry_count` is logged by the consumer, not by the message handler.** The worker spec lists it in the per-message log line, but the delivery count is an AMQP header on the incoming message and `EvaluationMessage` deliberately does not carry it.
8. **The mock client receives on `POST /hooks/{name}`, not `POST /webhooks/ecet`.** The [webhook-client spec](../../specs/03-infrastructure/webhook-client.md) names the latter, but the Phase 2 seed already points every tenant at `http://mock-client:8081/hooks/<slug>`, and a per-tenant path is what makes the per-tenant HMAC check meaningful.
9. **The mock client is not part of the `ecet` package and shares no code with it.** It re-derives the HMAC from the shared secret. Importing `sign` would make the signature test verify the sender against itself.
10. **`ECET_S3_BUCKET` is a new setting** the [config spec](../../specs/05-platform/config.md) does not list. It closes Phase 3 carry-over #22: without it both ingestion paths read whatever bucket the caller names.
11. **The worker waits for a healthy api in compose instead of waiting for the schema itself.** `ECET_AUTO_MIGRATE` is api-only, and two processes racing `alembic upgrade head` is worse than starting a few seconds later.
12. **`declare_topology` now returns a `Topology(exchange, queue)` NamedTuple** rather than the bare exchange, so the consumer does not re-declare the queue with its own copy of `QUEUE_ARGUMENTS`. The publisher's one call site takes `.exchange`.
13. **`WebhookTransientError` is classified DLQ, not requeue**, even though it is named "transient". UC-07 catches it and parks the claim in `NOTIFY_FAILED`; one reaching the consumer means a code path forgot to, and requeueing would re-run an LLM evaluation to retry an HTTP POST.
14. **The `does_not_meet.txt` fixture evaluates as `MEETS_NECESSITY` under the fake gateway**, because the fake's rule table keys on the substring "denied" (per the [llm-gateway spec](../../specs/03-infrastructure/llm-gateway.md#fake_gatewaypy)) and that note does not contain it. The fixture exists for the deterministic path; the fake's rules are covered by their own table test.
15. **Requeue has no delay, so the delivery budget is spent in milliseconds.** `RabbitMqConsumer._on_message` nacks with `requeue=True` for transient errors, which triggers immediate redelivery. With `x-delivery-limit: 5` on `claims.evaluate` and `max_retries=0` in the OpenAI gateway (which explicitly defers retrying to the message's own delivery budget), a provider 429 burns all five deliveries in a fraction of a second and dead-letters the claim. The retry mechanism the design chose therefore does not retry over any timescale a rate limit lives on, and with `ecet dlq-replay` deferred to Phase 5 those claims have no recovery path. Not fixed in Phase 4: a delayed-retry queue is a topology change and a plan-level redesign, and this phase ships with `ECET_LLM_PROVIDER=fake` everywhere while the vendor path is already deferred to Phase 5 by deviation 5. Closed by Phase 5, alongside the real-vendor run.
16. **Four compose/Makefile timing fixes the plan did not specify** were needed to make `make demo` pass reliably: the rabbitmq healthcheck uses `gosu` plus `check_port_connectivity` (the root-owned `.erlang.cookie` race, and `ping` reporting healthy before the AMQP listener binds); the api healthcheck gets `start_period: 180s` (worker and minio-setup now gate on `api: service_healthy`, and a 40s start period marks api unhealthy mid-Presidio-load and abandons its dependents); `make demo` runs `docker compose wait minio-setup` before dropping (minio-setup restarts MinIO to pick up the webhook target, and a drop before it finishes produces no notification at all).
