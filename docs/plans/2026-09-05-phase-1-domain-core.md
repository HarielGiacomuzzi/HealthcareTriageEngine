# Phase 1 — Domain Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every ECET business rule expressed as pure Python in `src/ecet/domain/` — claim lifecycle, policy coverage, ICD-10 codes, deterministic checks, triage threshold, review tasks — with zero I/O and zero framework imports.

**Architecture:** Pydantic v2 models only. Value objects are frozen; the two mutable aggregates (`Claim`, `ReviewTask`) use `validate_assignment=True` so a bad mutation fails where it happens. Identifiers live in their own module (`domain/ids.py`) so `claim`, `policy`, `evaluation` and `tenant` can reference each other's ids without an import cycle. Repository contracts are `typing.Protocol` classes in `domain/ports/`; the first implementations are in-memory fakes under `tests/fakes.py`, and Phase 2 writes the Postgres ones against the same Protocols. No use case, no adapter, no wiring in this phase.

**Tech Stack:** Python 3.12, Pydantic 2, pytest, mypy `--strict`, ruff, import-linter. No new runtime dependency is added by this phase.

**Spec:** [`specs/06-roadmap.md` §Phase 1](../../specs/06-roadmap.md#phase-1--domain-core), which pulls in [claim](../../specs/01-domain/claim.md), [policy](../../specs/01-domain/policy.md), [evaluation](../../specs/01-domain/evaluation.md), [tenant](../../specs/01-domain/tenant.md), plus [UC-04](../../specs/02-use-cases/UC-04-run-deterministic-checks.md) (rule details), [UC-07](../../specs/02-use-cases/UC-07-route-decision.md) (triage), [UC-09](../../specs/02-use-cases/UC-09-human-review.md) (review task), [testing](../../specs/05-platform/testing.md), [project-layout](../../specs/05-platform/project-layout.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- `ecet.domain` may import **stdlib and pydantic only**. Never fastapi, sqlalchemy, presidio, boto3, aio_pika, httpx, typer, and never `ecet.config`. `import-linter` enforces this — `make imports` must stay green.
- `mypy --strict` covers `src/ecet/domain`. Every function annotated, no implicit `Any`, no bare `dict`/`list`.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`.
- **No I/O of any kind in this phase** — no file reads, no clocks read implicitly inside a rule, no network, no DB. `datetime.now(UTC)` appears exactly once, as the fallback default of `Claim.transition(..., now=None)`.
- Domain errors all subclass `DomainError`. Nothing in `domain/` raises a bare `Exception`.
- Threshold values are **injected, never read from config inside the domain** (`triage(evaluation, threshold)`).
- Raw, pre-redaction text is never a field on any domain model ([ADR-001](../../specs/00-overview.md#4-adrs)). `RedactedText.text` is the only text a domain model holds.
- TDD: every step-pair is "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Commit after every task with a conventional prefix (`feat:`, `test:`, `chore:`). No Claude attribution in commit messages.
- Test layout per the [testing spec](../../specs/05-platform/testing.md): domain tests in `tests/unit/domain/`, fakes in `tests/fakes.py`.

### Explicitly out of scope for Phase 1 (do not add)

- Any `application/`, `infrastructure/` or `interfaces/` code — no use cases, no adapters, no wiring, no `container.py`.
- Alembic, ORM classes, mappers, SQL seeds (Phase 2).
- Application ports (`ObjectStorage`, `TextExtractor`, `PiiRedactor`, `EvaluationQueue`, `LLMGateway`, `WebhookClient`, `Clock`, `UnitOfWork`) — those are `application/ports/`, Phase 3/4. Only the four **domain repository** ports land here.
- `assert_no_pii` and `tests/fixtures/` (Phase 3 — nothing in Phase 1 handles unredacted text).
- Prompt text, `PolicySnapshot`, `EvaluationMessage`, `ClientNotification` — all `application/`, later phases.

---

### Task 1: Identifiers, domain errors, Tenant

**Files:**
- Create: `src/ecet/domain/ids.py`, `src/ecet/domain/errors.py`, `src/ecet/domain/tenant.py`
- Test: `tests/unit/domain/test_ids.py`, `tests/unit/domain/test_tenant.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `ecet.domain.ids`: `TENANT_ID_BODY: str`, `TENANT_ID_PATTERN: str`, `ClaimId = NewType("ClaimId", UUID)`, `PolicyId = NewType("PolicyId", UUID)`, `TenantId = NewType("TenantId", str)`, `TenantIdField = Annotated[TenantId, StringConstraints(pattern=TENANT_ID_PATTERN)]`.
  - `ecet.domain.errors`: `DomainError`, `InvalidObjectKey`, `InvalidTransition`, `PdfTooLarge`, `NoPoliciesForTenant`, `ClaimNotFound`, `TenantNotFound`, `ReviewTaskNotFound`, `ReviewAlreadyResolved`, `ConcurrentModification` (all `Exception` subclasses, no custom `__init__`).
  - `ecet.domain.tenant`: `Tenant` (frozen pydantic model), re-exports `TenantId`, `TenantIdField`.

- [ ] **Step 1: Create the test package directory**

```bash
cd /Users/harielgiacomuzzi/Development/FDE-Study/HealthcareTriageEngine
mkdir -p tests/unit/domain
```

- [ ] **Step 2: Write the failing tests**

`tests/unit/domain/test_ids.py`:

```python
import re
from uuid import uuid4

from ecet.domain.ids import TENANT_ID_PATTERN, ClaimId, PolicyId, TenantId


def test_ids_are_transparent_at_runtime() -> None:
    """NewType is a typing-only wrapper: the value is the plain UUID / str."""
    raw = uuid4()
    assert ClaimId(raw) == raw
    assert PolicyId(raw) == raw
    assert TenantId("tenant-a") == "tenant-a"


def test_tenant_id_pattern_matches_the_s3_key_segment() -> None:
    assert re.match(TENANT_ID_PATTERN, "tenant-a")
    assert re.match(TENANT_ID_PATTERN, "t1")
    assert not re.match(TENANT_ID_PATTERN, "Tenant-A")
    assert not re.match(TENANT_ID_PATTERN, "a")
```

`tests/unit/domain/test_tenant.py`:

```python
from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from ecet.domain.tenant import Tenant


def build_tenant(**overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": "tenant-a",
        "name": "Tenant A",
        "webhook_url": "http://mock-client:9000/hooks/ecet",
        "webhook_secret": SecretStr("s3cret"),
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


@pytest.mark.parametrize("raw", ["tenant-a", "t1", "a0-b1-c2", "x" * 63])
def test_valid_tenant_ids_are_accepted(raw: str) -> None:
    assert build_tenant(id=raw).id == raw


@pytest.mark.parametrize(
    "raw",
    ["", "a", "-lead", "UPPER", "under_score", "tenant a", "x" * 64],
)
def test_invalid_tenant_ids_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        build_tenant(id=raw)


def test_tenant_is_frozen() -> None:
    tenant = build_tenant()
    with pytest.raises(ValidationError):
        tenant.name = "renamed"  # type: ignore[misc]


def test_tenant_defaults_to_active() -> None:
    assert build_tenant().active is True


def test_webhook_secret_never_appears_in_a_dump() -> None:
    """ADR-001 hygiene: the HMAC key must not leak through model_dump / repr."""
    tenant = build_tenant()
    assert "s3cret" not in str(tenant.model_dump())
    assert "s3cret" not in repr(tenant)
    assert tenant.webhook_secret.get_secret_value() == "s3cret"


def test_empty_name_is_rejected() -> None:
    with pytest.raises(ValidationError):
        build_tenant(name="")
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/domain -v`
Expected: collection errors — `ModuleNotFoundError: No module named 'ecet.domain.ids'`.

- [ ] **Step 4: Write `src/ecet/domain/ids.py`**

```python
"""Identifier value objects.

Their own module so `claim`, `policy`, `evaluation` and `tenant` can each reference
another's identifier without an import cycle.
"""

from typing import Annotated, NewType
from uuid import UUID

from pydantic import StringConstraints

TENANT_ID_BODY = r"[a-z0-9][a-z0-9-]{1,62}"
TENANT_ID_PATTERN = rf"^{TENANT_ID_BODY}$"

ClaimId = NewType("ClaimId", UUID)
PolicyId = NewType("PolicyId", UUID)
TenantId = NewType("TenantId", str)

#: Use on a pydantic model field; validates the S3-key-safe tenant slug.
TenantIdField = Annotated[TenantId, StringConstraints(pattern=TENANT_ID_PATTERN)]
```

- [ ] **Step 5: Write `src/ecet/domain/errors.py`**

```python
"""Domain errors. Everything the domain can refuse to do raises one of these."""


class DomainError(Exception):
    """Base class for every domain-level failure."""


class InvalidObjectKey(DomainError):
    """Object key does not match `tenants/{tenant_id}/claims/{name}.pdf`."""


class InvalidTransition(DomainError):
    """Claim status transition is not allowed from the current status."""


class PdfTooLarge(DomainError):
    """Source object is larger than the configured `max_pdf_bytes`."""


class NoPoliciesForTenant(DomainError):
    """Tenant has no active, effective policy (ADR-005 — hard failure, no LLM call)."""


class ClaimNotFound(DomainError):
    """No claim with that id."""


class TenantNotFound(DomainError):
    """No active tenant with that id."""


class ReviewTaskNotFound(DomainError):
    """No review task with that id for that tenant."""


class ReviewAlreadyResolved(DomainError):
    """Review task has already been resolved."""


class ConcurrentModification(DomainError):
    """Optimistic save lost: the row changed since it was read."""
```

- [ ] **Step 6: Write `src/ecet/domain/tenant.py`**

```python
"""Tenant: owns webhook configuration and the data-isolation boundary."""

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr

from ecet.domain.ids import TenantId, TenantIdField

__all__ = ["Tenant", "TenantId", "TenantIdField"]


class Tenant(BaseModel):
    """A client of the platform. Never mutated in v1 — seeded and read."""

    model_config = ConfigDict(frozen=True)

    id: TenantIdField
    name: str = Field(min_length=1)
    webhook_url: HttpUrl
    webhook_secret: SecretStr
    active: bool = True
```

> If pydantic rejects `Annotated[NewType, StringConstraints(...)]` on your resolved
> pydantic version, replace `TenantIdField` in `ids.py` with
> `TenantIdField = Annotated[str, StringConstraints(pattern=TENANT_ID_PATTERN)]`
> and keep `TenantId` as the `NewType` used in function signatures. Nothing else changes.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/domain -v`
Expected: PASS, all of `test_ids.py` and `test_tenant.py`.

- [ ] **Step 8: Run the full gate**

Run: `make check`
Expected: ruff clean, mypy clean (strict on domain), `lint-imports` green, whole suite passing.

- [ ] **Step 9: Commit**

```bash
git add src/ecet/domain/ids.py src/ecet/domain/errors.py src/ecet/domain/tenant.py tests/unit/domain
git commit -m "feat(domain): identifiers, domain errors and the Tenant entity"
```

---

### Task 2: Policy and Icd10Code

**Files:**
- Create: `src/ecet/domain/policy.py`
- Test: `tests/unit/domain/test_policy.py`

**Interfaces:**
- Consumes: `ecet.domain.ids` (`PolicyId`, `TenantIdField`).
- Produces: `ICD10_PATTERN: str`, `Icd10Code` (frozen, hashable, field `code: str`, `__str__` returns the code), `Policy` (frozen, method `is_effective(on: date) -> bool`), re-export `PolicyId`.

- [ ] **Step 1: Write the failing test**

`tests/unit/domain/test_policy.py`:

```python
from datetime import date
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code, Policy


def code(raw: str) -> Icd10Code:
    return Icd10Code(code=raw)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {code("M54.5")},
        "excluded_codes": {code("Z00.0")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "required_evidence": ["conservative therapy >= 6 weeks", "imaging report"],
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


@pytest.mark.parametrize("raw", ["M54.5", "G89", "Z00.00", "T81.4XXA", "A01"])
def test_valid_icd10_codes_are_accepted(raw: str) -> None:
    assert code(raw).code == raw


@pytest.mark.parametrize("raw", ["U07.1", "154.5", "M5", "M54.", "M54.ABCDE", "", "MM4"])
def test_invalid_icd10_codes_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        code(raw)


def test_icd10_codes_are_normalised() -> None:
    assert code("  m54.5 ").code == "M54.5"


def test_icd10_codes_are_hashable_and_compare_by_value() -> None:
    assert {code("M54.5"), code("m54.5")} == {code("M54.5")}
    assert str(code("M54.5")) == "M54.5"


def test_covered_and_excluded_codes_may_not_overlap() -> None:
    with pytest.raises(ValidationError):
        build_policy(covered_codes={code("M54.5")}, excluded_codes={code("M54.5")})


def test_criteria_text_must_be_present() -> None:
    with pytest.raises(ValidationError):
        build_policy(criteria_text="")


def test_version_starts_at_one() -> None:
    with pytest.raises(ValidationError):
        build_policy(version=0)


def test_effective_to_may_not_precede_effective_from() -> None:
    with pytest.raises(ValidationError):
        build_policy(effective_from=date(2026, 5, 1), effective_to=date(2026, 4, 30))


@pytest.mark.parametrize(
    ("on", "expected"),
    [
        (date(2025, 12, 31), False),
        (date(2026, 1, 1), True),
        (date(2026, 6, 30), True),
        (date(2026, 7, 1), False),
    ],
)
def test_is_effective_is_inclusive_on_both_ends(on: date, expected: bool) -> None:
    policy = build_policy(effective_from=date(2026, 1, 1), effective_to=date(2026, 6, 30))
    assert policy.is_effective(on) is expected


def test_open_ended_policy_stays_effective() -> None:
    policy = build_policy(effective_to=None)
    assert policy.is_effective(date(2099, 1, 1)) is True


def test_inactive_policy_is_never_effective() -> None:
    policy = build_policy(active=False)
    assert policy.is_effective(date(2026, 6, 1)) is False
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/domain/test_policy.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.domain.policy'`.

- [ ] **Step 3: Write `src/ecet/domain/policy.py`**

```python
"""Policy: a tenant-owned rule saying when a diagnosis justifies a service.

Read-only in v1 — policies are seeded, not managed through the API.
"""

from datetime import date
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ecet.domain.ids import PolicyId, TenantIdField

__all__ = ["ICD10_PATTERN", "Icd10Code", "Policy", "PolicyId"]

ICD10_PATTERN = r"^[A-TV-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$"


class Icd10Code(BaseModel):
    """A single normalised ICD-10 code. Frozen, therefore hashable and set-safe."""

    model_config = ConfigDict(frozen=True)

    code: Annotated[str, Field(pattern=ICD10_PATTERN)]

    @field_validator("code", mode="before")
    @classmethod
    def _normalise(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    def __str__(self) -> str:
        return self.code


class Policy(BaseModel):
    """One version of one coverage rule. Only the highest active version is used."""

    model_config = ConfigDict(frozen=True)

    id: PolicyId
    tenant_id: TenantIdField
    name: str = Field(min_length=1)
    version: int = Field(ge=1)
    covered_codes: set[Icd10Code] = Field(default_factory=set)
    excluded_codes: set[Icd10Code] = Field(default_factory=set)
    criteria_text: str = Field(min_length=1)
    required_evidence: list[str] = Field(default_factory=list)
    active: bool = True
    effective_from: date
    effective_to: date | None = None

    @model_validator(mode="after")
    def _check_invariants(self) -> Self:
        overlap = self.covered_codes & self.excluded_codes
        if overlap:
            listed = ", ".join(sorted(item.code for item in overlap))
            raise ValueError(f"codes are both covered and excluded: {listed}")
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not precede effective_from")
        return self

    def is_effective(self, on: date) -> bool:
        """Active and inside the effective window (both ends inclusive)."""
        return (
            self.active
            and self.effective_from <= on
            and (self.effective_to is None or on <= self.effective_to)
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/domain/test_policy.py -v`
Expected: PASS.

- [ ] **Step 5: Run the gate and commit**

```bash
make check
git add src/ecet/domain/policy.py tests/unit/domain/test_policy.py
git commit -m "feat(domain): Icd10Code value object and Policy entity"
```

---

### Task 3: Evaluation, triage and ReviewTask

**Files:**
- Create: `src/ecet/domain/evaluation.py`
- Test: `tests/unit/domain/test_evaluation.py`

**Interfaces:**
- Consumes: `ecet.domain.ids` (`ClaimId`, `PolicyId`, `TenantIdField`), `ecet.domain.policy` (`Icd10Code`), `ecet.domain.errors` (`ReviewAlreadyResolved`).
- Produces:
  - `Verdict` (`PASS` / `REJECT` / `UNCERTAIN`), `CheckOutcome(name, passed, detail)`, `DeterministicResult(verdict, checks)`.
  - `Decision` (`MEETS_NECESSITY` / `DOES_NOT_MEET` / `INSUFFICIENT_EVIDENCE`), `Evaluation` (frozen, all vendor-neutral fields).
  - `Route` (`AUTO_NOTIFY` / `HUMAN_REVIEW`) and `triage(evaluation: Evaluation, threshold: float) -> Route`.
  - `ReviewReason`, `ReviewStatus`, `ReviewTask` with `resolve(*, resolution, reviewer, notes, now) -> None`.

- [ ] **Step 1: Write the failing test**

`tests/unit/domain/test_evaluation.py`:

```python
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ecet.domain.errors import ReviewAlreadyResolved
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
    Route,
    Verdict,
    triage,
)
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def build_evaluation(**overrides: Any) -> Evaluation:
    fields: dict[str, Any] = {
        "decision": Decision.MEETS_NECESSITY,
        "confidence": 0.9,
        "matched_policy_id": PolicyId(uuid4()),
        "cited_codes": [Icd10Code(code="M54.5")],
        "rationale": "Conservative therapy documented for eight weeks.",
        "evidence_found": ["imaging report"],
        "evidence_missing": [],
        "model": "claude-sonnet-5",
        "prompt_version": "v1",
        "latency_ms": 1200,
        "input_tokens": 900,
        "output_tokens": 120,
    }
    fields.update(overrides)
    return Evaluation.model_validate(fields)


def build_task(**overrides: Any) -> ReviewTask:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "claim_id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "reason": ReviewReason.LOW_CONFIDENCE,
        "created_at": NOW,
    }
    fields.update(overrides)
    return ReviewTask.model_validate(fields)


@pytest.mark.parametrize("confidence", [-0.01, 1.01])
def test_confidence_outside_zero_to_one_is_rejected(confidence: float) -> None:
    with pytest.raises(ValidationError):
        build_evaluation(confidence=confidence)


def test_rationale_is_capped_at_two_thousand_characters() -> None:
    with pytest.raises(ValidationError):
        build_evaluation(rationale="x" * 2001)


def test_evaluation_is_frozen() -> None:
    evaluation = build_evaluation()
    with pytest.raises(ValidationError):
        evaluation.confidence = 0.1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("decision", "confidence", "expected"),
    [
        (Decision.MEETS_NECESSITY, 0.85, Route.AUTO_NOTIFY),
        (Decision.MEETS_NECESSITY, 0.8499, Route.HUMAN_REVIEW),
        (Decision.DOES_NOT_MEET, 0.99, Route.AUTO_NOTIFY),
        (Decision.INSUFFICIENT_EVIDENCE, 0.99, Route.HUMAN_REVIEW),
        (Decision.MEETS_NECESSITY, 0.0, Route.HUMAN_REVIEW),
    ],
)
def test_triage_boundaries(decision: Decision, confidence: float, expected: Route) -> None:
    evaluation = build_evaluation(decision=decision, confidence=confidence)
    assert triage(evaluation, threshold=0.85) is expected


def test_triage_uses_the_injected_threshold() -> None:
    evaluation = build_evaluation(confidence=0.7)
    assert triage(evaluation, threshold=0.6) is Route.AUTO_NOTIFY
    assert triage(evaluation, threshold=0.8) is Route.HUMAN_REVIEW


def test_deterministic_result_round_trips_as_json() -> None:
    result = DeterministicResult(
        verdict=Verdict.UNCERTAIN,
        checks=[CheckOutcome(name="icd10_present", passed=False, detail="no codes found")],
    )
    assert DeterministicResult.model_validate(result.model_dump(mode="json")) == result


def test_review_task_starts_open_and_unresolved() -> None:
    task = build_task()
    assert task.status is ReviewStatus.OPEN
    assert task.resolution is None
    assert task.resolved_at is None


def test_resolving_a_task_records_the_decision() -> None:
    task = build_task()
    task.resolve(
        resolution=Decision.DOES_NOT_MEET,
        reviewer="nurse@tenant-a.example",
        notes="No conservative therapy documented.",
        now=NOW,
    )
    assert task.status is ReviewStatus.RESOLVED
    assert task.resolution is Decision.DOES_NOT_MEET
    assert task.reviewer == "nurse@tenant-a.example"
    assert task.resolved_at == NOW


def test_resolving_twice_raises() -> None:
    task = build_task()
    task.resolve(resolution=Decision.MEETS_NECESSITY, reviewer="a", notes=None, now=NOW)
    with pytest.raises(ReviewAlreadyResolved):
        task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="b", notes=None, now=NOW)


def test_a_human_may_not_resolve_as_insufficient_evidence() -> None:
    task = build_task()
    with pytest.raises(ValueError, match="MEETS_NECESSITY"):
        task.resolve(
            resolution=Decision.INSUFFICIENT_EVIDENCE,
            reviewer="a",
            notes=None,
            now=NOW,
        )
    assert task.status is ReviewStatus.OPEN
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/domain/test_evaluation.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.domain.evaluation'`.

- [ ] **Step 3: Write `src/ecet/domain/evaluation.py`**

```python
"""Evaluation results and the triage decision.

Three separate things live here because they are the same subject at three stages:
what the cheap rules concluded (`DeterministicResult`), what the LLM concluded
(`Evaluation`), and what a human is asked to conclude (`ReviewTask`).
"""

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ecet.domain.errors import ReviewAlreadyResolved
from ecet.domain.ids import ClaimId, PolicyId, TenantIdField
from ecet.domain.policy import Icd10Code


class Verdict(StrEnum):
    """Outcome of the deterministic pre-LLM checks (ADR-002)."""

    PASS = "PASS"
    REJECT = "REJECT"
    UNCERTAIN = "UNCERTAIN"


class CheckOutcome(BaseModel):
    """One deterministic rule's result."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    passed: bool
    detail: str = ""


class DeterministicResult(BaseModel):
    """Aggregated deterministic checks. Persisted as jsonb on the claim."""

    model_config = ConfigDict(frozen=True)

    verdict: Verdict
    checks: list[CheckOutcome] = Field(default_factory=list)


class Decision(StrEnum):
    """Medical-necessity decision, vendor-neutral."""

    MEETS_NECESSITY = "MEETS_NECESSITY"
    DOES_NOT_MEET = "DOES_NOT_MEET"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class Evaluation(BaseModel):
    """Validated LLM output. Vendor JSON is parsed straight into this model."""

    model_config = ConfigDict(frozen=True)

    decision: Decision
    confidence: float = Field(ge=0.0, le=1.0)
    matched_policy_id: PolicyId | None = None
    cited_codes: list[Icd10Code] = Field(default_factory=list)
    rationale: str = Field(default="", max_length=2000)
    evidence_found: list[str] = Field(default_factory=list)
    evidence_missing: list[str] = Field(default_factory=list)
    model: str = ""
    prompt_version: str = ""
    latency_ms: int = Field(default=0, ge=0)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


class Route(StrEnum):
    AUTO_NOTIFY = "AUTO_NOTIFY"
    HUMAN_REVIEW = "HUMAN_REVIEW"


def triage(evaluation: Evaluation, threshold: float) -> Route:
    """ADR-003: only a confident, decisive evaluation may skip a human.

    `threshold` is injected by the caller (`ECET_CONFIDENCE_THRESHOLD`); the domain
    never reads configuration.
    """
    if evaluation.decision is Decision.INSUFFICIENT_EVIDENCE:
        return Route.HUMAN_REVIEW
    return Route.AUTO_NOTIFY if evaluation.confidence >= threshold else Route.HUMAN_REVIEW


class ReviewReason(StrEnum):
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    DETERMINISTIC_REJECT = "DETERMINISTIC_REJECT"
    DETERMINISTIC_UNCERTAIN_LLM_LOW = "DETERMINISTIC_UNCERTAIN_LLM_LOW"
    EVALUATION_FAILED = "EVALUATION_FAILED"


class ReviewStatus(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


HumanResolution = Literal[Decision.MEETS_NECESSITY, Decision.DOES_NOT_MEET]


class ReviewTask(BaseModel):
    """Human-in-the-loop work item. One open task per claim."""

    model_config = ConfigDict(validate_assignment=True)

    id: UUID
    claim_id: ClaimId
    tenant_id: TenantIdField
    reason: ReviewReason
    status: ReviewStatus = ReviewStatus.OPEN
    resolution: HumanResolution | None = None
    reviewer: str | None = None
    notes: str | None = None
    created_at: AwareDatetime
    resolved_at: AwareDatetime | None = None

    def resolve(
        self,
        *,
        resolution: Decision,
        reviewer: str,
        notes: str | None,
        now: datetime,
    ) -> None:
        """Record a human decision. `now` is injected — the domain owns no clock."""
        if self.status is ReviewStatus.RESOLVED:
            raise ReviewAlreadyResolved(str(self.id))
        if resolution is Decision.INSUFFICIENT_EVIDENCE:
            raise ValueError("resolution must be MEETS_NECESSITY or DOES_NOT_MEET")
        self.resolution = resolution
        self.reviewer = reviewer
        self.notes = notes
        self.resolved_at = now
        self.status = ReviewStatus.RESOLVED
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/domain/test_evaluation.py -v`
Expected: PASS.

- [ ] **Step 5: Run the gate and commit**

```bash
make check
git add src/ecet/domain/evaluation.py tests/unit/domain/test_evaluation.py
git commit -m "feat(domain): evaluation results, triage decision and review tasks"
```

---

### Task 4: Claim aggregate and its state machine

**Files:**
- Create: `src/ecet/domain/claim.py`
- Test: `tests/unit/domain/test_claim.py`

**Interfaces:**
- Consumes: `ecet.domain.ids` (`ClaimId`, `PolicyId`, `TenantId`, `TenantIdField`, `TENANT_ID_BODY`), `ecet.domain.errors` (`InvalidObjectKey`, `InvalidTransition`, `PdfTooLarge`), `ecet.domain.evaluation` (`DeterministicResult`, `Evaluation`).
- Produces:
  - `OBJECT_KEY_RE: re.Pattern[str]`, `SourceObject` (frozen; `tenant_id() -> TenantId`, `ensure_size_within(max_bytes: int) -> None`).
  - `RedactedText` (frozen: `text`, `entity_counts`, `redactor`).
  - `ClaimStatus` (StrEnum) with `can_transition_to(nxt) -> bool`, `FAILURE_STATUSES: frozenset[ClaimStatus]`.
  - `Claim` with `transition(nxt, *, reason=None, now=None) -> None`.

**The transition table this task encodes** — derived from the [state list](../../specs/01-domain/claim.md#2-state-machine) plus the ADR-002 short-circuit; the two edges the state list does not draw explicitly are marked and justified in *Deviations* below:

| From | May go to |
|---|---|
| `RECEIVED` | `EXTRACTED`, `EXTRACTION_FAILED` |
| `EXTRACTED` | `REDACTED` |
| `REDACTED` | `POLICIES_ATTACHED`, `NO_POLICIES` |
| `POLICIES_ATTACHED` | `QUEUED`, `REVIEW_PENDING` *(ADR-002 short-circuit)* |
| `QUEUED` | `EVALUATED`, `EVALUATION_FAILED` |
| `EVALUATED` | `APPROVED_AUTO`, `REVIEW_PENDING` |
| `APPROVED_AUTO` | `NOTIFY_FAILED` |
| `REVIEW_PENDING` | `REVIEW_RESOLVED` |
| `REVIEW_RESOLVED` | `NOTIFY_FAILED` |
| `NOTIFY_FAILED` | `APPROVED_AUTO`, `REVIEW_RESOLVED` *(added — `POST /v1/claims/{id}/retry-notify`)* |
| `EVALUATION_FAILED` | `REVIEW_RESOLVED` *(added — UC-06 opens a review task on invalid LLM output)* |
| `EXTRACTION_FAILED`, `NO_POLICIES` | — (terminal) |

- [ ] **Step 1: Write the failing test**

`tests/unit/domain/test_claim.py`:

```python
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.errors import InvalidObjectKey, InvalidTransition, PdfTooLarge
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.ids import ClaimId

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
LATER = datetime(2026, 9, 5, 12, 5, tzinfo=UTC)

VALID_KEY = "tenants/tenant-a/claims/2026-09-05-mri.pdf"


def build_source(**overrides: Any) -> SourceObject:
    fields: dict[str, Any] = {
        "bucket": "claims",
        "key": VALID_KEY,
        "etag": "d41d8cd98f00b204e9800998ecf8427e",
        "size": 12_345,
    }
    fields.update(overrides)
    return SourceObject.model_validate(fields)


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": build_source(),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


def test_tenant_is_parsed_out_of_the_object_key() -> None:
    assert build_source().tenant_id() == "tenant-a"


@pytest.mark.parametrize(
    "key",
    [
        "tenant-a/claims/x.pdf",
        "tenants//claims/x.pdf",
        "tenants/TENANT-A/claims/x.pdf",
        "tenants/tenant-a/x.pdf",
        "tenants/tenant-a/claims/x.txt",
        "tenants/tenant-a/claims/nested/x.pdf",
        "",
    ],
)
def test_invalid_object_keys_raise(key: str) -> None:
    with pytest.raises(InvalidObjectKey):
        build_source(key=key)


@pytest.mark.parametrize("size", [0, -1])
def test_non_positive_size_is_rejected(size: int) -> None:
    with pytest.raises(ValidationError):
        build_source(size=size)


def test_size_limit_is_injected_not_read_from_config() -> None:
    source = build_source(size=1000)
    source.ensure_size_within(1000)
    with pytest.raises(PdfTooLarge):
        source.ensure_size_within(999)


def test_a_new_claim_starts_received() -> None:
    assert build_claim().status is ClaimStatus.RECEIVED


def test_legal_transition_bumps_updated_at() -> None:
    claim = build_claim()
    claim.transition(ClaimStatus.EXTRACTED, now=LATER)
    assert claim.status is ClaimStatus.EXTRACTED
    assert claim.updated_at == LATER


def test_illegal_transition_raises_and_changes_nothing() -> None:
    claim = build_claim()
    with pytest.raises(InvalidTransition):
        claim.transition(ClaimStatus.APPROVED_AUTO, now=LATER)
    assert claim.status is ClaimStatus.RECEIVED
    assert claim.updated_at == NOW


def test_the_happy_path_walks_end_to_end() -> None:
    claim = build_claim()
    for nxt in [
        ClaimStatus.EXTRACTED,
        ClaimStatus.REDACTED,
        ClaimStatus.POLICIES_ATTACHED,
        ClaimStatus.QUEUED,
        ClaimStatus.EVALUATED,
        ClaimStatus.APPROVED_AUTO,
    ]:
        claim.transition(nxt, now=LATER)
    assert claim.status is ClaimStatus.APPROVED_AUTO


def test_deterministic_reject_short_circuits_to_review() -> None:
    """ADR-002: a REJECT verdict skips QUEUED and EVALUATED entirely."""
    claim = build_claim(status=ClaimStatus.POLICIES_ATTACHED)
    claim.transition(ClaimStatus.REVIEW_PENDING, now=LATER)
    assert claim.status is ClaimStatus.REVIEW_PENDING


def test_failure_transition_requires_a_reason() -> None:
    claim = build_claim()
    with pytest.raises(InvalidTransition, match="reason"):
        claim.transition(ClaimStatus.EXTRACTION_FAILED, now=LATER)
    claim.transition(ClaimStatus.EXTRACTION_FAILED, reason="encrypted pdf", now=LATER)
    assert claim.failure_reason == "encrypted pdf"


def test_terminal_states_have_no_way_out() -> None:
    for terminal in (ClaimStatus.EXTRACTION_FAILED, ClaimStatus.NO_POLICIES):
        for nxt in ClaimStatus:
            assert terminal.can_transition_to(nxt) is False


def test_notify_failed_can_be_retried() -> None:
    claim = build_claim(status=ClaimStatus.NOTIFY_FAILED)
    claim.transition(ClaimStatus.APPROVED_AUTO, now=LATER)
    assert claim.status is ClaimStatus.APPROVED_AUTO


def test_transition_defaults_to_now_when_no_clock_is_given() -> None:
    claim = build_claim()
    before = datetime.now(UTC)
    claim.transition(ClaimStatus.EXTRACTED)
    assert before <= claim.updated_at <= datetime.now(UTC)


def test_claim_json_round_trip_is_stable() -> None:
    claim = build_claim(
        redacted=RedactedText(text="note", entity_counts={"PERSON": 3}, redactor="presidio-2.2.x"),
        deterministic=DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="non_empty_text", passed=True, detail="120 characters")],
        ),
    )
    assert Claim.model_validate(claim.model_dump(mode="json")) == claim


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError):
        build_claim(created_at=datetime(2026, 9, 5, 12, 0))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/domain/test_claim.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.domain.claim'`.

- [ ] **Step 3: Write `src/ecet/domain/claim.py`**

```python
"""Claim: the aggregate root. Owns the document reference, its redacted text and
the lifecycle every other component reads and advances.
"""

import re
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from ecet.domain.errors import InvalidObjectKey, InvalidTransition, PdfTooLarge
from ecet.domain.evaluation import DeterministicResult, Evaluation
from ecet.domain.ids import TENANT_ID_BODY, ClaimId, PolicyId, TenantId, TenantIdField

__all__ = [
    "OBJECT_KEY_RE",
    "Claim",
    "ClaimId",
    "ClaimStatus",
    "RedactedText",
    "SourceObject",
]

OBJECT_KEY_RE = re.compile(rf"^tenants/(?P<tenant>{TENANT_ID_BODY})/claims/[^/]+\.pdf$")


class SourceObject(BaseModel):
    """The S3 object the claim came from. `(bucket, key, etag)` is the idempotency
    key (ADR-006)."""

    model_config = ConfigDict(frozen=True)

    bucket: str = Field(min_length=1)
    key: str
    etag: str = Field(min_length=1)
    size: int = Field(gt=0)

    @field_validator("key")
    @classmethod
    def _key_matches_the_tenant_layout(cls, value: str) -> str:
        if OBJECT_KEY_RE.match(value) is None:
            raise InvalidObjectKey(
                f"expected tenants/{{tenant_id}}/claims/{{name}}.pdf, got {value!r}"
            )
        return value

    def tenant_id(self) -> TenantId:
        match = OBJECT_KEY_RE.match(self.key)
        if match is None:  # pragma: no cover - the field validator guarantees a match
            raise InvalidObjectKey(self.key)
        return TenantId(match.group("tenant"))

    def ensure_size_within(self, max_bytes: int) -> None:
        """`max_bytes` is injected (config `ECET_MAX_PDF_BYTES`); the domain reads no config."""
        if self.size > max_bytes:
            raise PdfTooLarge(f"{self.size} bytes exceeds the {max_bytes} byte limit")


class RedactedText(BaseModel):
    """Post-redaction text plus the audit trail of what was removed.

    ADR-001: raw text is never a field on any domain model — this is the only text
    a claim ever holds.
    """

    model_config = ConfigDict(frozen=True)

    text: str
    entity_counts: dict[str, int] = Field(default_factory=dict)
    redactor: str = Field(min_length=1)


class ClaimStatus(StrEnum):
    RECEIVED = "RECEIVED"
    EXTRACTED = "EXTRACTED"
    REDACTED = "REDACTED"
    POLICIES_ATTACHED = "POLICIES_ATTACHED"
    QUEUED = "QUEUED"
    EVALUATED = "EVALUATED"
    APPROVED_AUTO = "APPROVED_AUTO"
    REVIEW_PENDING = "REVIEW_PENDING"
    REVIEW_RESOLVED = "REVIEW_RESOLVED"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    NO_POLICIES = "NO_POLICIES"
    EVALUATION_FAILED = "EVALUATION_FAILED"
    NOTIFY_FAILED = "NOTIFY_FAILED"

    def can_transition_to(self, nxt: "ClaimStatus") -> bool:
        return nxt in _ALLOWED[self]


FAILURE_STATUSES = frozenset(
    {
        ClaimStatus.EXTRACTION_FAILED,
        ClaimStatus.NO_POLICIES,
        ClaimStatus.EVALUATION_FAILED,
        ClaimStatus.NOTIFY_FAILED,
    }
)

_ALLOWED: dict[ClaimStatus, frozenset[ClaimStatus]] = {
    ClaimStatus.RECEIVED: frozenset({ClaimStatus.EXTRACTED, ClaimStatus.EXTRACTION_FAILED}),
    ClaimStatus.EXTRACTED: frozenset({ClaimStatus.REDACTED}),
    ClaimStatus.REDACTED: frozenset({ClaimStatus.POLICIES_ATTACHED, ClaimStatus.NO_POLICIES}),
    # POLICIES_ATTACHED -> REVIEW_PENDING is the ADR-002 deterministic short-circuit.
    ClaimStatus.POLICIES_ATTACHED: frozenset({ClaimStatus.QUEUED, ClaimStatus.REVIEW_PENDING}),
    ClaimStatus.QUEUED: frozenset({ClaimStatus.EVALUATED, ClaimStatus.EVALUATION_FAILED}),
    ClaimStatus.EVALUATED: frozenset({ClaimStatus.APPROVED_AUTO, ClaimStatus.REVIEW_PENDING}),
    ClaimStatus.APPROVED_AUTO: frozenset({ClaimStatus.NOTIFY_FAILED}),
    ClaimStatus.REVIEW_PENDING: frozenset({ClaimStatus.REVIEW_RESOLVED}),
    ClaimStatus.REVIEW_RESOLVED: frozenset({ClaimStatus.NOTIFY_FAILED}),
    # Operator retry: POST /v1/claims/{id}/retry-notify.
    ClaimStatus.NOTIFY_FAILED: frozenset(
        {ClaimStatus.APPROVED_AUTO, ClaimStatus.REVIEW_RESOLVED}
    ),
    # UC-06 opens a review task on invalid LLM output; a human still resolves it.
    ClaimStatus.EVALUATION_FAILED: frozenset({ClaimStatus.REVIEW_RESOLVED}),
    ClaimStatus.EXTRACTION_FAILED: frozenset(),
    ClaimStatus.NO_POLICIES: frozenset(),
}


class Claim(BaseModel):
    """One claim document travelling the pipeline."""

    model_config = ConfigDict(validate_assignment=True)

    id: ClaimId
    tenant_id: TenantIdField
    source: SourceObject
    status: ClaimStatus = ClaimStatus.RECEIVED
    redacted: RedactedText | None = None
    policy_ids: list[PolicyId] = Field(default_factory=list)
    deterministic: DeterministicResult | None = None
    evaluation: Evaluation | None = None
    failure_reason: str | None = None
    notification_attempts: int = Field(default=0, ge=0)
    last_notify_error: str | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime

    def transition(
        self,
        nxt: ClaimStatus,
        *,
        reason: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Advance the lifecycle. Raises `InvalidTransition` on an illegal edge or on a
        failure state with no reason. Every transition bumps `updated_at`."""
        if not self.status.can_transition_to(nxt):
            raise InvalidTransition(f"{self.status} -> {nxt} is not an allowed transition")
        if nxt in FAILURE_STATUSES and not reason:
            raise InvalidTransition(f"transition to {nxt} requires a reason")
        self.status = nxt
        if reason is not None:
            self.failure_reason = reason
        self.updated_at = now if now is not None else datetime.now(UTC)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/domain/test_claim.py -v`
Expected: PASS.

- [ ] **Step 5: Run the gate and commit**

```bash
make check
git add src/ecet/domain/claim.py tests/unit/domain/test_claim.py
git commit -m "feat(domain): Claim aggregate with the lifecycle state machine"
```

---

### Task 5: Deterministic rules (ADR-002)

**Files:**
- Create: `src/ecet/domain/rules.py`
- Test: `tests/unit/domain/test_rules.py`

**Interfaces:**
- Consumes: `ecet.domain.claim` (`RedactedText`), `ecet.domain.policy` (`Icd10Code`, `Policy`), `ecet.domain.evaluation` (`CheckOutcome`, `DeterministicResult`, `Verdict`).
- Produces:
  - `MIN_TEXT_CHARS: int = 50`, `ICD10_TOKEN_RE: re.Pattern[str]`.
  - `extract_icd10_codes(text: str) -> list[Icd10Code]` — first-seen order, de-duplicated, normalised.
  - `run_checks(redacted: RedactedText, policies: Sequence[Policy]) -> DeterministicResult`.
  - `aggregate(checks: Sequence[CheckOutcome]) -> Verdict` — any REJECT wins, else any UNCERTAIN, else PASS.

- [ ] **Step 1: Write the failing test**

`tests/unit/domain/test_rules.py`:

```python
from datetime import date
from typing import Any
from uuid import uuid4

import pytest

from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import CheckOutcome, Verdict
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.rules import MIN_TEXT_CHARS, aggregate, extract_icd10_codes, run_checks

LONG_NOTE = (
    "Patient reports persistent lower back pain for eight weeks. "
    "Conservative therapy completed. Diagnosis M54.5 documented."
)


def code(raw: str) -> Icd10Code:
    return Icd10Code(code=raw)


def redacted(text: str) -> RedactedText:
    return RedactedText(text=text, entity_counts={"PERSON": 1}, redactor="presidio-2.2.x")


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {code("M54.5")},
        "excluded_codes": {code("Z00.0")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


def outcome(result: Any, name: str) -> CheckOutcome:
    return next(check for check in result.checks if check.name == name)


def test_codes_are_extracted_normalised_and_deduplicated() -> None:
    found = extract_icd10_codes("Codes m54.5, M54.5 and g89.4 were noted.")
    assert [item.code for item in found] == ["M54.5", "G89.4"]


def test_words_that_are_not_codes_are_ignored() -> None:
    assert extract_icd10_codes("No diagnosis recorded during this visit at all.") == []


def test_all_four_checks_are_always_reported() -> None:
    result = run_checks(redacted(LONG_NOTE), [build_policy()])
    assert [check.name for check in result.checks] == [
        "non_empty_text",
        "icd10_present",
        "excluded_code_hit",
        "covered_code_hit",
    ]


def test_a_covered_code_in_a_long_note_passes() -> None:
    result = run_checks(redacted(LONG_NOTE), [build_policy()])
    assert result.verdict is Verdict.PASS
    assert all(check.passed for check in result.checks)


def test_short_text_is_rejected() -> None:
    result = run_checks(redacted("M54.5"), [build_policy()])
    assert outcome(result, "non_empty_text").passed is False
    assert result.verdict is Verdict.REJECT


def test_text_exactly_at_the_minimum_passes_that_check() -> None:
    text = "M54.5 " + "a" * (MIN_TEXT_CHARS - 6)
    result = run_checks(redacted(text), [build_policy()])
    assert outcome(result, "non_empty_text").passed is True


def test_no_codes_is_uncertain() -> None:
    note = "The patient describes discomfort but no diagnosis code was recorded anywhere."
    result = run_checks(redacted(note), [build_policy()])
    assert outcome(result, "icd10_present").passed is False
    assert result.verdict is Verdict.UNCERTAIN


def test_an_excluded_code_is_rejected() -> None:
    note = LONG_NOTE + " Routine examination Z00.0 was also performed for the record."
    result = run_checks(redacted(note), [build_policy()])
    assert outcome(result, "excluded_code_hit").passed is False
    assert result.verdict is Verdict.REJECT


def test_a_code_covered_by_no_policy_is_uncertain() -> None:
    note = (
        "Patient reports persistent shoulder pain for eight weeks with no improvement. "
        "Diagnosis G89.4 documented by the attending physician."
    )
    result = run_checks(redacted(note), [build_policy()])
    assert outcome(result, "covered_code_hit").passed is False
    assert result.verdict is Verdict.UNCERTAIN


def test_reject_beats_uncertain() -> None:
    result = run_checks(redacted("Z00.0"), [build_policy()])
    assert result.verdict is Verdict.REJECT


def test_codes_are_matched_across_every_policy() -> None:
    other = build_policy(name="Physio", covered_codes={code("G89.4")}, excluded_codes=set())
    note = (
        "Patient reports persistent shoulder pain for eight weeks with no improvement. "
        "Diagnosis G89.4 documented by the attending physician."
    )
    result = run_checks(redacted(note), [build_policy(), other])
    assert result.verdict is Verdict.PASS


def test_no_policies_leaves_every_code_uncovered() -> None:
    result = run_checks(redacted(LONG_NOTE), [])
    assert outcome(result, "covered_code_hit").passed is False
    assert result.verdict is Verdict.UNCERTAIN


@pytest.mark.parametrize(
    ("failed", "expected"),
    [
        ([], Verdict.PASS),
        (["icd10_present"], Verdict.UNCERTAIN),
        (["covered_code_hit"], Verdict.UNCERTAIN),
        (["excluded_code_hit"], Verdict.REJECT),
        (["non_empty_text", "icd10_present"], Verdict.REJECT),
    ],
)
def test_aggregation_precedence(failed: list[str], expected: Verdict) -> None:
    names = ["non_empty_text", "icd10_present", "excluded_code_hit", "covered_code_hit"]
    checks = [CheckOutcome(name=name, passed=name not in failed, detail="") for name in names]
    assert aggregate(checks) is expected
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/domain/test_rules.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.domain.rules'`.

- [ ] **Step 3: Write `src/ecet/domain/rules.py`**

```python
"""Deterministic pre-LLM checks (ADR-002).

Cheap, pure rules over the redacted note and the tenant's policies. A REJECT here
means no LLM call is made at all — that skipped call is the cost saving the ADR is
about. REJECT never means an automatic denial: it routes to a human.
"""

import re
from collections.abc import Sequence

from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.policy import Icd10Code, Policy

MIN_TEXT_CHARS = 50

#: Word-bounded, case-insensitive ICD-10 token. Same shape as `ICD10_PATTERN`,
#: unanchored so it can be searched inside prose.
ICD10_TOKEN_RE = re.compile(r"\b[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?\b", re.IGNORECASE)

#: What a failing rule means for the overall verdict.
_VERDICT_ON_FAIL = {
    "non_empty_text": Verdict.REJECT,
    "icd10_present": Verdict.UNCERTAIN,
    "excluded_code_hit": Verdict.REJECT,
    "covered_code_hit": Verdict.UNCERTAIN,
}


def extract_icd10_codes(text: str) -> list[Icd10Code]:
    """Codes found in the note, normalised, de-duplicated, in first-seen order."""
    found: dict[str, Icd10Code] = {}
    for match in ICD10_TOKEN_RE.finditer(text):
        parsed = Icd10Code(code=match.group())
        found.setdefault(parsed.code, parsed)
    return list(found.values())


def aggregate(checks: Sequence[CheckOutcome]) -> Verdict:
    """Any REJECT wins; otherwise any UNCERTAIN; otherwise PASS."""
    failures = {_VERDICT_ON_FAIL[check.name] for check in checks if not check.passed}
    if Verdict.REJECT in failures:
        return Verdict.REJECT
    if Verdict.UNCERTAIN in failures:
        return Verdict.UNCERTAIN
    return Verdict.PASS


def run_checks(redacted: RedactedText, policies: Sequence[Policy]) -> DeterministicResult:
    """Run every rule, always, then aggregate. All four outcomes are reported so the
    claim record shows what was checked, not only what failed."""
    codes = extract_icd10_codes(redacted.text)
    covered = {code for policy in policies for code in policy.covered_codes}
    excluded = {code for policy in policies for code in policy.excluded_codes}

    checks = [
        _non_empty_text(redacted),
        _icd10_present(codes),
        _excluded_code_hit(codes, excluded),
        _covered_code_hit(codes, covered),
    ]
    return DeterministicResult(verdict=aggregate(checks), checks=checks)


def _non_empty_text(redacted: RedactedText) -> CheckOutcome:
    length = len(redacted.text.strip())
    return CheckOutcome(
        name="non_empty_text",
        passed=length >= MIN_TEXT_CHARS,
        detail=f"{length} characters after redaction (minimum {MIN_TEXT_CHARS})",
    )


def _icd10_present(codes: Sequence[Icd10Code]) -> CheckOutcome:
    return CheckOutcome(
        name="icd10_present",
        passed=bool(codes),
        detail=f"{len(codes)} ICD-10 code(s) found",
    )


def _excluded_code_hit(codes: Sequence[Icd10Code], excluded: set[Icd10Code]) -> CheckOutcome:
    hits = sorted(code.code for code in codes if code in excluded)
    return CheckOutcome(
        name="excluded_code_hit",
        passed=not hits,
        detail=f"excluded codes present: {', '.join(hits)}" if hits else "no excluded code found",
    )


def _covered_code_hit(codes: Sequence[Icd10Code], covered: set[Icd10Code]) -> CheckOutcome:
    hits = sorted(code.code for code in codes if code in covered)
    return CheckOutcome(
        name="covered_code_hit",
        passed=bool(hits),
        detail=f"covered codes present: {', '.join(hits)}" if hits else "no covered code found",
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/domain/test_rules.py -v`
Expected: PASS.

- [ ] **Step 5: Run the gate and commit**

```bash
make check
git add src/ecet/domain/rules.py tests/unit/domain/test_rules.py
git commit -m "feat(domain): deterministic pre-LLM rule checks (ADR-002)"
```

---

### Task 6: Repository ports, in-memory fakes, coverage gate

**Files:**
- Create: `src/ecet/domain/ports/claim_repository.py`, `src/ecet/domain/ports/policy_repository.py`, `src/ecet/domain/ports/tenant_repository.py`, `src/ecet/domain/ports/review_task_repository.py`
- Create: `tests/fakes.py`
- Test: `tests/unit/domain/test_ports.py`
- Modify: `pyproject.toml` (pytest `pythonpath`, coverage `exclude_also`), `.github/workflows/ci.yml` (domain coverage gate)

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces:
  - `ClaimRepository`: `add`, `get`, `find_by_source`, `save`, `list_by_status`.
  - `PolicyRepository`: `active_for_tenant`, `get_many`.
  - `TenantRepository`: `get`.
  - `ReviewTaskRepository`: `add`, `get`, `list_open`, `save`.
  - `tests.fakes`: `FakeClaimRepository`, `FakePolicyRepository`, `FakeTenantRepository`, `FakeReviewTaskRepository` — in-memory, used by Phase 3+ use-case tests. Phase 2's Postgres adapters implement the same Protocols.

- [ ] **Step 1: Write the failing test**

`tests/unit/domain/test_ports.py`:

```python
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from pydantic import SecretStr

from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.errors import ClaimNotFound, ReviewTaskNotFound, TenantNotFound
from ecet.domain.evaluation import ReviewReason, ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.ports.claim_repository import ClaimRepository
from ecet.domain.ports.policy_repository import PolicyRepository
from ecet.domain.ports.review_task_repository import ReviewTaskRepository
from ecet.domain.ports.tenant_repository import TenantRepository
from ecet.domain.tenant import Tenant
from tests.fakes import (
    FakeClaimRepository,
    FakePolicyRepository,
    FakeReviewTaskRepository,
    FakeTenantRepository,
)

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
TENANT = TenantId("tenant-a")


def build_claim(status: ClaimStatus = ClaimStatus.RECEIVED) -> Claim:
    return Claim(
        id=ClaimId(uuid4()),
        tenant_id=TENANT,
        source=SourceObject(
            bucket="claims",
            key="tenants/tenant-a/claims/a.pdf",
            etag="etag-1",
            size=10,
        ),
        status=status,
        created_at=NOW,
        updated_at=NOW,
    )


def build_policy(**overrides: object) -> Policy:
    fields: dict[str, object] = {
        "id": PolicyId(uuid4()),
        "tenant_id": TENANT,
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {Icd10Code(code="M54.5")},
        "criteria_text": "criteria",
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


def test_fakes_satisfy_their_ports() -> None:
    assert isinstance(FakeClaimRepository(), ClaimRepository)
    assert isinstance(FakePolicyRepository(), PolicyRepository)
    assert isinstance(FakeTenantRepository(), TenantRepository)
    assert isinstance(FakeReviewTaskRepository(), ReviewTaskRepository)


async def test_claim_repository_round_trip() -> None:
    repo = FakeClaimRepository()
    claim = build_claim()
    await repo.add(claim)

    assert await repo.get(claim.id) == claim
    assert await repo.find_by_source("claims", claim.source.key, "etag-1") == claim
    assert await repo.find_by_source("claims", claim.source.key, "other") is None
    assert await repo.list_by_status(ClaimStatus.RECEIVED) == [claim]
    assert await repo.list_by_status(ClaimStatus.QUEUED) == []

    claim.transition(ClaimStatus.EXTRACTED, now=NOW)
    await repo.save(claim)
    assert (await repo.get(claim.id)).status is ClaimStatus.EXTRACTED


async def test_claim_repository_raises_when_missing() -> None:
    with pytest.raises(ClaimNotFound):
        await FakeClaimRepository().get(ClaimId(uuid4()))


async def test_policy_repository_filters_on_effectiveness() -> None:
    active = build_policy()
    expired = build_policy(effective_to=date(2026, 2, 1))
    inactive = build_policy(active=False)
    repo = FakePolicyRepository([active, expired, inactive])

    assert await repo.active_for_tenant(TENANT, on=date(2026, 6, 1)) == [active]
    assert await repo.active_for_tenant(TenantId("tenant-b"), on=date(2026, 6, 1)) == []
    assert await repo.get_many([active.id]) == [active]


async def test_tenant_repository_round_trip() -> None:
    tenant = Tenant(
        id=TENANT,
        name="Tenant A",
        webhook_url="http://mock-client:9000/hooks/ecet",
        webhook_secret=SecretStr("s3cret"),
    )
    repo = FakeTenantRepository([tenant])
    assert await repo.get(TENANT) == tenant
    with pytest.raises(TenantNotFound):
        await repo.get(TenantId("nope"))


async def test_review_task_repository_lists_only_open_tasks_for_the_tenant() -> None:
    repo = FakeReviewTaskRepository()
    task = ReviewTask(
        id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TENANT,
        reason=ReviewReason.LOW_CONFIDENCE,
        created_at=NOW,
    )
    await repo.add(task)
    assert await repo.list_open(TENANT, limit=10) == [task]
    assert await repo.list_open(TenantId("tenant-b"), limit=10) == []

    task.status = ReviewStatus.RESOLVED
    await repo.save(task)
    assert await repo.list_open(TENANT, limit=10) == []
    assert await repo.get(task.id) == task

    with pytest.raises(ReviewTaskNotFound):
        await repo.get(uuid4())
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/domain/test_ports.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.domain.ports.claim_repository'`.

- [ ] **Step 3: Write the four port modules**

`src/ecet/domain/ports/claim_repository.py`:

```python
"""Claim persistence contract. Implemented by Postgres (Phase 2) and by test fakes."""

from typing import Protocol, runtime_checkable

from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.ids import ClaimId


@runtime_checkable
class ClaimRepository(Protocol):
    async def add(self, claim: Claim) -> None: ...

    async def get(self, claim_id: ClaimId) -> Claim:
        """Raises `ClaimNotFound`."""
        ...

    async def find_by_source(self, bucket: str, key: str, etag: str) -> Claim | None:
        """ADR-006 idempotency lookup."""
        ...

    async def save(self, claim: Claim) -> None:
        """Full overwrite, optimistic on `updated_at`. Raises `ConcurrentModification`."""
        ...

    async def list_by_status(self, status: ClaimStatus, *, limit: int = 50) -> list[Claim]: ...
```

`src/ecet/domain/ports/policy_repository.py`:

```python
"""Policy read contract. Policies are seeded, never written through the app."""

from collections.abc import Iterable
from datetime import date
from typing import Protocol, runtime_checkable

from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy


@runtime_checkable
class PolicyRepository(Protocol):
    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]:
        """Active, effective on `on`, highest version per name. Empty list is the
        caller's cue to raise `NoPoliciesForTenant` (ADR-005)."""
        ...

    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]: ...
```

`src/ecet/domain/ports/tenant_repository.py`:

```python
"""Tenant read contract."""

from typing import Protocol, runtime_checkable

from ecet.domain.ids import TenantId
from ecet.domain.tenant import Tenant


@runtime_checkable
class TenantRepository(Protocol):
    async def get(self, tenant_id: TenantId) -> Tenant:
        """Raises `TenantNotFound` for an unknown or inactive tenant."""
        ...
```

`src/ecet/domain/ports/review_task_repository.py`:

```python
"""Review task persistence contract."""

from typing import Protocol, runtime_checkable
from uuid import UUID

from ecet.domain.evaluation import ReviewTask
from ecet.domain.ids import TenantId


@runtime_checkable
class ReviewTaskRepository(Protocol):
    async def add(self, task: ReviewTask) -> None: ...

    async def get(self, task_id: UUID) -> ReviewTask:
        """Raises `ReviewTaskNotFound`."""
        ...

    async def list_open(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTask]:
        """Tenant-scoped; there is no cross-tenant listing."""
        ...

    async def save(self, task: ReviewTask) -> None: ...
```

- [ ] **Step 4: Make `tests` importable, then write `tests/fakes.py`**

`tests/fakes.py` is imported as `tests.fakes`, so the repository root must be on
`sys.path`. Add to `[tool.pytest.ini_options]` in `pyproject.toml`:

```toml
pythonpath = ["."]
```

(`tests/` needs no `__init__.py` — implicit namespace packages cover it.)

Then write `tests/fakes.py`:

```python
"""In-memory port implementations for tests.

Phase 1 ships the four domain repository fakes. The application-port fakes
(`FakeObjectStorage`, `FakePiiRedactor`, `FakeLLMGateway`, …) arrive with the phases
that define those ports.
"""

from collections.abc import Iterable
from datetime import date
from uuid import UUID

from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound, ReviewTaskNotFound, TenantNotFound
from ecet.domain.evaluation import ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant


class FakeClaimRepository:
    def __init__(self) -> None:
        self.claims: dict[ClaimId, Claim] = {}
        self.saved: list[ClaimId] = []

    async def add(self, claim: Claim) -> None:
        self.claims[claim.id] = claim

    async def get(self, claim_id: ClaimId) -> Claim:
        try:
            return self.claims[claim_id]
        except KeyError:
            raise ClaimNotFound(str(claim_id)) from None

    async def find_by_source(self, bucket: str, key: str, etag: str) -> Claim | None:
        for claim in self.claims.values():
            source = claim.source
            if (source.bucket, source.key, source.etag) == (bucket, key, etag):
                return claim
        return None

    async def save(self, claim: Claim) -> None:
        self.claims[claim.id] = claim
        self.saved.append(claim.id)

    async def list_by_status(self, status: ClaimStatus, *, limit: int = 50) -> list[Claim]:
        return [claim for claim in self.claims.values() if claim.status is status][:limit]


class FakePolicyRepository:
    def __init__(self, policies: Iterable[Policy] = ()) -> None:
        self.policies: list[Policy] = list(policies)

    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]:
        matching = [
            policy
            for policy in self.policies
            if policy.tenant_id == tenant_id and policy.is_effective(on)
        ]
        highest: dict[str, Policy] = {}
        for policy in matching:
            current = highest.get(policy.name)
            if current is None or policy.version > current.version:
                highest[policy.name] = policy
        return list(highest.values())

    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]:
        wanted = set(ids)
        return [policy for policy in self.policies if policy.id in wanted]


class FakeTenantRepository:
    def __init__(self, tenants: Iterable[Tenant] = ()) -> None:
        self.tenants: dict[str, Tenant] = {tenant.id: tenant for tenant in tenants}

    async def get(self, tenant_id: TenantId) -> Tenant:
        tenant = self.tenants.get(tenant_id)
        if tenant is None or not tenant.active:
            raise TenantNotFound(tenant_id)
        return tenant


class FakeReviewTaskRepository:
    def __init__(self) -> None:
        self.tasks: dict[UUID, ReviewTask] = {}

    async def add(self, task: ReviewTask) -> None:
        self.tasks[task.id] = task

    async def get(self, task_id: UUID) -> ReviewTask:
        try:
            return self.tasks[task_id]
        except KeyError:
            raise ReviewTaskNotFound(str(task_id)) from None

    async def list_open(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTask]:
        return [
            task
            for task in self.tasks.values()
            if task.tenant_id == tenant_id and task.status is ReviewStatus.OPEN
        ][:limit]

    async def save(self, task: ReviewTask) -> None:
        self.tasks[task.id] = task
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/unit/domain/test_ports.py -v`
Expected: PASS. (`pythonpath = ["."]` from Step 4 is what makes `tests.fakes` resolve;
`pytest-pythonpath` is not needed — the setting is built into pytest 8.)

- [ ] **Step 6: Exclude Protocol bodies from coverage**

Add to `pyproject.toml`, directly after the existing `[tool.coverage.run]` block:

```toml
[tool.coverage.report]
exclude_also = [
    '^\s*\.\.\.$',
    'if TYPE_CHECKING:',
]
```

Rationale: a `...` Protocol body is never executed, so without this every port file
drags the domain coverage number down for code that cannot be run.

- [ ] **Step 7: Gate domain coverage at 95 %**

In `.github/workflows/ci.yml`, replace the `Tests` step with:

```yaml
      - name: Tests
        run: uv run pytest --cov=ecet --cov-report=term-missing --cov-fail-under=85

      - name: Domain coverage gate
        run: uv run coverage report --include='src/ecet/domain/*' --fail-under=95
```

The second step reuses the `.coverage` data file the first step wrote — the suite runs once.

- [ ] **Step 8: Verify both gates locally**

Run:
```bash
uv run pytest --cov=ecet --cov-report=term-missing --cov-fail-under=85
uv run coverage report --include='src/ecet/domain/*' --fail-under=95
```
Expected: both exit 0. If the domain gate fails, the report names the uncovered lines —
add the missing test rather than lowering the number.

- [ ] **Step 9: Run the full gate and commit**

```bash
make check
git add src/ecet/domain/ports tests/fakes.py tests/unit/domain/test_ports.py pyproject.toml .github/workflows/ci.yml
git commit -m "feat(domain): repository ports, in-memory fakes and a domain coverage gate"
```

---

## Phase exit criteria

- [ ] `make check` green: ruff, `mypy --strict src/ecet/domain src/ecet/application`, `lint-imports`, full test suite.
- [ ] `uv run coverage report --include='src/ecet/domain/*' --fail-under=95` exits 0.
- [ ] `src/ecet/domain/` contains `ids.py`, `errors.py`, `tenant.py`, `policy.py`, `evaluation.py`, `claim.py`, `rules.py`, `ports/{claim,policy,tenant,review_task}_repository.py` — and nothing else.
- [ ] No file under `src/ecet/domain/` imports anything outside stdlib, pydantic and `ecet.domain`.
- [ ] `make up` still brings the stack healthy (Phase 1 touches no container).

## Deviations from spec (record in the PR description)

1. **`domain/ids.py` is a new module** not listed in the [project layout](../../specs/05-platform/project-layout.md). `claim` needs `Evaluation`, and `evaluation.ReviewTask` needs `ClaimId` — a direct import cycle. Identifiers move to their own leaf module; `claim.py`, `policy.py` and `tenant.py` re-export their own id so `from ecet.domain.policy import PolicyId` still reads naturally.
2. **`Claim.transition` takes an extra keyword `now: datetime | None = None`.** The spec signature is `transition(next, *, reason=None)`. The domain owns no clock, so tests would otherwise assert against wall-clock time; `now=None` falls back to `datetime.now(UTC)` and keeps the spec's call form valid.
3. **`transition` to a failure status without a `reason` raises `InvalidTransition`.** The spec says failure states carry `failure_reason` but does not enforce it; enforcing it here is what makes the field trustworthy for the API's error surface.
4. **Four transition edges are added beyond the state list**: `NOTIFY_FAILED → APPROVED_AUTO | REVIEW_RESOLVED` (required by `POST /v1/claims/{id}/retry-notify` in the [api spec](../../specs/04-interfaces/api.md), Phase 5); `EVALUATION_FAILED → REVIEW_RESOLVED` ([UC-06](../../specs/02-use-cases/UC-06-evaluate-claim.md) opens a review task with reason `EVALUATION_FAILED`, and [UC-09c](../../specs/02-use-cases/UC-09-human-review.md) resolves it); and `EVALUATED → NOTIFY_FAILED` ([UC-07](../../specs/02-use-cases/UC-07-route-decision.md) step 2 — "Failure after retries → `NOTIFY_FAILED` with reason") plus `REVIEW_PENDING → NOTIFY_FAILED` ([UC-09c](../../specs/02-use-cases/UC-09-human-review.md) step 4 — "Claim → `REVIEW_RESOLVED` (or `NOTIFY_FAILED` on delivery failure)"). The last two were missing until the whole-branch review: both use cases fail delivery *before* the claim reaches `APPROVED_AUTO`/`REVIEW_RESOLVED`, which [claim.md](../../specs/01-domain/claim.md) defines as "webhook delivered", so routing through those is not an option. `test_every_allowed_edge_survives_a_real_transition` now drives every `_ALLOWED` edge through `transition()`; the gap survived six task reviews because the table was only ever read, never walked.
5. **`errors.py` carries four errors the [claim spec](../../specs/01-domain/claim.md#3-domain-errors-ecetdomainerrorspy) list omits** — `TenantNotFound` ([tenant spec](../../specs/01-domain/tenant.md#2-port)), `ReviewTaskNotFound` and `ReviewAlreadyResolved` ([UC-09c](../../specs/02-use-cases/UC-09-human-review.md#uc-09c-resolvereview)), `ConcurrentModification` ([postgres spec](../../specs/03-infrastructure/postgres.md#unitofwork)). Each is named by another spec; writing the module once is cheaper than editing it in three later phases.
6. **`ReviewTask.resolve()` exists as a domain method.** The spec describes the mutation inside UC-09c. The invariant "a resolved task cannot be resolved again" belongs to the entity, so UC-09c will call this rather than reimplement it.
7. **`SourceObject.ensure_size_within(max_bytes)` replaces the spec's "`size ≤ max_pdf_bytes`" field constraint.** `max_pdf_bytes` is configuration, and the domain may not import `ecet.config`; the limit is injected by UC-01 instead. The model still enforces `size > 0`.
8. **`InvalidObjectKey` is raised from inside a pydantic field validator.** Pydantic v2 wraps only `ValueError`/`AssertionError`; other exceptions propagate untouched, so `SourceObject(...)` with a bad key raises `InvalidObjectKey` exactly as the spec asks — not `ValidationError`. Every other model constraint surfaces as `ValidationError`.
9. **Ports are `@runtime_checkable`** so `isinstance(fake, ClaimRepository)` is a real assertion in `test_ports.py`. It checks method presence only; signature conformance is mypy's job.
10. **`Evaluation`'s optional-looking fields (`model`, `prompt_version`, `rationale`, token counts, `latency_ms`) default to empty/zero.** The spec table lists them as required. The vendor adapter fills them; defaulting keeps the domain constructible in tests without inventing vendor metadata. `decision` and `confidence` stay required — the spec's "missing confidence → INSUFFICIENT_EVIDENCE, 0.0" rule is gateway behaviour (Phase 4), not a domain default.
11. **`pythonpath = ["."]` added to the pytest config** so the shared `tests/fakes.py` imports as `tests.fakes` from any test module, regardless of which directory pytest inserts into `sys.path`.
12. **Domain coverage is gated at 95 % via a second `coverage report --include` step** rather than a per-package `--cov-fail-under`, which coverage.py does not support. Closes the [testing spec](../../specs/05-platform/testing.md#coverage-target) target that Phase 0 left at the overall 85 %.
13. **`Claim` validates `tenant_id` against the tenant segment of `source.key`**, raising the new `TenantMismatch`. No spec asks for it: every repository query filters on `tenant_id` while object-storage reads use the key, so a disagreement between the two is an undetectable cross-tenant read.
14. **`errors.py` carries two more errors beyond deviation 5's four**: `TenantMismatch` (above) and `InvalidReviewResolution`, raised by `ReviewTask.resolve()` where a bare `ValueError` used to be. The [api spec](../../specs/04-interfaces/api.md) maps only `DomainError` subclasses to problem details, so a bare `ValueError` would have surfaced as a 500. `resolve()`'s `resolution` parameter is narrowed from `Decision` to the module's `HumanResolution` literal at the same time; the runtime guard stays, because a `Literal` is a static promise only.
15. **`ReviewTaskRepository` gains `find_open_by_claim(claim_id) -> ReviewTask | None`**, which no spec lists. [UC-09a](../../specs/02-use-cases/UC-09-human-review.md#uc-09a-requesthumanreview)'s "existing OPEN task for claim → return it, no duplicate" was otherwise reachable only by scanning `list_open(tenant_id, limit=50)`, which silently misses past the limit — and `review_tasks.claim_id` is unique in the [postgres schema](../../specs/03-infrastructure/postgres.md), so Phase 2 would have raised an integrity error instead of the specified idempotency.
