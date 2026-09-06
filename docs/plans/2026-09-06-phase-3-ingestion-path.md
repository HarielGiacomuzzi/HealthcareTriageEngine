# Phase 3 — Ingestion Path (API side) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A PDF dropped in the MinIO `claims` bucket becomes a persisted, redacted, policy-attached claim in Postgres and a message on `claims.evaluate` — driven by `POST /v1/events/s3`, with every use case unit-tested against fakes and every adapter tested against a real container.

**Architecture:** Application ports (`Clock`, `ObjectStorage`, `TextExtractor`, `PiiRedactor`, `EvaluationQueue`) are `typing.Protocol`s in `ecet/application/ports/`; the five ingestion use cases depend only on those and on the Phase 2 `UnitOfWork`. `IngestClaimDocument` orchestrates them and owns the transaction boundary through a `uow_factory`. Adapters (aiobotocore S3, pypdf, presidio, aio-pika) implement the ports and are wired once at API startup into an `ApiContainer` held on `app.state`; FastAPI routes are thin, doing auth, parsing and error mapping only. ADR-001 is enforced structurally: the raw extracted text lives in one local variable inside `IngestClaimDocument` and is dropped the moment `RedactedText` exists.

**Tech Stack:** Python 3.12, FastAPI 0.141, aiobotocore (S3/MinIO), pypdf 4.x, presidio-analyzer/anonymizer 2.2.x + spaCy `en_core_web_lg`, aio-pika 9.x, anyio, SQLAlchemy 2 async (from Phase 2), pytest + testcontainers.

**Spec:** [`specs/06-roadmap.md` §Phase 3](../../specs/06-roadmap.md#phase-3--ingestion-path-api-side), which pulls in [UC-01](../../specs/02-use-cases/UC-01-ingest-claim-document.md), [UC-02](../../specs/02-use-cases/UC-02-redact-pii.md), [UC-03](../../specs/02-use-cases/UC-03-attach-tenant-policies.md), [UC-04](../../specs/02-use-cases/UC-04-run-deterministic-checks.md), [UC-05](../../specs/02-use-cases/UC-05-enqueue-evaluation.md), [UC-09a](../../specs/02-use-cases/UC-09-human-review.md#uc-09a-requesthumanreview), [object-storage-minio](../../specs/03-infrastructure/object-storage-minio.md), [pdf-text-extractor](../../specs/03-infrastructure/pdf-text-extractor.md), [pii-redactor-presidio](../../specs/03-infrastructure/pii-redactor-presidio.md), [queue-rabbitmq](../../specs/03-infrastructure/queue-rabbitmq.md), [api](../../specs/04-interfaces/api.md), [config](../../specs/05-platform/config.md), [docker-compose](../../specs/05-platform/docker-compose.md), [testing](../../specs/05-platform/testing.md), [project-layout](../../specs/05-platform/project-layout.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- Layer rule (`import-linter`, `make imports` must stay green): `ecet.domain` imports stdlib + pydantic only; `ecet.application` imports domain + stdlib + pydantic — **never** `fastapi`, `httpx`, `pypdf`, `boto3`/`botocore`, `aio_pika`, `presidio_*`, `sqlalchemy`, or `ecet.config`. Only `ecet.infrastructure` and `ecet.interfaces` may import those. The existing `forbidden` contract already lists them; do not weaken it.
- `mypy --strict` covers `src/ecet/domain` and `src/ecet/application`; `mypy src/ecet` (repo-wide `disallow_untyped_defs = true`) covers infrastructure and interfaces. Every function added in this phase is annotated.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`.
- **ADR-001 is the hard rule of this phase.** Raw (unredacted) text may exist only as a local variable inside `IngestClaimDocument._run_pipeline` and inside the extractor adapter. It is never assigned to a domain model, never persisted, never published, never logged. Every test that touches text ends with `assert_no_pii(...)`.
- **Adapters log through `structlog` only** — never `logging.getLogger`. Phase 6 carry-over #12: stdlib records bypass the `drop_sensitive_fields` guard.
- **Never echo an exception message to an HTTP client.** `InvalidObjectKey` and `ObjectNotFound` embed the client-supplied filename (Phase 1 carry-over #4). `ProblemDetails.detail` strings are static constants.
- **Tenant isolation is the use case's job.** `ClaimRepository.get`, `list_by_status` and `PolicyRepository.get_many` take no tenant parameter (Phase 2 carry-over). Any route that returns a claim compares `claim.tenant_id` against the `X-Tenant-Id` header and raises `ClaimNotFound` on a mismatch.
- **`PostgresClaimRepository.save` refuses a claim it never read.** `add()` and `get()` register the optimistic baseline; a `save()` without one raises `ConcurrentModification`. UC-01 adds then saves inside one unit of work, which is fine — but do not construct a claim and save it directly.
- Adapter tests live in `tests/adapters/` and are auto-marked `slow` by that directory's `conftest.py`. The default run (`pytest -m 'not slow and not e2e'`) must stay green with no Docker and no spaCy model.
- TDD: every step-pair is "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Commit after every task with a conventional prefix. No Claude attribution in commit messages.

### Explicitly out of scope for Phase 3 (do not add)

- Anything on the worker side: `LLMGateway`, `WebhookClient`, UC-06/07/08, the AMQP **consumer**, `mock-client` (Phase 4). This phase ships the queue **publisher** and the topology declaration only.
- UC-09b / UC-09c, `/v1/reviews*`, `/v1/claims/{id}/retry-notify`, `ecet dlq-replay` (Phase 5).
- `/metrics`, prometheus counters, the `ecet_*` metric names, the worker healthcheck, routing uvicorn's logger through structlog (Phase 6).
- Validating extracted ICD-10 codes against `Icd10CodeRepository.known_codes()` (Phase 4).
- `infrastructure/queue/in_memory.py` and `infrastructure/pii/fake_redactor.py` from the layout spec — `tests/fakes.py` covers both needs; see deviation 6.

---

### Task 1: Application ports, errors, redaction policy, fakes and note fixtures

**Files:**
- Create: `src/ecet/application/errors.py`
- Create: `src/ecet/application/ports/clock.py`, `src/ecet/application/ports/object_storage.py`, `src/ecet/application/ports/text_extractor.py`, `src/ecet/application/ports/pii_redactor.py`
- Create: `src/ecet/application/redaction_policy.py`
- Modify: `tests/fakes.py`
- Create: `tests/pii.py`, `tests/fixtures/notes/meets.txt`, `tests/fixtures/notes/does_not_meet.txt`, `tests/fixtures/notes/unclear.txt`, `tests/fixtures/notes/excluded_code.txt`, `tests/fixtures/notes/no_codes.txt`
- Test: `tests/unit/application/test_ports.py`, `tests/unit/test_pii_helper.py`

**Interfaces:**
- Consumes: `ecet.domain.claim.RedactedText`, `ecet.domain.errors.DomainError` (Phase 1).
- Produces:
  - `ecet.application.errors`: `ObjectNotFound`, `ExtractionFailed`, `QueuePublishError` — all `DomainError` subclasses.
  - `ecet.application.ports.clock.Clock` (`now() -> datetime`, **sync**).
  - `ecet.application.ports.object_storage.ObjectHead` (`NamedTuple(etag: str, size: int)`) and `ObjectStorage` (`async get_bytes(bucket, key) -> bytes`, `async head(bucket, key) -> ObjectHead`).
  - `ecet.application.ports.text_extractor.TextExtractor` (`async extract(pdf: bytes) -> str`).
  - `ecet.application.ports.pii_redactor.PiiRedactor` (`async redact(text: str) -> RedactedText`).
  - `ecet.application.redaction_policy`: `ENTITY_REPLACEMENTS: dict[str, str]`, `KEPT_ENTITIES: frozenset[str]`, `CUSTOM_PATTERNS: dict[str, str]`, `SCORE_THRESHOLD: float`.
  - `tests.fakes`: `FixedClock`, `FakeObjectStorage`, `FakeTextExtractor`, `FakePiiRedactor`.
  - `tests.pii`: `PII_STRINGS: tuple[str, ...]`, `assert_no_pii(value: str) -> None`, `read_note(name: str) -> str`.

- [ ] **Step 1: Write the failing port-conformance test**

Create `tests/unit/application/__init__.py` (empty) and `tests/unit/application/test_ports.py`:

```python
"""Every fake must satisfy the Protocol it stands in for — otherwise a use case
tested against fakes proves nothing about the adapter that replaces them."""

from datetime import UTC, datetime

from ecet.application.ports.clock import Clock
from ecet.application.ports.object_storage import ObjectStorage
from ecet.application.ports.pii_redactor import PiiRedactor
from ecet.application.ports.text_extractor import TextExtractor
from tests.fakes import FakeObjectStorage, FakePiiRedactor, FakeTextExtractor, FixedClock


def test_fixed_clock_is_a_clock() -> None:
    assert isinstance(FixedClock(datetime(2026, 9, 6, tzinfo=UTC)), Clock)


def test_fake_object_storage_is_an_object_storage() -> None:
    assert isinstance(FakeObjectStorage(), ObjectStorage)


def test_fake_text_extractor_is_a_text_extractor() -> None:
    assert isinstance(FakeTextExtractor(), TextExtractor)


def test_fake_pii_redactor_is_a_pii_redactor() -> None:
    assert isinstance(FakePiiRedactor(), PiiRedactor)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_ports.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.ports.clock'`

- [ ] **Step 3: Write the application errors**

`src/ecet/application/errors.py`:

```python
"""Errors raised by application ports.

They subclass `DomainError` so the API error mapping stays a single tree, but they
live here rather than in `domain/errors.py` because no domain object raises them —
they are contract failures of an adapter (`ObjectStorage`, `TextExtractor`,
`EvaluationQueue`).
"""

from ecet.domain.errors import DomainError

__all__ = ["ExtractionFailed", "ObjectNotFound", "QueuePublishError"]


class ObjectNotFound(DomainError):
    """No object at that bucket/key. The message embeds the key — never echo it."""


class ExtractionFailed(DomainError):
    """No usable text. The message is a short token: `no_text`, `encrypted`,
    `too_many_pages`, `unreadable`, `object_unavailable`. It is persisted verbatim as
    `Claim.failure_reason`, so it must stay free of client-supplied strings."""


class QueuePublishError(DomainError):
    """The broker did not confirm the publish. UC-01 leaves the claim
    `POLICIES_ATTACHED` so a retry can re-publish it."""
```

- [ ] **Step 4: Write the four ports**

`src/ecet/application/ports/clock.py`:

```python
"""Time as a dependency. Injected so every use case is deterministic in tests and so
the domain never calls `datetime.now()` behind the caller's back."""

from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        """Timezone-aware UTC. Sync on purpose — reading a clock never blocks."""
        ...
```

`src/ecet/application/ports/object_storage.py`:

```python
"""Object storage contract. MinIO in compose, S3 in production — same interface."""

from typing import NamedTuple, Protocol, runtime_checkable


class ObjectHead(NamedTuple):
    """What a HEAD tells us: the two fields `SourceObject` needs."""

    etag: str
    size: int


@runtime_checkable
class ObjectStorage(Protocol):
    async def get_bytes(self, bucket: str, key: str) -> bytes:
        """Raises `ObjectNotFound`. Reads into memory; `SourceObject` already bounded
        the size."""
        ...

    async def head(self, bucket: str, key: str) -> ObjectHead:
        """Raises `ObjectNotFound`. Used by `POST /v1/claims/ingest`, which receives
        only `{bucket, key}` and has to fill in the idempotency key itself."""
        ...
```

`src/ecet/application/ports/text_extractor.py`:

```python
"""PDF → text contract."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class TextExtractor(Protocol):
    async def extract(self, pdf: bytes) -> str:
        """Raises `ExtractionFailed`. The returned text is **raw** — unredacted — and
        must reach `PiiRedactor` before anything else (ADR-001)."""
        ...
```

`src/ecet/application/ports/pii_redactor.py`:

```python
"""Local PII redaction contract (ADR-001). The only thing standing between raw
clinical text and everything that persists, queues or logs it."""

from typing import Protocol, runtime_checkable

from ecet.domain.claim import RedactedText


@runtime_checkable
class PiiRedactor(Protocol):
    async def redact(self, text: str) -> RedactedText: ...
```

- [ ] **Step 5: Write the redaction policy**

`src/ecet/application/redaction_policy.py`:

```python
"""Which entities are redacted and what replaces them (UC-02).

Application-owned, not adapter-owned: the presidio adapter receives this at
construction. Changing redaction behaviour is a policy change, not an infrastructure
change.
"""

#: Entity label -> replacement token. The adapter analyses for exactly these labels.
ENTITY_REPLACEMENTS: dict[str, str] = {
    "PERSON": "<PERSON>",
    "PHONE_NUMBER": "<PHONE>",
    "EMAIL_ADDRESS": "<EMAIL>",
    "US_SSN": "<SSN>",
    "LOCATION": "<LOCATION>",
    "MEDICAL_LICENSE": "<LICENSE>",
    "US_DRIVER_LICENSE": "<ID>",
    "CREDIT_CARD": "<ID>",
    "IBAN_CODE": "<ID>",
    "IP_ADDRESS": "<ID>",
    "MRN": "<MRN>",
    "MEMBER_ID": "<MEMBER_ID>",
}

#: Detected but deliberately left in place: dates of service drive policy evaluation.
KEPT_ENTITIES: frozenset[str] = frozenset({"DATE_TIME"})

#: Custom pattern recognisers registered into presidio's registry.
CUSTOM_PATTERNS: dict[str, str] = {
    "MRN": r"\bMRN[:# ]*\d{6,10}\b",
    "MEMBER_ID": r"\b[A-Z]{2,3}\d{7,10}\b",
}

#: Presidio's default. Below this, a detection is discarded.
SCORE_THRESHOLD = 0.35
```

- [ ] **Step 6: Append the four fakes to `tests/fakes.py`**

Add these imports at the top of `tests/fakes.py` (merge with the existing import block; ruff `I` will order them):

```python
import hashlib
import re
from collections.abc import Sequence
from datetime import date, datetime, timedelta

from ecet.application.errors import ObjectNotFound
from ecet.application.ports.object_storage import ObjectHead
from ecet.domain.claim import RedactedText
```

Append to the end of the file:

```python
class FixedClock:
    """A clock that does not move unless a test moves it."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


class FakeObjectStorage:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects: dict[tuple[str, str], bytes] = dict(objects or {})
        self.reads: list[tuple[str, str]] = []

    def put(self, bucket: str, key: str, data: bytes) -> ObjectHead:
        self.objects[(bucket, key)] = data
        return self._head(data)

    @staticmethod
    def _head(data: bytes) -> ObjectHead:
        # S3 etags for a single-part upload are the MD5 of the body; matching that
        # keeps the fake's idempotency key shaped like the real one.
        digest = hashlib.md5(data, usedforsecurity=False).hexdigest()
        return ObjectHead(etag=digest, size=len(data))

    async def get_bytes(self, bucket: str, key: str) -> bytes:
        self.reads.append((bucket, key))
        try:
            return self.objects[(bucket, key)]
        except KeyError:
            raise ObjectNotFound(f"{bucket}/{key}") from None

    async def head(self, bucket: str, key: str) -> ObjectHead:
        return self._head(await self.get_bytes(bucket, key))


class FakeTextExtractor:
    """Returns a canned string, or raises a canned error. `calls` counts invocations."""

    def __init__(self, text: str = "", *, error: Exception | None = None) -> None:
        self.text = text
        self.error = error
        self.calls = 0

    async def extract(self, pdf: bytes) -> str:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.text


#: Names the regex fake cannot infer. Kept in step with `tests/pii.py`.
FAKE_REDACTOR_NAMES: tuple[str, ...] = ("Marcus Whitfield", "Whitfield", "Alicia Ferreira")


class FakePiiRedactor:
    """Regex stand-in for presidio: deterministic, instant, no spaCy model.

    Ordered so the specific patterns (MRN, member id) win before the generic phone
    pattern. Names cannot be inferred by regex, so they are supplied explicitly —
    the real engine is exercised in `tests/adapters/test_presidio_redactor.py`.
    """

    PATTERNS: tuple[tuple[str, str, str], ...] = (
        ("EMAIL_ADDRESS", r"[\w.+-]+@[\w-]+\.[\w.]+", "<EMAIL>"),
        ("US_SSN", r"\b\d{3}-\d{2}-\d{4}\b", "<SSN>"),
        ("MRN", r"\bMRN[:# ]*\d{6,10}\b", "<MRN>"),
        ("MEMBER_ID", r"\b[A-Z]{2,3}\d{7,10}\b", "<MEMBER_ID>"),
        ("PHONE_NUMBER", r"\(?\b\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}\b", "<PHONE>"),
    )

    def __init__(self, names: Sequence[str] = FAKE_REDACTOR_NAMES) -> None:
        self.names = list(names)
        self.calls: list[int] = []

    async def redact(self, text: str) -> RedactedText:
        self.calls.append(len(text))
        counts: dict[str, int] = {}
        result = text
        for name in sorted(self.names, key=len, reverse=True):
            result, hits = re.subn(re.escape(name), "<PERSON>", result)
            if hits:
                counts["PERSON"] = counts.get("PERSON", 0) + hits
        for entity, pattern, replacement in self.PATTERNS:
            result, hits = re.subn(pattern, replacement, result)
            if hits:
                counts[entity] = counts.get(entity, 0) + hits
        return RedactedText(text=result, entity_counts=counts, redactor="fake")
```

- [ ] **Step 7: Run the port test to verify it passes**

Run: `uv run pytest tests/unit/application/test_ports.py -v`
Expected: PASS (4 tests)

- [ ] **Step 8: Write the failing `assert_no_pii` test**

`tests/unit/test_pii_helper.py`:

```python
import pytest

from tests.fakes import FakePiiRedactor
from tests.pii import PII_STRINGS, assert_no_pii, read_note


def test_assert_no_pii_accepts_clean_text() -> None:
    assert_no_pii("Patient <PERSON> reports low back pain, M54.5.")


def test_assert_no_pii_rejects_a_leaked_string() -> None:
    with pytest.raises(AssertionError, match="Marcus Whitfield"):
        assert_no_pii("Patient Marcus Whitfield reports low back pain.")


@pytest.mark.parametrize(
    "name", ["meets", "does_not_meet", "unclear", "excluded_code", "no_codes"]
)
def test_every_note_fixture_carries_the_pii_block(name: str) -> None:
    note = read_note(name)
    assert all(needle in note for needle in PII_STRINGS)


async def test_the_fake_redactor_clears_every_note() -> None:
    redactor = FakePiiRedactor()
    for name in ("meets", "does_not_meet", "unclear", "excluded_code", "no_codes"):
        assert_no_pii((await redactor.redact(read_note(name))).text)


async def test_the_fake_redactor_leaves_icd10_codes_alone() -> None:
    result = await FakePiiRedactor().redact(read_note("meets"))

    assert "M54.5" in result.text
```

- [ ] **Step 9: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_pii_helper.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tests.pii'`

- [ ] **Step 10: Write the PII helper**

`tests/pii.py`:

```python
"""The privacy assertion the testing spec makes mandatory for every layer that
touches text (ADR-001).

All strings here are synthetic. They appear verbatim in every note fixture, so any
string that survives redaction shows up as a failing assertion rather than as a
production incident.
"""

from pathlib import Path

NOTES_DIR = Path(__file__).resolve().parent / "fixtures" / "notes"

#: Every needle that must never appear downstream of redaction. `Whitfield` is listed
#: separately from the full name so a partial match still fails the assertion.
PII_STRINGS: tuple[str, ...] = (
    "Marcus Whitfield",
    "Whitfield",
    "Alicia Ferreira",
    "(415) 555-0137",
    "m.whitfield@example.com",
    "123-45-6789",
    "MRN: 4482910",
    "NWH2288341",
)


def read_note(name: str) -> str:
    """Read `tests/fixtures/notes/<name>.txt`."""
    return (NOTES_DIR / f"{name}.txt").read_text(encoding="utf-8")


def assert_no_pii(value: str) -> None:
    leaked = sorted({needle for needle in PII_STRINGS if needle in value})
    assert not leaked, f"PII leaked into the value under test: {leaked}"
```

- [ ] **Step 11: Write the note fixtures**

Every note opens with the same synthetic PII block, then diverges so each one lands
on a different deterministic verdict against `tenant-a`'s seeded policies.

`tests/fixtures/notes/meets.txt` — verdict `PASS` (covered code `M54.5` present):

```text
Northwind Health Plan — Prior Authorization Request
Patient: Marcus Whitfield
Member ID: NWH2288341
MRN: 4482910
SSN: 123-45-6789
Phone: (415) 555-0137
Email: m.whitfield@example.com
Referring provider: Alicia Ferreira, MD

Date of service: 2026-08-14

History: 47-year-old with chronic low back pain, M54.5, for eleven months.
Completed a supervised conservative therapy course of nine weeks (physical therapy
twice weekly plus NSAIDs) with no functional improvement. Straight leg raise
positive on the right at 40 degrees. No red-flag findings.

Requested service: MRI lumbar spine without contrast.
Attached: therapist progress note, clinical examination note.
```

`tests/fixtures/notes/does_not_meet.txt` — verdict `PASS` (the LLM, not the rules,
is what rejects this one):

```text
Northwind Health Plan — Prior Authorization Request
Patient: Marcus Whitfield
Member ID: NWH2288341
MRN: 4482910
SSN: 123-45-6789
Phone: (415) 555-0137
Email: m.whitfield@example.com
Referring provider: Alicia Ferreira, MD

Date of service: 2026-08-14

History: 47-year-old presenting with low back pain, M54.5, first reported nine days
ago. No conservative therapy attempted. No neurological deficit, no trauma, no
red-flag findings documented.

Requested service: MRI lumbar spine without contrast.
Attached: none.
```

`tests/fixtures/notes/unclear.txt` — verdict `PASS`, evidence too thin to decide:

```text
Northwind Health Plan — Prior Authorization Request
Patient: Marcus Whitfield
Member ID: NWH2288341
MRN: 4482910
SSN: 123-45-6789
Phone: (415) 555-0137
Email: m.whitfield@example.com
Referring provider: Alicia Ferreira, MD

Date of service: 2026-08-14

History: Ongoing back complaints, M54.5. Prior treatment described by the member as
"some therapy last year", duration unclear and not documented in the chart. Exam
findings are inconsistent between the two most recent visits.

Requested service: MRI lumbar spine without contrast.
Attached: illegible outside record.
```

`tests/fixtures/notes/excluded_code.txt` — verdict `REJECT` (`Z00.00` is excluded on
tenant-a's MRI policy):

```text
Northwind Health Plan — Prior Authorization Request
Patient: Marcus Whitfield
Member ID: NWH2288341
MRN: 4482910
SSN: 123-45-6789
Phone: (415) 555-0137
Email: m.whitfield@example.com
Referring provider: Alicia Ferreira, MD

Date of service: 2026-08-14

History: Routine general adult medical examination without abnormal findings,
Z00.00. Member requests imaging as part of the annual visit. No pain reported, no
functional limitation, no prior therapy.

Requested service: MRI lumbar spine without contrast.
Attached: visit summary.
```

`tests/fixtures/notes/no_codes.txt` — verdict `UNCERTAIN` (no ICD-10 token at all):

```text
Northwind Health Plan — Prior Authorization Request
Patient: Marcus Whitfield
Member ID: NWH2288341
MRN: 4482910
SSN: 123-45-6789
Phone: (415) 555-0137
Email: m.whitfield@example.com
Referring provider: Alicia Ferreira, MD

Date of service: 2026-08-14

History: Member reports discomfort in the lower back after moving house. Seen once
in clinic. No diagnosis recorded yet, no imaging to date, no therapy started.

Requested service: MRI lumbar spine without contrast.
Attached: intake form.
```

- [ ] **Step 12: Run the helper test to verify it passes**

Run: `uv run pytest tests/unit/test_pii_helper.py -v`
Expected: PASS (9 tests)

- [ ] **Step 13: Run the whole gate**

Run: `make check`
Expected: lint, mypy (strict on domain + application), import contracts and the full
default suite all green.

- [ ] **Step 14: Commit**

```bash
git add src/ecet/application tests/fakes.py tests/pii.py tests/fixtures/notes tests/unit/application tests/unit/test_pii_helper.py
git commit -m "feat(application): ingestion ports, redaction policy, fakes and note fixtures"
```

---

### Task 2: UC-02 RedactPii, UC-03 AttachTenantPolicies, UC-04 RunDeterministicChecks

The three leaf use cases UC-01 calls. Small enough to share one review gate, and they
have no dependency on each other.

**Files:**
- Create: `src/ecet/application/use_cases/redact_pii.py`, `src/ecet/application/use_cases/attach_tenant_policies.py`, `src/ecet/application/use_cases/run_deterministic_checks.py`
- Test: `tests/unit/application/test_redact_pii.py`, `tests/unit/application/test_attach_tenant_policies.py`, `tests/unit/application/test_run_deterministic_checks.py`

**Interfaces:**
- Consumes: `Clock`, `PiiRedactor` (Task 1); `PolicyRepository`, `NoPoliciesForTenant`, `RedactedText`, `Policy`, `DeterministicResult`, `run_checks` (Phase 1).
- Produces:
  - `RedactPii(redactor: PiiRedactor)` with `async execute(text: str) -> RedactedText`.
  - `AttachTenantPolicies(policies: PolicyRepository, clock: Clock)` with `async execute(tenant_id: TenantId) -> list[Policy]`, raising `NoPoliciesForTenant`.
  - `RunDeterministicChecks()` with `async execute(redacted: RedactedText, policies: Sequence[Policy]) -> DeterministicResult`.

- [ ] **Step 1: Write the failing UC-02 test**

`tests/unit/application/test_redact_pii.py`:

```python
from ecet.application.use_cases.redact_pii import RedactPii
from tests.fakes import FakePiiRedactor
from tests.pii import assert_no_pii, read_note


async def test_it_returns_the_ports_redacted_text() -> None:
    use_case = RedactPii(FakePiiRedactor())

    result = await use_case.execute(read_note("meets"))

    assert_no_pii(result.text)
    assert result.entity_counts["US_SSN"] == 1
    assert result.redactor == "fake"


async def test_empty_text_is_not_an_error() -> None:
    result = await RedactPii(FakePiiRedactor()).execute("")

    assert result.text == ""
    assert result.entity_counts == {}


async def test_icd10_codes_survive_redaction() -> None:
    result = await RedactPii(FakePiiRedactor()).execute(read_note("meets"))

    assert "M54.5" in result.text
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_redact_pii.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.redact_pii'`

- [ ] **Step 3: Write UC-02**

`src/ecet/application/use_cases/redact_pii.py`:

```python
"""UC-02 RedactPii (ADR-001).

Deliberately thin. It exists so that *which* entities are redacted and *what*
replaces them is an application decision (`redaction_policy.py`, handed to the
adapter at construction) rather than something buried in the presidio wiring — and
so Phase 6 has one place to hang the `pii_redaction_seconds` histogram.
"""

from ecet.application.ports.pii_redactor import PiiRedactor
from ecet.domain.claim import RedactedText


class RedactPii:
    def __init__(self, redactor: PiiRedactor) -> None:
        self._redactor = redactor

    async def execute(self, text: str) -> RedactedText:
        """`text` is raw and must not be logged, stored or returned — only the
        `RedactedText` leaves this call."""
        return await self._redactor.redact(text)
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run pytest tests/unit/application/test_redact_pii.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Write the failing UC-03 test**

`tests/unit/application/test_attach_tenant_policies.py`:

```python
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest

from ecet.application.use_cases.attach_tenant_policies import AttachTenantPolicies
from ecet.domain.errors import NoPoliciesForTenant
from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy
from tests.fakes import FakePolicyRepository, FixedClock

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_policy(name: str, **overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": name,
        "version": 1,
        "covered_codes": [{"code": "M54.5"}],
        "criteria_text": "Covered after six weeks of conservative therapy.",
        "effective_from": date(2025, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


async def test_it_returns_only_the_effective_policies() -> None:
    repository = FakePolicyRepository(
        [
            build_policy("MRI lumbar spine"),
            build_policy("Polysomnography"),
            build_policy("Knee arthroscopy", active=False),
            build_policy("Gene panel", effective_to=date(2026, 3, 31)),
        ]
    )
    use_case = AttachTenantPolicies(repository, FixedClock(NOW))

    found = await use_case.execute(TenantId("tenant-a"))

    assert sorted(policy.name for policy in found) == ["MRI lumbar spine", "Polysomnography"]


async def test_a_tenant_with_no_policies_is_a_hard_failure() -> None:
    use_case = AttachTenantPolicies(FakePolicyRepository(), FixedClock(NOW))

    with pytest.raises(NoPoliciesForTenant, match="tenant-empty"):
        await use_case.execute(TenantId("tenant-empty"))


async def test_it_asks_the_repository_for_todays_date() -> None:
    repository = FakePolicyRepository([build_policy("MRI lumbar spine")])
    use_case = AttachTenantPolicies(repository, FixedClock(NOW))

    # A policy that starts tomorrow must not come back today.
    repository.policies.append(
        build_policy("Future policy", effective_from=date(2026, 9, 7))
    )
    found = await use_case.execute(TenantId("tenant-a"))

    assert [policy.name for policy in found] == ["MRI lumbar spine"]
```

- [ ] **Step 6: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_attach_tenant_policies.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.attach_tenant_policies'`

- [ ] **Step 7: Write UC-03**

`src/ecet/application/use_cases/attach_tenant_policies.py`:

```python
"""UC-03 AttachTenantPolicies (ADR-005).

No active policy means no LLM call and no claim: the caller sets `NO_POLICIES` and
the request answers 422. Silently evaluating against an empty policy set would let
the model invent coverage rules.
"""

from ecet.application.ports.clock import Clock
from ecet.domain.errors import NoPoliciesForTenant
from ecet.domain.ids import TenantId
from ecet.domain.policy import Policy
from ecet.domain.ports.policy_repository import PolicyRepository


class AttachTenantPolicies:
    def __init__(self, policies: PolicyRepository, clock: Clock) -> None:
        self._policies = policies
        self._clock = clock

    async def execute(self, tenant_id: TenantId) -> list[Policy]:
        """The full `Policy` objects, not just ids: UC-04 needs the code sets and UC-05
        snapshots them into the queue message so the worker evaluates the same versions
        even if a policy changes mid-flight."""
        found = await self._policies.active_for_tenant(tenant_id, on=self._clock.now().date())
        if not found:
            raise NoPoliciesForTenant(str(tenant_id))
        return found
```

- [ ] **Step 8: Run it to verify it passes**

Run: `uv run pytest tests/unit/application/test_attach_tenant_policies.py -v`
Expected: PASS (3 tests)

- [ ] **Step 9: Write the failing UC-04 test**

`tests/unit/application/test_run_deterministic_checks.py`:

```python
"""The rules themselves are covered in `tests/unit/domain/test_rules.py`; this file
proves the wiring holds against the realistic note fixtures."""

from datetime import date
from typing import Any
from uuid import uuid4

import pytest

from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.domain.evaluation import Verdict
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Policy
from tests.fakes import FakePiiRedactor
from tests.pii import read_note

TENANT_A_MRI = {
    "id": PolicyId(uuid4()),
    "tenant_id": "tenant-a",
    "name": "MRI lumbar spine",
    "version": 2,
    "covered_codes": [{"code": "M54.5"}, {"code": "M51.26"}],
    "excluded_codes": [{"code": "Z00.00"}, {"code": "Z13.9"}],
    "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
    "effective_from": date(2026, 1, 1),
}


def policies() -> list[Policy]:
    fields: dict[str, Any] = dict(TENANT_A_MRI)
    return [Policy.model_validate(fields)]


@pytest.mark.parametrize(
    ("note", "expected"),
    [
        ("meets", Verdict.PASS),
        ("does_not_meet", Verdict.PASS),
        ("unclear", Verdict.PASS),
        ("excluded_code", Verdict.REJECT),
        ("no_codes", Verdict.UNCERTAIN),
    ],
)
async def test_each_note_fixture_reaches_its_verdict(note: str, expected: Verdict) -> None:
    redacted = await FakePiiRedactor().redact(read_note(note))

    result = await RunDeterministicChecks().execute(redacted, policies())

    assert result.verdict is expected


async def test_every_check_is_reported_not_only_the_failing_ones() -> None:
    redacted = await FakePiiRedactor().redact(read_note("meets"))

    result = await RunDeterministicChecks().execute(redacted, policies())

    assert [check.name for check in result.checks] == [
        "non_empty_text",
        "icd10_present",
        "excluded_code_hit",
        "covered_code_hit",
    ]
```

- [ ] **Step 10: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_run_deterministic_checks.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.run_deterministic_checks'`

- [ ] **Step 11: Write UC-04**

`src/ecet/application/use_cases/run_deterministic_checks.py`:

```python
"""UC-04 RunDeterministicChecks (ADR-002).

A wrapper with no ports: the rules are pure and live in `domain/rules.py`. It is a
use case anyway so UC-01 wires all five sub use cases the same way, and so Phase 6
has one place to increment `deterministic_verdict_total` and `llm_calls_avoided_total`.
"""

from collections.abc import Sequence

from ecet.domain.claim import RedactedText
from ecet.domain.evaluation import DeterministicResult
from ecet.domain.policy import Policy
from ecet.domain.rules import run_checks


class RunDeterministicChecks:
    async def execute(
        self, redacted: RedactedText, policies: Sequence[Policy]
    ) -> DeterministicResult:
        # `async` with nothing to await: the uniform `await use_case.execute(...)` call
        # shape in UC-01 is worth more than saving this frame.
        return run_checks(redacted, policies)
```

- [ ] **Step 12: Run it to verify it passes**

Run: `uv run pytest tests/unit/application/test_run_deterministic_checks.py -v`
Expected: PASS (6 tests)

- [ ] **Step 13: Run the whole gate**

Run: `make check`
Expected: all green.

- [ ] **Step 14: Commit**

```bash
git add src/ecet/application/use_cases tests/unit/application
git commit -m "feat(application): UC-02 redact, UC-03 attach policies, UC-04 deterministic checks"
```

---

### Task 3: The queue message contract and UC-05 EnqueueEvaluation

**Files:**
- Create: `src/ecet/application/messages.py`, `src/ecet/application/ports/evaluation_queue.py`, `src/ecet/application/use_cases/enqueue_evaluation.py`
- Modify: `tests/fakes.py`
- Test: `tests/unit/application/test_messages.py`, `tests/unit/application/test_enqueue_evaluation.py`

**Interfaces:**
- Consumes: `Clock`, `QueuePublishError` (Task 1); `Claim`, `Policy`, `Verdict`, `extract_icd10_codes` (Phase 1).
- Produces:
  - `ecet.application.messages.PolicySnapshot` with classmethod `of(policy: Policy) -> PolicySnapshot`.
  - `ecet.application.messages.EvaluationMessage` — the wire contract Phase 4's consumer validates against.
  - `ecet.application.ports.evaluation_queue.EvaluationQueue` (`async publish(message: EvaluationMessage) -> None`).
  - `EnqueueEvaluation(queue: EvaluationQueue, clock: Clock)` with `async execute(claim: Claim, policies: Sequence[Policy]) -> UUID` returning the `message_id`.
  - `tests.fakes.FakeEvaluationQueue` with `published: list[EvaluationMessage]` and an optional `error`.

- [ ] **Step 1: Write the failing message-contract test**

`tests/unit/application/test_messages.py`:

```python
"""The message is the only thing that crosses the process boundary, so its shape is
tested on its own — a field renamed here silently breaks the Phase 4 worker."""

import json
from datetime import UTC, date, datetime
from uuid import uuid4

from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Policy
from tests.pii import assert_no_pii

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_policy() -> Policy:
    return Policy.model_validate(
        {
            "id": PolicyId(uuid4()),
            "tenant_id": "tenant-a",
            "name": "MRI lumbar spine",
            "version": 2,
            "covered_codes": [{"code": "M54.5"}, {"code": "M51.26"}],
            "excluded_codes": [{"code": "Z00.00"}],
            "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
            "required_evidence": ["clinical examination note"],
            "effective_from": date(2026, 1, 1),
        }
    )


def test_policy_snapshot_flattens_the_code_sets_deterministically() -> None:
    snapshot = PolicySnapshot.of(build_policy())

    assert snapshot.covered_codes == ["M51.26", "M54.5"]
    assert snapshot.excluded_codes == ["Z00.00"]
    assert snapshot.required_evidence == ["clinical examination note"]


def test_the_serialised_message_carries_no_raw_text_and_no_secret() -> None:
    message = EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        entity_counts={"PERSON": 1},
        policies=[PolicySnapshot.of(build_policy())],
        deterministic_verdict="PASS",
        found_codes=["M54.5"],
        enqueued_at=NOW,
    )

    body = json.loads(message.model_dump_json())

    assert body["schema_version"] == 1
    assert set(body) == {
        "schema_version",
        "message_id",
        "claim_id",
        "tenant_id",
        "redacted_text",
        "entity_counts",
        "policies",
        "deterministic_verdict",
        "found_codes",
        "enqueued_at",
    }
    assert "raw_text" not in body
    assert "webhook_secret" not in body
    assert "webhook_url" not in body
    assert_no_pii(message.model_dump_json())


def test_it_round_trips_through_json() -> None:
    message = EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[PolicySnapshot.of(build_policy())],
        deterministic_verdict="UNCERTAIN",
        found_codes=[],
        enqueued_at=NOW,
    )

    assert EvaluationMessage.model_validate_json(message.model_dump_json()) == message
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_messages.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.messages'`

- [ ] **Step 3: Write the message contract**

`src/ecet/application/messages.py`:

```python
"""The `claims.evaluate` wire contract.

Pydantic on both ends: the API builds it, the worker validates it. `schema_version`
is a `Literal[1]`, so a future version 2 fails validation loudly on an old consumer
instead of being half-understood.

Nothing here may carry raw text, a webhook URL or a secret — the queue is the first
place a claim leaves the API process (ADR-001).
"""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ecet.domain.ids import ClaimId, PolicyId, TenantIdField
from ecet.domain.policy import Policy


class PolicySnapshot(BaseModel):
    """A policy frozen at enqueue time. The worker evaluates against this, not against
    a fresh read, so a policy edited mid-flight cannot change a claim's basis."""

    model_config = ConfigDict(frozen=True)

    id: PolicyId
    name: str
    version: int
    covered_codes: list[str]
    excluded_codes: list[str]
    criteria_text: str
    required_evidence: list[str]

    @classmethod
    def of(cls, policy: Policy) -> "PolicySnapshot":
        # `covered_codes`/`excluded_codes` are sets on the domain model; sorting makes
        # the serialised message byte-stable, which keeps message diffs readable.
        return cls(
            id=policy.id,
            name=policy.name,
            version=policy.version,
            covered_codes=sorted(code.code for code in policy.covered_codes),
            excluded_codes=sorted(code.code for code in policy.excluded_codes),
            criteria_text=policy.criteria_text,
            required_evidence=list(policy.required_evidence),
        )


class EvaluationMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1] = 1
    message_id: UUID
    claim_id: ClaimId
    tenant_id: TenantIdField
    redacted_text: str
    entity_counts: dict[str, int] = Field(default_factory=dict)
    policies: list[PolicySnapshot]
    #: `REJECT` never reaches the queue — it short-circuits to human review (UC-01 §9).
    deterministic_verdict: Literal["PASS", "UNCERTAIN"]
    found_codes: list[str] = Field(default_factory=list)
    enqueued_at: AwareDatetime
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run pytest tests/unit/application/test_messages.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Write the failing UC-05 test**

`tests/unit/application/test_enqueue_evaluation.py`:

```python
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest

from ecet.application.errors import QueuePublishError
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Policy
from tests.fakes import FakeEvaluationQueue, FixedClock
from tests.pii import assert_no_pii

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_policy() -> Policy:
    return Policy.model_validate(
        {
            "id": PolicyId(uuid4()),
            "tenant_id": "tenant-a",
            "name": "MRI lumbar spine",
            "version": 2,
            "covered_codes": [{"code": "M54.5"}],
            "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
            "effective_from": date(2026, 1, 1),
        }
    )


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(
            bucket="claims",
            key="tenants/tenant-a/claims/note.pdf",
            etag="etag-1",
            size=12_345,
        ),
        "status": ClaimStatus.POLICIES_ATTACHED,
        "redacted": RedactedText(
            text="Patient <PERSON> with chronic low back pain, M54.5, for eleven months.",
            entity_counts={"PERSON": 1},
            redactor="fake",
        ),
        "deterministic": DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="icd10_present", passed=True)],
        ),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def test_it_publishes_one_message_built_from_the_claim() -> None:
    queue = FakeEvaluationQueue()
    claim = build_claim()

    message_id = await EnqueueEvaluation(queue, FixedClock(NOW)).execute(claim, [build_policy()])

    (published,) = queue.published
    assert published.message_id == message_id
    assert published.claim_id == claim.id
    assert published.tenant_id == "tenant-a"
    assert published.deterministic_verdict == "PASS"
    assert published.found_codes == ["M54.5"]
    assert published.enqueued_at == NOW
    assert [snapshot.name for snapshot in published.policies] == ["MRI lumbar spine"]
    assert_no_pii(published.model_dump_json())


async def test_a_publish_failure_propagates_so_the_claim_stays_policies_attached() -> None:
    queue = FakeEvaluationQueue(error=QueuePublishError("no confirm"))

    with pytest.raises(QueuePublishError):
        await EnqueueEvaluation(queue, FixedClock(NOW)).execute(build_claim(), [build_policy()])

    assert queue.published == []


async def test_a_claim_without_redacted_text_is_a_programming_error() -> None:
    claim = build_claim(redacted=None)

    with pytest.raises(ValueError, match="redacted"):
        await EnqueueEvaluation(FakeEvaluationQueue(), FixedClock(NOW)).execute(
            claim, [build_policy()]
        )


async def test_a_rejected_claim_never_reaches_the_queue() -> None:
    claim = build_claim(
        deterministic=DeterministicResult(
            verdict=Verdict.REJECT,
            checks=[CheckOutcome(name="excluded_code_hit", passed=False)],
        )
    )

    with pytest.raises(ValueError, match="REJECT"):
        await EnqueueEvaluation(FakeEvaluationQueue(), FixedClock(NOW)).execute(
            claim, [build_policy()]
        )
```

- [ ] **Step 6: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_enqueue_evaluation.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.ports.evaluation_queue'`

- [ ] **Step 7: Write the queue port**

`src/ecet/application/ports/evaluation_queue.py`:

```python
"""Publish side of `claims.evaluate`. The consumer side lives in the worker (Phase 4)
and is not a port — it drives the process rather than being called by a use case."""

from typing import Protocol, runtime_checkable

from ecet.application.messages import EvaluationMessage


@runtime_checkable
class EvaluationQueue(Protocol):
    async def publish(self, message: EvaluationMessage) -> None:
        """Raises `QueuePublishError` when the broker does not confirm the publish."""
        ...
```

- [ ] **Step 8: Write UC-05**

`src/ecet/application/use_cases/enqueue_evaluation.py`:

```python
"""UC-05 EnqueueEvaluation.

The hand-off point. Everything the worker needs travels in the message — nothing it
needs is re-read from the API's memory, and nothing the tenant owns (webhook URL,
HMAC secret) travels with it.
"""

from collections.abc import Sequence
from uuid import UUID, uuid4

from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.application.ports.clock import Clock
from ecet.application.ports.evaluation_queue import EvaluationQueue
from ecet.domain.claim import Claim
from ecet.domain.evaluation import Verdict
from ecet.domain.policy import Policy
from ecet.domain.rules import extract_icd10_codes


class EnqueueEvaluation:
    def __init__(self, queue: EvaluationQueue, clock: Clock) -> None:
        self._queue = queue
        self._clock = clock

    async def execute(self, claim: Claim, policies: Sequence[Policy]) -> UUID:
        redacted = claim.redacted
        if redacted is None:
            raise ValueError(f"claim {claim.id} has no redacted text to enqueue")
        deterministic = claim.deterministic
        if deterministic is None:
            raise ValueError(f"claim {claim.id} has no deterministic result to enqueue")
        if deterministic.verdict is Verdict.REJECT:
            raise ValueError(f"claim {claim.id} is a deterministic REJECT and must not be queued")

        message = EvaluationMessage(
            message_id=uuid4(),
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            redacted_text=redacted.text,
            entity_counts=dict(redacted.entity_counts),
            policies=[PolicySnapshot.of(policy) for policy in policies],
            deterministic_verdict=deterministic.verdict.value,
            # Recomputed rather than carried on `DeterministicResult`, which records
            # check outcomes, not the codes themselves.
            found_codes=[code.code for code in extract_icd10_codes(redacted.text)],
            enqueued_at=self._clock.now(),
        )
        await self._queue.publish(message)
        return message.message_id
```

- [ ] **Step 9: Add `FakeEvaluationQueue` to `tests/fakes.py`**

Append (and add `from ecet.application.messages import EvaluationMessage` to the
imports):

```python
class FakeEvaluationQueue:
    """Records publishes. `error` makes the next publish fail, which is how UC-01's
    'claim stays POLICIES_ATTACHED' branch is exercised."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.published: list[EvaluationMessage] = []
        self.error = error

    async def publish(self, message: EvaluationMessage) -> None:
        if self.error is not None:
            raise self.error
        self.published.append(message)
```

- [ ] **Step 10: Run it to verify it passes**

Run: `uv run pytest tests/unit/application/test_enqueue_evaluation.py -v`
Expected: PASS (4 tests)

- [ ] **Step 11: Add the queue port to the conformance test**

Append to `tests/unit/application/test_ports.py`:

```python
def test_fake_evaluation_queue_is_an_evaluation_queue() -> None:
    from ecet.application.ports.evaluation_queue import EvaluationQueue
    from tests.fakes import FakeEvaluationQueue

    assert isinstance(FakeEvaluationQueue(), EvaluationQueue)
```

Run: `uv run pytest tests/unit/application/test_ports.py -v` → PASS (5 tests).

- [ ] **Step 12: Run the whole gate**

Run: `make check`
Expected: all green.

- [ ] **Step 13: Commit**

```bash
git add src/ecet/application tests/fakes.py tests/unit/application
git commit -m "feat(application): evaluation message contract and UC-05 enqueue"
```

---

### Task 4: UC-09a RequestHumanReview

Lands in Phase 3, not Phase 4: UC-01's deterministic-`REJECT` branch opens the review
task. Phase 4 reuses this class unchanged for the low-confidence branch.

**Files:**
- Create: `src/ecet/application/use_cases/human_review.py`
- Test: `tests/unit/application/test_request_human_review.py`

**Interfaces:**
- Consumes: `Clock` (Task 1); `ReviewTaskRepository`, `ReviewTask`, `ReviewReason`, `ReviewStatus` (Phase 1).
- Produces: `RequestHumanReview(review_tasks: ReviewTaskRepository, clock: Clock)` with `async execute(claim: Claim, reason: ReviewReason) -> ReviewTask`.

- [ ] **Step 1: Write the failing test**

`tests/unit/application/test_request_human_review.py`:

```python
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import ReviewReason, ReviewStatus
from ecet.domain.ids import ClaimId
from tests.fakes import FakeReviewTaskRepository, FixedClock

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(
            bucket="claims",
            key="tenants/tenant-a/claims/note.pdf",
            etag="etag-1",
            size=12_345,
        ),
        "status": ClaimStatus.POLICIES_ATTACHED,
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def test_it_opens_a_task_for_the_claims_tenant() -> None:
    repository = FakeReviewTaskRepository()
    claim = build_claim()

    task = await RequestHumanReview(repository, FixedClock(NOW)).execute(
        claim, ReviewReason.DETERMINISTIC_REJECT
    )

    assert task.claim_id == claim.id
    assert task.tenant_id == "tenant-a"
    assert task.reason is ReviewReason.DETERMINISTIC_REJECT
    assert task.status is ReviewStatus.OPEN
    assert task.created_at == NOW
    assert list(repository.tasks) == [task.id]


async def test_a_second_request_returns_the_open_task_instead_of_duplicating_it() -> None:
    repository = FakeReviewTaskRepository()
    claim = build_claim()
    use_case = RequestHumanReview(repository, FixedClock(NOW))

    first = await use_case.execute(claim, ReviewReason.DETERMINISTIC_REJECT)
    second = await use_case.execute(claim, ReviewReason.LOW_CONFIDENCE)

    assert second.id == first.id
    assert second.reason is ReviewReason.DETERMINISTIC_REJECT
    assert len(repository.tasks) == 1
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_request_human_review.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.human_review'`

- [ ] **Step 3: Write UC-09a**

`src/ecet/application/use_cases/human_review.py`:

```python
"""UC-09a RequestHumanReview.

Idempotent by claim: `review_tasks.claim_id` is unique in the schema, so a second
request for a claim that already has an open task returns that task rather than
racing the constraint. The caller — not this use case — transitions the claim to
`REVIEW_PENDING`, because only the caller knows whether the claim is also being
saved in the same unit of work.

UC-09b (list) and UC-09c (resolve) arrive in Phase 5 and will share this module.
"""

from uuid import uuid4

from ecet.application.ports.clock import Clock
from ecet.domain.claim import Claim
from ecet.domain.evaluation import ReviewReason, ReviewTask
from ecet.domain.ports.review_task_repository import ReviewTaskRepository


class RequestHumanReview:
    def __init__(self, review_tasks: ReviewTaskRepository, clock: Clock) -> None:
        self._review_tasks = review_tasks
        self._clock = clock

    async def execute(self, claim: Claim, reason: ReviewReason) -> ReviewTask:
        existing = await self._review_tasks.find_open_by_claim(claim.id)
        if existing is not None:
            return existing

        task = ReviewTask(
            id=uuid4(),
            claim_id=claim.id,
            tenant_id=claim.tenant_id,
            reason=reason,
            created_at=self._clock.now(),
        )
        await self._review_tasks.add(task)
        return task
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run pytest tests/unit/application/test_request_human_review.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Run the whole gate**

Run: `make check`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/human_review.py tests/unit/application/test_request_human_review.py
git commit -m "feat(application): UC-09a request human review"
```

---

### Task 5: UC-01 IngestClaimDocument

The orchestrator. It owns the transaction boundary, the claim's lifecycle and the
ADR-001 guarantee that raw text never outlives one local variable.

**Files:**
- Create: `src/ecet/application/use_cases/ingest_claim_document.py`
- Test: `tests/unit/application/test_ingest_claim_document.py`

**Interfaces:**
- Consumes: `UnitOfWork` (Phase 2); `ObjectStorage`, `TextExtractor`, `Clock`, `ExtractionFailed`, `ObjectNotFound`, `QueuePublishError` (Task 1); `RedactPii`, `AttachTenantPolicies`, `RunDeterministicChecks` (Task 2); `EnqueueEvaluation` (Task 3); `RequestHumanReview` (Task 4).
- Produces:
  - `IngestCommand(bucket: str, key: str, etag: str, size: int)`.
  - `IngestResult(claim_id: ClaimId, status: ClaimStatus, duplicate: bool = False)`.
  - `IngestClaimDocument(...)` with `async execute(command: IngestCommand) -> IngestResult`. Constructor is keyword-only: `uow_factory`, `storage`, `extractor`, `redact_pii`, `run_checks`, `enqueue`, `clock`, `max_pdf_bytes`.
  - UC-03 and UC-09a are **not** constructor arguments: they depend on repositories that live inside a unit of work, whose lifetime is one request. UC-01 builds them per request from the live `uow`.

- [ ] **Step 1: Write the failing test file**

`tests/unit/application/test_ingest_claim_document.py`:

```python
"""UC-01 against fakes only. Every test asserts on the claim as it was *persisted*,
because that is where an ADR-001 leak would show up."""

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest

from ecet.application.errors import ExtractionFailed, ObjectNotFound, QueuePublishError
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.ingest_claim_document import (
    IngestClaimDocument,
    IngestCommand,
)
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.domain.claim import ClaimStatus
from ecet.domain.errors import InvalidObjectKey, PdfTooLarge, TenantNotFound
from ecet.domain.evaluation import ReviewReason, ReviewStatus
from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant
from tests.fakes import (
    FakeEvaluationQueue,
    FakeObjectStorage,
    FakePiiRedactor,
    FakeTextExtractor,
    FakeUnitOfWork,
    FixedClock,
)
from tests.pii import assert_no_pii, read_note

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
BUCKET = "claims"
KEY = "tenants/tenant-a/claims/note-1.pdf"
PDF = b"%PDF-1.7 fake bytes"


def build_tenant(tenant_id: str = "tenant-a", **overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": tenant_id,
        "name": tenant_id,
        "webhook_url": f"http://mock-client:8081/hooks/{tenant_id}",
        "webhook_secret": f"dev-hmac-{tenant_id}",
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 2,
        "covered_codes": [{"code": "M54.5"}, {"code": "M51.26"}],
        "excluded_codes": [{"code": "Z00.00"}],
        "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


class Harness:
    """Everything a UC-01 test needs, wired once. Attributes are the fakes, so a test
    can assert on `harness.queue.published` or `harness.uow.claims.claims`."""

    def __init__(
        self,
        *,
        note: str = "meets",
        tenants: list[Tenant] | None = None,
        policies: list[Policy] | None = None,
        extractor: FakeTextExtractor | None = None,
        queue: FakeEvaluationQueue | None = None,
        max_pdf_bytes: int = 20_000_000,
    ) -> None:
        self.clock = FixedClock(NOW)
        self.uow = FakeUnitOfWork(
            tenants=tenants if tenants is not None else [build_tenant()],
            policies=policies if policies is not None else [build_policy()],
        )
        self.storage = FakeObjectStorage({(BUCKET, KEY): PDF})
        self.extractor = extractor or FakeTextExtractor(read_note(note))
        self.queue = queue or FakeEvaluationQueue()
        self.use_case = IngestClaimDocument(
            uow_factory=lambda: self.uow,
            storage=self.storage,
            extractor=self.extractor,
            redact_pii=RedactPii(FakePiiRedactor()),
            run_checks=RunDeterministicChecks(),
            enqueue=EnqueueEvaluation(self.queue, self.clock),
            clock=self.clock,
            max_pdf_bytes=max_pdf_bytes,
        )

    async def ingest(self, **overrides: Any) -> Any:
        fields: dict[str, Any] = {
            "bucket": BUCKET,
            "key": KEY,
            "etag": "etag-1",
            "size": len(PDF),
        }
        fields.update(overrides)
        return await self.use_case.execute(IngestCommand(**fields))


async def test_the_happy_path_queues_the_claim() -> None:
    harness = Harness()

    result = await harness.ingest()

    assert result.duplicate is False
    assert result.status is ClaimStatus.QUEUED
    stored = harness.uow.claims.claims[result.claim_id]
    assert stored.status is ClaimStatus.QUEUED
    assert stored.policy_ids
    assert stored.deterministic is not None
    assert len(harness.queue.published) == 1


async def test_the_persisted_claim_holds_redacted_text_only() -> None:
    harness = Harness()

    result = await harness.ingest()

    stored = harness.uow.claims.claims[result.claim_id]
    assert stored.redacted is not None
    assert_no_pii(stored.model_dump_json())
    assert_no_pii(harness.queue.published[0].model_dump_json())


async def test_a_duplicate_source_object_is_a_no_op() -> None:
    harness = Harness()
    first = await harness.ingest()

    second = await harness.ingest()

    assert second.duplicate is True
    assert second.claim_id == first.claim_id
    assert second.status is ClaimStatus.QUEUED
    assert len(harness.queue.published) == 1
    assert harness.extractor.calls == 1


async def test_an_unknown_tenant_creates_no_claim() -> None:
    harness = Harness(tenants=[])

    with pytest.raises(TenantNotFound):
        await harness.ingest()

    assert harness.uow.claims.claims == {}


async def test_an_inactive_tenant_creates_no_claim() -> None:
    harness = Harness(tenants=[build_tenant(active=False)])

    with pytest.raises(TenantNotFound):
        await harness.ingest()

    assert harness.uow.claims.claims == {}


async def test_a_key_outside_the_tenant_layout_is_rejected_before_any_io() -> None:
    harness = Harness()

    with pytest.raises(InvalidObjectKey):
        await harness.ingest(key="uploads/note.pdf")

    assert harness.storage.reads == []
    assert harness.uow.claims.claims == {}


async def test_an_oversized_object_is_rejected_before_any_io() -> None:
    harness = Harness(max_pdf_bytes=10)

    with pytest.raises(PdfTooLarge):
        await harness.ingest()

    assert harness.storage.reads == []
    assert harness.uow.claims.claims == {}


async def test_a_tenant_with_no_policies_persists_no_policies_and_raises() -> None:
    from ecet.domain.errors import NoPoliciesForTenant

    harness = Harness(policies=[])

    with pytest.raises(NoPoliciesForTenant):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.NO_POLICIES
    assert stored.failure_reason == "tenant-a"
    assert harness.queue.published == []


async def test_an_extraction_failure_persists_extraction_failed() -> None:
    harness = Harness(extractor=FakeTextExtractor(error=ExtractionFailed("no_text")))

    with pytest.raises(ExtractionFailed):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.EXTRACTION_FAILED
    assert stored.failure_reason == "no_text"


async def test_a_missing_object_is_reported_as_an_extraction_failure() -> None:
    harness = Harness()
    harness.storage.objects.clear()

    with pytest.raises(ExtractionFailed):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.EXTRACTION_FAILED
    assert stored.failure_reason == "object_unavailable"
    assert_no_pii(stored.model_dump_json())


async def test_empty_extracted_text_is_an_extraction_failure() -> None:
    harness = Harness(extractor=FakeTextExtractor("   \n\n  "))

    with pytest.raises(ExtractionFailed):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.EXTRACTION_FAILED
    assert stored.failure_reason == "empty_text"


async def test_a_deterministic_reject_opens_a_review_and_skips_the_queue() -> None:
    harness = Harness(note="excluded_code")

    result = await harness.ingest()

    assert result.status is ClaimStatus.REVIEW_PENDING
    assert harness.queue.published == []
    (task,) = harness.uow.review_tasks.tasks.values()
    assert task.claim_id == result.claim_id
    assert task.reason is ReviewReason.DETERMINISTIC_REJECT
    assert task.status is ReviewStatus.OPEN


async def test_an_uncertain_verdict_still_reaches_the_queue() -> None:
    harness = Harness(note="no_codes")

    result = await harness.ingest()

    assert result.status is ClaimStatus.QUEUED
    assert harness.queue.published[0].deterministic_verdict == "UNCERTAIN"


async def test_a_publish_failure_leaves_the_claim_policies_attached() -> None:
    harness = Harness(queue=FakeEvaluationQueue(error=QueuePublishError("no confirm")))

    with pytest.raises(QueuePublishError):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.POLICIES_ATTACHED
    assert stored.redacted is not None


async def test_the_claim_carries_the_key_derived_tenant() -> None:
    harness = Harness()

    result = await harness.ingest()

    assert harness.uow.claims.claims[result.claim_id].tenant_id == TenantId("tenant-a")


async def test_object_not_found_never_reaches_the_caller_verbatim() -> None:
    harness = Harness()
    harness.storage.objects.clear()

    with pytest.raises(ExtractionFailed) as caught:
        await harness.ingest()

    # The key embeds a client-supplied filename; the reason token must not carry it.
    assert "note-1.pdf" not in str(caught.value)
    assert not isinstance(caught.value, ObjectNotFound)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.application.use_cases.ingest_claim_document'`

- [ ] **Step 3: Write UC-01**

`src/ecet/application/use_cases/ingest_claim_document.py`:

```python
"""UC-01 IngestClaimDocument — the ingestion orchestrator.

Two properties the code is arranged around:

**ADR-001.** The raw extracted text exists as exactly one local variable, `text`, in
`_run_pipeline`. It is passed to `RedactPii` and then deleted. It is never assigned to
the claim, never logged, never returned.

**Auditability.** The claim row is inserted and committed before any of the fallible
steps run, so a failure leaves a claim to look at rather than nothing at all. The two
recoverable failure states the state machine allows from that point —
`EXTRACTION_FAILED` and `NO_POLICIES` — are persisted and committed on their own path
before the error is re-raised.

The publish (step 10) happens *after* the `POLICIES_ATTACHED` commit. A publish that
fails therefore leaves a durable `POLICIES_ATTACHED` claim that a retry can re-send;
the transactional outbox that would make this atomic is deferred out of v1.
"""

from collections.abc import Callable, Sequence
from uuid import uuid4

import structlog
from pydantic import BaseModel, ConfigDict

from ecet.application.errors import ExtractionFailed, ObjectNotFound
from ecet.application.ports.clock import Clock
from ecet.application.ports.object_storage import ObjectStorage
from ecet.application.ports.text_extractor import TextExtractor
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.attach_tenant_policies import AttachTenantPolicies
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.errors import NoPoliciesForTenant
from ecet.domain.evaluation import ReviewReason, Verdict
from ecet.domain.ids import ClaimId
from ecet.domain.policy import Policy

log = structlog.get_logger(__name__)


class IngestCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str
    etag: str
    size: int


class IngestResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    claim_id: ClaimId
    status: ClaimStatus
    duplicate: bool = False


class IngestClaimDocument:
    def __init__(
        self,
        *,
        uow_factory: Callable[[], UnitOfWork],
        storage: ObjectStorage,
        extractor: TextExtractor,
        redact_pii: RedactPii,
        run_checks: RunDeterministicChecks,
        enqueue: EnqueueEvaluation,
        clock: Clock,
        max_pdf_bytes: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._storage = storage
        self._extractor = extractor
        self._redact_pii = redact_pii
        self._run_checks = run_checks
        self._enqueue = enqueue
        self._clock = clock
        self._max_pdf_bytes = max_pdf_bytes

    async def execute(self, command: IngestCommand) -> IngestResult:
        # `SourceObject` validates the key layout and yields the tenant; both failures
        # happen before any I/O, so a malformed event costs one validation.
        source = SourceObject(
            bucket=command.bucket, key=command.key, etag=command.etag, size=command.size
        )
        source.ensure_size_within(self._max_pdf_bytes)
        tenant_id = source.tenant_id()

        async with self._uow_factory() as uow:
            duplicate = await uow.claims.find_by_source(source.bucket, source.key, source.etag)
            if duplicate is not None:
                # ADR-006: same object, same content — nothing to redo.
                log.info(
                    "claim.duplicate",
                    claim_id=str(duplicate.id),
                    tenant_id=str(tenant_id),
                    status=duplicate.status.value,
                )
                return IngestResult(
                    claim_id=duplicate.id, status=duplicate.status, duplicate=True
                )

            await uow.tenants.get(tenant_id)  # raises TenantNotFound; no claim is created

            now = self._clock.now()
            claim = Claim(
                id=ClaimId(uuid4()),
                tenant_id=tenant_id,
                source=source,
                created_at=now,
                updated_at=now,
            )
            await uow.claims.add(claim)
            await uow.commit()

            await self._run_pipeline(uow, claim)
            await uow.commit()
            return IngestResult(claim_id=claim.id, status=claim.status)

    async def _run_pipeline(self, uow: UnitOfWork, claim: Claim) -> None:
        # Both depend on repositories owned by this unit of work, so they are built
        # here rather than injected — a request-scoped dependency cannot be a
        # process-scoped constructor argument.
        attach_policies = AttachTenantPolicies(uow.policies, self._clock)
        request_review = RequestHumanReview(uow.review_tasks, self._clock)

        try:
            text = await self._read_text(claim.source)
        except ExtractionFailed as error:
            await self._fail(uow, claim, ClaimStatus.EXTRACTION_FAILED, str(error))
            raise
        self._advance(claim, ClaimStatus.EXTRACTED)

        redacted = await self._redact_pii.execute(text)
        del text  # ADR-001: the raw note stops existing here.
        claim.redacted = redacted
        self._advance(claim, ClaimStatus.REDACTED)

        try:
            policies: Sequence[Policy] = await attach_policies.execute(claim.tenant_id)
        except NoPoliciesForTenant as error:
            await self._fail(uow, claim, ClaimStatus.NO_POLICIES, str(error))
            raise
        claim.policy_ids = [policy.id for policy in policies]
        self._advance(claim, ClaimStatus.POLICIES_ATTACHED)

        deterministic = await self._run_checks.execute(redacted, policies)
        claim.deterministic = deterministic

        if deterministic.verdict is Verdict.REJECT:
            # ADR-002's saving: a human looks at it, the LLM is never called.
            await request_review.execute(claim, ReviewReason.DETERMINISTIC_REJECT)
            self._advance(claim, ClaimStatus.REVIEW_PENDING)
            await uow.claims.save(claim)
            return

        # Commit before publishing: a publish failure must leave a durable
        # POLICIES_ATTACHED claim, not a rolled-back one.
        await uow.claims.save(claim)
        await uow.commit()

        await self._enqueue.execute(claim, policies)
        self._advance(claim, ClaimStatus.QUEUED)
        await uow.claims.save(claim)

    async def _read_text(self, source: SourceObject) -> str:
        try:
            data = await self._storage.get_bytes(source.bucket, source.key)
        except ObjectNotFound as error:
            # The message embeds the client-supplied key; the reason token must not.
            raise ExtractionFailed("object_unavailable") from error
        text = await self._extractor.extract(data)
        if not text.strip():
            raise ExtractionFailed("empty_text")
        return text

    def _advance(self, claim: Claim, status: ClaimStatus) -> None:
        claim.transition(status, now=self._clock.now())
        log.info(
            "claim.transition",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
        )

    async def _fail(
        self, uow: UnitOfWork, claim: Claim, status: ClaimStatus, reason: str
    ) -> None:
        claim.transition(status, reason=reason, now=self._clock.now())
        log.warning(
            "claim.failed",
            claim_id=str(claim.id),
            tenant_id=str(claim.tenant_id),
            to=status.value,
            reason=reason,
        )
        await uow.claims.save(claim)
        await uow.commit()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/application/test_ingest_claim_document.py -v`
Expected: PASS (16 tests)

If `test_a_tenant_with_no_policies_persists_no_policies_and_raises` fails on
`failure_reason`, check `NoPoliciesForTenant(str(tenant_id))` in UC-03 — the reason
persisted is the exception's `str()`, which is the bare tenant id by design (safe to
echo, unlike a key).

- [ ] **Step 5: Run the whole gate**

Run: `make check`
Expected: all green. `mypy --strict` must accept `redacted` being used after
`claim.redacted = redacted` — the code deliberately keeps using the local, not the
attribute, because an attribute's narrowing does not survive an `await`.

- [ ] **Step 6: Commit**

```bash
git add src/ecet/application/use_cases/ingest_claim_document.py tests/unit/application/test_ingest_claim_document.py
git commit -m "feat(application): UC-01 ingest claim document orchestrator"
```

---

### Task 6: MinIO / S3 object storage adapter

**Files:**
- Modify: `pyproject.toml` (dependency + mypy overrides)
- Create: `src/ecet/infrastructure/storage/__init__.py`, `src/ecet/infrastructure/storage/s3.py`
- Create: `tests/adapters/containers.py`
- Test: `tests/adapters/test_s3_storage.py`

**Interfaces:**
- Consumes: `ObjectStorage`, `ObjectHead`, `ObjectNotFound` (Task 1).
- Produces:
  - `ecet.infrastructure.storage.s3.S3ObjectStorage(*, endpoint_url: str | None, access_key: str, secret_key: str, region: str = "us-east-1")` implementing `ObjectStorage`.
  - `tests.adapters.containers.wait_until(check: Callable[[], Awaitable[None]], *, timeout: float = 60.0) -> None` — a retry helper reused by the RabbitMQ test in Task 9.

- [ ] **Step 1: Add the dependency and the mypy overrides**

```bash
cd /Users/harielgiacomuzzi/Development/FDE-Study/HealthcareTriageEngine
uv add 'aiobotocore>=2.15,<3'
```

`aiobotocore` pins `botocore` itself — never add `botocore` or `boto3` by hand.

Neither ships type information, so append to `pyproject.toml`:

```toml
[[tool.mypy.overrides]]
module = ["aiobotocore.*", "botocore.*"]
ignore_missing_imports = true
```

- [ ] **Step 2: Write the failing adapter test**

`tests/adapters/test_s3_storage.py`:

```python
"""Real MinIO in Docker. The generic `DockerContainer` is used rather than a
testcontainers MinIO module so the test does not depend on which extra package
provides it in the resolved version."""

from collections.abc import AsyncIterator, Iterator

import pytest
from testcontainers.core.container import DockerContainer

from ecet.application.errors import ObjectNotFound
from ecet.infrastructure.storage.s3 import S3ObjectStorage
from tests.adapters.containers import wait_until

BUCKET = "claims"
KEY = "tenants/tenant-a/claims/note with spaces.pdf"
BODY = b"%PDF-1.7 not really a pdf, just bytes"


@pytest.fixture(scope="session")
def minio_endpoint() -> Iterator[str]:
    container = (
        DockerContainer("minio/minio:RELEASE.2025-09-07T16-13-09Z")
        .with_command("server /data")
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_exposed_ports(9000)
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(9000)
        yield f"http://{host}:{port}"


@pytest.fixture
async def storage(minio_endpoint: str) -> AsyncIterator[S3ObjectStorage]:
    adapter = S3ObjectStorage(
        endpoint_url=minio_endpoint, access_key="minioadmin", secret_key="minioadmin"
    )

    async def create_bucket() -> None:
        async with adapter.client() as client:
            try:
                await client.create_bucket(Bucket=BUCKET)
            except client.exceptions.BucketAlreadyOwnedByYou:
                pass

    # MinIO answers the port before it answers the API; retry until a call succeeds.
    await wait_until(create_bucket)
    yield adapter


async def test_round_trip(storage: S3ObjectStorage) -> None:
    async with storage.client() as client:
        await client.put_object(Bucket=BUCKET, Key=KEY, Body=BODY)

    assert await storage.get_bytes(BUCKET, KEY) == BODY


async def test_head_returns_the_etag_and_size(storage: S3ObjectStorage) -> None:
    async with storage.client() as client:
        await client.put_object(Bucket=BUCKET, Key=KEY, Body=BODY)

    head = await storage.head(BUCKET, KEY)

    assert head.size == len(BODY)
    assert head.etag and '"' not in head.etag


async def test_a_missing_key_raises_object_not_found(storage: S3ObjectStorage) -> None:
    with pytest.raises(ObjectNotFound):
        await storage.get_bytes(BUCKET, "tenants/tenant-a/claims/absent.pdf")


async def test_a_missing_key_raises_object_not_found_on_head(
    storage: S3ObjectStorage,
) -> None:
    with pytest.raises(ObjectNotFound):
        await storage.head(BUCKET, "tenants/tenant-a/claims/absent.pdf")


async def test_a_missing_bucket_raises_object_not_found(storage: S3ObjectStorage) -> None:
    with pytest.raises(ObjectNotFound):
        await storage.get_bytes("no-such-bucket", KEY)
```

- [ ] **Step 3: Run it to verify it fails**

Run: `uv run pytest tests/adapters/test_s3_storage.py -m slow -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.storage.s3'`

- [ ] **Step 4: Write the container retry helper**

`tests/adapters/containers.py`:

```python
"""Container readiness without log-scraping.

A container's port opens before its API answers, and the log lines that mean "ready"
change between image releases. Retrying the call the test actually needs is both
shorter and more durable than either.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable


async def wait_until(check: Callable[[], Awaitable[None]], *, timeout: float = 60.0) -> None:
    """Call `check` until it returns without raising, or re-raise the last error."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            await check()
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.5)
```

- [ ] **Step 5: Write the adapter**

`src/ecet/infrastructure/storage/__init__.py` (empty) and
`src/ecet/infrastructure/storage/s3.py`:

```python
"""`ObjectStorage` over the S3 API.

One code path for MinIO and AWS: the only difference is `endpoint_url`. A client is
created per call — `aiobotocore` clients are async context managers bound to the
running loop, and the cost is a local object, not a connection handshake, because
botocore pools the underlying HTTP session.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from ecet.application.errors import ObjectNotFound
from ecet.application.ports.object_storage import ObjectHead

log = structlog.get_logger(__name__)

#: What S3 and MinIO return for "the object, or its bucket, is not there".
_MISSING_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})


class S3ObjectStorage:
    def __init__(
        self,
        *,
        endpoint_url: str | None,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._session = get_session()
        self._client_kwargs: dict[str, Any] = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
        }

    @asynccontextmanager
    async def client(self) -> AsyncIterator[Any]:
        """Exposed so adapter tests can seed objects through the same credentials."""
        async with self._session.create_client("s3", **self._client_kwargs) as client:
            yield client

    async def get_bytes(self, bucket: str, key: str) -> bytes:
        async with self.client() as client:
            try:
                response = await client.get_object(Bucket=bucket, Key=key)
            except ClientError as error:
                raise _translate(error, bucket, key) from error
            async with response["Body"] as stream:
                data: bytes = await stream.read()
        # Never log the key at info level with the body size only — the key carries a
        # client-supplied filename. Debug-level, bucket only.
        log.debug("storage.read", bucket=bucket, bytes=len(data))
        return data

    async def head(self, bucket: str, key: str) -> ObjectHead:
        async with self.client() as client:
            try:
                response = await client.head_object(Bucket=bucket, Key=key)
            except ClientError as error:
                raise _translate(error, bucket, key) from error
        # S3 quotes etags; `SourceObject` compares them as plain strings.
        return ObjectHead(
            etag=str(response["ETag"]).strip('"'), size=int(response["ContentLength"])
        )


def _translate(error: ClientError, bucket: str, key: str) -> Exception:
    code = str(error.response.get("Error", {}).get("Code", ""))
    if code in _MISSING_CODES:
        return ObjectNotFound(f"{bucket}/{key}")
    return error
```

- [ ] **Step 6: Run the adapter test to verify it passes**

Run: `uv run pytest tests/adapters/test_s3_storage.py -m slow -v`
Expected: PASS (5 tests). Needs a running Docker daemon.

- [ ] **Step 7: Confirm the default run is unaffected**

Run: `uv run pytest`
Expected: PASS, with the S3 tests deselected (`tests/adapters/` is auto-marked `slow`).

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock src/ecet/infrastructure/storage tests/adapters
git commit -m "feat(storage): aiobotocore S3/MinIO object storage adapter"
```

---

### Task 7: pypdf text extractor and the PDF fixtures

**Files:**
- Modify: `pyproject.toml` (dependencies), `Makefile`, `.gitignore`
- Create: `src/ecet/infrastructure/pdf/__init__.py`, `src/ecet/infrastructure/pdf/pypdf_extractor.py`
- Create: `scripts/make_fixtures.py`
- Test: `tests/unit/infrastructure/__init__.py`, `tests/unit/infrastructure/test_pypdf_extractor.py`

**Interfaces:**
- Consumes: `TextExtractor`, `ExtractionFailed` (Task 1); the note fixtures (Task 1).
- Produces:
  - `ecet.infrastructure.pdf.pypdf_extractor.PypdfTextExtractor(*, max_pages: int, min_chars: int = 50)` implementing `TextExtractor`.
  - `scripts.make_fixtures.build_all(destination: Path) -> dict[str, Path]` writing `note_simple.pdf`, `note_multipage.pdf`, `scanned.pdf`, `encrypted.pdf`.

- [ ] **Step 1: Add the dependencies**

```bash
uv add 'pypdf>=4,<5' 'anyio>=4.4,<5'
uv add --dev 'reportlab>=4.2,<5'
```

`anyio` already arrives transitively through starlette; it is added explicitly
because this module imports it directly. `reportlab` is dev-only — fixtures are
generated, never shipped.

- [ ] **Step 2: Write the fixture generator**

`scripts/make_fixtures.py`:

```python
"""Generate the PDF fixtures from the note fixtures.

Reproducible instead of committed: a binary in git is a binary nobody can review, and
the text inside these ones is the thing under test. `tests/fixtures/pdfs/` is
gitignored; the test session regenerates anything missing.

Run directly: `make fixtures`.
"""

from pathlib import Path

from reportlab.lib.pagesizes import LETTER
from reportlab.lib.pdfencrypt import StandardEncryption
from reportlab.pdfgen import canvas

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTES_DIR = REPO_ROOT / "tests" / "fixtures" / "notes"
PDFS_DIR = REPO_ROOT / "tests" / "fixtures" / "pdfs"

LEFT_MARGIN = 72
TOP_MARGIN = 720
LINE_HEIGHT = 14


def _write_lines(target: canvas.Canvas, lines: list[str]) -> None:
    y = TOP_MARGIN
    for line in lines:
        if y < LEFT_MARGIN:
            target.showPage()
            y = TOP_MARGIN
        target.setFont("Helvetica", 10)
        target.drawString(LEFT_MARGIN, y, line[:110])
        y -= LINE_HEIGHT


def _note_lines(name: str) -> list[str]:
    return (NOTES_DIR / f"{name}.txt").read_text(encoding="utf-8").splitlines()


def build_all(destination: Path) -> dict[str, Path]:
    destination.mkdir(parents=True, exist_ok=True)
    built: dict[str, Path] = {}

    simple = destination / "note_simple.pdf"
    target = canvas.Canvas(str(simple), pagesize=LETTER)
    _write_lines(target, _note_lines("meets"))
    target.save()
    built["note_simple"] = simple

    multipage = destination / "note_multipage.pdf"
    target = canvas.Canvas(str(multipage), pagesize=LETTER)
    for note in ("meets", "does_not_meet", "unclear"):
        _write_lines(target, _note_lines(note))
        target.showPage()
    target.save()
    built["note_multipage"] = multipage

    # No text operators at all — the stand-in for a scanned page. OCR is out of scope
    # for v1, so this must fail extraction rather than half-succeed.
    scanned = destination / "scanned.pdf"
    target = canvas.Canvas(str(scanned), pagesize=LETTER)
    target.rect(100, 100, 400, 500, stroke=1, fill=0)
    target.save()
    built["scanned"] = scanned

    encrypted = destination / "encrypted.pdf"
    target = canvas.Canvas(
        str(encrypted),
        pagesize=LETTER,
        encrypt=StandardEncryption("owner-secret", userPassword="user-secret"),
    )
    _write_lines(target, _note_lines("meets"))
    target.save()
    built["encrypted"] = encrypted

    return built


if __name__ == "__main__":
    for name, path in build_all(PDFS_DIR).items():
        print(f"{name}: {path.relative_to(REPO_ROOT)}")
```

Add to `.gitignore`:

```gitignore
tests/fixtures/pdfs/
```

Add to the `Makefile` (and to `.PHONY`):

```make
fixtures:
	uv run python scripts/make_fixtures.py
```

- [ ] **Step 3: Generate the fixtures and eyeball one**

```bash
make fixtures
uv run python -c "from pypdf import PdfReader; print(PdfReader('tests/fixtures/pdfs/note_simple.pdf').pages[0].extract_text()[:120])"
```

Expected: the first lines of `meets.txt`. If `extract_text()` is empty, the
`drawString` calls did not run — check `_note_lines` found the notes directory.

- [ ] **Step 4: Write the failing extractor test**

`tests/unit/infrastructure/__init__.py` (empty) and
`tests/unit/infrastructure/test_pypdf_extractor.py`:

```python
"""The pypdf adapter needs no container, so it stays in the default run rather than
living under `tests/adapters/` where the whole directory is marked `slow`."""

from pathlib import Path

import pytest

from ecet.application.errors import ExtractionFailed
from ecet.infrastructure.pdf.pypdf_extractor import PypdfTextExtractor
from scripts.make_fixtures import build_all

PDFS_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "pdfs"


@pytest.fixture(scope="session")
def pdfs() -> dict[str, Path]:
    """Regenerate on demand — `tests/fixtures/pdfs/` is gitignored."""
    return build_all(PDFS_DIR)


def read(pdfs: dict[str, Path], name: str) -> bytes:
    return pdfs[name].read_bytes()


async def test_a_simple_note_extracts_its_text(pdfs: dict[str, Path]) -> None:
    text = await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "note_simple"))

    assert "M54.5" in text
    assert "Marcus Whitfield" in text  # raw text is raw — redaction happens later


async def test_pages_are_joined_with_a_blank_line(pdfs: dict[str, Path]) -> None:
    text = await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "note_multipage"))

    assert "\n\n" in text
    assert text.count("Prior Authorization Request") == 3


async def test_a_scanned_page_fails_with_no_text(pdfs: dict[str, Path]) -> None:
    with pytest.raises(ExtractionFailed, match="no_text"):
        await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "scanned"))


async def test_an_encrypted_pdf_fails(pdfs: dict[str, Path]) -> None:
    with pytest.raises(ExtractionFailed, match="encrypted"):
        await PypdfTextExtractor(max_pages=50).extract(read(pdfs, "encrypted"))


async def test_the_page_cap_is_enforced(pdfs: dict[str, Path]) -> None:
    with pytest.raises(ExtractionFailed, match="too_many_pages"):
        await PypdfTextExtractor(max_pages=1).extract(read(pdfs, "note_multipage"))


async def test_garbage_bytes_fail_as_unreadable() -> None:
    with pytest.raises(ExtractionFailed, match="unreadable"):
        await PypdfTextExtractor(max_pages=50).extract(b"not a pdf at all")


async def test_the_normaliser_collapses_runs_of_blank_lines() -> None:
    from ecet.infrastructure.pdf.pypdf_extractor import normalise

    assert normalise("a   \n\n\n\n\nb  \n") == "a\n\nb"
```

- [ ] **Step 5: Run it to verify it fails**

Run: `uv run pytest tests/unit/infrastructure/test_pypdf_extractor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.pdf.pypdf_extractor'`

- [ ] **Step 6: Write the extractor**

`src/ecet/infrastructure/pdf/__init__.py` (empty) and
`src/ecet/infrastructure/pdf/pypdf_extractor.py`:

```python
"""`TextExtractor` over pypdf.

CPU-bound, so the parse runs in a worker thread and the event loop stays free to
answer the next S3 event. Every failure mode is one short token, because the token is
persisted as `Claim.failure_reason` and read by operators.
"""

import re
from io import BytesIO

import anyio.to_thread
import structlog
from pypdf import PasswordType, PdfReader

from ecet.application.errors import ExtractionFailed

log = structlog.get_logger(__name__)

#: Below this, the file is almost certainly a scan. OCR is out of scope for v1.
DEFAULT_MIN_CHARS = 50

_RUNS_OF_BLANK_LINES = re.compile(r"\n{3,}")


def normalise(text: str) -> str:
    """Strip trailing whitespace per line and collapse runs of blank lines."""
    stripped = "\n".join(line.rstrip() for line in text.splitlines())
    return _RUNS_OF_BLANK_LINES.sub("\n\n", stripped).strip()


class PypdfTextExtractor:
    def __init__(self, *, max_pages: int, min_chars: int = DEFAULT_MIN_CHARS) -> None:
        self._max_pages = max_pages
        self._min_chars = min_chars

    async def extract(self, pdf: bytes) -> str:
        return await anyio.to_thread.run_sync(self._extract, pdf)

    def _extract(self, pdf: bytes) -> str:
        try:
            reader = PdfReader(BytesIO(pdf))
        except Exception as error:  # noqa: BLE001 - pypdf raises a wide family here
            raise ExtractionFailed("unreadable") from error

        if reader.is_encrypted and reader.decrypt("") == PasswordType.NOT_DECRYPTED:
            # An empty user password is worth one try; anything else needs a key we
            # do not have and never will.
            raise ExtractionFailed("encrypted")

        pages = len(reader.pages)
        if pages > self._max_pages:
            raise ExtractionFailed("too_many_pages")

        text = normalise("\n\n".join(page.extract_text() or "" for page in reader.pages))
        if len(text) < self._min_chars:
            raise ExtractionFailed("no_text")

        # Never log the text or a sample of it (ADR-001) — pages and length only.
        log.debug("pdf.extracted", pages=pages, characters=len(text))
        return text
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `uv run pytest tests/unit/infrastructure/test_pypdf_extractor.py -v`
Expected: PASS (7 tests)

If `test_an_encrypted_pdf_fails` reports `unreadable` instead of `encrypted`,
`PdfReader` construction itself raised — move the `is_encrypted` check into the
`except` branch by catching the pypdf encryption error explicitly.

- [ ] **Step 8: Run the whole gate**

Run: `make check`
Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml uv.lock Makefile .gitignore scripts/make_fixtures.py src/ecet/infrastructure/pdf tests/unit/infrastructure
git commit -m "feat(pdf): pypdf text extractor and generated PDF fixtures"
```

---

### Task 8: Presidio PII redactor, spaCy model in the image, CI

**Files:**
- Modify: `pyproject.toml` (dependencies + mypy overrides), `Dockerfile`, `Makefile`, `.github/workflows/ci.yml`
- Create: `src/ecet/infrastructure/pii/__init__.py`, `src/ecet/infrastructure/pii/presidio_redactor.py`
- Test: `tests/adapters/test_presidio_redactor.py`

**Interfaces:**
- Consumes: `PiiRedactor`, `RedactedText`, `redaction_policy` (Task 1).
- Produces: `ecet.infrastructure.pii.presidio_redactor.PresidioPiiRedactor(*, replacements: Mapping[str, str], custom_patterns: Mapping[str, str], score_threshold: float, spacy_model: str, concurrency: int, chunk_chars: int = 20_000)` implementing `PiiRedactor`, plus module constant `REDACTOR = "presidio-2.2"`.

- [ ] **Step 1: Add the dependencies**

```bash
uv add 'presidio-analyzer>=2.2,<2.3' 'presidio-anonymizer>=2.2,<2.3'
uv run python -m spacy download en_core_web_lg
```

The model is ~590 MB and is **not** a project dependency — it is downloaded into the
environment. Append to `pyproject.toml`:

```toml
[[tool.mypy.overrides]]
module = ["presidio_analyzer.*", "presidio_anonymizer.*"]
ignore_missing_imports = true
```

Add to the `Makefile` (and to `.PHONY`):

```make
spacy-model:
	uv run python -m spacy download $(or $(SPACY_MODEL),en_core_web_lg)
```

- [ ] **Step 2: Write the failing adapter test**

`tests/adapters/test_presidio_redactor.py`:

```python
"""The real engine, marked `slow` by the directory conftest.

Needs `make spacy-model` (or the CI step that runs it). No Docker — but a 2-4 s model
load and ~600 MB of RAM, which is exactly why it does not belong in the default run.
"""

import pytest

from ecet.application.redaction_policy import (
    CUSTOM_PATTERNS,
    ENTITY_REPLACEMENTS,
    SCORE_THRESHOLD,
)
from ecet.infrastructure.pii.presidio_redactor import PresidioPiiRedactor
from tests.pii import assert_no_pii, read_note


@pytest.fixture(scope="session")
def redactor() -> PresidioPiiRedactor:
    return PresidioPiiRedactor(
        replacements=ENTITY_REPLACEMENTS,
        custom_patterns=CUSTOM_PATTERNS,
        score_threshold=SCORE_THRESHOLD,
        spacy_model="en_core_web_lg",
        concurrency=2,
    )


async def test_it_removes_every_pii_string_from_a_note(
    redactor: PresidioPiiRedactor,
) -> None:
    result = await redactor.redact(read_note("meets"))

    assert_no_pii(result.text)
    assert result.redactor == "presidio-2.2"


async def test_icd10_codes_survive(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact(read_note("meets"))

    assert "M54.5" in result.text


async def test_the_custom_recognisers_fire(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("MRN: 4482910 for member NWH2288341.")

    assert "<MRN>" in result.text
    assert "<MEMBER_ID>" in result.text
    assert result.entity_counts["MRN"] == 1
    assert result.entity_counts["MEMBER_ID"] == 1


async def test_dates_of_service_are_kept(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("Date of service: 2026-08-14. Follow-up in six weeks.")

    assert "2026-08-14" in result.text


async def test_empty_text_is_not_an_error(redactor: PresidioPiiRedactor) -> None:
    result = await redactor.redact("")

    assert result.text == ""
    assert result.entity_counts == {}


async def test_text_beyond_one_chunk_is_still_redacted(
    redactor: PresidioPiiRedactor,
) -> None:
    filler = "\n\n".join("The member attended the scheduled session." for _ in range(400))
    long_note = f"{filler}\n\n{read_note('meets')}"

    result = await redactor.redact(long_note)

    assert_no_pii(result.text)


async def test_every_entity_in_the_policy_is_reported_in_the_counts(
    redactor: PresidioPiiRedactor,
) -> None:
    result = await redactor.redact(read_note("meets"))

    assert set(result.entity_counts) <= set(ENTITY_REPLACEMENTS)
    assert result.entity_counts.get("US_SSN") == 1
```

- [ ] **Step 3: Run it to verify it fails**

Run: `uv run pytest tests/adapters/test_presidio_redactor.py -m slow -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.pii.presidio_redactor'`

- [ ] **Step 4: Write the adapter**

`src/ecet/infrastructure/pii/__init__.py` (empty) and
`src/ecet/infrastructure/pii/presidio_redactor.py`:

```python
"""`PiiRedactor` over Presidio 2.2 + spaCy (ADR-001).

Three things this file is careful about:

**Cost.** `AnalyzerEngine` loads a ~600 MB spaCy model in 2-4 s. It is built once, at
process start, by the API container wiring — never per request.

**Concurrency.** spaCy is not cheap to run in parallel, so the CPU work goes to a
worker thread behind a semaphore sized by `ECET_PII_CONCURRENCY`.

**Policy.** Which entities are redacted and what replaces them is passed in from
`application/redaction_policy.py`. Nothing about the policy is hardcoded here — and
`DATE_TIME` is simply absent from the analysed entity list, which is how dates of
service survive.
"""

from collections.abc import Mapping

import anyio
import anyio.to_thread
import structlog
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

from ecet.domain.claim import RedactedText

log = structlog.get_logger(__name__)

REDACTOR = "presidio-2.2"
DEFAULT_CHUNK_CHARS = 20_000


def _build_analyzer(
    spacy_model: str, custom_patterns: Mapping[str, str]
) -> AnalyzerEngine:
    provider = NlpEngineProvider(
        nlp_configuration={
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": spacy_model}],
        }
    )
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=provider.create_engine(), languages=["en"])
    for entity, regex in custom_patterns.items():
        registry.add_recognizer(
            PatternRecognizer(
                supported_entity=entity,
                patterns=[Pattern(name=f"{entity.lower()}_pattern", regex=regex, score=0.9)],
            )
        )
    return AnalyzerEngine(nlp_engine=provider.create_engine(), registry=registry)


class PresidioPiiRedactor:
    def __init__(
        self,
        *,
        replacements: Mapping[str, str],
        custom_patterns: Mapping[str, str],
        score_threshold: float,
        spacy_model: str,
        concurrency: int,
        chunk_chars: int = DEFAULT_CHUNK_CHARS,
    ) -> None:
        self._analyzer = _build_analyzer(spacy_model, custom_patterns)
        self._anonymizer = AnonymizerEngine()
        self._entities = list(replacements)
        self._operators = {
            entity: OperatorConfig("replace", {"new_value": replacement})
            for entity, replacement in replacements.items()
        }
        self._score_threshold = score_threshold
        self._chunk_chars = chunk_chars
        self._limiter = anyio.Semaphore(concurrency)
        log.info("pii.engine_ready", model=spacy_model, entities=len(self._entities))

    async def redact(self, text: str) -> RedactedText:
        if not text.strip():
            return RedactedText(text=text, entity_counts={}, redactor=REDACTOR)
        async with self._limiter:
            return await anyio.to_thread.run_sync(self._redact, text)

    def _redact(self, text: str) -> RedactedText:
        counts: dict[str, int] = {}
        pieces: list[str] = []
        for chunk in _chunks(text, self._chunk_chars):
            results = self._analyzer.analyze(
                text=chunk,
                language="en",
                entities=self._entities,
                score_threshold=self._score_threshold,
            )
            for result in results:
                counts[result.entity_type] = counts.get(result.entity_type, 0) + 1
            pieces.append(
                self._anonymizer.anonymize(
                    text=chunk, analyzer_results=results, operators=self._operators
                ).text
            )
        # Never log the text or a sample of it — counts and duration only (ADR-001).
        log.info("pii.redacted", entities=sum(counts.values()), characters=len(text))
        return RedactedText(text="\n\n".join(pieces), entity_counts=counts, redactor=REDACTOR)


def _chunks(text: str, limit: int) -> list[str]:
    """Split on paragraph breaks into pieces no larger than `limit`.

    An entity that straddles a `\\n\\n` boundary is missed; that is an accepted
    limitation of chunking, and the paragraph split makes it rare in clinical notes.
    A single paragraph longer than `limit` is passed through whole rather than cut
    mid-sentence, since a hard cut would break more entities than it saves memory.
    """
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current = ""
    for paragraph in text.split("\n\n"):
        candidate = f"{current}\n\n{paragraph}" if current else paragraph
        if current and len(candidate) > limit:
            chunks.append(current)
            current = paragraph
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks
```

- [ ] **Step 5: Run the adapter test to verify it passes**

Run: `uv run pytest tests/adapters/test_presidio_redactor.py -m slow -v`
Expected: PASS (7 tests)

If `test_icd10_codes_survive` fails, a predefined recogniser (most likely
`US_DRIVER_LICENSE`) is matching a short code token: drop that entity from
`ENTITY_REPLACEMENTS` and record it as a deviation rather than weakening the test.

- [ ] **Step 6: Add the model to the image**

In `Dockerfile`, in the `builder` stage, after the final `uv sync`:

```dockerfile
ARG SPACY_MODEL=en_core_web_lg
RUN /opt/venv/bin/python -m spacy download ${SPACY_MODEL}
```

`SPACY_MODEL` is a build arg so a demo can build the ~1.5 GB image down to something
smaller with `--build-arg SPACY_MODEL=en_core_web_md`. Note in the build output that
the runtime stage already copies `/opt/venv` wholesale, so no second copy step is
needed.

- [ ] **Step 7: Add the model to the CI `slow` job**

In `.github/workflows/ci.yml`, in the `slow` job, between "Sync dependencies" and the
test step:

```yaml
      - name: Download the spaCy model
        run: uv run python -m spacy download en_core_web_lg
```

- [ ] **Step 8: Verify the image still builds**

Run: `docker build -t ecet:phase3 .`
Expected: success. The image is now ~1.5 GB; that is the documented cost of ADR-001.

- [ ] **Step 9: Run the whole gate**

Run: `make check`
Expected: all green (the presidio test is `slow` and stays deselected).

- [ ] **Step 10: Commit**

```bash
git add pyproject.toml uv.lock Dockerfile Makefile .github/workflows/ci.yml src/ecet/infrastructure/pii tests/adapters/test_presidio_redactor.py
git commit -m "feat(pii): presidio redactor adapter with the spaCy model in the image"
```

---

### Task 9: RabbitMQ publisher and topology

**Files:**
- Modify: `pyproject.toml` (dependency)
- Create: `src/ecet/infrastructure/queue/__init__.py`, `src/ecet/infrastructure/queue/rabbitmq.py`
- Test: `tests/adapters/test_rabbitmq_queue.py`

**Interfaces:**
- Consumes: `EvaluationQueue`, `EvaluationMessage`, `QueuePublishError` (Tasks 1 and 3).
- Produces:
  - `ecet.infrastructure.queue.rabbitmq`: constants `EXCHANGE`, `DLX`, `QUEUE`, `DLQ`, `ROUTING_KEY`; `async declare_topology(channel: AbstractChannel) -> AbstractExchange`; `RabbitMqEvaluationQueue(url: str)` with `async start()`, `async stop()`, `async publish(message)`.
  - Phase 4's consumer imports `declare_topology` and the constants — do not inline them.

- [ ] **Step 1: Add the dependency**

```bash
uv add 'aio-pika>=9.4,<10'
```

`aio-pika` ships `py.typed`; no mypy override is needed.

- [ ] **Step 2: Write the failing adapter test**

`tests/adapters/test_rabbitmq_queue.py`:

```python
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from uuid import uuid4

import aio_pika
import pytest
from testcontainers.core.container import DockerContainer

from ecet.application.errors import QueuePublishError
from ecet.application.messages import EvaluationMessage, PolicySnapshot
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.infrastructure.queue.rabbitmq import QUEUE, RabbitMqEvaluationQueue
from tests.adapters.containers import wait_until

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


def build_message() -> EvaluationMessage:
    return EvaluationMessage(
        message_id=uuid4(),
        claim_id=ClaimId(uuid4()),
        tenant_id=TenantId("tenant-a"),
        redacted_text="Patient <PERSON> with M54.5.",
        policies=[
            PolicySnapshot(
                id=PolicyId(uuid4()),
                name="MRI lumbar spine",
                version=2,
                covered_codes=["M54.5"],
                excluded_codes=[],
                criteria_text="Covered after six weeks of conservative therapy.",
                required_evidence=[],
            )
        ],
        deterministic_verdict="PASS",
        found_codes=["M54.5"],
        enqueued_at=NOW,
    )


@pytest.fixture(scope="session")
def amqp_url() -> Iterator[str]:
    container = DockerContainer("rabbitmq:3.13-management").with_exposed_ports(5672)
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
async def queue(amqp_url: str) -> AsyncIterator[RabbitMqEvaluationQueue]:
    adapter = RabbitMqEvaluationQueue(amqp_url)
    # RabbitMQ opens 5672 well before it accepts AMQP handshakes.
    await wait_until(adapter.start)
    yield adapter
    await adapter.stop()


async def test_a_published_message_lands_on_the_queue(
    queue: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    message = build_message()

    await queue.publish(message)

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        declared = await channel.get_queue(QUEUE)
        delivered = await declared.get(timeout=10)
        assert delivered is not None
        await delivered.ack()

    assert delivered.content_type == "application/json"
    assert delivered.message_id == str(message.message_id)
    assert delivered.headers["x-tenant-id"] == "tenant-a"
    assert delivered.headers["x-schema-version"] == 1
    assert delivered.delivery_mode == 2
    assert json.loads(delivered.body)["claim_id"] == str(message.claim_id)


async def test_the_topology_is_declared_idempotently(
    queue: RabbitMqEvaluationQueue, amqp_url: str
) -> None:
    # A second start against an existing topology must not raise (the api and the
    # worker both declare it).
    second = RabbitMqEvaluationQueue(amqp_url)
    await second.start()
    await second.stop()


async def test_publishing_after_stop_raises_queue_publish_error(
    queue: RabbitMqEvaluationQueue,
) -> None:
    await queue.stop()

    with pytest.raises(QueuePublishError):
        await queue.publish(build_message())
```

- [ ] **Step 3: Run it to verify it fails**

Run: `uv run pytest tests/adapters/test_rabbitmq_queue.py -m slow -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.queue.rabbitmq'`

- [ ] **Step 4: Write the adapter**

`src/ecet/infrastructure/queue/__init__.py` (empty) and
`src/ecet/infrastructure/queue/rabbitmq.py`:

```python
"""`EvaluationQueue` over aio-pika, plus the topology both processes declare.

The topology is declared by whoever connects first — api or worker — and declaring it
twice is a no-op as long as the arguments match, which is why they live in one
constant here rather than being spelled out at each call site.

The queue is a quorum queue with `x-delivery-limit=5`: after five failed deliveries
RabbitMQ itself moves the message to the DLQ, so the worker never has to count
attempts. Phase 4's consumer imports `declare_topology` from this module.
"""

import structlog
from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection

from ecet.application.errors import QueuePublishError
from ecet.application.messages import EvaluationMessage

log = structlog.get_logger(__name__)

EXCHANGE = "ecet"
DLX = "ecet.dlx"
QUEUE = "claims.evaluate"
DLQ = "claims.evaluate.dlq"
ROUTING_KEY = "claims.evaluate"

QUEUE_ARGUMENTS: dict[str, object] = {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": DLX,
    "x-delivery-limit": 5,
}


async def declare_topology(channel: AbstractChannel) -> AbstractExchange:
    """Declare exchanges, queues and bindings. Idempotent. Returns the `ecet` exchange."""
    exchange = await channel.declare_exchange(EXCHANGE, "topic", durable=True)
    dlx = await channel.declare_exchange(DLX, "topic", durable=True)

    dead_letters = await channel.declare_queue(DLQ, durable=True)
    await dead_letters.bind(dlx, routing_key=ROUTING_KEY)

    evaluations = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGUMENTS)
    await evaluations.bind(exchange, routing_key=ROUTING_KEY)

    return exchange


class RabbitMqEvaluationQueue:
    """One robust connection per process, opened at startup and closed at shutdown."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: AbstractRobustConnection | None = None
        self._exchange: AbstractExchange | None = None

    async def start(self) -> None:
        self._connection = await connect_robust(self._url)
        # Publisher confirms: without them a publish is fire-and-forget and the
        # "claim stays POLICIES_ATTACHED on failure" contract is unenforceable.
        channel = await self._connection.channel(publisher_confirms=True)
        self._exchange = await declare_topology(channel)
        log.info("queue.connected", exchange=EXCHANGE, queue=QUEUE)

    async def stop(self) -> None:
        self._exchange = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def is_healthy(self) -> bool:
        """Used by `/readyz`; never raises."""
        return self._connection is not None and not self._connection.is_closed

    async def publish(self, message: EvaluationMessage) -> None:
        exchange = self._exchange
        if exchange is None:
            raise QueuePublishError("queue connection is not open")
        try:
            await exchange.publish(
                Message(
                    body=message.model_dump_json().encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=DeliveryMode.PERSISTENT,
                    message_id=str(message.message_id),
                    headers={
                        "x-tenant-id": str(message.tenant_id),
                        "x-schema-version": message.schema_version,
                    },
                ),
                routing_key=ROUTING_KEY,
            )
        except Exception as error:  # noqa: BLE001 - any broker failure is one failure here
            raise QueuePublishError(str(error)) from error
        log.info(
            "queue.published",
            claim_id=str(message.claim_id),
            tenant_id=str(message.tenant_id),
            message_id=str(message.message_id),
        )
```

- [ ] **Step 5: Run the adapter test to verify it passes**

Run: `uv run pytest tests/adapters/test_rabbitmq_queue.py -m slow -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Add the queue port conformance check**

Append to `tests/adapters/test_rabbitmq_queue.py`:

```python
def test_the_adapter_satisfies_the_port(amqp_url: str) -> None:
    from ecet.application.ports.evaluation_queue import EvaluationQueue

    assert isinstance(RabbitMqEvaluationQueue(amqp_url), EvaluationQueue)
```

Run: `uv run pytest tests/adapters/test_rabbitmq_queue.py -m slow -v` → PASS (4 tests).

- [ ] **Step 7: Run the whole gate**

Run: `make check`
Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock src/ecet/infrastructure/queue tests/adapters/test_rabbitmq_queue.py
git commit -m "feat(queue): aio-pika publisher and the claims.evaluate topology"
```

---

### Task 10: API foundations — error mapping, auth, container, lifespan, `/readyz`

No routes yet. This task builds everything a route needs and proves it with an app
that has only `/healthz` and `/readyz` on it.

**Files:**
- Create: `src/ecet/infrastructure/postgres/migrations.py`
- Create: `src/ecet/interfaces/api/errors.py`, `src/ecet/interfaces/api/dependencies.py`, `src/ecet/interfaces/api/container.py`
- Modify: `src/ecet/interfaces/api/app.py`, `src/ecet/interfaces/api/routes/health.py`
- Create: `tests/api/conftest.py`
- Test: `tests/api/test_errors.py`, `tests/api/test_auth.py`, `tests/api/test_readyz.py`, `tests/adapters/test_migrations.py`

**Interfaces:**
- Consumes: everything from Tasks 1–9, plus `Settings` and the Phase 2 `SqlAlchemyUnitOfWork` / `create_engine` / `create_session_factory` / `load_seed`.
- Produces:
  - `ecet.infrastructure.postgres.migrations`: `async upgrade_to_head(database_url: str) -> None`, `async assert_at_head(engine: AsyncEngine) -> None`.
  - `ecet.interfaces.api.errors`: `ProblemDetails` model, `PROBLEMS: dict[type[DomainError], Problem]`, `register_error_handlers(app: FastAPI) -> None`.
  - `ecet.interfaces.api.dependencies`: `require_api_key`, `require_event_token`, `require_tenant_id`, `get_container`.
  - `ecet.interfaces.api.container`: `ApiContainer` dataclass (`settings`, `uow_factory`, `storage`, `ingest`, `probes`, `aclose`) and `async build_container(settings: Settings) -> ApiContainer`.
  - `create_app(settings: Settings, container: ApiContainer | None = None) -> FastAPI` — passing a container skips the lifespan build, which is how the API tests run with fakes.

- [ ] **Step 1: Write the failing migration-helper test**

`tests/adapters/test_migrations.py`:

```python
import pytest
from sqlalchemy import text

from ecet.infrastructure.postgres.migrations import assert_at_head, upgrade_to_head
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


async def test_a_database_behind_head_is_rejected(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("UPDATE alembic_version SET version_num = 'nope'"))
        with pytest.raises(RuntimeError, match="pending migration"):
            await assert_at_head(engine)
    finally:
        # Put it back: `postgres_url` is session-scoped and later tests rely on it.
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM alembic_version"))
        await upgrade_to_head(postgres_url)
        await engine.dispose()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/adapters/test_migrations.py -m slow -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.postgres.migrations'`

- [ ] **Step 3: Write the migration helpers**

`src/ecet/infrastructure/postgres/migrations.py`:

```python
"""Running and checking Alembic from inside the API process.

Alembic runs in a **subprocess**, not in-process: `migrations/env.py` calls
`asyncio.run`, which cannot nest inside the running event loop the API starts under.
That is the same reason the Phase 2 test harness shells out.

The project root is found from the working directory rather than from `__file__` —
in the container the package lives in site-packages while `alembic.ini` and
`migrations/` sit in `/app`, the working directory.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import structlog
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

log = structlog.get_logger(__name__)


def project_root() -> Path:
    """The directory holding `alembic.ini`: the working directory, or an ancestor."""
    start = Path.cwd().resolve()
    for candidate in (start, *start.parents):
        if (candidate / "alembic.ini").is_file():
            return candidate
    raise RuntimeError(f"no alembic.ini at or above {start}")


async def upgrade_to_head(database_url: str) -> None:
    await asyncio.to_thread(_upgrade, database_url)
    log.info("db.migrated")


def _upgrade(database_url: str) -> None:
    subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=project_root(),
        env={**os.environ, "ECET_DATABASE_URL": database_url},
        check=True,
    )


async def assert_at_head(engine: AsyncEngine) -> None:
    """Fail fast when the schema is behind the code (`ECET_AUTO_MIGRATE=false`)."""
    async with engine.connect() as connection:
        await connection.run_sync(_assert_at_head)


def _assert_at_head(connection: Connection) -> None:
    root = project_root()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    head = ScriptDirectory.from_config(config).get_current_head()
    current = MigrationContext.configure(connection).get_current_revision()
    if current != head:
        raise RuntimeError(f"pending migration: database at {current!r}, code expects {head!r}")
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run pytest tests/adapters/test_migrations.py -m slow -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Write the failing error-mapping and auth tests**

`tests/api/conftest.py`:

```python
"""API tests run against the real app with fake ports.

`create_app(settings, container=...)` skips the lifespan build, so no Postgres, no
RabbitMQ, no spaCy model — the routes, the auth and the error mapping are what is
under test here.
"""

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest

from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.ingest_claim_document import IngestClaimDocument
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.config import Settings
from ecet.domain.ids import PolicyId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant
from ecet.interfaces.api.app import create_app
from ecet.interfaces.api.container import ApiContainer
from tests.fakes import (
    FakeEvaluationQueue,
    FakeObjectStorage,
    FakePiiRedactor,
    FakeTextExtractor,
    FakeUnitOfWork,
    FixedClock,
)
from tests.pii import read_note

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
BUCKET = "claims"
KEY = "tenants/tenant-a/claims/note-1.pdf"
PDF = b"%PDF-1.7 fake bytes"
API_KEY = "test-key"
EVENT_TOKEN = "test-token"


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": "dev-hmac-tenant-a",
        }
    )


def build_policy() -> Policy:
    return Policy.model_validate(
        {
            "id": PolicyId(uuid4()),
            "tenant_id": "tenant-a",
            "name": "MRI lumbar spine",
            "version": 2,
            "covered_codes": [{"code": "M54.5"}],
            "excluded_codes": [{"code": "Z00.00"}],
            "criteria_text": "Imaging is covered after six weeks of conservative therapy.",
            "effective_from": date(2026, 1, 1),
        }
    )


class ApiHarness:
    def __init__(self, *, note: str = "meets", tenants: list[Tenant] | None = None) -> None:
        self.clock = FixedClock(NOW)
        self.uow = FakeUnitOfWork(
            tenants=tenants if tenants is not None else [build_tenant()],
            policies=[build_policy()],
        )
        self.storage = FakeObjectStorage({(BUCKET, KEY): PDF})
        self.queue = FakeEvaluationQueue()
        self.settings = Settings(_env_file=None, s3_event_token=EVENT_TOKEN, api_key=API_KEY)
        uow_factory: Callable[[], UnitOfWork] = lambda: self.uow
        self.ingest = IngestClaimDocument(
            uow_factory=uow_factory,
            storage=self.storage,
            extractor=FakeTextExtractor(read_note(note)),
            redact_pii=RedactPii(FakePiiRedactor()),
            run_checks=RunDeterministicChecks(),
            enqueue=EnqueueEvaluation(self.queue, self.clock),
            clock=self.clock,
            max_pdf_bytes=self.settings.max_pdf_bytes,
        )
        self.probe_results: dict[str, bool] = {
            "database": True,
            "queue": True,
            "redactor": True,
        }
        self.container = ApiContainer(
            settings=self.settings,
            uow_factory=uow_factory,
            storage=self.storage,
            ingest=self.ingest,
            probes={name: self._probe(name) for name in self.probe_results},
            aclose=self._aclose,
        )
        self.app = create_app(self.settings, container=self.container)

    def _probe(self, name: str) -> Callable[[], Any]:
        async def check() -> bool:
            return self.probe_results[name]

        return check

    async def _aclose(self) -> None:
        return None

    def client(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test", **kwargs
        )


@pytest.fixture
def harness() -> ApiHarness:
    return ApiHarness()


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY, "X-Tenant-Id": "tenant-a"}


@pytest.fixture
def event_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {EVENT_TOKEN}"}
```

`tests/api/test_errors.py`:

```python
import httpx
import pytest
from fastapi import FastAPI

from ecet.application.errors import ExtractionFailed, QueuePublishError
from ecet.domain.errors import (
    ClaimNotFound,
    DomainError,
    InvalidObjectKey,
    NoPoliciesForTenant,
    PdfTooLarge,
    TenantNotFound,
)
from ecet.interfaces.api.errors import register_error_handlers


def build_app(error: Exception) -> FastAPI:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise error

    return app


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (InvalidObjectKey("expected tenants/... got uploads/secret-patient-name.pdf"), 400),
        (PdfTooLarge("40000000 bytes exceeds the 20000000 byte limit"), 413),
        (TenantNotFound("tenant-x"), 404),
        (ExtractionFailed("no_text"), 422),
        (NoPoliciesForTenant("tenant-empty"), 422),
        (ClaimNotFound("00000000-0000-4000-8000-000000000000"), 404),
        (QueuePublishError("no confirm"), 503),
    ],
)
async def test_each_domain_error_maps_to_its_status(error: DomainError, status: int) -> None:
    transport = httpx.ASGITransport(app=build_app(error), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    assert response.status_code == status
    body = response.json()
    assert body["status"] == status
    assert body["title"]
    assert response.headers["content-type"].startswith("application/problem+json")


async def test_the_detail_never_echoes_the_exception_message() -> None:
    error = InvalidObjectKey("expected tenants/... got uploads/secret-patient-name.pdf")
    transport = httpx.ASGITransport(app=build_app(error), raise_app_exceptions=False)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    assert "secret-patient-name" not in response.text


async def test_an_unmapped_domain_error_is_a_500() -> None:
    class Surprise(DomainError):
        pass

    transport = httpx.ASGITransport(app=build_app(Surprise("x")), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    assert response.status_code == 500
    assert response.json()["title"] == "Internal error"
```

`tests/api/test_auth.py`:

```python
from tests.api.conftest import API_KEY, ApiHarness


async def test_healthz_needs_no_auth(harness: ApiHarness) -> None:
    async with harness.client() as client:
        assert (await client.get("/healthz")).status_code == 200


async def test_a_missing_api_key_is_401(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest", json={"bucket": "claims", "key": "k"}
        )

    assert response.status_code == 401


async def test_a_wrong_api_key_is_401(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": "claims", "key": "k"},
            headers={"X-API-Key": "wrong"},
        )

    assert response.status_code == 401


async def test_a_missing_bearer_token_is_401_on_the_event_route(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json={"Records": []})

    assert response.status_code == 401


async def test_the_api_key_is_not_accepted_on_the_event_route(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json={"Records": []}, headers={"X-API-Key": API_KEY}
        )

    assert response.status_code == 401
```

These two auth tests reference routes that arrive in Task 11 — write them now, watch
them fail with 404, and they turn green in Task 11. Mark neither `xfail`: a 404 is a
failing assertion, which is what "the route does not exist yet" should look like.

`tests/api/test_readyz.py`:

```python
from tests.api.conftest import ApiHarness


async def test_readyz_is_200_when_every_probe_passes(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"database": True, "queue": True, "redactor": True}


async def test_readyz_is_503_when_a_probe_fails(harness: ApiHarness) -> None:
    harness.probe_results["queue"] = False

    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["queue"] is False


async def test_a_raising_probe_reports_false_rather_than_500(harness: ApiHarness) -> None:
    async def explode() -> bool:
        raise RuntimeError("connection refused")

    harness.container.probes["database"] = explode

    async with harness.client() as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["database"] is False
```

- [ ] **Step 6: Run them to verify they fail**

Run: `uv run pytest tests/api -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.interfaces.api.errors'`

- [ ] **Step 7: Write the error mapping**

`src/ecet/interfaces/api/errors.py`:

```python
"""Domain errors → RFC 7807 problem responses.

The rule that shapes this file: **`detail` is a constant, never the exception's
message.** `InvalidObjectKey` and `ObjectNotFound` both embed the client-supplied
object key, which carries a filename that may itself be PII. The message still goes
to the log, where the redaction guard and access controls apply; it does not go to
the client.
"""

from typing import NamedTuple

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ecet.application.errors import ExtractionFailed, ObjectNotFound, QueuePublishError
from ecet.domain.errors import (
    ClaimNotFound,
    ConcurrentModification,
    DomainError,
    InvalidObjectKey,
    InvalidTransition,
    NoPoliciesForTenant,
    PdfTooLarge,
    ReviewAlreadyResolved,
    ReviewTaskNotFound,
    TenantNotFound,
)

log = structlog.get_logger(__name__)

PROBLEM_MEDIA_TYPE = "application/problem+json"


class Problem(NamedTuple):
    status: int
    title: str
    detail: str


class ProblemDetails(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    claim_id: str | None = None


UNKNOWN = Problem(500, "Internal error", "The request could not be completed.")

PROBLEMS: dict[type[DomainError], Problem] = {
    InvalidObjectKey: Problem(
        400,
        "Invalid object key",
        "The object key must look like tenants/{tenant_id}/claims/{name}.pdf.",
    ),
    PdfTooLarge: Problem(413, "PDF too large", "The object exceeds the configured size limit."),
    TenantNotFound: Problem(404, "Tenant not found", "No active tenant for that object key."),
    ObjectNotFound: Problem(404, "Object not found", "No object at that bucket and key."),
    ClaimNotFound: Problem(404, "Claim not found", "No claim with that id for this tenant."),
    ReviewTaskNotFound: Problem(404, "Review task not found", "No review task with that id."),
    ExtractionFailed: Problem(
        422, "Text extraction failed", "No usable text could be read from the document."
    ),
    NoPoliciesForTenant: Problem(
        422, "No active policies", "The tenant has no active policy effective today."
    ),
    ReviewAlreadyResolved: Problem(
        409, "Review already resolved", "That review task has already been resolved."
    ),
    ConcurrentModification: Problem(
        409, "Concurrent modification", "The claim changed while the request was running."
    ),
    InvalidTransition: Problem(
        409, "Invalid state transition", "The claim is not in a state that allows this."
    ),
    QueuePublishError: Problem(
        503, "Queue unavailable", "The evaluation could not be queued; retry later."
    ),
}


def problem_for(error: BaseException) -> Problem:
    for candidate in type(error).__mro__:
        problem = PROBLEMS.get(candidate)  # type: ignore[arg-type]  # mro walks past DomainError
        if problem is not None:
            return problem
    return UNKNOWN


def register_error_handlers(app: FastAPI) -> None:
    async def handle(request: Request, error: Exception) -> JSONResponse:
        problem = problem_for(error)
        claim_id = getattr(error, "claim_id", None)
        # The message is logged, not returned: it may embed a client-supplied filename.
        log.warning(
            "api.domain_error",
            error=type(error).__name__,
            message=str(error),
            status=problem.status,
            path=request.url.path,
        )
        body = ProblemDetails(
            type=f"https://ecet.invalid/problems/{type(error).__name__}",
            title=problem.title,
            status=problem.status,
            detail=problem.detail,
            claim_id=str(claim_id) if claim_id is not None else None,
        )
        return JSONResponse(
            body.model_dump(), status_code=problem.status, media_type=PROBLEM_MEDIA_TYPE
        )

    app.add_exception_handler(DomainError, handle)
```

- [ ] **Step 8: Write the dependencies**

`src/ecet/interfaces/api/dependencies.py`:

```python
"""Route-edge dependencies: auth and container access.

Auth is v1-simple on purpose — a static API key and a tenant header. Production would
use per-tenant keys or JWT; the README says so. `compare_digest` is used anyway,
because a timing-safe comparison costs nothing.
"""

from hmac import compare_digest
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from ecet.domain.ids import TenantId
from ecet.interfaces.api.container import ApiContainer


def get_container(request: Request) -> ApiContainer:
    container: ApiContainer = request.app.state.container
    return container


ContainerDep = Annotated[ApiContainer, Depends(get_container)]


def require_api_key(
    container: ContainerDep,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    expected = container.settings.api_key.get_secret_value()
    if x_api_key is None or not compare_digest(x_api_key, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")


def require_event_token(
    container: ContainerDep,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """MinIO sends `Authorization: Bearer <ECET_S3_EVENT_TOKEN>`."""
    expected = container.settings.s3_event_token.get_secret_value()
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not compare_digest(token, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing bearer token")


def require_tenant_id(x_tenant_id: Annotated[str, Header()]) -> TenantId:
    """The tenant a read is scoped to. Repositories do not filter by tenant, so every
    route that returns tenant data compares this against the record it loaded."""
    return TenantId(x_tenant_id)


TenantDep = Annotated[TenantId, Depends(require_tenant_id)]
```

- [ ] **Step 9: Write the container**

`src/ecet/interfaces/api/container.py`:

```python
"""Wiring. Plain functions, no DI framework (per the api spec).

Everything expensive is built once here and lives for the process: the presidio
engine (2-4 s, ~600 MB), the SQLAlchemy engine, the AMQP connection. Routes reach it
through `request.app.state.container`.

The container's field types are the **ports**, not the adapters, so the API tests can
build one out of `tests/fakes.py` and exercise the real routes with no infrastructure.
"""

from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from dataclasses import dataclass, field

import structlog
from sqlalchemy import text

from ecet.application.ports.object_storage import ObjectStorage
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.application.redaction_policy import (
    CUSTOM_PATTERNS,
    ENTITY_REPLACEMENTS,
    SCORE_THRESHOLD,
)
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.ingest_claim_document import IngestClaimDocument
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.config import Settings
from ecet.infrastructure.clock import SystemClock
from ecet.infrastructure.pdf.pypdf_extractor import PypdfTextExtractor
from ecet.infrastructure.pii.presidio_redactor import PresidioPiiRedactor
from ecet.infrastructure.postgres.migrations import assert_at_head, upgrade_to_head
from ecet.infrastructure.postgres.seed import load_seed
from ecet.infrastructure.postgres.session import create_engine, create_session_factory
from ecet.infrastructure.postgres.unit_of_work import SqlAlchemyUnitOfWork
from ecet.infrastructure.queue.rabbitmq import RabbitMqEvaluationQueue
from ecet.infrastructure.storage.s3 import S3ObjectStorage

log = structlog.get_logger(__name__)

Probe = Callable[[], Awaitable[bool]]


@dataclass
class ApiContainer:
    settings: Settings
    uow_factory: Callable[[], UnitOfWork]
    storage: ObjectStorage
    ingest: IngestClaimDocument
    #: `/readyz` checks. Mutable so a test can swap one out.
    probes: MutableMapping[str, Probe] = field(default_factory=dict)
    aclose: Callable[[], Awaitable[None]] = field(default_factory=lambda: _noop)


async def _noop() -> None:
    return None


async def build_container(settings: Settings) -> ApiContainer:
    engine = create_engine(settings.database_url.get_secret_value())

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

    async def aclose() -> None:
        await queue.stop()
        await engine.dispose()

    return ApiContainer(
        settings=settings,
        uow_factory=uow_factory,
        storage=storage,
        ingest=ingest,
        probes={
            "database": database_ready,
            "queue": queue_ready,
            "redactor": redactor_ready,
        },
        aclose=aclose,
    )
```

Note what is **not** in the container: `AttachTenantPolicies` and
`RequestHumanReview`. Both take repositories that belong to a unit of work, and a
unit of work lives for one request — so UC-01 builds them per request from the live
`uow` (Task 5, `_run_pipeline`). Everything the container does hold is either
process-scoped (the engine, the AMQP connection, the presidio engine) or stateless.

- [ ] **Step 10: Write the system clock**

`src/ecet/infrastructure/clock.py`:

```python
"""The one real `Clock`. Trivial, but it keeps `datetime.now()` out of every use
case, which is what makes them testable with `FixedClock`."""

from datetime import UTC, datetime


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)
```

- [ ] **Step 11: Wire the app factory, lifespan and `/readyz`**

`src/ecet/interfaces/api/app.py`:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ecet import __version__
from ecet.config import Settings
from ecet.interfaces.api.container import ApiContainer, build_container
from ecet.interfaces.api.errors import register_error_handlers
from ecet.interfaces.api.routes import health


def create_app(settings: Settings, container: ApiContainer | None = None) -> FastAPI:
    """`container` is injected by tests: passing one skips the lifespan build, so the
    routes can be exercised with fakes and no infrastructure at all."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        built = container if container is not None else await build_container(settings)
        app.state.container = built
        try:
            yield
        finally:
            if container is None:
                await built.aclose()

    app = FastAPI(title="ECET API", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    register_error_handlers(app)
    app.include_router(health.router)
    return app
```

`src/ecet/interfaces/api/routes/health.py`:

```python
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ecet.interfaces.api.dependencies import ContainerDep

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe. No dependencies — it must answer even when Postgres is down."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(container: ContainerDep) -> JSONResponse:
    """Readiness: database, AMQP and the presidio engine. A probe that raises counts
    as not ready rather than as a 500 — an unready service is a normal state."""
    results: dict[str, bool] = {}
    for name, probe in container.probes.items():
        try:
            results[name] = await probe()
        except Exception:  # noqa: BLE001 - any failure means "not ready"
            results[name] = False
    status_code = 200 if all(results.values()) else 503
    return JSONResponse(results, status_code=status_code)
```

- [ ] **Step 12: Run the API tests**

Run: `uv run pytest tests/api -v`
Expected: `test_errors.py` and `test_readyz.py` PASS; the two auth tests that call
`/v1/claims/ingest` and `/v1/events/s3` still FAIL with 404 — Task 11 adds those
routes. Everything else green.

- [ ] **Step 13: Run the whole gate**

Run: `make check`
Expected: lint, mypy and imports green; the two known route-404 failures remain.

- [ ] **Step 14: Commit**

```bash
git add src/ecet/infrastructure/clock.py src/ecet/infrastructure/postgres/migrations.py src/ecet/interfaces/api src/ecet/application/use_cases/ingest_claim_document.py tests/api tests/adapters/test_migrations.py tests/unit/application/test_ingest_claim_document.py
git commit -m "feat(api): error mapping, auth, container wiring, lifespan and readyz"
```

---

### Task 11: The three ingestion routes

**Files:**
- Create: `src/ecet/interfaces/api/routes/events.py`, `src/ecet/interfaces/api/routes/claims.py`
- Modify: `src/ecet/interfaces/api/app.py`
- Create: `tests/fixtures/s3_events/minio_put.json`, `tests/fixtures/s3_events/minio_delete.json`
- Test: `tests/api/test_events_route.py`, `tests/api/test_claims_routes.py`

**Interfaces:**
- Consumes: `IngestClaimDocument`, `IngestCommand`, `IngestResult` (Task 5); the dependencies and container from Task 10.
- Produces:
  - `ecet.interfaces.api.routes.events`: `S3EventEnvelope`, `S3Record`, and `POST /v1/events/s3`.
  - `ecet.interfaces.api.routes.claims`: `ManualIngestRequest`, `ClaimView`, `POST /v1/claims/ingest`, `GET /v1/claims/{claim_id}`.

- [ ] **Step 1: Write the S3 event fixtures**

`tests/fixtures/s3_events/minio_put.json` — trimmed from a real MinIO notification.
The key is URL-encoded, which is the detail the parser has to get right:

```json
{
  "EventName": "s3:ObjectCreated:Put",
  "Key": "claims/tenants/tenant-a/claims/note+with+spaces.pdf",
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "minio:s3",
      "awsRegion": "",
      "eventTime": "2026-09-06T12:00:00.000Z",
      "eventName": "s3:ObjectCreated:Put",
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "Config",
        "bucket": {
          "name": "claims",
          "ownerIdentity": { "principalId": "minioadmin" },
          "arn": "arn:aws:s3:::claims"
        },
        "object": {
          "key": "tenants/tenant-a/claims/note+with+spaces.pdf",
          "size": 24512,
          "eTag": "b1946ac92492d2347c6235b4d2611184",
          "contentType": "application/pdf",
          "sequencer": "17F0A3B0C1D2E3F4"
        }
      }
    }
  ]
}
```

`tests/fixtures/s3_events/minio_delete.json` — the event that must be ignored:

```json
{
  "EventName": "s3:ObjectRemoved:Delete",
  "Records": [
    {
      "eventName": "s3:ObjectRemoved:Delete",
      "s3": {
        "bucket": { "name": "claims" },
        "object": { "key": "tenants/tenant-a/claims/note-1.pdf", "size": 0, "eTag": "" }
      }
    }
  ]
}
```

- [ ] **Step 2: Write the failing event-route test**

`tests/api/test_events_route.py`:

```python
import json
from pathlib import Path
from typing import Any

from ecet.domain.claim import ClaimStatus
from tests.api.conftest import ApiHarness

EVENTS_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "s3_events"


def load_event(name: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads((EVENTS_DIR / f"{name}.json").read_text())
    return payload


def put_event(key: str = "tenants/tenant-a/claims/note-1.pdf", **overrides: Any) -> dict[str, Any]:
    event = load_event("minio_put")
    event["Records"][0]["s3"]["object"]["key"] = key
    event["Records"][0]["s3"]["object"].update(overrides)
    return event


async def test_a_put_event_ingests_the_claim(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(), headers=event_headers)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == ClaimStatus.QUEUED.value
    assert body["duplicate"] is False
    assert len(harness.queue.published) == 1


async def test_the_url_encoded_key_is_unescaped_before_use(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    key = "tenants/tenant-a/claims/note+with+spaces.pdf"
    harness.storage.put("claims", "tenants/tenant-a/claims/note with spaces.pdf", b"%PDF-1.7 x")

    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=put_event(key=key), headers=event_headers
        )

    assert response.status_code == 200
    assert ("claims", "tenants/tenant-a/claims/note with spaces.pdf") in harness.storage.reads


async def test_a_delete_event_is_ignored(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=load_event("minio_delete"), headers=event_headers
        )

    assert response.status_code == 200
    assert response.json() == {"ignored": 1}
    assert harness.uow.claims.claims == {}


async def test_a_non_pdf_key_is_ignored(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3",
            json=put_event(key="tenants/tenant-a/claims/notes.txt"),
            headers=event_headers,
        )

    assert response.status_code == 200
    assert response.json() == {"ignored": 1}


async def test_an_empty_records_list_is_ignored(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json={"Records": []}, headers=event_headers
        )

    assert response.status_code == 200


async def test_a_key_outside_the_tenant_layout_is_400(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json=put_event(key="uploads/note-1.pdf"), headers=event_headers
        )

    assert response.status_code == 400
    assert "uploads/note-1.pdf" not in response.text


async def test_a_tenant_with_no_policies_is_422(event_headers: dict[str, str]) -> None:
    harness = ApiHarness()
    harness.uow.policies.policies.clear()

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(), headers=event_headers)

    assert response.status_code == 422
    assert response.json()["title"] == "No active policies"


async def test_an_unknown_tenant_is_404(event_headers: dict[str, str]) -> None:
    harness = ApiHarness(tenants=[])

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=put_event(), headers=event_headers)

    assert response.status_code == 404


async def test_multiple_records_answer_207_with_one_result_each(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    harness.storage.put("claims", "tenants/tenant-a/claims/note-2.pdf", b"%PDF-1.7 y")
    first = put_event()
    second = put_event(key="tenants/tenant-a/claims/note-2.pdf", eTag="etag-2")
    envelope = {"Records": [first["Records"][0], second["Records"][0]]}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 207
    results = response.json()["results"]
    assert len(results) == 2
    assert all(entry["result"]["status"] == "QUEUED" for entry in results)


async def test_one_failing_record_does_not_abort_the_batch(
    harness: ApiHarness, event_headers: dict[str, str]
) -> None:
    good = put_event()
    bad = put_event(key="tenants/tenant-a/claims/absent.pdf", eTag="etag-3")
    envelope = {"Records": [bad["Records"][0], good["Records"][0]]}

    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json=envelope, headers=event_headers)

    assert response.status_code == 207
    results = response.json()["results"]
    assert results[0]["error"] == "ExtractionFailed"
    assert results[1]["result"]["status"] == "QUEUED"
```

- [ ] **Step 3: Run it to verify it fails**

Run: `uv run pytest tests/api/test_events_route.py -v`
Expected: FAIL — every test 404s; the route does not exist.

- [ ] **Step 4: Write the event route**

`src/ecet/interfaces/api/routes/events.py`:

```python
"""`POST /v1/events/s3` — the MinIO bucket notification endpoint.

Two response shapes, because MinIO sends one record per notification in the compose
setup but the S3 format allows many:

- one actionable record → the plain `IngestResult`, or the mapped error status;
- many → 207 with one entry per record, so a single bad object does not discard the
  good ones. (Not a real WebDAV multi-status body — the api spec calls it
  "207-style".)

Anything that is not an `ObjectCreated:*` event on a `.pdf` key answers 200 and is
counted as ignored: MinIO retries non-2xx, and retrying a delete notification forever
helps nobody.
"""

from typing import Any
from urllib.parse import unquote_plus

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ecet.application.use_cases.ingest_claim_document import IngestCommand
from ecet.domain.errors import DomainError
from ecet.interfaces.api.dependencies import ContainerDep, require_event_token

log = structlog.get_logger(__name__)

router = APIRouter(tags=["events"])

CREATED_PREFIX = "s3:ObjectCreated:"


class S3Bucket(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str


class S3Object(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    key: str
    size: int = 0
    etag: str = Field(default="", alias="eTag")


class S3Payload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bucket: S3Bucket
    object: S3Object


class S3Record(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    event_name: str = Field(default="", alias="eventName")
    s3: S3Payload

    def is_claim_pdf(self) -> bool:
        created = not self.event_name or self.event_name.startswith(CREATED_PREFIX)
        return created and self.decoded_key().lower().endswith(".pdf")

    def decoded_key(self) -> str:
        """MinIO URL-encodes the key; spaces arrive as `+`."""
        return unquote_plus(self.s3.object.key)

    def to_command(self) -> IngestCommand:
        return IngestCommand(
            bucket=self.s3.bucket.name,
            key=self.decoded_key(),
            etag=self.s3.object.etag,
            size=self.s3.object.size,
        )


class S3EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    records: list[S3Record] = Field(default_factory=list, alias="Records")


@router.post("/v1/events/s3", dependencies=[Depends(require_event_token)])
async def receive_s3_event(
    envelope: S3EventEnvelope, container: ContainerDep
) -> JSONResponse:
    actionable = [record for record in envelope.records if record.is_claim_pdf()]
    ignored = len(envelope.records) - len(actionable)
    if not actionable:
        log.info("s3_event.ignored", records=ignored)
        return JSONResponse({"ignored": ignored}, status_code=200)

    if len(actionable) == 1:
        # Let the error handlers map a failure to its status — the single-record case
        # is the one MinIO actually sends, and it wants a real status code.
        result = await container.ingest.execute(actionable[0].to_command())
        return JSONResponse(result.model_dump(mode="json"), status_code=200)

    results: list[dict[str, Any]] = []
    for record in actionable:
        entry: dict[str, Any] = {"bucket": record.s3.bucket.name}
        try:
            entry["result"] = (
                await container.ingest.execute(record.to_command())
            ).model_dump(mode="json")
        except DomainError as error:
            # The class name only: the message embeds the client-supplied key.
            entry["error"] = type(error).__name__
            log.warning("s3_event.record_failed", error=type(error).__name__)
        results.append(entry)
    return JSONResponse({"results": results, "ignored": ignored}, status_code=207)
```

- [ ] **Step 5: Register the router**

In `src/ecet/interfaces/api/app.py`, extend the imports and the includes:

```python
from ecet.interfaces.api.routes import claims, events, health
...
    app.include_router(health.router)
    app.include_router(events.router)
    app.include_router(claims.router)
```

- [ ] **Step 6: Write the failing claims-route test**

`tests/api/test_claims_routes.py`:

```python
from uuid import uuid4

from ecet.domain.claim import ClaimStatus
from tests.api.conftest import BUCKET, KEY, ApiHarness
from tests.pii import assert_no_pii


async def test_manual_ingest_fills_the_etag_and_size_from_head(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )

    assert response.status_code == 200
    assert response.json()["status"] == ClaimStatus.QUEUED.value
    (stored,) = harness.uow.claims.claims.values()
    assert stored.source.size == len(b"%PDF-1.7 fake bytes")
    assert stored.source.etag


async def test_manual_ingest_of_a_missing_object_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": BUCKET, "key": "tenants/tenant-a/claims/absent.pdf"},
            headers=api_headers,
        )

    assert response.status_code == 404
    assert "absent.pdf" not in response.text


async def test_a_claim_can_be_read_back(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        claim_id = created.json()["claim_id"]

        response = await client.get(f"/v1/claims/{claim_id}", headers=api_headers)

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == claim_id
    assert body["tenant_id"] == "tenant-a"
    assert body["status"] == ClaimStatus.QUEUED.value
    assert body["deterministic"]["verdict"] == "PASS"
    assert body["entity_counts"]["US_SSN"] == 1


async def test_the_claim_view_exposes_no_text_and_no_secret(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        response = await client.get(
            f"/v1/claims/{created.json()['claim_id']}", headers=api_headers
        )

    body = response.json()
    assert "redacted_text" not in body
    assert "webhook_secret" not in body
    assert_no_pii(response.text)


async def test_another_tenants_claim_is_404_not_403(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        created = await client.post(
            "/v1/claims/ingest", json={"bucket": BUCKET, "key": KEY}, headers=api_headers
        )
        response = await client.get(
            f"/v1/claims/{created.json()['claim_id']}",
            headers={**api_headers, "X-Tenant-Id": "tenant-b"},
        )

    # 404, not 403: a 403 confirms the claim exists, which is itself a leak.
    assert response.status_code == 404


async def test_an_unknown_claim_is_404(
    harness: ApiHarness, api_headers: dict[str, str]
) -> None:
    async with harness.client() as client:
        response = await client.get(f"/v1/claims/{uuid4()}", headers=api_headers)

    assert response.status_code == 404


async def test_reading_a_claim_needs_the_tenant_header(
    harness: ApiHarness,
) -> None:
    async with harness.client() as client:
        response = await client.get(
            f"/v1/claims/{uuid4()}", headers={"X-API-Key": "test-key"}
        )

    assert response.status_code == 422
```

- [ ] **Step 7: Run it to verify it fails**

Run: `uv run pytest tests/api/test_claims_routes.py -v`
Expected: FAIL — 404s; the routes do not exist.

- [ ] **Step 8: Write the claims routes**

`src/ecet/interfaces/api/routes/claims.py`:

```python
"""`/v1/claims` — manual ingest and the claim view.

`POST /v1/claims/ingest` exists for the demo and for local development: it takes
`{bucket, key}` and fills in the etag and size with a HEAD, so a developer can ingest
an object without forging an S3 notification.

`GET /v1/claims/{id}` is where tenant isolation is enforced by hand. `ClaimRepository`
has no tenant parameter, so the route compares the loaded claim's tenant against the
`X-Tenant-Id` header and answers 404 — never 403 — on a mismatch: a 403 would confirm
that the claim exists.
"""

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from ecet.application.use_cases.ingest_claim_document import IngestCommand, IngestResult
from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound
from ecet.domain.evaluation import DeterministicResult, Evaluation
from ecet.domain.ids import ClaimId, PolicyId
from ecet.interfaces.api.dependencies import ContainerDep, TenantDep, require_api_key

router = APIRouter(tags=["claims"], dependencies=[Depends(require_api_key)])


class ManualIngestRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    bucket: str
    key: str


class ClaimView(BaseModel):
    """What a client may see. Deliberately excludes the redacted text: it is not a
    secret, but it is clinical content, and no caller in this phase needs it."""

    model_config = ConfigDict(frozen=True)

    id: UUID
    tenant_id: str
    status: ClaimStatus
    policy_ids: list[PolicyId]
    entity_counts: dict[str, int]
    deterministic: DeterministicResult | None
    evaluation: Evaluation | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def of(cls, claim: Claim) -> "ClaimView":
        return cls(
            id=claim.id,
            tenant_id=str(claim.tenant_id),
            status=claim.status,
            policy_ids=list(claim.policy_ids),
            entity_counts=dict(claim.redacted.entity_counts) if claim.redacted else {},
            deterministic=claim.deterministic,
            evaluation=claim.evaluation,
            failure_reason=claim.failure_reason,
            created_at=claim.created_at,
            updated_at=claim.updated_at,
        )


@router.post("/v1/claims/ingest")
async def ingest_claim(
    body: ManualIngestRequest, container: ContainerDep
) -> IngestResult:
    head = await container.storage.head(body.bucket, body.key)
    return await container.ingest.execute(
        IngestCommand(bucket=body.bucket, key=body.key, etag=head.etag, size=head.size)
    )


@router.get("/v1/claims/{claim_id}")
async def get_claim(
    claim_id: UUID, tenant_id: TenantDep, container: ContainerDep
) -> ClaimView:
    async with container.uow_factory() as uow:
        claim = await uow.claims.get(ClaimId(claim_id))
    if claim.tenant_id != tenant_id:
        # Same answer as "no such claim" — anything else confirms its existence.
        raise ClaimNotFound(str(claim_id))
    return ClaimView.of(claim)
```

- [ ] **Step 9: Run the API suite to verify it passes**

Run: `uv run pytest tests/api -v`
Expected: PASS — including the two auth tests from Task 10 that were failing with 404.

- [ ] **Step 10: Run the whole gate**

Run: `make check`
Expected: all green.

- [ ] **Step 11: Commit**

```bash
git add src/ecet/interfaces/api tests/api tests/fixtures/s3_events
git commit -m "feat(api): s3 event, manual ingest and claim view routes"
```

---

### Task 12: Compose wiring — `minio-setup`, `/readyz` healthcheck, demo script

**Files:**
- Modify: `docker-compose.yml`, `Makefile`, `.env.example`
- Create: `scripts/demo_drop.sh`
- Test: `tests/unit/test_compose.py`

**Interfaces:**
- Consumes: `/readyz` (Task 10), the routes (Task 11), `ECET_S3_EVENT_TOKEN` (existing config).
- Produces: a `docker compose up` stack where dropping a PDF in the bucket reaches
  `POST /v1/events/s3` with no manual step.

- [ ] **Step 1: Write the failing compose test**

`tests/unit/test_compose.py`:

```python
"""The compose file is configuration, so it gets a configuration test: the pieces the
phase depends on are present and point at the right things. Cheap insurance against a
silent edit — the alternative is finding out during a demo."""

from pathlib import Path
from typing import Any

import pytest

yaml = pytest.importorskip("yaml")

COMPOSE = Path(__file__).resolve().parents[2] / "docker-compose.yml"


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    parsed: dict[str, Any] = yaml.safe_load(COMPOSE.read_text())
    return parsed


def test_the_api_healthcheck_probes_readyz(compose: dict[str, Any]) -> None:
    probe = " ".join(compose["services"]["api"]["healthcheck"]["test"])

    assert "/readyz" in probe
    assert "/healthz" not in probe


def test_minio_setup_registers_the_webhook_and_the_event(compose: dict[str, Any]) -> None:
    setup = compose["services"]["minio-setup"]
    script = " ".join(setup["entrypoint"])

    assert "notify_webhook:ecet" in script
    assert "/v1/events/s3" in script
    assert "mc event add" in script
    assert "--suffix .pdf" in script
    assert setup["depends_on"]["api"]["condition"] == "service_healthy"
    assert setup["depends_on"]["minio"]["condition"] == "service_healthy"


def test_the_api_waits_for_minio_too(compose: dict[str, Any]) -> None:
    assert "minio" in compose["services"]["api"]["depends_on"]
```

`pyyaml` arrives with several existing dev dependencies; `importorskip` keeps the
test from being a new hard requirement. If it skips, add it explicitly:
`uv add --dev 'pyyaml>=6,<7'`.

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: FAIL — no `minio-setup` service, and the api healthcheck still probes
`/healthz`.

- [ ] **Step 3: Switch the api healthcheck and add the minio dependency**

In `docker-compose.yml`, in the `api` service:

```yaml
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
      minio:
        condition: service_healthy
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - "import urllib.request; urllib.request.urlopen('http://localhost:8000/readyz')"
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 40s
```

`start_period` rises from 10 s to 40 s: the API now loads a 600 MB spaCy model,
runs migrations and opens an AMQP connection before it is ready.

- [ ] **Step 4: Add the `minio-setup` service**

Append to `docker-compose.yml` `services:`:

```yaml
  minio-setup:
    image: minio/mc:RELEASE.2025-08-13T08-35-41Z
    depends_on:
      minio:
        condition: service_healthy
      api:
        condition: service_healthy
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      ECET_S3_EVENT_TOKEN: ${ECET_S3_EVENT_TOKEN}
    # One-shot. It depends on `api` being healthy because MinIO validates the webhook
    # endpoint when the event is registered — an unreachable target fails `mc event add`.
    entrypoint:
      - /bin/sh
      - -c
      - |
        set -e
        mc alias set local http://minio:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD"
        mc mb -p local/claims
        mc admin config set local notify_webhook:ecet \
          endpoint=http://api:8000/v1/events/s3 \
          auth_token="$$ECET_S3_EVENT_TOKEN"
        mc admin service restart local
        sleep 5
        mc event add local/claims arn:minio:sqs::ecet:webhook --event put --suffix .pdf
        echo "minio notifications wired"
    restart: "no"
```

`$$` escapes compose's own interpolation so the variable is read by the shell inside
the container, not by compose on the host.

- [ ] **Step 5: Run the compose test to verify it passes**

Run: `uv run pytest tests/unit/test_compose.py -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Write the demo drop script**

`scripts/demo_drop.sh`:

```bash
#!/usr/bin/env bash
# Drop a fixture PDF into a tenant's prefix and watch the api pick it up.
#
#   ./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-a
#
# Requires the compose stack to be up (`make up`) and the fixtures to exist
# (`make fixtures`).
set -euo pipefail

PDF="${1:-tests/fixtures/pdfs/note_simple.pdf}"
TENANT="${2:-tenant-a}"
KEY="tenants/${TENANT}/claims/$(basename "${PDF%.pdf}")-$(date +%s).pdf"

if [ ! -f "$PDF" ]; then
  echo "no such file: $PDF (run 'make fixtures' first)" >&2
  exit 1
fi

docker compose cp "$PDF" minio:/tmp/drop.pdf
docker compose exec -T minio mc alias set local http://localhost:9000 \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}" >/dev/null
docker compose exec -T minio mc cp /tmp/drop.pdf "local/claims/${KEY}"

echo "dropped ${KEY}"
docker compose logs --since 30s api
```

```bash
chmod +x scripts/demo_drop.sh
```

Add to the `Makefile` (and to `.PHONY`):

```make
drop: fixtures
	./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf $(or $(TENANT),tenant-a)
```

- [ ] **Step 7: Run the stack end to end**

```bash
make up
# wait for the api to report healthy (the spaCy load takes ~10-30 s)
docker compose ps
curl -sf localhost:8000/readyz | jq
make drop
```

Expected, in order:
1. `/readyz` returns `{"database": true, "queue": true, "redactor": true}`.
2. `make drop` prints `dropped tenants/tenant-a/claims/note_simple-....pdf`.
3. The api log shows `claim.transition` up to `to=QUEUED` and `queue.published`.
4. The message is visible in the RabbitMQ UI at <http://localhost:15672>
   (guest/guest), queue `claims.evaluate`, ready = 1. It stays there — the consumer
   arrives in Phase 4.

Then the ADR-005 failure path:

```bash
make drop TENANT=tenant-empty
```

Expected: the api log shows `claim.failed to=NO_POLICIES reason=tenant-empty`, and
MinIO's own log reports the webhook answering 422.

Verify the claim landed in the database:

```bash
docker compose exec -T postgres psql -U ecet -d ecet \
  -c "select tenant_id, status, failure_reason from claims order by created_at desc limit 5;"
```

Expected: one `tenant-a`/`QUEUED` row and one `tenant-empty`/`NO_POLICIES` row.

- [ ] **Step 8: Confirm no raw text reached the database**

```bash
docker compose exec -T postgres psql -U ecet -d ecet \
  -c "select redacted_text from claims where status = 'QUEUED' limit 1;" | grep -c "Marcus Whitfield" || echo "no PII found"
```

Expected: `no PII found`. This is the ADR-001 acceptance check for the whole phase —
if it prints a count, stop and fix before continuing.

- [ ] **Step 9: Tear down and commit**

```bash
make down
git add docker-compose.yml Makefile scripts/demo_drop.sh tests/unit/test_compose.py
git commit -m "feat(compose): minio webhook wiring, readyz healthcheck and the demo drop script"
```

---

### Task 13: Record the phase in the docs

**Files:**
- Modify: `specs/06-roadmap.md`
- Modify: `docs/plans/2026-09-06-phase-3-ingestion-path.md` (this file — tick the boxes)

**Interfaces:**
- Consumes: the deviation list at the bottom of this plan.
- Produces: a roadmap that carries every Phase 3 deferral into the phase that closes it.

- [ ] **Step 1: Link the plan from the roadmap**

Under `## Phase 3 — Ingestion Path (API side)`, add a `Plan:` line matching the Phase 2
entry's shape:

```markdown
Plan: [`docs/plans/2026-09-06-phase-3-ingestion-path.md`](../docs/plans/2026-09-06-phase-3-ingestion-path.md).
```

- [ ] **Step 2: Add the Phase 3 carry-over table**

After the `## Carried over from Phase 2` table, add:

```markdown
## Carried over from Phase 3

Every deferral recorded in [`docs/plans/2026-09-06-phase-3-ingestion-path.md`](../docs/plans/2026-09-06-phase-3-ingestion-path.md#deviations-from-spec-record-in-the-pr-description), with the phase that closes it. Each is also listed inline in its phase above.

| # | Deferred in Phase 3 | Closed by |
|---|---------------------|-----------|
| 1 | `infrastructure/queue/in_memory.py` and `infrastructure/pii/fake_redactor.py` from the layout spec are not built; `tests/fakes.py` covers both | accepted unless a compose profile needs a fake redactor |
| 2 | UC-01's "wrap steps 5–11 so any unexpected exception sets a failure state" is implemented only for the two failure states the machine actually allows (`EXTRACTION_FAILED`, `NO_POLICIES`); anything else rolls back and leaves the claim `RECEIVED` | accepted — the state machine has no generic failure edge |
| 3 | A publish failure leaves the claim `POLICIES_ATTACHED` with no automatic retry | Phase 5 (`/v1/claims/{id}/retry-notify` and a sweeper); the outbox stays out of v1 |
| 4 | `GET /v1/claims/{id}` does not return the redacted text; UC-09b will need it | Phase 5 |
| 5 | No metrics: `ecet_ingest_seconds`, `ecet_pdf_extract_seconds`, `ecet_pii_*`, `ecet_deterministic_verdict_total`, `ecet_llm_calls_avoided_total` are all unimplemented, and the UC-04 metrics hook is a comment | Phase 6 |
| 6 | `request_id` is not bound to the log context and not propagated as `x-request-id` on the queue message | Phase 6 |
| 7 | An entity spanning a `\n\n` chunk boundary is not redacted by the presidio adapter | accepted, documented limitation |
| 8 | The presidio adapter test needs `en_core_web_lg` present locally (`make spacy-model`); it is not downloaded automatically | accepted, permanent |
| 9 | Migrations run through a subprocess from the API lifespan, because `migrations/env.py` calls `asyncio.run` | accepted, permanent |
| 10 | `ECET_AUTO_MIGRATE=true` also seeds, but only when `ECET_ENV=dev`; a non-dev deploy with auto-migrate on migrates without seeding | accepted, deliberate |
```

- [ ] **Step 3: Add the inline Phase 4/5/6 carry-over lines**

In `## Phase 4`, append to the existing `- Carried from Phase 2:` block a new line:

```markdown
- Carried from Phase 3: the `claims.evaluate` topology, `declare_topology()` and the queue constants live in `infrastructure/queue/rabbitmq.py` — the consumer imports them rather than redeclaring. `RequestHumanReview` (UC-09a) already exists and is built per unit of work, not injected. `EvaluationMessage` is the frozen wire contract: a change to it is a `schema_version` bump, not an edit.
```

In `## Phase 5`, add:

```markdown
- Carried from Phase 3: a publish failure leaves the claim `POLICIES_ATTACHED` with no retry path yet, and `ClaimView` omits the redacted text that UC-09b needs.
```

In `## Phase 6`, append to the existing `- Carried from Phase 0:` block:

```markdown
- Carried from Phase 3: no metric is emitted anywhere in the ingestion path, and `request_id` is neither bound to the structlog context nor propagated to the queue as `x-request-id`.
```

- [ ] **Step 4: Verify every roadmap link resolves**

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

Expected: `all links resolve`.

- [ ] **Step 5: Commit**

```bash
git add specs/06-roadmap.md docs/plans/2026-09-06-phase-3-ingestion-path.md
git commit -m "docs(phase-3): record the deviations and the Phase 3 carry-overs"
```

---

## Phase exit criteria

Everything below must be true before the phase is called done:

1. `make check` green (lint, `mypy --strict` on domain + application, `mypy src/ecet`, import contracts, default test suite).
2. `uv run pytest -m 'not e2e' --cov=ecet --cov-fail-under=85` green with Docker and `en_core_web_lg` available.
3. `make up` → `curl -sf localhost:8000/readyz` returns all-true.
4. `make drop` → a `QUEUED` claim in `claims` and one ready message on `claims.evaluate`.
5. `make drop TENANT=tenant-empty` → `NO_POLICIES` persisted, webhook answered 422.
6. `select redacted_text from claims` contains no fixture PII string (the ADR-001 check).
7. A second drop of the same object (same etag) creates no second claim and no second message.

## Deviations from spec (record in the PR description)

1. **`ExtractionFailed`, `ObjectNotFound` and `QueuePublishError` live in a new `src/ecet/application/errors.py`**, which the [project layout](../../specs/05-platform/project-layout.md) does not list. They are contract failures of application ports, not domain rules, so `domain/errors.py` is the wrong home — but they subclass `DomainError` so the API error mapping stays one tree.
2. **`ObjectStorage` gained a `head()` method** beyond the [use-case index](../../specs/02-use-cases/README.md#ports-defined-in-ecetapplicationports)'s `get_bytes`. `POST /v1/claims/ingest` receives only `{bucket, key}` and the [api spec](../../specs/04-interfaces/api.md#endpoints) says the API fetches the etag and size via HEAD; without the method there is nothing to fetch them with.
3. **`Clock` is synchronous** (`def now()`, not `async def now()`), unlike every other port. Reading a clock never blocks, and an `await` on it would be noise at eleven call sites.
4. **`IngestClaimDocument` takes a `uow_factory: Callable[[], UnitOfWork]`, not a `UnitOfWork`** as [UC-01](../../specs/02-use-cases/UC-01-ingest-claim-document.md#dependencies) lists. The Phase 2 `SqlAlchemyUnitOfWork` is single-use (re-entering it does not create a fresh session), and the [api spec](../../specs/04-interfaces/api.md#container-containerpy) builds the use case once at startup and reuses it per request. A factory is the only shape that satisfies both.
5. **UC-03 and UC-09a are constructed per request inside UC-01, not injected.** They depend on repositories owned by a unit of work, whose lifetime is one request; a process-scoped constructor argument would capture a dead session.
6. **`infrastructure/queue/in_memory.py` and `infrastructure/pii/fake_redactor.py` are not built**, though the [project layout](../../specs/05-platform/project-layout.md) lists them. `tests/fakes.py` already provides `FakeEvaluationQueue` and `FakePiiRedactor`, and nothing in the compose stack runs without a real broker or a real redactor. Adding them now would be two modules with no caller.
7. **UC-01's "steps 5–11 wrapped so any unexpected exception sets failure state + reason before re-raising"** is implemented as two explicit failure paths rather than one blanket handler. The state machine allows `EXTRACTION_FAILED` only from `RECEIVED` and `NO_POLICIES` only from `REDACTED`; there is no failure edge reachable from every intermediate state, so a blanket wrapper would raise `InvalidTransition` while handling the original error. An unexpected exception now rolls back to the committed `RECEIVED` claim and re-raises.
8. **The pypdf adapter test lives in `tests/unit/infrastructure/`, not `tests/adapters/`.** It needs no container, and `tests/adapters/conftest.py` marks its whole directory `slow` — putting it there would drop the extractor out of the default run for no benefit.
9. **MinIO and RabbitMQ containers are started through the generic `DockerContainer`** rather than a testcontainers module, and readiness is a retry loop around the call the test needs (`tests/adapters/containers.py`). Module import paths move between testcontainers releases and "ready" log lines move between image releases; the operation the test performs moves neither.
10. **The presidio adapter analyses only the entities named in `ENTITY_REPLACEMENTS`.** `DATE_TIME` is kept per [UC-02](../../specs/02-use-cases/UC-02-redact-pii.md#redaction-policy-application-constant-applicationredaction_policypy) by simply never being analysed, rather than by being detected and then skipped — one fewer pass over the text, same result.
11. **A paragraph longer than the chunk limit is passed to presidio whole** instead of being hard-cut at 20 k characters. Cutting mid-sentence breaks more entities than the memory bound saves; the chunker only splits on `\n\n`.
12. **Alembic runs in a subprocess from the API lifespan** (`python -m alembic upgrade head`), not through `alembic.command`. `migrations/env.py` calls `asyncio.run`, which cannot nest inside the loop uvicorn is already running — the same constraint the Phase 2 test harness hit.
13. **The Alembic project root is resolved from the working directory**, not from `__file__`. In the image the package is installed into site-packages while `alembic.ini` and `migrations/` sit in `/app`, the working directory; a `__file__`-relative walk finds neither.
14. **`ECET_AUTO_MIGRATE=true` also runs the seed, but only when `ECET_ENV=dev`.** The [docker-compose spec](../../specs/05-platform/docker-compose.md#docker-composeyml-services) says "migrations + seed"; `ecet seed` already refuses to run outside dev (Phase 2 deviation 3), and calling `load_seed` directly would bypass that guard, so the guard is re-applied here.
15. **`GET /v1/claims/{id}` omits the redacted text.** The [api spec](../../specs/04-interfaces/api.md#endpoints) asks for "status, deterministic, evaluation, no secrets" and does not list the text; UC-09b (Phase 5) is the caller that actually needs it, and it can add the field with its own review.
16. **`ProblemDetails.detail` is a per-error constant, never the exception message.** Closes Phase 1 carry-over #4, and extends the same rule to `ObjectNotFound` and to the 207 batch body, which reports only the exception class name. The message still reaches the structlog record.
17. **A cross-tenant claim read answers 404, not 403.** A 403 confirms the claim exists, which is a cross-tenant information leak in a system whose whole premise is tenant isolation.
18. **The api container healthcheck `start_period` rises from 10 s to 40 s.** The API now loads a ~600 MB spaCy model, runs migrations and opens an AMQP connection before `/readyz` can answer, and `minio-setup` waits on that healthcheck.
19. **`tests/fixtures/pdfs/` is gitignored and regenerated** by `scripts/make_fixtures.py`, including `scanned.pdf` — the [pdf-text-extractor spec](../../specs/03-infrastructure/pdf-text-extractor.md#fixtures-testsfixturespdfs) allows committing that one binary, but a reportlab-drawn page with no text operators reproduces it exactly, so nothing binary needs committing at all.
20. **`en_core_web_lg` is downloaded, never declared as a dependency.** It is a 590 MB wheel from a non-PyPI index; `uv.lock` would either fail to resolve it or pin a URL that rots. `make spacy-model`, the CI `slow` step and the Dockerfile `SPACY_MODEL` build arg are the three places it is fetched.
