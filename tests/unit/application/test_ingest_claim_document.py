"""UC-01 against fakes only. Every test asserts on the claim as it was *persisted*,
because that is where an ADR-001 leak would show up."""

import json
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY
from tests.fakes import (
    FakeEvaluationQueue,
    FakeObjectStorage,
    FakePiiRedactor,
    FakeTextExtractor,
    FakeUnitOfWork,
    FixedClock,
)
from tests.pii import assert_no_pii, read_note

from ecet.application.errors import ExtractionFailed, ObjectNotFound, QueuePublishError
from ecet.application.use_cases.enqueue_evaluation import EnqueueEvaluation
from ecet.application.use_cases.ingest_claim_document import (
    IngestClaimDocument,
    IngestCommand,
)
from ecet.application.use_cases.redact_pii import RedactPii
from ecet.application.use_cases.run_deterministic_checks import RunDeterministicChecks
from ecet.domain.claim import ClaimStatus
from ecet.domain.errors import (
    InvalidObjectKey,
    NoPoliciesForTenant,
    PdfTooLarge,
    TenantNotFound,
)
from ecet.domain.evaluation import ReviewReason, ReviewStatus
from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.observability.logging import configure_logging

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
BUCKET = "claims"
KEY = "tenants/tenant-a/claims/note-1.pdf"
PDF = b"%PDF-1.7 fake bytes"


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


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
        self.redactor = FakePiiRedactor()
        self.use_case = IngestClaimDocument(
            uow_factory=lambda: self.uow,
            storage=self.storage,
            extractor=self.extractor,
            redact_pii=RedactPii(self.redactor),
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
    # add+commit, save+commit before publish, then the outer execute() commit
    # after `_run_pipeline` returns: a deleted `await uow.commit()` on any of
    # the three legs would silently leave the claim unpersisted.
    assert harness.uow.commits == 3


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
    harness = Harness(policies=[])

    with pytest.raises(NoPoliciesForTenant):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.NO_POLICIES
    # A short token, like every other failure_reason — not the tenant slug that
    # `str(NoPoliciesForTenant)` happens to be.
    assert stored.failure_reason == "no_policies"
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
    # Same condition as pypdf's "no_text" (a scan), so the same token: operators
    # should not have to know which layer noticed the page was blank.
    assert stored.failure_reason == "no_text"


async def test_a_deterministic_reject_opens_a_review_and_skips_the_queue() -> None:
    harness = Harness(note="excluded_code")

    result = await harness.ingest()

    assert result.status is ClaimStatus.REVIEW_PENDING
    assert harness.queue.published == []
    (task,) = harness.uow.review_tasks.tasks.values()
    assert task.claim_id == result.claim_id
    assert task.reason is ReviewReason.DETERMINISTIC_REJECT
    assert task.status is ReviewStatus.OPEN
    # add+commit, then the outer execute() commit after `_run_pipeline` returns —
    # the REJECT branch itself saves but does not commit.
    assert harness.uow.commits == 2


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
    # One commit after `claims.add`, one before `enqueue.execute`; the publish
    # raises before any third commit — proves the commit happened before publish
    # was attempted, not merely that the save landed.
    assert harness.uow.commits == 2


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
    # first ingest: insert + POLICIES_ATTACHED; re-publish: QUEUED write
    assert harness.uow.commits == 3


async def test_a_re_publish_that_fails_again_leaves_the_claim_policies_attached() -> None:
    harness = Harness(queue=FakeEvaluationQueue(error=QueuePublishError("no confirm")))
    with pytest.raises(QueuePublishError):
        await harness.ingest()

    with pytest.raises(QueuePublishError):
        await harness.ingest()

    (stored,) = harness.uow.claims.claims.values()
    assert stored.status is ClaimStatus.POLICIES_ATTACHED
    assert harness.queue.published == []


async def test_a_transition_log_line_names_where_the_claim_came_from(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The observability spec asks for one `claim.transition` per state change, with
    `from`, `to` and `reason` — `to` alone does not say what moved."""
    configure_logging("INFO")
    harness = Harness()

    await harness.ingest()

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    transitions = [line for line in lines if line["event"] == "claim.transition"]
    assert transitions, "no claim.transition line was logged"
    assert all(line["from"] and line["to"] for line in transitions)
    assert transitions[0]["from"] == "RECEIVED"
    assert transitions[0]["to"] == "EXTRACTED"


async def test_an_ingest_is_timed_under_its_outcome() -> None:
    before_ok = sample("ecet_ingest_seconds_count", outcome="ingested")
    before_dup = sample("ecet_ingest_seconds_count", outcome="duplicate")
    harness = Harness()

    await harness.ingest()
    await harness.ingest()  # same object: ADR-006 duplicate

    assert sample("ecet_ingest_seconds_count", outcome="ingested") == before_ok + 1
    assert sample("ecet_ingest_seconds_count", outcome="duplicate") == before_dup + 1


async def test_a_failed_ingest_is_timed_as_failed() -> None:
    before = sample("ecet_ingest_seconds_count", outcome="failed")
    harness = Harness(extractor=FakeTextExtractor(error=ExtractionFailed("no_text")))

    with pytest.raises(ExtractionFailed):
        await harness.ingest()

    assert sample("ecet_ingest_seconds_count", outcome="failed") == before + 1


@pytest.mark.parametrize(
    ("note", "minimum_writes"),
    [
        ("meets", 3),  # add, save POLICIES_ATTACHED, save QUEUED
        ("unclear", 3),
        ("excluded_code", 2),  # add, save REVIEW_PENDING
    ],
)
async def test_no_claim_written_during_ingestion_carries_pii(
    note: str, minimum_writes: int
) -> None:
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


def spy_on_unit_of_work(harness: Harness, target: object, method: str) -> list[bool]:
    """Wrap `target.method` so each call records whether the unit of work was open.
    An instance attribute shadows the class method, so the fake itself is unchanged."""
    open_during_call: list[bool] = []
    original = getattr(target, method)

    async def recording(*args: Any, **kwargs: Any) -> Any:
        open_during_call.append(harness.uow.active)
        return await original(*args, **kwargs)

    setattr(target, method, recording)
    return open_during_call


async def test_no_external_call_runs_inside_a_unit_of_work() -> None:
    """The object read, pypdf, spaCy and the broker can each take seconds; none of them
    may hold a pooled connection while they do (the Phase 6 fix, on the ingest side)."""
    harness = Harness()
    calls = {
        "get_bytes": spy_on_unit_of_work(harness, harness.storage, "get_bytes"),
        "extract": spy_on_unit_of_work(harness, harness.extractor, "extract"),
        "redact": spy_on_unit_of_work(harness, harness.redactor, "redact"),
        "publish": spy_on_unit_of_work(harness, harness.queue, "publish"),
    }

    result = await harness.ingest()

    assert result.status is ClaimStatus.QUEUED
    assert calls == {name: [False] for name in calls}


async def test_a_re_publish_runs_outside_the_unit_of_work_too() -> None:
    harness = Harness(queue=FakeEvaluationQueue(error=QueuePublishError("no confirm")))
    with pytest.raises(QueuePublishError):
        await harness.ingest()
    harness.queue.error = None
    publish_calls = spy_on_unit_of_work(harness, harness.queue, "publish")

    result = await harness.ingest()

    assert result.status is ClaimStatus.QUEUED
    assert publish_calls == [False]


async def test_a_claim_queued_by_a_concurrent_re_publish_is_not_an_error() -> None:
    """Between UC-01's `POLICIES_ATTACHED` commit and its `QUEUED` write, the same object
    arriving again can re-publish and mark the claim `QUEUED` first. The late writer
    finds it already `QUEUED` and leaves it, instead of raising `InvalidTransition`."""
    harness = Harness()

    async def racing_publish(message: Any) -> None:
        harness.queue.published.append(message)
        stored = harness.uow.claims.claims[message.claim_id]
        stored.transition(ClaimStatus.QUEUED, now=NOW)

    harness.queue.publish = racing_publish  # type: ignore[method-assign]

    result = await harness.ingest()

    assert result.status is ClaimStatus.QUEUED
    assert harness.uow.claims.claims[result.claim_id].status is ClaimStatus.QUEUED
