"""UC-01 against fakes only. Every test asserts on the claim as it was *persisted*,
because that is where an ADR-001 leak would show up."""

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import pytest
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
from ecet.domain.errors import InvalidObjectKey, PdfTooLarge, TenantNotFound
from ecet.domain.evaluation import ReviewReason, ReviewStatus
from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant

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
