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
            checks=[CheckOutcome(name="covered_code_hit", passed=False, detail="no covered code")],
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
    assert_no_pii(view.model_dump_json())


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
    await uow.review_tasks.save(task)

    assert await build_use_case(uow).execute(TENANT) == []


async def test_the_limit_is_honoured() -> None:
    uow = FakeUnitOfWork()
    for index in range(3):
        await seed_review(uow, name=f"note-{index}", minutes=index)

    views = await build_use_case(uow).execute(TENANT, limit=2)

    assert len(views) == 2
