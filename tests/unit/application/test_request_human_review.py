from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from tests.fakes import FakeReviewTaskRepository, FixedClock

from ecet.application.use_cases.human_review import RequestHumanReview
from ecet.domain.claim import Claim, ClaimStatus, SourceObject
from ecet.domain.evaluation import ReviewReason, ReviewStatus
from ecet.domain.ids import ClaimId

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
