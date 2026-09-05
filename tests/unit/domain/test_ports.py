from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from pydantic import SecretStr
from tests.fakes import (
    FakeClaimRepository,
    FakePolicyRepository,
    FakeReviewTaskRepository,
    FakeTenantRepository,
)

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
