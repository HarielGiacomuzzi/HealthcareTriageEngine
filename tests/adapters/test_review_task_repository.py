from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.adapters.helpers import (
    NOW,
    build_claim,
    build_tenant,
    insert_claim,
    insert_tenant,
)

from ecet.domain.claim import Claim
from ecet.domain.errors import ReviewTaskNotFound
from ecet.domain.evaluation import Decision, ReviewReason, ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId, TenantId
from ecet.infrastructure.postgres.repositories import PostgresReviewTaskRepository


def build_task(claim: Claim, **overrides: Any) -> ReviewTask:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "claim_id": claim.id,
        "tenant_id": claim.tenant_id,
        "reason": ReviewReason.LOW_CONFIDENCE,
        "created_at": NOW,
    }
    fields.update(overrides)
    return ReviewTask.model_validate(fields)


@pytest.fixture
async def claim(session_factory: async_sessionmaker[AsyncSession]) -> Claim:
    made = build_claim()
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await insert_claim(session, made)
        await session.commit()
    return made


async def test_a_task_round_trips(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        await PostgresReviewTaskRepository(session).add(task)
        await session.commit()

    async with session_factory() as session:
        assert await PostgresReviewTaskRepository(session).get(task.id) == task


async def test_an_unknown_task_raises(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        with pytest.raises(ReviewTaskNotFound):
            await PostgresReviewTaskRepository(session).get(uuid4())


async def test_find_open_by_claim_is_the_uc09a_idempotency_lookup(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert await repository.find_open_by_claim(claim.id) is None
        await repository.add(task)
        await session.commit()
        assert await repository.find_open_by_claim(claim.id) == task


async def test_a_resolved_task_is_no_longer_open_for_its_claim(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(task)
        task.resolve(resolution=Decision.MEETS_NECESSITY, reviewer="nurse", notes=None, now=NOW)
        await repository.save(task)
        await session.commit()

    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert await repository.find_open_by_claim(claim.id) is None
        stored = await repository.get(task.id)
        assert stored.status is ReviewStatus.RESOLVED
        assert stored.resolution is Decision.MEETS_NECESSITY
        assert stored.resolved_at == NOW


async def test_one_claim_cannot_carry_two_tasks(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(build_task(claim))
        with pytest.raises(IntegrityError):
            await repository.add(build_task(claim))


async def test_list_open_is_tenant_scoped_and_limited(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    mine = [build_claim(suffix=f"m{index}") for index in range(3)]
    theirs = build_claim(suffix="t0", tenant_id="tenant-b")
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await insert_tenant(session, build_tenant("tenant-b"))
        repository = PostgresReviewTaskRepository(session)
        for made in [*mine, theirs]:
            await insert_claim(session, made)
            await repository.add(build_task(made))
        await session.commit()

    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert len(await repository.list_open(TenantId("tenant-a"))) == 3
        assert len(await repository.list_open(TenantId("tenant-a"), 2)) == 2
        assert len(await repository.list_open(TenantId("tenant-b"))) == 1


async def test_a_resolved_task_disappears_from_the_open_list(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(task)
        task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="nurse", notes=None, now=NOW)
        await repository.save(task)
        await session.commit()
        assert await repository.list_open(TenantId("tenant-a")) == []


async def test_saving_a_task_that_is_not_stored_raises(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    async with session_factory() as session:
        with pytest.raises(ReviewTaskNotFound):
            await PostgresReviewTaskRepository(session).save(build_task(claim))


async def test_claim_id_survives_the_round_trip_as_a_claim_id(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(task)
        await session.commit()
        assert (await repository.get(task.id)).claim_id == ClaimId(claim.id)
