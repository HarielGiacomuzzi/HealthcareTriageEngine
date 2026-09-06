from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.adapters.helpers import LATER, build_claim, build_tenant, insert_tenant

from ecet.domain.claim import ClaimStatus, RedactedText
from ecet.domain.errors import ClaimNotFound, ConcurrentModification
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.ids import ClaimId
from ecet.infrastructure.postgres.repositories import PostgresClaimRepository


@pytest.fixture
async def seeded_tenants(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await session.commit()


async def test_a_claim_round_trips_with_every_optional_field_set(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim(
        status=ClaimStatus.REDACTED,
        redacted=RedactedText(text="note", entity_counts={"PERSON": 2}, redactor="presidio-2.2.x"),
        deterministic=DeterministicResult(
            verdict=Verdict.UNCERTAIN,
            checks=[CheckOutcome(name="icd10_present", passed=False, detail="no codes found")],
        ),
    )
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(claim)
        await session.commit()

    async with session_factory() as session:
        loaded = await PostgresClaimRepository(session).get(claim.id)
    assert loaded == claim


async def test_an_unknown_claim_raises(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    async with session_factory() as session:
        with pytest.raises(ClaimNotFound):
            await PostgresClaimRepository(session).get(ClaimId(uuid4()))


async def test_find_by_source_is_the_idempotency_lookup(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        await PostgresClaimRepository(session).add(claim)
        await session.commit()

    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        found = await repository.find_by_source("claims", claim.source.key, "etag-a")
        assert found == claim
        assert await repository.find_by_source("claims", claim.source.key, "other-etag") is None


async def test_the_same_object_cannot_be_ingested_twice(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    """ADR-006: the unique (bucket, key, etag) index is the last line of defence."""
    first = build_claim()
    second = build_claim(source=first.source)
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(first)
        with pytest.raises(IntegrityError):
            await repository.add(second)


async def test_saving_persists_the_transition(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(claim)
        claim.transition(ClaimStatus.EXTRACTED, now=LATER)
        await repository.save(claim)
        await session.commit()

    async with session_factory() as session:
        loaded = await PostgresClaimRepository(session).get(claim.id)
    assert loaded.status is ClaimStatus.EXTRACTED
    assert loaded.updated_at == LATER


async def test_a_second_writer_loses_the_optimistic_save(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        await PostgresClaimRepository(session).add(claim)
        await session.commit()

    async with session_factory() as first_session, session_factory() as second_session:
        first_repository = PostgresClaimRepository(first_session)
        second_repository = PostgresClaimRepository(second_session)
        first = await first_repository.get(claim.id)
        stale = await second_repository.get(claim.id)

        first.transition(ClaimStatus.EXTRACTED, now=LATER)
        await first_repository.save(first)
        await first_session.commit()

        stale.transition(ClaimStatus.EXTRACTION_FAILED, reason="encrypted pdf", now=LATER)
        with pytest.raises(ConcurrentModification):
            await second_repository.save(stale)


async def test_saving_a_claim_this_repository_never_loaded_is_refused(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        await PostgresClaimRepository(session).add(claim)
        await session.commit()

    async with session_factory() as session:
        with pytest.raises(ConcurrentModification, match="not loaded"):
            await PostgresClaimRepository(session).save(claim)


async def test_repeated_saves_from_the_same_repository_keep_working(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    """The baseline must advance on every save, or the second one would lose."""
    claim = build_claim()
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(claim)
        claim.transition(ClaimStatus.EXTRACTED, now=LATER)
        await repository.save(claim)
        claim.transition(ClaimStatus.REDACTED, now=LATER + timedelta(minutes=1))
        await repository.save(claim)
        await session.commit()

    async with session_factory() as session:
        assert (await PostgresClaimRepository(session).get(claim.id)).status is ClaimStatus.REDACTED


async def test_list_by_status_is_filtered_and_limited(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    queued = [build_claim(suffix=f"q{index}", status=ClaimStatus.QUEUED) for index in range(3)]
    received = build_claim(suffix="r0")
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        for claim in [*queued, received]:
            await repository.add(claim)
        await session.commit()

    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        assert len(await repository.list_by_status(ClaimStatus.QUEUED)) == 3
        assert len(await repository.list_by_status(ClaimStatus.QUEUED, limit=2)) == 2
        assert await repository.list_by_status(ClaimStatus.NO_POLICIES) == []
