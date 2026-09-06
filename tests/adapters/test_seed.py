from datetime import date

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.ids import TenantId
from ecet.infrastructure.postgres.orm import Icd10CodeRow, PolicyRow, TenantRow
from ecet.infrastructure.postgres.repositories import (
    PostgresIcd10CodeRepository,
    PostgresPolicyRepository,
)
from ecet.infrastructure.postgres.seed import load_seed

TODAY = date(2026, 9, 6)


async def test_the_seed_loads_every_table(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(TenantRow)) == 4
        assert await session.scalar(select(func.count()).select_from(PolicyRow)) == 15
        assert (await session.scalar(select(func.count()).select_from(Icd10CodeRow)) or 0) >= 80


async def test_the_seed_is_rerunnable(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(TenantRow)) == 4


async def test_every_seeded_policy_code_exists_in_the_catalogue(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    """A policy citing a code the catalogue does not know is a typo, every time."""
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        unknown = (
            await session.execute(
                text(
                    "SELECT unnest(covered_codes || excluded_codes) FROM policies "
                    "EXCEPT SELECT code FROM icd10_codes"
                )
            )
        ).all()
    assert unknown == []


async def test_the_seeded_policy_sets_are_what_the_demo_expects(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        repository = PostgresPolicyRepository(session)
        a = await repository.active_for_tenant(TenantId("tenant-a"), on=TODAY)
        b = await repository.active_for_tenant(TenantId("tenant-b"), on=TODAY)
        empty = await repository.active_for_tenant(TenantId("tenant-empty"), on=TODAY)
        codes = await PostgresIcd10CodeRepository(session).known_codes()

    assert len(a) == 5
    assert len(b) == 5
    assert empty == []
    # Superseded and expired versions never surface.
    assert {policy.version for policy in a if policy.name == "MRI lumbar spine"} == {2}
    assert "Hereditary cancer gene panel" not in {policy.name for policy in a}
    assert len(codes) >= 80
