import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.adapters.helpers import LATER, build_claim, build_tenant, insert_claim, insert_tenant

from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.domain.claim import ClaimStatus
from ecet.domain.ids import TenantId
from ecet.infrastructure.postgres.orm import TenantRow
from ecet.infrastructure.postgres.unit_of_work import SqlAlchemyUnitOfWork


async def test_the_unit_of_work_satisfies_the_port(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert isinstance(SqlAlchemyUnitOfWork(session_factory), UnitOfWork)


async def _count_tenants(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(TenantRow)) or 0


async def test_committed_work_is_visible_to_the_next_unit_of_work(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.session.execute(TenantRow.__table__.insert().values(**_values()))
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        assert (await uow.tenants.get(TenantId("tenant-a"))).id == "tenant-a"


async def test_work_that_is_not_committed_is_rolled_back(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.session.execute(TenantRow.__table__.insert().values(**_values()))

    assert await _count_tenants(session_factory) == 0


async def test_an_exception_inside_the_block_rolls_back_and_propagates(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.session.execute(TenantRow.__table__.insert().values(**_values()))
            raise RuntimeError("boom")

    assert await _count_tenants(session_factory) == 0


async def test_a_reentered_unit_of_work_does_not_raise_a_spurious_concurrent_modification(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """`__aexit__` rolls back and must also drop the claim repository's baseline, or a
    reused instance would compare a later save against an `updated_at` a rollback
    already undid."""
    claim = build_claim()
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await insert_claim(session, claim)
        await session.commit()

    uow = SqlAlchemyUnitOfWork(session_factory)
    async with uow:
        loaded = await uow.claims.get(claim.id)
        loaded.transition(ClaimStatus.EXTRACTED, now=LATER)
        await uow.claims.save(loaded)
        # No commit: __aexit__ rolls this back.

    async with uow:
        reloaded = await uow.claims.get(claim.id)
        assert reloaded.status is ClaimStatus.RECEIVED
        reloaded.transition(ClaimStatus.EXTRACTED, now=LATER)
        await uow.claims.save(reloaded)
        await uow.commit()


def _values() -> dict[str, object]:
    from ecet.infrastructure.postgres.mappers import tenant_to_row_values

    return tenant_to_row_values(build_tenant("tenant-a"))
