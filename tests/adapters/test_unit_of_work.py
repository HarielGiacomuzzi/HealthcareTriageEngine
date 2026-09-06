import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.adapters.helpers import build_tenant

from ecet.application.ports.unit_of_work import UnitOfWork
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


def _values() -> dict[str, object]:
    from ecet.infrastructure.postgres.mappers import tenant_to_row_values

    return tenant_to_row_values(build_tenant("tenant-a"))
