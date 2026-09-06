import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.adapters.helpers import build_tenant, insert_tenant

from ecet.domain.errors import TenantNotFound
from ecet.domain.ids import TenantId
from ecet.infrastructure.postgres.orm import TenantRow
from ecet.infrastructure.postgres.repositories import PostgresTenantRepository


async def test_a_seeded_tenant_round_trips(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    tenant = build_tenant("tenant-a", name="Northwind Health Plan")
    async with session_factory() as session:
        await insert_tenant(session, tenant)
        await session.commit()

    async with session_factory() as session:
        loaded = await PostgresTenantRepository(session).get(TenantId("tenant-a"))
    assert loaded == tenant
    assert loaded.webhook_secret.get_secret_value() == "dev-hmac-tenant-a"


async def test_the_secret_is_stored_in_plain_text_not_masked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await session.commit()
        stored = await session.scalar(select(TenantRow.webhook_secret))
    assert stored == "dev-hmac-tenant-a"


async def test_an_unknown_tenant_raises(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        with pytest.raises(TenantNotFound):
            await PostgresTenantRepository(session).get(TenantId("nobody"))


async def test_an_inactive_tenant_is_treated_as_missing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-legacy", active=False))
        await session.commit()
        with pytest.raises(TenantNotFound):
            await PostgresTenantRepository(session).get(TenantId("tenant-legacy"))
