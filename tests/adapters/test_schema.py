"""The migration and the ORM metadata must describe the same schema."""

from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from ecet.infrastructure.postgres.orm import Base

EXPECTED_TABLES = {
    "alembic_version",
    "claims",
    "icd10_codes",
    "policies",
    "review_tasks",
    "tenants",
}


def _diff(connection: Connection) -> list[object]:
    context = MigrationContext.configure(connection)
    return list(compare_metadata(context, Base.metadata))


async def test_migration_creates_every_table(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    async with engine.connect() as connection:
        tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
    await engine.dispose()
    assert tables == EXPECTED_TABLES


async def test_migration_matches_the_orm_metadata(postgres_url: str) -> None:
    """An empty autogenerate diff is the only proof that `orm.py` and `0001` agree."""
    engine = create_async_engine(postgres_url)
    async with engine.connect() as connection:
        diff = await connection.run_sync(_diff)
    await engine.dispose()
    assert diff == []
