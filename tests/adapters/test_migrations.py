import pytest
from sqlalchemy import text

from ecet.infrastructure.postgres.migrations import assert_at_head, upgrade_to_head
from ecet.infrastructure.postgres.session import create_engine


async def test_a_migrated_database_is_at_head(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    try:
        await assert_at_head(engine)
    finally:
        await engine.dispose()


async def test_upgrade_to_head_is_idempotent(postgres_url: str) -> None:
    await upgrade_to_head(postgres_url)

    engine = create_engine(postgres_url)
    try:
        await assert_at_head(engine)
    finally:
        await engine.dispose()


async def test_a_database_behind_head_is_rejected(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("UPDATE alembic_version SET version_num = 'nope'"))
        with pytest.raises(RuntimeError, match="pending migration"):
            await assert_at_head(engine)
    finally:
        # Put it back: `postgres_url` is session-scoped and later tests rely on it.
        # A DELETE + upgrade_to_head would make alembic think nothing was ever
        # applied and re-run every migration against tables that already exist —
        # restoring the stamped revision directly is the correct undo.
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE alembic_version SET version_num = '0001_initial'")
            )
        await assert_at_head(engine)
        await engine.dispose()
