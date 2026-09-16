import pytest
from sqlalchemy import text

from ecet.infrastructure.postgres.migrations import assert_at_head, head_revision, upgrade_to_head
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


def test_head_revision_names_a_revision() -> None:
    assert head_revision()


async def test_a_database_behind_head_is_rejected(postgres_url: str) -> None:
    """Genuinely behind, not stamped with garbage: with one migration, "behind head" is
    the base — an `alembic_version` table with no row, which Alembic reads as `None`."""
    head = head_revision()
    engine = create_engine(postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM alembic_version"))
        with pytest.raises(RuntimeError, match="database at None"):
            await assert_at_head(engine)
    finally:
        # `postgres_url` is session-scoped and later tests rely on it. Re-stamping head
        # directly is the undo; `upgrade_to_head` would re-run the initial migration
        # against tables that already exist.
        async with engine.begin() as connection:
            await connection.execute(
                text("INSERT INTO alembic_version (version_num) VALUES (:head)"), {"head": head}
            )
        await assert_at_head(engine)
        await engine.dispose()


async def test_a_database_alembic_never_touched_is_rejected(postgres_url: str) -> None:
    """No `alembic_version` table at all: `get_current_revision()` returns `None` here
    too, and the api must refuse to start rather than run against an empty schema."""
    admin = create_engine(postgres_url)
    try:
        async with admin.connect() as connection:
            # CREATE DATABASE cannot run inside a transaction block.
            autocommit = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await autocommit.execute(text("DROP DATABASE IF EXISTS unstamped"))
            await autocommit.execute(text("CREATE DATABASE unstamped"))
    finally:
        await admin.dispose()

    engine = create_engine(postgres_url.rsplit("/", 1)[0] + "/unstamped")
    try:
        with pytest.raises(RuntimeError, match="database at None"):
            await assert_at_head(engine)
    finally:
        await engine.dispose()
