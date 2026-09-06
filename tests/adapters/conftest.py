"""Adapter tests: one real Postgres 16 in Docker per session, migrations applied once.

Everything under `tests/adapters/` is marked `slow`, so the default
`pytest -m 'not slow and not e2e'` run stays Docker-free.
"""

import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer

from ecet.infrastructure.postgres.session import create_engine, create_session_factory

REPO_ROOT = Path(__file__).resolve().parents[2]
THIS_DIR = Path(__file__).resolve().parent
TABLES = "claims, icd10_codes, policies, review_tasks, tenants"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # `pytest_collection_modifyitems` sees the whole session's items, not just this
    # directory's — filter, or every test in the suite ends up marked `slow`.
    for item in items:
        if THIS_DIR in item.path.parents:
            item.add_marker(pytest.mark.slow)


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """A migrated database. Alembic runs in a subprocess: it owns its own event loop."""
    with PostgresContainer(
        "postgres:16", driver="asyncpg", username="ecet", password="ecet", dbname="ecet"
    ) as container:
        url = container.get_connection_url()
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=REPO_ROOT,
            env={**os.environ, "ECET_DATABASE_URL": url},
            check=True,
        )
        yield url


@pytest.fixture
async def session_factory(postgres_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A clean database per test.

    The engine is function-scoped on purpose: asyncpg connections belong to the event
    loop that opened them, and pytest-asyncio gives every test a fresh loop.
    """
    engine = create_engine(postgres_url)
    async with engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    yield create_session_factory(engine)
    await engine.dispose()
