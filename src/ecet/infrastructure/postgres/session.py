"""Engine and session factory. Built once per process and handed to the unit of work."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(database_url: str) -> AsyncEngine:
    """`database_url` is `ECET_DATABASE_URL` — the caller unwraps the `SecretStr`."""
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: repositories return domain models, so nothing needs a
    # post-commit refresh, and an expired attribute would trigger lazy IO.
    return async_sessionmaker(engine, expire_on_commit=False)
