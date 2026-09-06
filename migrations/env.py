"""Alembic environment.

The URL comes from `ECET_DATABASE_URL`; `Settings` is deliberately not imported —
migrations run before the app is wired, and `Settings` also demands API secrets
that a migration has no use for.
"""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from ecet.infrastructure.postgres.orm import Base

DEFAULT_URL = "postgresql+asyncpg://ecet:ecet@localhost:5432/ecet"

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# `%` is configparser's interpolation character; dev credentials contain none.
config.set_main_option("sqlalchemy.url", os.environ.get("ECET_DATABASE_URL", DEFAULT_URL))

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
