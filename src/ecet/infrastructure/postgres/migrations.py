"""Running and checking Alembic from inside the API process.

Alembic runs in a **subprocess**, not in-process: `migrations/env.py` calls
`asyncio.run`, which cannot nest inside the running event loop the API starts under.
That is the same reason the Phase 2 test harness shells out.

The project root is found from the working directory rather than from `__file__` —
in the container the package lives in site-packages while `alembic.ini` and
`migrations/` sit in `/app`, the working directory.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import structlog
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

log = structlog.get_logger(__name__)


def project_root() -> Path:
    """The directory holding `alembic.ini`: the working directory, or an ancestor."""
    start = Path.cwd().resolve()
    for candidate in (start, *start.parents):
        if (candidate / "alembic.ini").is_file():
            return candidate
    raise RuntimeError(f"no alembic.ini at or above {start}")


async def upgrade_to_head(database_url: str) -> None:
    await asyncio.to_thread(_upgrade, database_url)
    log.info("db.migrated")


def _upgrade(database_url: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=project_root(),
        env={**os.environ, "ECET_DATABASE_URL": database_url},
        check=True,
    )


async def assert_at_head(engine: AsyncEngine) -> None:
    """Fail fast when the schema is behind the code (`ECET_AUTO_MIGRATE=false`)."""
    async with engine.connect() as connection:
        await connection.run_sync(_assert_at_head)


def _assert_at_head(connection: Connection) -> None:
    root = project_root()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    head = ScriptDirectory.from_config(config).get_current_head()
    current = MigrationContext.configure(connection).get_current_revision()
    if current != head:
        raise RuntimeError(f"pending migration: database at {current!r}, code expects {head!r}")
