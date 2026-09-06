# Phase 2 — Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Phase 1 domain repository port gets a real PostgreSQL implementation — Alembic schema, SQLAlchemy 2 ORM rows, explicit mappers, a `UnitOfWork`, and a rich dev-only seed (tenants, policies, ICD-10 catalogue) loadable with `ecet seed`.

**Architecture:** SQLAlchemy 2.x async + asyncpg, one `AsyncSession` per unit of work. Mapped classes in `orm.py` are *rows*, never domain models; `mappers.py` holds pure `*_to_row_values()` / `*_from_row()` functions with no session and no I/O, so they are unit-tested without Docker. Repositories are thin: build a statement, run it, hand the row to a mapper. Optimistic concurrency lives in `PostgresClaimRepository`, which remembers the `updated_at` it read and uses it as the `WHERE` guard on `UPDATE` — the domain has no `version` field and the schema has no version column, so the adapter carries it (Phase 1 carry-over #1). Adapter tests run against a real Postgres 16 in testcontainers, marked `slow`.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 (asyncio), asyncpg, Alembic 1.19, PostgreSQL 16, testcontainers 4.15, pytest.

**Spec:** [`specs/06-roadmap.md` §Phase 2](../../specs/06-roadmap.md#phase-2--persistence), which pulls in [postgres](../../specs/03-infrastructure/postgres.md), [claim](../../specs/01-domain/claim.md), [policy](../../specs/01-domain/policy.md), [tenant](../../specs/01-domain/tenant.md), [evaluation](../../specs/01-domain/evaluation.md), [project-layout](../../specs/05-platform/project-layout.md), [config](../../specs/05-platform/config.md), [testing](../../specs/05-platform/testing.md), [docker-compose](../../specs/05-platform/docker-compose.md).

## Global Constraints

- Python **3.12** exactly. Every command runs through uv: `uv run <tool>`.
- Layer rule (`import-linter`, `make imports` must stay green): `ecet.domain` imports stdlib + pydantic only; `ecet.application` imports domain + stdlib + pydantic; **only `ecet.infrastructure` may import `sqlalchemy`, `alembic`, `asyncpg`**. The existing `forbidden` contract already lists them for domain and application — do not weaken it.
- `mypy --strict` covers `src/ecet/domain` and `src/ecet/application`; `mypy src/ecet` (with the repo-wide `disallow_untyped_defs = true`) covers infrastructure. Every function in this phase is annotated.
- Line length 100 (ruff). Lint rules in force: `E, F, I, UP, B, SIM, ASYNC, RUF`. `migrations/` **is** linted by ruff (it is not in `extend-exclude`).
- Domain models never reach SQLAlchemy and SQLAlchemy rows never reach the application: the only crossing point is `mappers.py`.
- `Tenant.webhook_secret` is a `SecretStr`; persistence reads it with `get_secret_value()` (Phase 1 carry-over #2). It is never logged.
- Seed data is **dev-only**: `ecet seed` exits 1 unless `ECET_ENV=dev`.
- Adapter tests live in `tests/adapters/` and are automatically marked `slow` by that directory's `conftest.py`; the default `pytest` run (`-m 'not slow and not e2e'`) must stay green without Docker.
- TDD: every step-pair is "write the failing test" → "watch it fail" → "minimal implementation" → "watch it pass". Commit after every task with a conventional prefix. No Claude attribution in commit messages.

### Explicitly out of scope for Phase 2 (do not add)

- Any use case, any FastAPI route, any AMQP or S3 adapter (Phases 3–4).
- Wiring migrations or the seed into the api container startup (`ECET_AUTO_MIGRATE`) — Phase 3, when the api process gains a database at all. This phase only makes the image capable of it by shipping `alembic.ini` + `migrations/` inside it.
- Application ports other than `UnitOfWork` (`ObjectStorage`, `TextExtractor`, `PiiRedactor`, `EvaluationQueue`, `LLMGateway`, `WebhookClient`, `Clock`) — Phases 3/4.
- Validating extracted ICD-10 codes against the catalogue — Phase 4. This phase only seeds it and exposes `Icd10CodeRepository.known_codes()`.
- `ecet dlq-replay` (Phase 5), `/metrics` (Phase 6).

---

### Task 1: Dependencies, ORM rows, Alembic schema and the testcontainers harness

**Files:**
- Modify: `pyproject.toml`, `.github/workflows/ci.yml`
- Create: `alembic.ini`, `migrations/env.py`, `migrations/script.py.mako`, `migrations/README`, `migrations/versions/0001_initial_schema.py`
- Create: `src/ecet/infrastructure/postgres/__init__.py`, `src/ecet/infrastructure/postgres/orm.py`
- Test: `tests/adapters/conftest.py`, `tests/adapters/test_schema.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `ecet.infrastructure.postgres.orm`: `Base` (`DeclarativeBase`), `TenantRow`, `PolicyRow`, `ClaimRow`, `ReviewTaskRow`, `Icd10CodeRow`.
  - `tests/adapters/conftest.py` fixtures: `postgres_url` (session-scoped, migrated container URL), `session_factory` (function-scoped `async_sessionmaker[AsyncSession]` against a truncated database) — Task 3 adds `session_factory`; Task 1 ships `postgres_url` only.

- [ ] **Step 1: Add the dependencies**

```bash
cd /Users/harielgiacomuzzi/Development/FDE-Study/HealthcareTriageEngine
uv add 'sqlalchemy[asyncio]>=2.0.52,<3' 'asyncpg>=0.31,<0.32' 'alembic>=1.19,<2'
uv add --dev 'testcontainers[postgres]>=4.15,<5'
```

`sqlalchemy[asyncio]` pulls `greenlet`; do not add it by hand. `testcontainers[postgres]` needs no sync driver — its readiness probe runs `psql` inside the container.

- [ ] **Step 2: Scaffold Alembic (async template)**

```bash
uv run alembic init -t async migrations
```

This writes `alembic.ini`, `migrations/env.py`, `migrations/script.py.mako`, `migrations/README`, `migrations/versions/`. Keep the generated `script.py.mako` and `README` as they are.

In `alembic.ini`, set the script location and blank the URL (the environment supplies it):

```ini
[alembic]
script_location = migrations
prepend_sys_path = src
sqlalchemy.url =
```

Leave the rest of the generated file untouched.

- [ ] **Step 3: Replace `migrations/env.py`**

```python
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
```

- [ ] **Step 4: Write the failing test**

`tests/adapters/conftest.py`:

```python
"""Adapter tests: one real Postgres 16 in Docker per session, migrations applied once.

Everything under `tests/adapters/` is marked `slow`, so the default
`pytest -m 'not slow and not e2e'` run stays Docker-free.
"""

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from testcontainers.community.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[2]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
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
            env={"PATH": "/usr/bin:/bin", "ECET_DATABASE_URL": url},
            check=True,
        )
        yield url
```

`tests/adapters/test_schema.py`:

```python
"""The migration and the ORM metadata must describe the same schema."""

from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from ecet.infrastructure.postgres.orm import Base

EXPECTED_TABLES = {"alembic_version", "claims", "icd10_codes", "policies", "review_tasks", "tenants"}


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
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `uv run pytest tests/adapters -m slow -v`
Expected: collection error — `ModuleNotFoundError: No module named 'ecet.infrastructure.postgres'`.

- [ ] **Step 6: Write `src/ecet/infrastructure/postgres/__init__.py`**

```python
"""PostgreSQL adapters: ORM rows, mappers, repositories, unit of work, dev seed."""
```

- [ ] **Step 7: Write `src/ecet/infrastructure/postgres/orm.py`**

```python
"""SQLAlchemy row classes. These are *rows*, never domain models — `mappers.py` is
the only place the two meet.
"""

from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TenantRow(Base):
    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    webhook_secret: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))


class Icd10CodeRow(Base):
    """Reference catalogue: what a real ICD-10 code looks like, seeded per environment."""

    __tablename__ = "icd10_codes"

    code: Mapped[str] = mapped_column(Text, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)


class PolicyRow(Base):
    __tablename__ = "policies"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", "version", name="uq_policies_tenant_name_version"),
        Index("ix_policies_tenant_active", "tenant_id", "active"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Text, ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    covered_codes: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    excluded_codes: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    criteria_text: Mapped[str] = mapped_column(Text, nullable=False)
    required_evidence: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)


class ClaimRow(Base):
    __tablename__ = "claims"
    __table_args__ = (
        # ADR-006 idempotency.
        UniqueConstraint("bucket", "key", "etag", name="uq_claims_source_object"),
        Index("ix_claims_tenant_status", "tenant_id", "status"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Text, ForeignKey("tenants.id"), nullable=False)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[str] = mapped_column(Text, nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    redacted_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_counts: Mapped[dict[str, int] | None] = mapped_column(JSONB, nullable=True)
    redactor: Mapped[str | None] = mapped_column(Text, nullable=True)
    policy_ids: Mapped[list[UUID]] = mapped_column(ARRAY(Uuid), nullable=False)
    deterministic: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    evaluation: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    notification_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_notify_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReviewTaskRow(Base):
    __tablename__ = "review_tasks"
    __table_args__ = (Index("ix_review_tasks_tenant_status", "tenant_id", "status"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    # Unique: UC-09a must return the existing OPEN task instead of creating a second one.
    claim_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("claims.id"), nullable=False, unique=True
    )
    tenant_id: Mapped[str] = mapped_column(Text, ForeignKey("tenants.id"), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    resolution: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewer: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
```

- [ ] **Step 8: Write `migrations/versions/0001_initial_schema.py`**

```python
"""initial schema

Revision ID: 0001_initial
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("webhook_secret", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "icd10_codes",
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("code"),
    )
    op.create_table(
        "policies",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("covered_codes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("excluded_codes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("criteria_text", sa.Text(), nullable=False),
        sa.Column("required_evidence", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "name", "version", name="uq_policies_tenant_name_version"),
    )
    op.create_index("ix_policies_tenant_active", "policies", ["tenant_id", "active"])
    op.create_table(
        "claims",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("bucket", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("redacted_text", sa.Text(), nullable=True),
        sa.Column("entity_counts", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("redactor", sa.Text(), nullable=True),
        sa.Column("policy_ids", postgresql.ARRAY(sa.Uuid()), nullable=False),
        sa.Column("deterministic", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("evaluation", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column(
            "notification_attempts", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("last_notify_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("bucket", "key", "etag", name="uq_claims_source_object"),
    )
    op.create_index("ix_claims_tenant_status", "claims", ["tenant_id", "status"])
    op.create_table(
        "review_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("claim_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("reviewer", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["claim_id"], ["claims.id"]),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("claim_id"),
    )
    op.create_index("ix_review_tasks_tenant_status", "review_tasks", ["tenant_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_review_tasks_tenant_status", table_name="review_tasks")
    op.drop_table("review_tasks")
    op.drop_index("ix_claims_tenant_status", table_name="claims")
    op.drop_table("claims")
    op.drop_index("ix_policies_tenant_active", table_name="policies")
    op.drop_table("policies")
    op.drop_table("icd10_codes")
    op.drop_table("tenants")
```

- [ ] **Step 9: Run the test to verify it passes**

Run: `uv run pytest tests/adapters -m slow -v`
Expected: PASS, both tests. Docker must be running.

If `test_migration_matches_the_orm_metadata` reports a diff, the diff names the exact column or constraint that disagrees — **fix the migration to match `orm.py`**, never the reverse: `orm.py` is what the repositories query through.

- [ ] **Step 10: Add the CI `slow` job**

In `.github/workflows/ci.yml`, after the existing `check` job, add:

```yaml
  slow:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - uses: astral-sh/setup-uv@v10.0.1
        with:
          enable-cache: true

      - name: Install Python 3.12
        run: uv python install 3.12

      - name: Sync dependencies
        run: uv sync --frozen

      - name: Adapter tests (testcontainers)
        run: uv run pytest -m slow
```

`-m slow` on the command line overrides the `-m 'not slow and not e2e'` in `addopts`. The ubuntu runner already has Docker.

- [ ] **Step 11: Run the full gate and commit**

```bash
make check
git add pyproject.toml uv.lock alembic.ini migrations tests/adapters \
        src/ecet/infrastructure/postgres .github/workflows/ci.yml
git commit -m "feat(postgres): ORM rows, initial Alembic migration and the adapter test harness"
```

---

### Task 2: Mappers (domain ↔ row, no database)

**Files:**
- Create: `src/ecet/infrastructure/postgres/mappers.py`
- Test: `tests/unit/test_mappers.py`

**Interfaces:**
- Consumes: `ecet.infrastructure.postgres.orm` (all row classes), the Phase 1 domain models.
- Produces, all pure functions:
  - `tenant_to_row_values(tenant: Tenant) -> dict[str, Any]`, `tenant_from_row(row: TenantRow) -> Tenant`
  - `policy_to_row_values(policy: Policy) -> dict[str, Any]`, `policy_from_row(row: PolicyRow) -> Policy`
  - `claim_to_row_values(claim: Claim) -> dict[str, Any]`, `claim_from_row(row: ClaimRow) -> Claim`
  - `review_task_to_row_values(task: ReviewTask) -> dict[str, Any]`, `review_task_from_row(row: ReviewTaskRow) -> ReviewTask`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_mappers.py`:

```python
"""Mappers are pure: every case here runs without Docker and without a session."""

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from pydantic import SecretStr

from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    CheckOutcome,
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
    Verdict,
)
from ecet.domain.ids import ClaimId, PolicyId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import (
    claim_from_row,
    claim_to_row_values,
    policy_from_row,
    policy_to_row_values,
    review_task_from_row,
    review_task_to_row_values,
    tenant_from_row,
    tenant_to_row_values,
)
from ecet.infrastructure.postgres.orm import ClaimRow, PolicyRow, ReviewTaskRow, TenantRow

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
KEY = "tenants/tenant-a/claims/2026-09-06-mri.pdf"


def build_tenant(**overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": "tenant-a",
        "name": "Northwind Health Plan",
        "webhook_url": "http://mock-client:8081/hooks/northwind",
        "webhook_secret": SecretStr("dev-hmac-tenant-a"),
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 2,
        "covered_codes": {Icd10Code(code="M54.5"), Icd10Code(code="M51.26")},
        "excluded_codes": {Icd10Code(code="Z00.00")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "required_evidence": ["conservative therapy >= 6 weeks", "imaging report"],
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


def build_claim(**overrides: Any) -> Claim:
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "source": SourceObject(bucket="claims", key=KEY, etag="etag-1", size=12_345),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


def build_task(**overrides: Any) -> ReviewTask:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "claim_id": ClaimId(uuid4()),
        "tenant_id": "tenant-a",
        "reason": ReviewReason.LOW_CONFIDENCE,
        "created_at": NOW,
    }
    fields.update(overrides)
    return ReviewTask.model_validate(fields)


def test_tenant_round_trips() -> None:
    tenant = build_tenant()
    assert tenant_from_row(TenantRow(**tenant_to_row_values(tenant))) == tenant


def test_the_webhook_secret_is_stored_unmasked() -> None:
    """`model_dump` masks a SecretStr; persistence must use `get_secret_value()`."""
    values = tenant_to_row_values(build_tenant())
    assert values["webhook_secret"] == "dev-hmac-tenant-a"


def test_policy_round_trips_including_code_sets() -> None:
    policy = build_policy()
    restored = policy_from_row(PolicyRow(**policy_to_row_values(policy)))
    assert restored == policy


def test_policy_code_sets_are_stored_sorted_for_stable_diffs() -> None:
    values = policy_to_row_values(build_policy())
    assert values["covered_codes"] == ["M51.26", "M54.5"]


def test_a_bare_claim_round_trips() -> None:
    claim = build_claim()
    assert claim_from_row(ClaimRow(**claim_to_row_values(claim))) == claim


def test_a_fully_populated_claim_round_trips() -> None:
    policy_id = PolicyId(uuid4())
    claim = build_claim(
        status=ClaimStatus.EVALUATED,
        redacted=RedactedText(text="note text", entity_counts={"PERSON": 3}, redactor="presidio"),
        policy_ids=[policy_id],
        deterministic=DeterministicResult(
            verdict=Verdict.PASS,
            checks=[CheckOutcome(name="non_empty_text", passed=True, detail="120 characters")],
        ),
        evaluation=Evaluation(
            decision=Decision.MEETS_NECESSITY,
            confidence=0.91,
            matched_policy_id=policy_id,
            cited_codes=[Icd10Code(code="M54.5")],
            rationale="Documented eight weeks of conservative therapy.",
            model="claude-sonnet-5",
            prompt_version="v1",
            latency_ms=1200,
            input_tokens=900,
            output_tokens=120,
        ),
        notification_attempts=2,
        last_notify_error="connect timeout",
    )
    assert claim_from_row(ClaimRow(**claim_to_row_values(claim))) == claim


def test_an_empty_redacted_note_is_not_confused_with_no_redaction() -> None:
    """`redacted_text` may legitimately be ''; `redactor` is the None marker."""
    claim = build_claim(redacted=RedactedText(text="", entity_counts={}, redactor="presidio"))
    restored = claim_from_row(ClaimRow(**claim_to_row_values(claim)))
    assert restored.redacted is not None
    assert restored.redacted.text == ""


def test_review_task_round_trips_open_and_resolved() -> None:
    task = build_task()
    assert review_task_from_row(ReviewTaskRow(**review_task_to_row_values(task))) == task

    task.resolve(
        resolution=Decision.DOES_NOT_MEET,
        reviewer="nurse@tenant-a.example",
        notes="No conservative therapy documented.",
        now=NOW,
    )
    restored = review_task_from_row(ReviewTaskRow(**review_task_to_row_values(task)))
    assert restored == task
    assert restored.status is ReviewStatus.RESOLVED
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_mappers.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.infrastructure.postgres.mappers'`.

- [ ] **Step 3: Write `src/ecet/infrastructure/postgres/mappers.py`**

```python
"""The one place domain models and SQLAlchemy rows meet.

Pure functions, no session, no I/O — so the whole mapping layer is unit-tested
without Docker. `*_to_row_values` returns a plain dict so the repositories can feed
it to `insert()` / `update()` as well as to a row constructor.
"""

from typing import Any

from ecet.domain.claim import Claim, ClaimStatus, RedactedText, SourceObject
from ecet.domain.evaluation import (
    Decision,
    DeterministicResult,
    Evaluation,
    ReviewReason,
    ReviewStatus,
    ReviewTask,
)
from ecet.domain.ids import ClaimId, PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.orm import ClaimRow, PolicyRow, ReviewTaskRow, TenantRow


def tenant_to_row_values(tenant: Tenant) -> dict[str, Any]:
    return {
        "id": str(tenant.id),
        "name": tenant.name,
        "webhook_url": str(tenant.webhook_url),
        # `model_dump` would write "**********" here.
        "webhook_secret": tenant.webhook_secret.get_secret_value(),
        "active": tenant.active,
    }


def tenant_from_row(row: TenantRow) -> Tenant:
    return Tenant(
        id=TenantId(row.id),
        name=row.name,
        webhook_url=row.webhook_url,  # type: ignore[arg-type]  # pydantic parses the str
        webhook_secret=row.webhook_secret,  # type: ignore[arg-type]  # pydantic wraps it
        active=row.active,
    )


def policy_to_row_values(policy: Policy) -> dict[str, Any]:
    return {
        "id": policy.id,
        "tenant_id": str(policy.tenant_id),
        "name": policy.name,
        "version": policy.version,
        # Sorted so a row diff means a real change, not set iteration order.
        "covered_codes": sorted(code.code for code in policy.covered_codes),
        "excluded_codes": sorted(code.code for code in policy.excluded_codes),
        "criteria_text": policy.criteria_text,
        "required_evidence": list(policy.required_evidence),
        "active": policy.active,
        "effective_from": policy.effective_from,
        "effective_to": policy.effective_to,
    }


def policy_from_row(row: PolicyRow) -> Policy:
    return Policy(
        id=PolicyId(row.id),
        tenant_id=TenantId(row.tenant_id),
        name=row.name,
        version=row.version,
        covered_codes={Icd10Code(code=code) for code in row.covered_codes},
        excluded_codes={Icd10Code(code=code) for code in row.excluded_codes},
        criteria_text=row.criteria_text,
        required_evidence=list(row.required_evidence),
        active=row.active,
        effective_from=row.effective_from,
        effective_to=row.effective_to,
    )


def claim_to_row_values(claim: Claim) -> dict[str, Any]:
    redacted = claim.redacted
    return {
        "id": claim.id,
        "tenant_id": str(claim.tenant_id),
        "bucket": claim.source.bucket,
        "key": claim.source.key,
        "etag": claim.source.etag,
        "size": claim.source.size,
        "status": claim.status.value,
        "redacted_text": None if redacted is None else redacted.text,
        "entity_counts": None if redacted is None else dict(redacted.entity_counts),
        "redactor": None if redacted is None else redacted.redactor,
        "policy_ids": list(claim.policy_ids),
        "deterministic": (
            None if claim.deterministic is None else claim.deterministic.model_dump(mode="json")
        ),
        "evaluation": (
            None if claim.evaluation is None else claim.evaluation.model_dump(mode="json")
        ),
        "failure_reason": claim.failure_reason,
        "notification_attempts": claim.notification_attempts,
        "last_notify_error": claim.last_notify_error,
        "created_at": claim.created_at,
        "updated_at": claim.updated_at,
    }


def claim_from_row(row: ClaimRow) -> Claim:
    # `redactor` is the marker, not `redacted_text`: an all-PII note redacts to "".
    redacted = (
        None
        if row.redactor is None
        else RedactedText(
            text=row.redacted_text or "",
            entity_counts=row.entity_counts or {},
            redactor=row.redactor,
        )
    )
    return Claim(
        id=ClaimId(row.id),
        tenant_id=TenantId(row.tenant_id),
        source=SourceObject(bucket=row.bucket, key=row.key, etag=row.etag, size=row.size),
        status=ClaimStatus(row.status),
        redacted=redacted,
        policy_ids=[PolicyId(policy_id) for policy_id in row.policy_ids],
        deterministic=(
            None
            if row.deterministic is None
            else DeterministicResult.model_validate(row.deterministic)
        ),
        evaluation=(
            None if row.evaluation is None else Evaluation.model_validate(row.evaluation)
        ),
        failure_reason=row.failure_reason,
        notification_attempts=row.notification_attempts,
        last_notify_error=row.last_notify_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def review_task_to_row_values(task: ReviewTask) -> dict[str, Any]:
    return {
        "id": task.id,
        "claim_id": task.claim_id,
        "tenant_id": str(task.tenant_id),
        "reason": task.reason.value,
        "status": task.status.value,
        "resolution": None if task.resolution is None else task.resolution.value,
        "reviewer": task.reviewer,
        "notes": task.notes,
        "created_at": task.created_at,
        "resolved_at": task.resolved_at,
    }


def review_task_from_row(row: ReviewTaskRow) -> ReviewTask:
    return ReviewTask(
        id=row.id,
        claim_id=ClaimId(row.claim_id),
        tenant_id=TenantId(row.tenant_id),
        reason=ReviewReason(row.reason),
        status=ReviewStatus(row.status),
        resolution=None if row.resolution is None else Decision(row.resolution),  # type: ignore[arg-type]
        reviewer=row.reviewer,
        notes=row.notes,
        created_at=row.created_at,
        resolved_at=row.resolved_at,
    )
```

The `resolution` ignore is real: the column is free text, so only the runtime
`Decision(...)` narrows it; `ReviewTask` re-validates it against `HumanResolution`
on assignment, which is what actually protects the invariant.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_mappers.py -v`
Expected: PASS.

- [ ] **Step 5: Run the gate and commit**

```bash
make check
git add src/ecet/infrastructure/postgres/mappers.py tests/unit/test_mappers.py
git commit -m "feat(postgres): pure domain-to-row mappers"
```

---
### Task 3: Session factory, UnitOfWork and the Tenant repository

**Files:**
- Create: `src/ecet/infrastructure/postgres/session.py`, `src/ecet/infrastructure/postgres/repositories.py`, `src/ecet/infrastructure/postgres/unit_of_work.py`, `src/ecet/application/ports/unit_of_work.py`
- Modify: `tests/adapters/conftest.py` (add the `session_factory` fixture), `tests/fakes.py` (add `FakeUnitOfWork`)
- Create: `tests/adapters/helpers.py`
- Test: `tests/adapters/test_tenant_repository.py`, `tests/adapters/test_unit_of_work.py`

**Interfaces:**
- Consumes: Task 1 `orm`, Task 2 `mappers`.
- Produces:
  - `ecet.infrastructure.postgres.session`: `create_engine(database_url: str) -> AsyncEngine`, `create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]`.
  - `ecet.infrastructure.postgres.repositories`: `PostgresTenantRepository(session: AsyncSession)`. Tasks 4–6 add `PostgresPolicyRepository`, `PostgresIcd10CodeRepository`, `PostgresClaimRepository`, `PostgresReviewTaskRepository` to the same module.
  - `ecet.infrastructure.postgres.unit_of_work`: `SqlAlchemyUnitOfWork(session_factory)`, attributes `claims`, `policies`, `tenants`, `review_tasks`, `icd10_codes`; `async with`; `commit()`.
  - `ecet.application.ports.unit_of_work`: `UnitOfWork` Protocol.
  - `tests.fakes`: `FakeUnitOfWork`.
  - `tests/adapters/conftest.py`: the `session_factory` fixture.
  - `tests/adapters/helpers.py`: `NOW`, `LATER`, `build_tenant(tenant_id, **overrides)`, `build_claim(**overrides)`, `insert_tenant`, `insert_policy`, `insert_claim`.

> Tasks 4–6 each add one repository class to `repositories.py` and wire it into
> `SqlAlchemyUnitOfWork`. To keep this task's unit of work constructible before those
> classes exist, it is written **once, complete**, in Step 5 below, and Tasks 4–6 only
> replace the placeholder repository imports as they land. Read Step 5 before starting
> Task 4.

- [ ] **Step 1: Write the failing tests**

`tests/adapters/test_tenant_repository.py`:

```python
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.errors import TenantNotFound
from ecet.domain.ids import TenantId
from ecet.infrastructure.postgres.orm import TenantRow
from ecet.infrastructure.postgres.repositories import PostgresTenantRepository

from tests.adapters.helpers import build_tenant, insert_tenant


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
```

`tests/adapters/test_unit_of_work.py`:

```python
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.domain.ids import TenantId
from ecet.infrastructure.postgres.orm import TenantRow
from ecet.infrastructure.postgres.unit_of_work import SqlAlchemyUnitOfWork

from tests.adapters.helpers import build_tenant


async def _count_tenants(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(TenantRow)) or 0


async def test_the_unit_of_work_satisfies_the_port(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert isinstance(SqlAlchemyUnitOfWork(session_factory), UnitOfWork)


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
```

- [ ] **Step 2: Extend `tests/adapters/conftest.py`**

Append to the file written in Task 1:

```python
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.infrastructure.postgres.session import create_engine, create_session_factory

TABLES = "claims, icd10_codes, policies, review_tasks, tenants"


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


```

`tests/adapters/helpers.py` — builders and inserts, kept out of `conftest.py` because a
test module must never import another module's `conftest`:

```python
"""Shared builders and raw inserts for the adapter tests."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from pydantic import SecretStr
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ecet.domain.claim import Claim, SourceObject
from ecet.domain.ids import ClaimId, TenantId
from ecet.domain.policy import Policy
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import (
    claim_to_row_values,
    policy_to_row_values,
    tenant_to_row_values,
)
from ecet.infrastructure.postgres.orm import ClaimRow, PolicyRow, TenantRow

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)


def build_tenant(tenant_id: str, **overrides: Any) -> Tenant:
    fields: dict[str, Any] = {
        "id": TenantId(tenant_id),
        "name": tenant_id,
        "webhook_url": f"http://mock-client:8081/hooks/{tenant_id}",
        "webhook_secret": SecretStr(f"dev-hmac-{tenant_id}"),
    }
    fields.update(overrides)
    return Tenant.model_validate(fields)


def build_claim(**overrides: Any) -> Claim:
    """A `RECEIVED` claim for `tenant-a`; `suffix` keeps keys and etags unique."""
    suffix = overrides.pop("suffix", "a")
    tenant_id = overrides.get("tenant_id", "tenant-a")
    fields: dict[str, Any] = {
        "id": ClaimId(uuid4()),
        "tenant_id": tenant_id,
        "source": SourceObject(
            bucket="claims",
            key=f"tenants/{tenant_id}/claims/note-{suffix}.pdf",
            etag=f"etag-{suffix}",
            size=12_345,
        ),
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return Claim.model_validate(fields)


async def insert_tenant(session: AsyncSession, tenant: Tenant) -> None:
    await session.execute(insert(TenantRow).values(**tenant_to_row_values(tenant)))


async def insert_policy(session: AsyncSession, policy: Policy) -> None:
    await session.execute(insert(PolicyRow).values(**policy_to_row_values(policy)))


async def insert_claim(session: AsyncSession, claim: Claim) -> None:
    await session.execute(insert(ClaimRow).values(**claim_to_row_values(claim)))
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/adapters -m slow -v`
Expected: `ModuleNotFoundError: No module named 'ecet.infrastructure.postgres.session'`.

- [ ] **Step 4: Write `src/ecet/infrastructure/postgres/session.py`**

```python
"""Engine and session factory. Built once per process and handed to the unit of work."""

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def create_engine(database_url: str) -> AsyncEngine:
    """`database_url` is `ECET_DATABASE_URL` — the caller unwraps the `SecretStr`."""
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: repositories return domain models, so nothing needs a
    # post-commit refresh, and an expired attribute would trigger lazy IO.
    return async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 5: Write `src/ecet/infrastructure/postgres/repositories.py` (tenant only for now)**

```python
"""Postgres implementations of the domain repository ports.

Each repository is thin: build a statement, run it, hand the row to a mapper. All
five share one `AsyncSession`, owned by the unit of work.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from ecet.domain.errors import TenantNotFound
from ecet.domain.ids import TenantId
from ecet.domain.tenant import Tenant
from ecet.infrastructure.postgres.mappers import tenant_from_row
from ecet.infrastructure.postgres.orm import TenantRow


class PostgresTenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, tenant_id: TenantId) -> Tenant:
        row = await self._session.get(TenantRow, str(tenant_id))
        if row is None or not row.active:
            raise TenantNotFound(str(tenant_id))
        return tenant_from_row(row)
```

- [ ] **Step 6: Write `src/ecet/application/ports/unit_of_work.py`**

```python
"""Transaction boundary. One unit of work per HTTP request / per queue message.

Lives in `application/ports` because use cases depend on it; the SQLAlchemy
implementation lives in `infrastructure/postgres`.
"""

from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from ecet.domain.ports.claim_repository import ClaimRepository
from ecet.domain.ports.icd10_repository import Icd10CodeRepository
from ecet.domain.ports.policy_repository import PolicyRepository
from ecet.domain.ports.review_task_repository import ReviewTaskRepository
from ecet.domain.ports.tenant_repository import TenantRepository


@runtime_checkable
class UnitOfWork(Protocol):
    claims: ClaimRepository
    policies: PolicyRepository
    tenants: TenantRepository
    review_tasks: ReviewTaskRepository
    icd10_codes: Icd10CodeRepository

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Rolls back anything not committed, then closes the session."""
        ...

    async def commit(self) -> None: ...
```

`ecet.domain.ports.icd10_repository` arrives in Task 4. Until then this import fails —
so **write Task 4's port module now** (it is four lines) or run Tasks 3 and 4 back to
back. The plan orders them this way because the unit of work is the thing every later
task hangs off; splitting the port out would leave a half-typed Protocol.

- [ ] **Step 7: Write `src/ecet/infrastructure/postgres/unit_of_work.py`**

```python
"""SQLAlchemy unit of work: one session, five repositories, one transaction."""

from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.infrastructure.postgres.repositories import (
    PostgresClaimRepository,
    PostgresIcd10CodeRepository,
    PostgresPolicyRepository,
    PostgresReviewTaskRepository,
    PostgresTenantRepository,
)


class SqlAlchemyUnitOfWork:
    """`AsyncSession` construction is lazy — no connection is taken until the first
    statement — so building the repositories in `__init__` costs nothing and keeps
    every attribute non-optional for the type checker."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session: AsyncSession = session_factory()
        self.claims = PostgresClaimRepository(self.session)
        self.policies = PostgresPolicyRepository(self.session)
        self.tenants = PostgresTenantRepository(self.session)
        self.review_tasks = PostgresReviewTaskRepository(self.session)
        self.icd10_codes = PostgresIcd10CodeRepository(self.session)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # A rollback after a commit is a no-op, so "always roll back" is both the
        # safe default and the whole implementation.
        try:
            await self.session.rollback()
        finally:
            await self.session.close()

    async def commit(self) -> None:
        await self.session.commit()
```

Tasks 4–6 supply the four repository classes this imports. Until they exist the
module will not import — that is intentional: write Task 4's and Task 5's and Task 6's
classes as they come, and re-run this task's tests at the end of Task 6.

To keep Task 3 independently green, temporarily comment out the four not-yet-written
repositories in both the imports and `__init__`, and uncomment each as its task lands.
`tests/adapters/test_unit_of_work.py` only exercises `tenants`.

- [ ] **Step 8: Add `FakeUnitOfWork` to `tests/fakes.py`**

Append:

```python
class FakeUnitOfWork:
    """In-memory unit of work. `commit()` records the call; the fakes never roll back,
    because nothing they hold is transactional."""

    def __init__(
        self,
        *,
        tenants: Iterable[Tenant] = (),
        policies: Iterable[Policy] = (),
        known_codes: Iterable[Icd10Code] = (),
    ) -> None:
        self.claims = FakeClaimRepository()
        self.policies = FakePolicyRepository(policies)
        self.tenants = FakeTenantRepository(tenants)
        self.review_tasks = FakeReviewTaskRepository()
        self.icd10_codes = FakeIcd10CodeRepository(known_codes)
        self.commits = 0

    async def __aenter__(self) -> "FakeUnitOfWork":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        self.commits += 1
```

with these added to the existing imports at the top of `tests/fakes.py`:

```python
from types import TracebackType

from ecet.domain.policy import Icd10Code, Policy
```

(`FakeIcd10CodeRepository` lands in Task 4; add `FakeUnitOfWork` in the same commit as
that fake if you are running the tasks strictly in order.)

- [ ] **Step 9: Run the tests to verify they pass**

Run: `uv run pytest tests/adapters -m slow -v`
Expected: PASS — schema tests, tenant repository tests, unit-of-work tests.

- [ ] **Step 10: Run the gate and commit**

```bash
make check
git add src/ecet/infrastructure/postgres src/ecet/application/ports/unit_of_work.py \
        tests/adapters tests/fakes.py
git commit -m "feat(postgres): session factory, unit of work and the tenant repository"
```

---

### Task 4: Policy repository and the ICD-10 catalogue

**Files:**
- Create: `src/ecet/domain/ports/icd10_repository.py`
- Modify: `src/ecet/infrastructure/postgres/repositories.py`, `tests/fakes.py`
- Test: `tests/adapters/test_policy_repository.py`, `tests/unit/domain/test_ports.py` (extend)

**Interfaces:**
- Consumes: Task 2 `policy_from_row`, Task 3 `session_factory` fixture and `insert_policy` / `insert_tenant` helpers.
- Produces:
  - `ecet.domain.ports.icd10_repository`: `Icd10CodeRepository` Protocol with `async def known_codes(self) -> frozenset[Icd10Code]`.
  - `ecet.infrastructure.postgres.repositories`: `PostgresPolicyRepository`, `PostgresIcd10CodeRepository`.
  - `tests.fakes`: `FakeIcd10CodeRepository`.

- [ ] **Step 1: Write the failing test**

`tests/adapters/test_policy_repository.py`:

```python
from datetime import date
from typing import Any
from uuid import uuid4

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.ids import PolicyId, TenantId
from ecet.domain.policy import Icd10Code, Policy
from ecet.infrastructure.postgres.orm import Icd10CodeRow
from ecet.infrastructure.postgres.repositories import (
    PostgresIcd10CodeRepository,
    PostgresPolicyRepository,
)

from tests.adapters.helpers import build_tenant, insert_policy, insert_tenant

ON = date(2026, 6, 1)


def build_policy(**overrides: Any) -> Policy:
    fields: dict[str, Any] = {
        "id": PolicyId(uuid4()),
        "tenant_id": "tenant-a",
        "name": "MRI lumbar spine",
        "version": 1,
        "covered_codes": {Icd10Code(code="M54.5")},
        "excluded_codes": {Icd10Code(code="Z00.00")},
        "criteria_text": "Conservative therapy for at least six weeks.",
        "required_evidence": ["imaging report"],
        "effective_from": date(2026, 1, 1),
    }
    fields.update(overrides)
    return Policy.model_validate(fields)


async def _seed(session: AsyncSession, *policies: Policy) -> None:
    for tenant_id in {policy.tenant_id for policy in policies}:
        await insert_tenant(session, build_tenant(str(tenant_id)))
    for policy in policies:
        await insert_policy(session, policy)
    await session.commit()


async def test_only_the_highest_version_of_a_name_is_returned(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    v1 = build_policy(version=1)
    v2 = build_policy(version=2, covered_codes={Icd10Code(code="M54.4")})
    async with session_factory() as session:
        await _seed(session, v1, v2)
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-a"), on=ON
        )
    assert found == [v2]


async def test_inactive_expired_and_future_policies_are_filtered_out(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    current = build_policy(name="MRI lumbar spine")
    inactive = build_policy(name="Knee arthroscopy", active=False)
    expired = build_policy(name="Genetic panel", effective_to=date(2026, 3, 31))
    future = build_policy(name="Insulin pump", effective_from=date(2026, 9, 1))
    async with session_factory() as session:
        await _seed(session, current, inactive, expired, future)
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-a"), on=ON
        )
    assert found == [current]


async def test_the_effective_window_is_inclusive_on_both_ends(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    policy = build_policy(effective_from=date(2026, 6, 1), effective_to=date(2026, 6, 1))
    async with session_factory() as session:
        await _seed(session, policy)
        repository = PostgresPolicyRepository(session)
        assert await repository.active_for_tenant(TenantId("tenant-a"), on=ON) == [policy]
        assert await repository.active_for_tenant(TenantId("tenant-a"), on=date(2026, 6, 2)) == []


async def test_policies_are_scoped_to_their_tenant(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    mine = build_policy(tenant_id="tenant-a")
    theirs = build_policy(tenant_id="tenant-b")
    async with session_factory() as session:
        await _seed(session, mine, theirs)
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-a"), on=ON
        )
    assert found == [mine]


async def test_a_tenant_with_no_policies_gets_an_empty_list(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-empty"))
        await session.commit()
        found = await PostgresPolicyRepository(session).active_for_tenant(
            TenantId("tenant-empty"), on=ON
        )
    assert found == []


async def test_get_many_returns_only_the_requested_ids(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    wanted = build_policy(name="MRI lumbar spine")
    other = build_policy(name="Sleep study")
    async with session_factory() as session:
        await _seed(session, wanted, other)
        repository = PostgresPolicyRepository(session)
        assert await repository.get_many([wanted.id]) == [wanted]
        assert await repository.get_many([]) == []


async def test_the_icd10_catalogue_is_read_as_a_set_of_codes(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await session.execute(
            insert(Icd10CodeRow),
            [
                {"code": "M54.5", "description": "Low back pain"},
                {"code": "G47.33", "description": "Obstructive sleep apnea"},
            ],
        )
        await session.commit()
        codes = await PostgresIcd10CodeRepository(session).known_codes()
    assert codes == {Icd10Code(code="M54.5"), Icd10Code(code="G47.33")}
```

Extend `tests/unit/domain/test_ports.py` with:

```python
async def test_the_icd10_fake_satisfies_its_port() -> None:
    from ecet.domain.ports.icd10_repository import Icd10CodeRepository

    from tests.fakes import FakeIcd10CodeRepository

    fake = FakeIcd10CodeRepository([Icd10Code(code="M54.5")])
    assert isinstance(fake, Icd10CodeRepository)
    assert await fake.known_codes() == {Icd10Code(code="M54.5")}
```

(`Icd10Code` is already imported at the top of that module; if it is not, add
`from ecet.domain.policy import Icd10Code`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/adapters/test_policy_repository.py tests/unit/domain/test_ports.py -m 'slow or not slow' -v`
Expected: `ImportError: cannot import name 'PostgresPolicyRepository'` and
`ModuleNotFoundError: No module named 'ecet.domain.ports.icd10_repository'`.

- [ ] **Step 3: Write `src/ecet/domain/ports/icd10_repository.py`**

```python
"""ICD-10 catalogue read contract.

Seeded reference data, not tenant data. Phase 4 uses it to reject pattern matches
that are not real codes (`extract_icd10_codes` fires on "B12" in "Vitamin B12").
"""

from typing import Protocol, runtime_checkable

from ecet.domain.policy import Icd10Code


@runtime_checkable
class Icd10CodeRepository(Protocol):
    async def known_codes(self) -> frozenset[Icd10Code]:
        """Every code in the catalogue. Small enough (hundreds of rows) to read whole."""
        ...
```

- [ ] **Step 4: Add both repositories to `repositories.py`**

Add the imports:

```python
from collections.abc import Iterable
from datetime import date

from sqlalchemy import or_, select

from ecet.domain.ids import PolicyId
from ecet.domain.policy import Icd10Code, Policy
from ecet.infrastructure.postgres.mappers import policy_from_row
from ecet.infrastructure.postgres.orm import Icd10CodeRow, PolicyRow
```

and the classes:

```python
class PostgresPolicyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def active_for_tenant(self, tenant_id: TenantId, *, on: date) -> list[Policy]:
        """Active, effective on `on`, highest version per name.

        `DISTINCT ON (name)` with `ORDER BY name, version DESC` is Postgres' one-pass
        way to say "highest version per name"; an empty list is the caller's cue to
        raise `NoPoliciesForTenant` (ADR-005).
        """
        statement = (
            select(PolicyRow)
            .where(
                PolicyRow.tenant_id == str(tenant_id),
                PolicyRow.active.is_(True),
                PolicyRow.effective_from <= on,
                or_(PolicyRow.effective_to.is_(None), PolicyRow.effective_to >= on),
            )
            .order_by(PolicyRow.name, PolicyRow.version.desc())
            .distinct(PolicyRow.name)
        )
        rows = (await self._session.scalars(statement)).all()
        return [policy_from_row(row) for row in rows]

    async def get_many(self, ids: Iterable[PolicyId]) -> list[Policy]:
        wanted = list(ids)
        if not wanted:
            return []
        statement = select(PolicyRow).where(PolicyRow.id.in_(wanted)).order_by(PolicyRow.name)
        rows = (await self._session.scalars(statement)).all()
        return [policy_from_row(row) for row in rows]


class PostgresIcd10CodeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def known_codes(self) -> frozenset[Icd10Code]:
        # ponytail: one full-table read per call; cache it in the caller if a hot path
        # ever calls this per claim.
        codes = (await self._session.scalars(select(Icd10CodeRow.code))).all()
        return frozenset(Icd10Code(code=code) for code in codes)
```

- [ ] **Step 5: Add `FakeIcd10CodeRepository` to `tests/fakes.py`**

```python
class FakeIcd10CodeRepository:
    def __init__(self, codes: Iterable[Icd10Code] = ()) -> None:
        self.codes = frozenset(codes)

    async def known_codes(self) -> frozenset[Icd10Code]:
        return self.codes
```

- [ ] **Step 6: Uncomment the policy and ICD-10 repositories in `unit_of_work.py`**

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/adapters -m slow -v && uv run pytest tests/unit -v`
Expected: PASS.

- [ ] **Step 8: Run the gate and commit**

```bash
make check
git add src/ecet/domain/ports/icd10_repository.py src/ecet/infrastructure/postgres \
        tests/fakes.py tests/adapters/test_policy_repository.py tests/unit/domain/test_ports.py
git commit -m "feat(postgres): policy repository and the ICD-10 catalogue port"
```

---
### Task 5: Claim repository, with optimistic concurrency

**Files:**
- Modify: `src/ecet/infrastructure/postgres/repositories.py`, `src/ecet/infrastructure/postgres/unit_of_work.py`
- Test: `tests/adapters/test_claim_repository.py`

**Interfaces:**
- Consumes: Task 2 `claim_to_row_values` / `claim_from_row`, Task 3 fixtures.
- Produces: `PostgresClaimRepository(session)` implementing `ClaimRepository`: `add`, `get`, `find_by_source`, `save`, `list_by_status(status, *, limit=50)`.

**The concurrency contract this task encodes** (Phase 1 carry-over #1): `Claim` has no
`version` field and `transition()` overwrites `updated_at` in place, so the *adapter*
remembers the `updated_at` it read. `save()` issues
`UPDATE … WHERE id = ? AND updated_at = <the value this repository last saw>`; zero
rows means somebody else wrote the row first → `ConcurrentModification`. A claim that
this repository never loaded cannot be saved — there is no baseline to compare.

- [ ] **Step 1: Write the failing test**

`tests/adapters/test_claim_repository.py`:

```python
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.claim import ClaimStatus, RedactedText
from ecet.domain.errors import ClaimNotFound, ConcurrentModification
from ecet.domain.evaluation import CheckOutcome, DeterministicResult, Verdict
from ecet.domain.ids import ClaimId
from ecet.infrastructure.postgres.repositories import PostgresClaimRepository

from tests.adapters.helpers import LATER, build_claim, build_tenant, insert_tenant


@pytest.fixture
async def seeded_tenants(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await session.commit()


async def test_a_claim_round_trips_with_every_optional_field_set(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim(
        status=ClaimStatus.REDACTED,
        redacted=RedactedText(text="note", entity_counts={"PERSON": 2}, redactor="presidio-2.2.x"),
        deterministic=DeterministicResult(
            verdict=Verdict.UNCERTAIN,
            checks=[CheckOutcome(name="icd10_present", passed=False, detail="no codes found")],
        ),
    )
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(claim)
        await session.commit()

    async with session_factory() as session:
        loaded = await PostgresClaimRepository(session).get(claim.id)
    assert loaded == claim


async def test_an_unknown_claim_raises(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    async with session_factory() as session:
        with pytest.raises(ClaimNotFound):
            await PostgresClaimRepository(session).get(ClaimId(uuid4()))


async def test_find_by_source_is_the_idempotency_lookup(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        await PostgresClaimRepository(session).add(claim)
        await session.commit()

    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        found = await repository.find_by_source("claims", claim.source.key, "etag-a")
        assert found == claim
        assert await repository.find_by_source("claims", claim.source.key, "other-etag") is None


async def test_the_same_object_cannot_be_ingested_twice(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    """ADR-006: the unique (bucket, key, etag) index is the last line of defence."""
    first = build_claim()
    second = build_claim(source=first.source)
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(first)
        with pytest.raises(IntegrityError):
            await repository.add(second)


async def test_saving_persists_the_transition(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(claim)
        claim.transition(ClaimStatus.EXTRACTED, now=LATER)
        await repository.save(claim)
        await session.commit()

    async with session_factory() as session:
        loaded = await PostgresClaimRepository(session).get(claim.id)
    assert loaded.status is ClaimStatus.EXTRACTED
    assert loaded.updated_at == LATER


async def test_a_second_writer_loses_the_optimistic_save(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        await PostgresClaimRepository(session).add(claim)
        await session.commit()

    async with session_factory() as first_session, session_factory() as second_session:
        first_repository = PostgresClaimRepository(first_session)
        second_repository = PostgresClaimRepository(second_session)
        first = await first_repository.get(claim.id)
        stale = await second_repository.get(claim.id)

        first.transition(ClaimStatus.EXTRACTED, now=LATER)
        await first_repository.save(first)
        await first_session.commit()

        stale.transition(ClaimStatus.EXTRACTION_FAILED, reason="encrypted pdf", now=LATER)
        with pytest.raises(ConcurrentModification):
            await second_repository.save(stale)


async def test_saving_a_claim_this_repository_never_loaded_is_refused(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    claim = build_claim()
    async with session_factory() as session:
        await PostgresClaimRepository(session).add(claim)
        await session.commit()

    async with session_factory() as session:
        with pytest.raises(ConcurrentModification, match="not loaded"):
            await PostgresClaimRepository(session).save(claim)


async def test_repeated_saves_from_the_same_repository_keep_working(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    """The baseline must advance on every save, or the second one would lose."""
    claim = build_claim()
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        await repository.add(claim)
        claim.transition(ClaimStatus.EXTRACTED, now=LATER)
        await repository.save(claim)
        claim.transition(ClaimStatus.REDACTED, now=LATER + timedelta(minutes=1))
        await repository.save(claim)
        await session.commit()

    async with session_factory() as session:
        assert (await PostgresClaimRepository(session).get(claim.id)).status is ClaimStatus.REDACTED


async def test_list_by_status_is_filtered_and_limited(
    session_factory: async_sessionmaker[AsyncSession], seeded_tenants: None
) -> None:
    queued = [build_claim(suffix=f"q{index}", status=ClaimStatus.QUEUED) for index in range(3)]
    received = build_claim(suffix="r0")
    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        for claim in [*queued, received]:
            await repository.add(claim)
        await session.commit()

    async with session_factory() as session:
        repository = PostgresClaimRepository(session)
        assert len(await repository.list_by_status(ClaimStatus.QUEUED)) == 3
        assert len(await repository.list_by_status(ClaimStatus.QUEUED, limit=2)) == 2
        assert await repository.list_by_status(ClaimStatus.NO_POLICIES) == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/adapters/test_claim_repository.py -m slow -v`
Expected: `ImportError: cannot import name 'PostgresClaimRepository'`.

- [ ] **Step 3: Add `PostgresClaimRepository` to `repositories.py`**

Extend the imports:

```python
from datetime import datetime
from uuid import UUID

from sqlalchemy import insert, update

from ecet.domain.claim import Claim, ClaimStatus
from ecet.domain.errors import ClaimNotFound, ConcurrentModification
from ecet.domain.ids import ClaimId
from ecet.infrastructure.postgres.mappers import claim_from_row, claim_to_row_values
from ecet.infrastructure.postgres.orm import ClaimRow
```

and add:

```python
class PostgresClaimRepository:
    """`save` is optimistic on `updated_at` (see the postgres spec).

    `Claim` carries no version column, and `transition()` overwrites `updated_at` in
    place, so the pre-mutation value has to be remembered here — one entry per claim
    this repository has seen. The repository lives exactly as long as its unit of
    work, so the map cannot grow unbounded.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._baseline: dict[UUID, datetime] = {}

    def _track(self, claim: Claim) -> Claim:
        self._baseline[claim.id] = claim.updated_at
        return claim

    async def add(self, claim: Claim) -> None:
        await self._session.execute(insert(ClaimRow).values(**claim_to_row_values(claim)))
        self._track(claim)

    async def get(self, claim_id: ClaimId) -> Claim:
        # populate_existing: a core UPDATE does not refresh the identity map, so a
        # second read in the same session would otherwise hand back the stale row.
        row = await self._session.get(ClaimRow, claim_id, populate_existing=True)
        if row is None:
            raise ClaimNotFound(str(claim_id))
        return self._track(claim_from_row(row))

    async def find_by_source(self, bucket: str, key: str, etag: str) -> Claim | None:
        statement = (
            select(ClaimRow)
            .where(ClaimRow.bucket == bucket, ClaimRow.key == key, ClaimRow.etag == etag)
            .execution_options(populate_existing=True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return None if row is None else self._track(claim_from_row(row))

    async def save(self, claim: Claim) -> None:
        baseline = self._baseline.get(claim.id)
        if baseline is None:
            raise ConcurrentModification(
                f"claim {claim.id} was not loaded by this unit of work; re-read it first"
            )
        result = await self._session.execute(
            update(ClaimRow)
            .where(ClaimRow.id == claim.id, ClaimRow.updated_at == baseline)
            .values(**claim_to_row_values(claim))
            .execution_options(synchronize_session=False)
        )
        if result.rowcount == 0:
            raise ConcurrentModification(f"claim {claim.id} changed since it was read")
        self._track(claim)

    async def list_by_status(self, status: ClaimStatus, *, limit: int = 50) -> list[Claim]:
        statement = (
            select(ClaimRow)
            .where(ClaimRow.status == status.value)
            .order_by(ClaimRow.created_at)
            .limit(limit)
            .execution_options(populate_existing=True)
        )
        rows = (await self._session.scalars(statement)).all()
        return [self._track(claim_from_row(row)) for row in rows]
```

- [ ] **Step 4: Uncomment the claim repository in `unit_of_work.py`**

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/adapters/test_claim_repository.py -m slow -v`
Expected: PASS.

If `test_a_second_writer_loses_the_optimistic_save` hangs instead of failing, the two
sessions are fighting over a row lock — check that the first session **commits** before
the second one issues its `UPDATE`.

- [ ] **Step 6: Run the gate and commit**

```bash
make check
git add src/ecet/infrastructure/postgres tests/adapters/test_claim_repository.py
git commit -m "feat(postgres): claim repository with optimistic saves"
```

---

### Task 6: Review task repository

**Files:**
- Modify: `src/ecet/infrastructure/postgres/repositories.py`, `src/ecet/infrastructure/postgres/unit_of_work.py`
- Test: `tests/adapters/test_review_task_repository.py`

**Interfaces:**
- Consumes: Task 2 `review_task_to_row_values` / `review_task_from_row`, Task 5 `PostgresClaimRepository` (a review task needs its claim row to exist — FK).
- Produces: `PostgresReviewTaskRepository(session)` implementing `ReviewTaskRepository`: `add`, `get`, `find_open_by_claim`, `list_open(tenant_id, limit=50)`, `save`.

- [ ] **Step 1: Write the failing test**

`tests/adapters/test_review_task_repository.py`:

```python
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.claim import Claim
from ecet.domain.errors import ReviewTaskNotFound
from ecet.domain.evaluation import Decision, ReviewReason, ReviewStatus, ReviewTask
from ecet.domain.ids import ClaimId, TenantId
from ecet.infrastructure.postgres.repositories import PostgresReviewTaskRepository

from tests.adapters.helpers import (
    NOW,
    build_claim,
    build_tenant,
    insert_claim,
    insert_tenant,
)


def build_task(claim: Claim, **overrides: Any) -> ReviewTask:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "claim_id": claim.id,
        "tenant_id": claim.tenant_id,
        "reason": ReviewReason.LOW_CONFIDENCE,
        "created_at": NOW,
    }
    fields.update(overrides)
    return ReviewTask.model_validate(fields)


@pytest.fixture
async def claim(session_factory: async_sessionmaker[AsyncSession]) -> Claim:
    made = build_claim()
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await insert_claim(session, made)
        await session.commit()
    return made


async def test_a_task_round_trips(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        await PostgresReviewTaskRepository(session).add(task)
        await session.commit()

    async with session_factory() as session:
        assert await PostgresReviewTaskRepository(session).get(task.id) == task


async def test_an_unknown_task_raises(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        with pytest.raises(ReviewTaskNotFound):
            await PostgresReviewTaskRepository(session).get(uuid4())


async def test_find_open_by_claim_is_the_uc09a_idempotency_lookup(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert await repository.find_open_by_claim(claim.id) is None
        await repository.add(task)
        await session.commit()
        assert await repository.find_open_by_claim(claim.id) == task


async def test_a_resolved_task_is_no_longer_open_for_its_claim(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(task)
        task.resolve(resolution=Decision.MEETS_NECESSITY, reviewer="nurse", notes=None, now=NOW)
        await repository.save(task)
        await session.commit()

    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert await repository.find_open_by_claim(claim.id) is None
        stored = await repository.get(task.id)
        assert stored.status is ReviewStatus.RESOLVED
        assert stored.resolution is Decision.MEETS_NECESSITY
        assert stored.resolved_at == NOW


async def test_one_claim_cannot_carry_two_tasks(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(build_task(claim))
        with pytest.raises(IntegrityError):
            await repository.add(build_task(claim))


async def test_list_open_is_tenant_scoped_and_limited(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    mine = [build_claim(suffix=f"m{index}") for index in range(3)]
    theirs = build_claim(suffix="t0", tenant_id="tenant-b")
    async with session_factory() as session:
        await insert_tenant(session, build_tenant("tenant-a"))
        await insert_tenant(session, build_tenant("tenant-b"))
        repository = PostgresReviewTaskRepository(session)
        for made in [*mine, theirs]:
            await insert_claim(session, made)
            await repository.add(build_task(made))
        await session.commit()

    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        assert len(await repository.list_open(TenantId("tenant-a"))) == 3
        assert len(await repository.list_open(TenantId("tenant-a"), 2)) == 2
        assert len(await repository.list_open(TenantId("tenant-b"))) == 1


async def test_a_resolved_task_disappears_from_the_open_list(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(task)
        task.resolve(resolution=Decision.DOES_NOT_MEET, reviewer="nurse", notes=None, now=NOW)
        await repository.save(task)
        await session.commit()
        assert await repository.list_open(TenantId("tenant-a")) == []


async def test_saving_a_task_that_is_not_stored_raises(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    async with session_factory() as session:
        with pytest.raises(ReviewTaskNotFound):
            await PostgresReviewTaskRepository(session).save(build_task(claim))


async def test_claim_id_survives_the_round_trip_as_a_claim_id(
    session_factory: async_sessionmaker[AsyncSession], claim: Claim
) -> None:
    task = build_task(claim)
    async with session_factory() as session:
        repository = PostgresReviewTaskRepository(session)
        await repository.add(task)
        await session.commit()
        assert (await repository.get(task.id)).claim_id == ClaimId(claim.id)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/adapters/test_review_task_repository.py -m slow -v`
Expected: `ImportError: cannot import name 'PostgresReviewTaskRepository'`.

- [ ] **Step 3: Add `PostgresReviewTaskRepository` to `repositories.py`**

Extend the imports:

```python
from ecet.domain.errors import ReviewTaskNotFound
from ecet.domain.evaluation import ReviewStatus, ReviewTask
from ecet.infrastructure.postgres.mappers import review_task_from_row, review_task_to_row_values
from ecet.infrastructure.postgres.orm import ReviewTaskRow
```

and add:

```python
class PostgresReviewTaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, task: ReviewTask) -> None:
        await self._session.execute(insert(ReviewTaskRow).values(**review_task_to_row_values(task)))

    async def get(self, task_id: UUID) -> ReviewTask:
        row = await self._session.get(ReviewTaskRow, task_id, populate_existing=True)
        if row is None:
            raise ReviewTaskNotFound(str(task_id))
        return review_task_from_row(row)

    async def find_open_by_claim(self, claim_id: ClaimId) -> ReviewTask | None:
        """UC-09a idempotency. `review_tasks.claim_id` is unique, so this is at most one row."""
        statement = (
            select(ReviewTaskRow)
            .where(
                ReviewTaskRow.claim_id == claim_id,
                ReviewTaskRow.status == ReviewStatus.OPEN.value,
            )
            .execution_options(populate_existing=True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return None if row is None else review_task_from_row(row)

    async def list_open(self, tenant_id: TenantId, limit: int = 50) -> list[ReviewTask]:
        statement = (
            select(ReviewTaskRow)
            .where(
                ReviewTaskRow.tenant_id == str(tenant_id),
                ReviewTaskRow.status == ReviewStatus.OPEN.value,
            )
            .order_by(ReviewTaskRow.created_at)
            .limit(limit)
            .execution_options(populate_existing=True)
        )
        rows = (await self._session.scalars(statement)).all()
        return [review_task_from_row(row) for row in rows]

    async def save(self, task: ReviewTask) -> None:
        result = await self._session.execute(
            update(ReviewTaskRow)
            .where(ReviewTaskRow.id == task.id)
            .values(**review_task_to_row_values(task))
            .execution_options(synchronize_session=False)
        )
        if result.rowcount == 0:
            raise ReviewTaskNotFound(str(task.id))
```

- [ ] **Step 4: Uncomment the review task repository in `unit_of_work.py`**

Every repository the unit of work names now exists; no commented-out imports may remain.

- [ ] **Step 5: Run the whole adapter suite to verify it passes**

Run: `uv run pytest -m slow -v`
Expected: PASS, every adapter test including `test_unit_of_work.py`.

- [ ] **Step 6: Run the gate and commit**

```bash
make check
git add src/ecet/infrastructure/postgres tests/adapters/test_review_task_repository.py
git commit -m "feat(postgres): review task repository"
```

---
### Task 7: Dev seed data and `ecet seed`

**Files:**
- Create: `src/ecet/infrastructure/postgres/seed/__init__.py`, `src/ecet/infrastructure/postgres/seed/icd10_codes.sql`, `src/ecet/infrastructure/postgres/seed/tenants.sql`, `src/ecet/infrastructure/postgres/seed/policies.sql`
- Modify: `src/ecet/cli.py`, `Makefile`, `Dockerfile`
- Test: `tests/unit/test_seed_sql.py`, `tests/adapters/test_seed.py`, `tests/unit/test_cli.py` (extend)

**Interfaces:**
- Consumes: Task 3 `create_engine`, Task 4 `PostgresPolicyRepository` (the adapter test asserts the seed through it).
- Produces:
  - `ecet.infrastructure.postgres.seed`: `SEED_FILES: tuple[str, ...]`, `split_statements(sql: str) -> list[str]`, `async def load_seed(engine: AsyncEngine) -> int` (returns the statement count).
  - `ecet seed` CLI command, refusing to run unless `ECET_ENV=dev`.
  - `make migrate`, `make seed`.

**Seed contents** — deliberately richer than the three tenants the spec's demo needs, so
the demo and the adapter tests exercise version supersession, expiry, deactivation and
an inactive tenant:

| Tenant | Rows | What it proves |
|---|---|---|
| `tenant-a` (Northwind Health Plan) | 8 policies | 5 effective today; one superseded v1, one expired, one deactivated |
| `tenant-b` (Cascade Mutual Benefits) | 6 policies | 5 effective today; `M54.5` is *excluded* here and *covered* for `tenant-a` |
| `tenant-empty` (Harbor Point Onboarding) | 0 policies | ADR-005 hard failure at ingestion |
| `tenant-legacy` (Meridian Legacy Care), inactive | 1 policy | `TenantNotFound` is about the tenant, not the policy count |

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_seed_sql.py`:

```python
"""The seed loader splits SQL itself; these cases are why that is not a `.split(';')`."""

from ecet.infrastructure.postgres.seed import SEED_FILES, seed_sql, split_statements


def test_plain_statements_are_split() -> None:
    assert split_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]


def test_a_semicolon_inside_a_string_literal_does_not_split() -> None:
    sql = "INSERT INTO t VALUES ('six weeks; then imaging');"
    assert split_statements(sql) == [sql.rstrip(";")]


def test_a_doubled_apostrophe_stays_inside_the_literal() -> None:
    sql = "INSERT INTO t VALUES ('the member''s notes; signed');"
    assert split_statements(sql) == [sql.rstrip(";")]


def test_comment_lines_are_dropped() -> None:
    sql = "-- tenant-a's policies; five of them\nSELECT 1;"
    assert split_statements(sql) == ["SELECT 1"]


def test_a_trailing_statement_without_a_semicolon_still_counts() -> None:
    assert split_statements("SELECT 1") == ["SELECT 1"]


def test_every_seed_file_holds_only_idempotent_inserts() -> None:
    """A seed file is data, never a migration and never a delete."""
    for name in SEED_FILES:
        statements = split_statements(seed_sql(name))
        assert statements, name
        for statement in statements:
            assert statement.upper().startswith("INSERT INTO "), (name, statement[:40])
            assert "ON CONFLICT" in statement.upper(), (name, statement[:40])
```

`tests/adapters/test_seed.py`:

```python
from datetime import date

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecet.domain.ids import TenantId
from ecet.infrastructure.postgres.orm import Icd10CodeRow, PolicyRow, TenantRow
from ecet.infrastructure.postgres.repositories import (
    PostgresIcd10CodeRepository,
    PostgresPolicyRepository,
)
from ecet.infrastructure.postgres.seed import load_seed

TODAY = date(2026, 9, 6)


async def test_the_seed_loads_every_table(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(TenantRow)) == 4
        assert await session.scalar(select(func.count()).select_from(PolicyRow)) == 15
        assert (await session.scalar(select(func.count()).select_from(Icd10CodeRow)) or 0) >= 80


async def test_the_seed_is_rerunnable(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(TenantRow)) == 4


async def test_every_seeded_policy_code_exists_in_the_catalogue(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    """A policy citing a code the catalogue does not know is a typo, every time."""
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        unknown = (
            await session.execute(
                text(
                    "SELECT unnest(covered_codes || excluded_codes) FROM policies "
                    "EXCEPT SELECT code FROM icd10_codes"
                )
            )
        ).all()
    assert unknown == []


async def test_the_seeded_policy_sets_are_what_the_demo_expects(
    session_factory: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    from ecet.infrastructure.postgres.session import create_engine

    engine = create_engine(postgres_url)
    await load_seed(engine)
    await engine.dispose()

    async with session_factory() as session:
        repository = PostgresPolicyRepository(session)
        a = await repository.active_for_tenant(TenantId("tenant-a"), on=TODAY)
        b = await repository.active_for_tenant(TenantId("tenant-b"), on=TODAY)
        empty = await repository.active_for_tenant(TenantId("tenant-empty"), on=TODAY)
        codes = await PostgresIcd10CodeRepository(session).known_codes()

    assert len(a) == 5
    assert len(b) == 5
    assert empty == []
    # Superseded and expired versions never surface.
    assert {policy.version for policy in a if policy.name == "MRI lumbar spine"} == {2}
    assert "Hereditary cancer gene panel" not in {policy.name for policy in a}
    assert len(codes) >= 80
```

Extend `tests/unit/test_cli.py`:

```python
def test_seed_refuses_outside_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ECET_ENV", "prod")
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "t")
    monkeypatch.setenv("ECET_API_KEY", "k")
    result = CliRunner().invoke(app, ["seed"])
    assert result.exit_code == 1
    assert "dev-only" in result.output
```

(match the imports the existing `tests/unit/test_cli.py` already uses for `app` and
`CliRunner`; the autouse `_no_ambient_ecet_env` fixture runs before `monkeypatch.setenv`,
so setting the vars inside the test is enough.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_seed_sql.py -v`
Expected: `ModuleNotFoundError: No module named 'ecet.infrastructure.postgres.seed'`.

- [ ] **Step 3: Write `src/ecet/infrastructure/postgres/seed/__init__.py`**

```python
"""Dev seed data.

SQL files rather than Python objects, because seeding is a data concern and the files
double as a readable description of the demo environment. `ecet seed` refuses to run
outside `ECET_ENV=dev`.

Format rules the splitter relies on: statements end with `;`, string literals use
single quotes with `''` for an embedded apostrophe, and comments occupy a whole line.
"""

from importlib.resources import files

from sqlalchemy.ext.asyncio import AsyncEngine

#: Applied in order — policies reference tenants, and every policy code must already
#: exist in the catalogue.
SEED_FILES = ("icd10_codes.sql", "tenants.sql", "policies.sql")


def seed_sql(name: str) -> str:
    return (files(__package__) / name).read_text(encoding="utf-8")


def split_statements(sql: str) -> list[str]:
    """Split on `;` while respecting single-quoted literals.

    `str.split(";")` breaks the moment a criteria text contains a semicolon — and the
    seeded policies do.
    """
    without_comments = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    statements: list[str] = []
    current: list[str] = []
    in_literal = False
    for character in without_comments:
        if character == "'":
            in_literal = not in_literal
            current.append(character)
        elif character == ";" and not in_literal:
            statements.append("".join(current))
            current = []
        else:
            current.append(character)
    statements.append("".join(current))
    return [statement.strip() for statement in statements if statement.strip()]


async def load_seed(engine: AsyncEngine) -> int:
    """Apply every seed file in one transaction. Returns the statement count."""
    applied = 0
    async with engine.begin() as connection:
        for name in SEED_FILES:
            for statement in split_statements(seed_sql(name)):
                await connection.exec_driver_sql(statement)
                applied += 1
    return applied
```

A doubled `''` inside a literal flips `in_literal` twice, which lands back on "inside" —
that is why the naive toggle is correct here and `test_a_doubled_apostrophe_stays_inside_the_literal`
is in the suite.

- [ ] **Step 4: Write `src/ecet/infrastructure/postgres/seed/icd10_codes.sql`**

```sql
-- ICD-10-CM reference catalogue for the dev environment.
-- Phase 4 validates codes extracted from clinical notes against these rows, which is
-- what stops "Vitamin B12" from reading as a diagnosis. "T12" is a real code, so that
-- particular false positive survives the catalogue.
INSERT INTO icd10_codes (code, description) VALUES
  ('A04.7', 'Enterocolitis due to Clostridioides difficile'),
  ('A41.9', 'Sepsis, unspecified organism'),
  ('B18.2', 'Chronic viral hepatitis C'),
  ('B20', 'Human immunodeficiency virus disease'),
  ('C18.9', 'Malignant neoplasm of colon, unspecified'),
  ('C34.90', 'Malignant neoplasm of unspecified part of unspecified bronchus or lung'),
  ('C50.911', 'Malignant neoplasm of unspecified site of right female breast'),
  ('C61', 'Malignant neoplasm of prostate'),
  ('D50.9', 'Iron deficiency anemia, unspecified'),
  ('D64.9', 'Anemia, unspecified'),
  ('E03.9', 'Hypothyroidism, unspecified'),
  ('E10.9', 'Type 1 diabetes mellitus without complications'),
  ('E11.65', 'Type 2 diabetes mellitus with hyperglycemia'),
  ('E11.9', 'Type 2 diabetes mellitus without complications'),
  ('E66.01', 'Morbid (severe) obesity due to excess calories'),
  ('E66.2', 'Morbid (severe) obesity with alveolar hypoventilation'),
  ('E78.5', 'Hyperlipidemia, unspecified'),
  ('F32.9', 'Major depressive disorder, single episode, unspecified'),
  ('F41.1', 'Generalized anxiety disorder'),
  ('F50.2', 'Bulimia nervosa'),
  ('F51.01', 'Primary insomnia'),
  ('G43.909', 'Migraine, unspecified, not intractable, without status migrainosus'),
  ('G47.30', 'Sleep apnea, unspecified'),
  ('G47.33', 'Obstructive sleep apnea'),
  ('G56.01', 'Carpal tunnel syndrome, right upper limb'),
  ('G89.4', 'Chronic pain syndrome'),
  ('H25.11', 'Age-related nuclear cataract, right eye'),
  ('H66.90', 'Otitis media, unspecified, unspecified ear'),
  ('I10', 'Essential (primary) hypertension'),
  ('I21.4', 'Non-ST elevation (NSTEMI) myocardial infarction'),
  ('I25.10', 'Atherosclerotic heart disease of native coronary artery without angina pectoris'),
  ('I48.91', 'Unspecified atrial fibrillation'),
  ('I50.32', 'Chronic diastolic (congestive) heart failure'),
  ('J01.90', 'Acute sinusitis, unspecified'),
  ('J18.9', 'Pneumonia, unspecified organism'),
  ('J44.1', 'Chronic obstructive pulmonary disease with (acute) exacerbation'),
  ('J45.909', 'Unspecified asthma, uncomplicated'),
  ('J96.11', 'Chronic respiratory failure with hypoxia'),
  ('K21.9', 'Gastro-esophageal reflux disease without esophagitis'),
  ('K57.30', 'Diverticulosis of large intestine without perforation or abscess'),
  ('K80.20', 'Calculus of gallbladder without cholecystitis without obstruction'),
  ('L40.0', 'Psoriasis vulgaris'),
  ('M06.9', 'Rheumatoid arthritis, unspecified'),
  ('M17.11', 'Unilateral primary osteoarthritis, right knee'),
  ('M23.51', 'Chronic instability of knee, right knee'),
  ('M25.511', 'Pain in right shoulder'),
  ('M25.561', 'Pain in right knee'),
  ('M43.16', 'Spondylolisthesis, lumbar region'),
  ('M48.061', 'Spinal stenosis, lumbar region without neurogenic claudication'),
  ('M51.26', 'Other intervertebral disc displacement, lumbar region'),
  ('M54.16', 'Radiculopathy, lumbar region'),
  ('M54.4', 'Lumbago with sciatica'),
  ('M54.5', 'Low back pain'),
  ('M54.50', 'Low back pain, unspecified'),
  ('M75.100', 'Unspecified rotator cuff tear of unspecified shoulder, not traumatic'),
  ('M75.41', 'Impingement syndrome of right shoulder'),
  ('M79.7', 'Fibromyalgia'),
  ('N18.30', 'Chronic kidney disease, stage 3 unspecified'),
  ('N39.0', 'Urinary tract infection, site not specified'),
  ('N40.0', 'Benign prostatic hyperplasia without lower urinary tract symptoms'),
  ('O09.90', 'Supervision of high risk pregnancy, unspecified, unspecified trimester'),
  ('Q21.0', 'Ventricular septal defect'),
  ('R05.1', 'Acute cough'),
  ('R07.9', 'Chest pain, unspecified'),
  ('R10.9', 'Unspecified abdominal pain'),
  ('R42', 'Dizziness and giddiness'),
  ('R51.9', 'Headache, unspecified'),
  ('R53.83', 'Other fatigue'),
  ('R56.9', 'Unspecified convulsions'),
  ('S06.0X0A', 'Concussion without loss of consciousness, initial encounter'),
  ('S72.001A', 'Fracture of unspecified part of neck of right femur, initial encounter'),
  ('S83.511A', 'Sprain of anterior cruciate ligament of right knee, initial encounter'),
  ('T12', 'Fracture of lower leg, level unspecified'),
  ('T81.4XXA', 'Infection following a procedure, initial encounter'),
  ('Z00.00', 'Encounter for general adult medical examination without abnormal findings'),
  ('Z13.6', 'Encounter for screening for cardiovascular disorders'),
  ('Z13.9', 'Encounter for screening, unspecified'),
  ('Z51.11', 'Encounter for antineoplastic chemotherapy'),
  ('Z79.4', 'Long term (current) use of insulin'),
  ('Z80.3', 'Family history of malignant neoplasm of breast'),
  ('Z87.891', 'Personal history of nicotine dependence'),
  ('Z99.81', 'Dependence on supplemental oxygen')
ON CONFLICT (code) DO NOTHING;
```

- [ ] **Step 5: Write `src/ecet/infrastructure/postgres/seed/tenants.sql`**

```sql
-- Dev tenants. Secrets are dev HMAC keys and are worthless outside compose.
INSERT INTO tenants (id, name, webhook_url, webhook_secret, active) VALUES
  ('tenant-a', 'Northwind Health Plan', 'http://mock-client:8081/hooks/northwind', 'dev-hmac-tenant-a', true),
  ('tenant-b', 'Cascade Mutual Benefits', 'http://mock-client:8081/hooks/cascade', 'dev-hmac-tenant-b', true),
  ('tenant-empty', 'Harbor Point Onboarding', 'http://mock-client:8081/hooks/harbor', 'dev-hmac-tenant-empty', true),
  ('tenant-legacy', 'Meridian Legacy Care', 'http://mock-client:8081/hooks/meridian', 'dev-hmac-tenant-legacy', false)
ON CONFLICT (id) DO NOTHING;
```

- [ ] **Step 6: Write `src/ecet/infrastructure/postgres/seed/policies.sql`**

```sql
-- Coverage policies. Fixed UUIDs so reruns are no-ops and demos can cite an id.
-- tenant-a: 8 rows, 5 of them effective today (v1 of the MRI policy is superseded,
-- the gene panel expired, the knee arthroscopy policy was deactivated).
INSERT INTO policies (
  id, tenant_id, name, version, covered_codes, excluded_codes,
  criteria_text, required_evidence, active, effective_from, effective_to
) VALUES
  ('1a000000-0000-4000-8000-000000000001', 'tenant-a', 'MRI lumbar spine', 1,
   '{M54.5,M51.26}', '{Z00.00}',
   'Imaging is covered after documented conservative therapy of at least six weeks.',
   '{"conservative therapy >= 6 weeks","clinical examination note"}',
   true, '2025-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000002', 'tenant-a', 'MRI lumbar spine', 2,
   '{M54.5,M54.4,M54.16,M51.26}', '{Z00.00,Z13.9}',
   'Imaging is covered after six weeks of conservative therapy; red-flag findings (progressive neurological deficit, suspected malignancy) waive the waiting period.',
   '{"conservative therapy >= 6 weeks","clinical examination note","prior imaging report"}',
   true, '2026-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000003', 'tenant-a', 'CT head without contrast', 1,
   '{R51.9,G43.909,S06.0X0A}', '{R42}',
   'Covered for post-traumatic headache or new neurological findings within 72 hours of onset.',
   '{"neurological examination","onset documented"}',
   true, '2025-06-01', NULL),
  ('1a000000-0000-4000-8000-000000000004', 'tenant-a', 'Bariatric surgery', 1,
   '{E66.01,E66.2}', '{F50.2}',
   'Documented supervised weight-loss program of at least six months; the member''s BMI must be >= 40, or >= 35 with an obesity-related comorbidity.',
   '{"supervised weight-loss program >= 6 months","BMI measurement","psychological evaluation"}',
   true, '2025-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000005', 'tenant-a', 'Polysomnography', 1,
   '{G47.33,G47.30}', '{F51.01}',
   'In-laboratory sleep study covered when home testing is inconclusive or contraindicated.',
   '{"Epworth sleepiness score","home sleep test result"}',
   true, '2025-03-01', NULL),
  ('1a000000-0000-4000-8000-000000000006', 'tenant-a', 'Extended physical therapy course', 1,
   '{M54.5,M25.561,M17.11}', '{M79.7}',
   'Sessions beyond the initial twelve require documented functional improvement.',
   '{"functional outcome measure","therapist progress note"}',
   true, '2025-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000007', 'tenant-a', 'Knee arthroscopy', 1,
   '{M23.51,S83.511A}', '{M17.11}',
   'Covered for mechanical symptoms with imaging-confirmed instability. Retired in favour of the physical therapy pathway.',
   '{"MRI report","instability examination"}',
   false, '2024-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000008', 'tenant-a', 'Hereditary cancer gene panel', 1,
   '{C50.911,Z80.3}', '{Z13.9}',
   'Covered with a qualifying family history and pre-test genetic counselling.',
   '{"three-generation family history","genetic counselling note"}',
   true, '2025-01-01', '2026-03-31')
ON CONFLICT (id) DO NOTHING;

-- tenant-b: 6 rows, 5 effective today. M54.5 is excluded here and covered for
-- tenant-a, which is the point: policies are tenant data, not global truth.
INSERT INTO policies (
  id, tenant_id, name, version, covered_codes, excluded_codes,
  criteria_text, required_evidence, active, effective_from, effective_to
) VALUES
  ('1b000000-0000-4000-8000-000000000001', 'tenant-b', 'Shoulder MRI', 1,
   '{M75.100,M75.41,M25.511}', '{}',
   'Covered after six weeks of conservative management without improvement.',
   '{"range of motion assessment","conservative therapy >= 6 weeks"}',
   true, '2025-01-01', NULL),
  ('1b000000-0000-4000-8000-000000000002', 'tenant-b', 'Cardiac stress test', 1,
   '{I25.10,R07.9,I21.4}', '{Z13.6}',
   'Covered for symptomatic members; screening of asymptomatic members is excluded.',
   '{"symptom description","resting ECG"}',
   true, '2025-01-01', NULL),
  ('1b000000-0000-4000-8000-000000000003', 'tenant-b', 'Lumbar spinal fusion', 1,
   '{M43.16,M48.061}', '{M54.5}',
   'Covered for documented instability; isolated low back pain is not an indication.',
   '{"flexion-extension radiographs","conservative therapy >= 6 months"}',
   true, '2025-01-01', NULL),
  ('1b000000-0000-4000-8000-000000000004', 'tenant-b', 'Lumbar spinal fusion', 2,
   '{M43.16,M48.061,M51.26}', '{M54.5,M54.50}',
   'Covered for documented instability or stenosis with neurogenic claudication; isolated low back pain remains excluded.',
   '{"flexion-extension radiographs","conservative therapy >= 6 months","neurogenic claudication note"}',
   true, '2026-02-01', NULL),
  ('1b000000-0000-4000-8000-000000000005', 'tenant-b', 'Home oxygen therapy', 1,
   '{J44.1,J96.11,Z99.81}', '{Z87.891}',
   'Covered with a qualifying arterial blood gas or oximetry result on room air.',
   '{"oximetry on room air","pulmonary function test"}',
   true, '2025-04-01', NULL),
  ('1b000000-0000-4000-8000-000000000006', 'tenant-b', 'Continuous insulin pump', 1,
   '{E10.9,E11.65,Z79.4}', '{E11.9}',
   'Covered for members on intensive insulin therapy with documented glucose logs.',
   '{"90 days of glucose logs","HbA1c result"}',
   true, '2025-01-01', NULL)
ON CONFLICT (id) DO NOTHING;

-- tenant-legacy is inactive: ingestion fails on the tenant, never reaching this policy.
INSERT INTO policies (
  id, tenant_id, name, version, covered_codes, excluded_codes,
  criteria_text, required_evidence, active, effective_from, effective_to
) VALUES
  ('1c000000-0000-4000-8000-000000000001', 'tenant-legacy', 'Annual wellness visit', 1,
   '{Z00.00}', '{}',
   'Legacy contract, retained for historical claims only.',
   '{"visit summary"}',
   true, '2023-01-01', NULL)
ON CONFLICT (id) DO NOTHING;
```

- [ ] **Step 7: Add the `seed` command to `src/ecet/cli.py`**

```python
@app.command()
def seed() -> None:
    """Load the dev seed data (ICD-10 catalogue, tenants, policies). Dev only."""
    from ecet.infrastructure.postgres.seed import load_seed
    from ecet.infrastructure.postgres.session import create_engine

    settings = _load_settings()
    configure_logging(settings.log_level)
    if settings.env != "dev":
        typer.echo(f"refusing to seed: ECET_ENV={settings.env!r}, the seed is dev-only", err=True)
        raise typer.Exit(code=1)

    async def run() -> int:
        engine = create_engine(settings.database_url.get_secret_value())
        try:
            return await load_seed(engine)
        finally:
            await engine.dispose()

    typer.echo(f"seed applied: {asyncio.run(run())} statements")
```

The import sits inside the function so `ecet api` and `ecet worker` do not pay for
SQLAlchemy import time, matching how `api` already imports uvicorn.

- [ ] **Step 8: Add the Makefile targets**

```make
DB_URL ?= postgresql+asyncpg://ecet:ecet@localhost:5432/ecet

migrate: .env
	ECET_DATABASE_URL=$(DB_URL) uv run alembic upgrade head

seed: .env
	ECET_DATABASE_URL=$(DB_URL) ECET_ENV=dev uv run ecet seed
```

Add `migrate seed` to the `.PHONY` line. Both run from the host against the compose
Postgres, hence `localhost` rather than the `postgres` service name; the remaining
required settings come from `.env`.

- [ ] **Step 9: Ship the migrations inside the image**

In `Dockerfile`, in the `runtime` stage after `WORKDIR /app`:

```dockerfile
COPY alembic.ini ./
COPY migrations/ ./migrations/
```

Phase 3 turns this into an actual startup migration when `ECET_AUTO_MIGRATE=true`;
Phase 2 only makes the image capable of it.

- [ ] **Step 10: Run the tests to verify they pass**

```bash
uv run pytest tests/unit -v
uv run pytest -m slow -v
```
Expected: PASS.

- [ ] **Step 11: Prove it end to end against compose**

```bash
make up
make migrate
make seed
docker compose exec postgres psql -U ecet -d ecet -c \
  "select id, active from tenants order by id;"
docker compose exec postgres psql -U ecet -d ecet -c \
  "select tenant_id, count(*) from policies group by tenant_id order by tenant_id;"
```
Expected: 4 tenants (`tenant-legacy` inactive), policy counts 8 / 6 / 1 for
`tenant-a` / `tenant-b` / `tenant-legacy`, no row for `tenant-empty`.
Running `make seed` a second time must print the same statement count and change nothing.

- [ ] **Step 12: Run the gate and commit**

```bash
make check
git add src/ecet/infrastructure/postgres/seed src/ecet/cli.py Makefile Dockerfile \
        tests/unit/test_seed_sql.py tests/unit/test_cli.py tests/adapters/test_seed.py
git commit -m "feat(postgres): dev seed data and the ecet seed command"
```

---

### Task 8: Record the phase in the docs

**Files:**
- Modify: `specs/06-roadmap.md`, `docs/plans/2026-09-06-phase-2-persistence.md` (this file)

- [ ] **Step 1: Write down every deviation discovered during execution**

Append to the *Deviations from spec* section at the end of this plan: anything the
implementation had to do differently from the spec or from this plan, with the reason.
The section already lists the deviations known when the plan was written.

- [ ] **Step 2: Fold the new deferrals into the roadmap**

In `specs/06-roadmap.md`:
- add a `## Carried over from Phase 2` table in the same shape as the Phase 0 and
  Phase 1 tables, one row per deferral, each with the phase that closes it;
- mirror every row inline in the phase that closes it, so a reader of that phase sees
  it without scrolling to the table.

- [ ] **Step 3: Commit**

```bash
git add specs/06-roadmap.md docs/plans/2026-09-06-phase-2-persistence.md
git commit -m "docs(phase-2): record the deviations and the Phase 2 carry-overs"
```

---

## Phase exit criteria

- [ ] `make check` green: ruff, `mypy --strict src/ecet/domain src/ecet/application`, `mypy src/ecet`, `lint-imports`, the default (Docker-free) test suite.
- [ ] `uv run pytest -m slow` green with Docker running: schema, all four repositories, unit of work, seed.
- [ ] `uv run coverage report --include='src/ecet/domain/*' --fail-under=95` still exits 0.
- [ ] `make up && make migrate && make seed` loads 4 tenants and 15 policies, and rerunning `make seed` changes nothing.
- [ ] `ECET_ENV=prod uv run ecet seed` exits 1 without touching the database.
- [ ] No file under `src/ecet/domain/` or `src/ecet/application/` imports `sqlalchemy`, `alembic` or `asyncpg`.
- [ ] `src/ecet/infrastructure/postgres/` contains `orm.py`, `mappers.py`, `repositories.py`, `session.py`, `unit_of_work.py`, `seed/` — and nothing else.

## Deviations from spec (record in the PR description)

1. **`icd10_codes` table + `Icd10CodeRepository` port are new**, listed in neither the [postgres schema](../../specs/03-infrastructure/postgres.md#schema) nor the [project layout](../../specs/05-platform/project-layout.md). Roadmap carry-over #3 from Phase 1 requires Phase 2 to seed an ICD-10 code set for Phase 4 to validate against; a seeded table plus a read port is how the seed reaches the worker. Note the residual limitation: `T12` is a genuine ICD-10 code, so "the T12 vertebra" still reads as a diagnosis even with the catalogue.
2. **The compose `postgres` init-dir seeding path is dropped.** The [postgres spec](../../specs/03-infrastructure/postgres.md#seed) applies `seed/*.sql` through the Postgres image's init directory, which runs *before* Alembic creates the tables. `ecet seed` is the only seeding path.
3. **`ecet seed` refuses to run unless `ECET_ENV=dev`.** No spec asks for the guard; the seed contains fixed webhook secrets and fabricated tenants, which must never reach a non-dev database.
4. **The seed is larger than the spec's "2 tenants, ~5 policies each, 1 tenant with zero policies"**: 4 tenants (one inactive) and 15 policies, including a superseded version, an expired policy and a deactivated policy. The extra rows are what make `active_for_tenant`'s filtering demonstrable in the demo rather than only in a unit test.
5. **Seed statements are split by a small quote-aware splitter** (`split_statements`) instead of being handed to the driver whole: asyncpg executes one statement per call, and the seeded criteria texts contain both semicolons and apostrophes. The alternative — a SQL-parsing dependency — is not worth it for three files we control.
6. **`claims` and `policies` columns are `NOT NULL`** where the spec's DDL sketch leaves nullability unstated (`bucket`, `key`, `etag`, `size`, `policy_ids`, `required_evidence`, …). The domain models require them; a nullable column would only invent a state the domain cannot represent.
7. **Optimistic concurrency is tracked inside `PostgresClaimRepository`** (a `dict[UUID, datetime]` of the last `updated_at` the repository saw) rather than through SQLAlchemy's `version_id_col`. Closes Phase 1 carry-over #1: `Claim` has no version field, `transition()` overwrites `updated_at` in place, and the schema has no version column. A claim the repository never loaded cannot be saved.
8. **`ReviewTaskRepository.save` raises `ReviewTaskNotFound` when the row is gone.** The port documents no error for that case; silently updating zero rows would make UC-09c report success for a task that does not exist.
9. **`UnitOfWork` (the application port) lands in Phase 2**, though the Phase 1 plan scheduled application ports for Phases 3/4. The [postgres spec](../../specs/03-infrastructure/postgres.md#unitofwork) puts `SqlAlchemyUnitOfWork` in this phase, and an adapter with no port to implement is not testable against a contract.
10. **`SqlAlchemyUnitOfWork` builds its session and repositories in `__init__`, not in `__aenter__`.** `AsyncSession` takes no connection until its first statement, so this costs nothing and keeps every attribute non-optional for `mypy --strict` on the port.
11. **Adapter tests share one migrated container and truncate between tests**, with a function-scoped engine. asyncpg connections belong to the loop that opened them and pytest-asyncio hands every test a fresh loop, so a session-scoped engine would fail on the second test.
12. **Migrations run in a subprocess in the test harness** (`python -m alembic upgrade head`) rather than through `alembic.command`: `env.py` calls `asyncio.run`, which cannot nest inside a running loop.
13. **`tests/adapters/conftest.py`'s `pytest_collection_modifyitems` is scoped to items under the conftest's own directory**, not left as written in the plan. The hook is collected once, session-wide, so marking every item unconditionally `slow` marked the *whole* suite `slow` and the Docker-free default run (`-m 'not slow and not e2e'`) selected zero tests.
14. **The overall 85% coverage gate moved out of the `check` job.** `check` keeps `--cov` reporting plus the 95% domain gate, but no longer enforces `--cov-fail-under=85`; the new `slow` job runs `pytest -m 'not e2e' --cov=ecet --cov-fail-under=85` and enforces the overall gate there. Phase 2 adds roughly 400 lines of infrastructure reachable only by Docker tests, so the gate has to live in the job that exercises them. The [testing spec](../../specs/05-platform/testing.md#coverage-target) target is unchanged — only which job enforces it.
15. **`src/ecet/domain/ports/icd10_repository.py` and `FakeIcd10CodeRepository` were written in Task 3, not Task 4**, because `UnitOfWork` imports the port; the unit-of-work port-conformance test moved the other way, from Task 3 to Task 6, because `UnitOfWork` is `runtime_checkable` with five data members and `isinstance` cannot pass until all five repositories exist. Sequencing only — nothing built changed.
16. **`PostgresClaimRepository.save` and `PostgresReviewTaskRepository.save` carry a narrow `# type: ignore[attr-defined]` on `result.rowcount`.** `session.execute()` on a Core `UPDATE` returns a `CursorResult` at runtime, but the async session's declared return type does not expose `rowcount` statically.
17. **The adapter-test harness runs Alembic through `subprocess.run` with the full environment plus an `ECET_DATABASE_URL` override**, not the plan's stripped `PATH`-only environment: a `PATH`-only environment broke the uv-provisioned interpreter's own module resolution.
