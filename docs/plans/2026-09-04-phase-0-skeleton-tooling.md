# Phase 0 — Skeleton & Tooling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An empty but runnable ECET project: `make up` brings the full compose stack healthy, `make test` passes, CI is green.

**Architecture:** Single `uv`-managed Python package `src/ecet` laid out in CLEAN layers (`domain` / `application` / `infrastructure` / `interfaces`) with import boundaries enforced by `import-linter` from day one. One Docker image, two entrypoints (`ecet api`, `ecet worker`) selected by the Typer CLI. Phase 0 ships only the shells: an API serving `/healthz`, a worker that idles until SIGTERM, and the `Settings` object every later phase reads from.

**Tech Stack:** Python 3.12, uv, FastAPI, uvicorn, Pydantic 2 + pydantic-settings, Typer, structlog, pytest + pytest-asyncio + httpx, ruff, mypy, import-linter, pre-commit, Docker Compose (postgres:16, rabbitmq:3.13-management, minio).

**Spec:** [`specs/06-roadmap.md` §Phase 0](../../specs/06-roadmap.md#phase-0--skeleton--tooling), which pulls in [project-layout](../../specs/05-platform/project-layout.md), [config](../../specs/05-platform/config.md), [docker-compose](../../specs/05-platform/docker-compose.md), [testing](../../specs/05-platform/testing.md), [observability](../../specs/05-platform/observability.md).

## Global Constraints

- Python **3.12** exactly (`requires-python = "==3.12.*"`; pinned with `uv python pin 3.12`).
- Package lives under `src/ecet/`. Import name is `ecet`. Console script is `ecet`.
- Import boundaries (enforced, not documented): `ecet.interfaces` → `ecet.infrastructure` → `ecet.application` → `ecet.domain`. `ecet.domain` and `ecet.application` may import stdlib + pydantic only — never fastapi, sqlalchemy, presidio, boto3, aio_pika, httpx, typer, or `ecet.config`.
- Settings env prefix is `ECET_`. All secrets are `SecretStr`. `Settings` is built once in `cli.py` and passed down — never imported as a module-level global.
- Every command is run through uv: `uv run <tool>`. No global installs, no `pip install`.
- Default test selection excludes markers `slow` and `e2e`.
- mypy runs `--strict` over `src/ecet/domain` and `src/ecet/application`; the rest of `src/ecet` runs under the lenient config.
- Commit after every task. Conventional commit prefixes (`chore:`, `feat:`, `test:`).

### Explicitly out of scope for Phase 0 (do not add)

These belong to later phases; adding them now breaks the "empty but runnable" deliverable:

- Empty placeholder modules for files no phase-0 task writes (`domain/claim.py`, `application/use_cases/*.py`, adapters, …). Only package `__init__.py` files are created, because `import-linter` needs the packages to resolve.
- spaCy model download in the Dockerfile (arrives with presidio in Phase 3 — keeps the phase-0 image small and the build offline-friendly).
- `minio-setup` and `mock-client` compose services (Phase 3 / Phase 4).
- `/readyz`, `/metrics`, Alembic, seeds, `ecet seed`, `ecet dlq-replay`, testcontainers, the nightly `slow` CI job.

---

### Task 1: Project scaffold, tooling and Makefile

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `.python-version` (via uv), `Makefile`
- Create: `src/ecet/__init__.py`, `src/ecet/domain/__init__.py`, `src/ecet/domain/ports/__init__.py`, `src/ecet/application/__init__.py`, `src/ecet/application/ports/__init__.py`, `src/ecet/application/use_cases/__init__.py`, `src/ecet/application/prompts/__init__.py`, `src/ecet/infrastructure/__init__.py`, `src/ecet/infrastructure/observability/__init__.py`, `src/ecet/interfaces/__init__.py`, `src/ecet/interfaces/api/__init__.py`, `src/ecet/interfaces/api/routes/__init__.py`, `src/ecet/interfaces/worker/__init__.py`
- Test: `tests/unit/test_package.py`

**Interfaces:**
- Consumes: nothing.
- Produces: importable package `ecet` with `ecet.__version__: str`; Makefile targets `install`, `lint`, `format`, `typecheck`, `imports`, `test`, `check`; the `dev` dependency group.

- [ ] **Step 1: Pin the interpreter and create the package tree**

```bash
cd /Users/harielgiacomuzzi/Development/FDE-Study/HealthcareTriageEngine
uv python pin 3.12
mkdir -p src/ecet/domain/ports \
         src/ecet/application/ports src/ecet/application/use_cases src/ecet/application/prompts \
         src/ecet/infrastructure/observability \
         src/ecet/interfaces/api/routes src/ecet/interfaces/worker \
         tests/unit tests/api
touch src/ecet/domain/__init__.py src/ecet/domain/ports/__init__.py \
      src/ecet/application/__init__.py src/ecet/application/ports/__init__.py \
      src/ecet/application/use_cases/__init__.py src/ecet/application/prompts/__init__.py \
      src/ecet/infrastructure/__init__.py src/ecet/infrastructure/observability/__init__.py \
      src/ecet/interfaces/__init__.py src/ecet/interfaces/api/__init__.py \
      src/ecet/interfaces/api/routes/__init__.py src/ecet/interfaces/worker/__init__.py
```

- [ ] **Step 2: Write `src/ecet/__init__.py`**

```python
"""ECET — Enterprise Claims Extraction & Triage Engine."""

__version__ = "0.1.0"
```

- [ ] **Step 3: Write the failing test**

`tests/unit/test_package.py`:

```python
import importlib

import pytest

import ecet

LAYER_PACKAGES = [
    "ecet.domain",
    "ecet.domain.ports",
    "ecet.application",
    "ecet.application.ports",
    "ecet.application.use_cases",
    "ecet.application.prompts",
    "ecet.infrastructure",
    "ecet.infrastructure.observability",
    "ecet.interfaces",
    "ecet.interfaces.api",
    "ecet.interfaces.api.routes",
    "ecet.interfaces.worker",
]


def test_version_is_exposed() -> None:
    assert ecet.__version__ == "0.1.0"


@pytest.mark.parametrize("name", LAYER_PACKAGES)
def test_layer_package_is_importable(name: str) -> None:
    assert importlib.import_module(name) is not None
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_package.py -v`
Expected: FAIL — no `pyproject.toml` yet, so `uv run` errors with "No `pyproject.toml` found" (or, once uv creates a bare project, `ModuleNotFoundError: No module named 'ecet'`).

- [ ] **Step 5: Write `pyproject.toml`**

```toml
[project]
name = "ecet"
version = "0.1.0"
description = "Enterprise Claims Extraction & Triage Engine"
readme = "README.md"
requires-python = "==3.12.*"
dependencies = [
    "fastapi>=0.141,<0.142",
    "uvicorn[standard]>=0.34,<1",
    "pydantic>=2.9,<3",
    "pydantic-settings>=2.5,<3",
    "typer>=0.15,<1",
    "structlog>=24.4,<26",
]

[dependency-groups]
dev = [
    "pytest>=8.3,<9",
    "pytest-asyncio>=0.24,<2",
    "pytest-cov>=5,<8",
    "httpx>=0.27,<0.28",
    "ruff>=0.8,<1",
    "mypy>=1.13,<2",
    "import-linter>=2.1,<3",
    "pre-commit>=4,<5",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/ecet"]

[tool.ruff]
line-length = 100
src = ["src", "tests"]
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "ASYNC", "RUF"]

[tool.mypy]
python_version = "3.12"
mypy_path = "src"
warn_unused_configs = true
warn_redundant_casts = true
warn_return_any = true
disallow_untyped_defs = true

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q -m 'not slow and not e2e'"
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
markers = [
    "slow: needs docker / testcontainers",
    "e2e: needs the full compose stack",
]

[tool.coverage.run]
source = ["ecet"]

[tool.importlinter]
root_package = "ecet"

[[tool.importlinter.contracts]]
name = "CLEAN layers"
type = "layers"
layers = [
    "ecet.interfaces",
    "ecet.infrastructure",
    "ecet.application",
    "ecet.domain",
]

[[tool.importlinter.contracts]]
name = "Domain and application stay framework-free"
type = "forbidden"
source_modules = ["ecet.domain", "ecet.application"]
forbidden_modules = [
    "fastapi",
    "starlette",
    "uvicorn",
    "typer",
    "sqlalchemy",
    "alembic",
    "asyncpg",
    "aio_pika",
    "boto3",
    "botocore",
    "httpx",
    "pypdf",
    "openai",
    "presidio_analyzer",
    "presidio_anonymizer",
]
```

`"ecet.config"` is deliberately absent from `forbidden_modules` here — the module does
not exist yet, and `lint-imports` cannot match a module that is not in the graph. Task 2
adds it once `src/ecet/config.py` exists.

If `uv sync` cannot resolve `fastapi>=0.141,<0.142` or `httpx>=0.27,<0.28` on this machine, widen that single constraint to the nearest published release, note the deviation in the commit message, and continue — do not change any other pin.

- [ ] **Step 6: Write `.gitignore`**

```gitignore
__pycache__/
*.py[cod]
.venv/
.env
.mypy_cache/
.ruff_cache/
.pytest_cache/
.coverage
htmlcov/
dist/
```

- [ ] **Step 7: Write `Makefile`** (tabs, not spaces, for recipe lines)

```makefile
.PHONY: install lint format typecheck imports test check

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy --strict src/ecet/domain src/ecet/application
	uv run mypy src/ecet

imports:
	uv run lint-imports

test:
	uv run pytest

check: lint typecheck imports test
```

- [ ] **Step 8: Sync and run the test to verify it passes**

Run: `uv sync && uv run pytest tests/unit/test_package.py -v`
Expected: PASS — 13 tests (1 version + 12 packages).

- [ ] **Step 9: Verify the full tooling gate is green**

Run: `make check`
Expected: ruff clean, both mypy passes clean, `lint-imports` prints `Contracts: 2 kept, 0 broken.`, pytest passes.

- [ ] **Step 10: Commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore Makefile src/ tests/
git commit -m "chore: scaffold uv project, CLEAN package tree and tooling gates"
```

---

### Task 2: Settings and `.env.example`

**Files:**
- Create: `src/ecet/config.py`
- Create: `.env.example`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: the package tree from Task 1.
- Produces:
  - `ecet.config.LlmProvider` — `StrEnum` with members `FAKE = "fake"` and `OPENAI = "openai"`.
  - `ecet.config.Settings(BaseSettings)` — env prefix `ECET_`, reads `.env`. Fields: `env: str`, `log_level: str`, `database_url: SecretStr`, `auto_migrate: bool`, `amqp_url: SecretStr`, `worker_prefetch: int`, `s3_endpoint: str | None`, `s3_access_key: SecretStr`, `s3_secret_key: SecretStr`, `s3_event_token: SecretStr` (required), `api_key: SecretStr` (required), `max_pdf_bytes: int`, `max_pdf_pages: int`, `pii_concurrency: int`, `spacy_model: str`, `confidence_threshold: float`, `llm_provider: LlmProvider`, `llm_base_url: str`, `llm_api_key: SecretStr | None`, `llm_model: str`, `llm_timeout_s: int`, `prompt_version: str`, `webhook_timeout_s: int`, `webhook_max_attempts: int`, `metrics_port: int`.
  - Later tasks construct test settings with `Settings(_env_file=None, s3_event_token="test-token", api_key="test-key")`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_config.py`:

```python
import pytest
from pydantic import ValidationError

from ecet.config import LlmProvider, Settings

REQUIRED = {"ECET_S3_EVENT_TOKEN": "tok", "ECET_API_KEY": "key"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ambient ECET_* vars must not leak into these assertions."""
    for var in (
        "ECET_ENV",
        "ECET_LOG_LEVEL",
        "ECET_S3_EVENT_TOKEN",
        "ECET_API_KEY",
        "ECET_LLM_PROVIDER",
        "ECET_LLM_API_KEY",
        "ECET_CONFIDENCE_THRESHOLD",
    ):
        monkeypatch.delenv(var, raising=False)


def test_defaults_match_config_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, value in REQUIRED.items():
        monkeypatch.setenv(var, value)

    settings = Settings(_env_file=None)

    assert settings.env == "dev"
    assert settings.log_level == "INFO"
    assert settings.auto_migrate is False
    assert settings.worker_prefetch == 4
    assert settings.s3_endpoint == "http://minio:9000"
    assert settings.max_pdf_bytes == 20_000_000
    assert settings.max_pdf_pages == 50
    assert settings.pii_concurrency == 2
    assert settings.spacy_model == "en_core_web_lg"
    assert settings.confidence_threshold == 0.85
    assert settings.llm_provider is LlmProvider.FAKE
    assert settings.llm_model == "claude-sonnet-5"
    assert settings.llm_timeout_s == 60
    assert settings.prompt_version == "v1"
    assert settings.webhook_timeout_s == 10
    assert settings.webhook_max_attempts == 3
    assert settings.metrics_port == 9100
    assert settings.database_url.get_secret_value().startswith("postgresql+asyncpg://")
    assert settings.amqp_url.get_secret_value().startswith("amqp://")


def test_env_prefix_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, value in REQUIRED.items():
        monkeypatch.setenv(var, value)
    monkeypatch.setenv("ECET_ENV", "prod")
    monkeypatch.setenv("ECET_CONFIDENCE_THRESHOLD", "0.5")

    settings = Settings(_env_file=None)

    assert settings.env == "prod"
    assert settings.confidence_threshold == 0.5


def test_missing_required_secrets_are_reported_together() -> None:
    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    missing = {error["loc"][0] for error in exc_info.value.errors()}
    assert missing == {"s3_event_token", "api_key"}


@pytest.mark.parametrize("value", ["0", "0.0", "1.5", "-0.2"])
def test_confidence_threshold_out_of_range_is_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_CONFIDENCE_THRESHOLD", value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_confidence_threshold_one_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_CONFIDENCE_THRESHOLD", "1")

    assert Settings(_env_file=None).confidence_threshold == 1.0


def test_openai_provider_requires_an_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_LLM_PROVIDER", "openai")

    with pytest.raises(ValidationError, match="ECET_LLM_API_KEY"):
        Settings(_env_file=None)


def test_openai_provider_with_api_key_is_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_LLM_PROVIDER", "openai")
    monkeypatch.setenv("ECET_LLM_API_KEY", "sk-test")

    settings = Settings(_env_file=None)

    assert settings.llm_provider is LlmProvider.OPENAI
    assert settings.llm_api_key is not None
    assert settings.llm_api_key.get_secret_value() == "sk-test"


def test_secrets_are_not_leaked_by_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "super-secret-token")
    monkeypatch.setenv("ECET_API_KEY", "super-secret-key")

    dumped = repr(Settings(_env_file=None))

    assert "super-secret-token" not in dumped
    assert "super-secret-key" not in dumped
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.config'`

- [ ] **Step 3: Write `src/ecet/config.py`**

```python
"""Application settings. Built once in the CLI and passed down — never a global."""

from enum import StrEnum
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LlmProvider(StrEnum):
    FAKE = "fake"
    OPENAI = "openai"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ECET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = "dev"
    log_level: str = "INFO"

    database_url: SecretStr = SecretStr("postgresql+asyncpg://ecet:ecet@postgres:5432/ecet")
    auto_migrate: bool = False

    amqp_url: SecretStr = SecretStr("amqp://guest:guest@rabbitmq:5672/")
    worker_prefetch: int = 4

    s3_endpoint: str | None = "http://minio:9000"
    s3_access_key: SecretStr = SecretStr("minioadmin")
    s3_secret_key: SecretStr = SecretStr("minioadmin")
    s3_event_token: SecretStr
    api_key: SecretStr

    max_pdf_bytes: int = 20_000_000
    max_pdf_pages: int = 50
    pii_concurrency: int = 2
    spacy_model: str = "en_core_web_lg"

    confidence_threshold: float = Field(default=0.85, gt=0, le=1)

    llm_provider: LlmProvider = LlmProvider.FAKE
    llm_base_url: str = "https://api.anthropic.com/v1/"
    llm_api_key: SecretStr | None = None
    llm_model: str = "claude-sonnet-5"
    llm_timeout_s: int = 60
    prompt_version: str = "v1"

    webhook_timeout_s: int = 10
    webhook_max_attempts: int = 3

    metrics_port: int = 9100

    @model_validator(mode="after")
    def _openai_provider_needs_a_key(self) -> Self:
        if self.llm_provider is LlmProvider.OPENAI and self.llm_api_key is None:
            raise ValueError("ECET_LLM_API_KEY is required when ECET_LLM_PROVIDER=openai")
        return self
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS — 11 tests.

- [ ] **Step 5: Write `.env.example`**

```dotenv
# Copy to .env before `make up`. Dev values only — never real credentials.
ECET_ENV=dev
ECET_LOG_LEVEL=INFO

ECET_DATABASE_URL=postgresql+asyncpg://ecet:ecet@postgres:5432/ecet
ECET_AUTO_MIGRATE=true

ECET_AMQP_URL=amqp://guest:guest@rabbitmq:5672/
ECET_WORKER_PREFETCH=4

ECET_S3_ENDPOINT=http://minio:9000
ECET_S3_ACCESS_KEY=minioadmin
ECET_S3_SECRET_KEY=minioadmin
ECET_S3_EVENT_TOKEN=dev-s3-event-token
ECET_API_KEY=dev-api-key

ECET_MAX_PDF_BYTES=20000000
ECET_MAX_PDF_PAGES=50
ECET_PII_CONCURRENCY=2
ECET_SPACY_MODEL=en_core_web_lg

ECET_CONFIDENCE_THRESHOLD=0.85

ECET_LLM_PROVIDER=fake
ECET_LLM_BASE_URL=https://api.anthropic.com/v1/
ECET_LLM_API_KEY=
ECET_LLM_MODEL=claude-sonnet-5
ECET_LLM_TIMEOUT_S=60
ECET_PROMPT_VERSION=v1

ECET_WEBHOOK_TIMEOUT_S=10
ECET_WEBHOOK_MAX_ATTEMPTS=3

ECET_METRICS_PORT=9100
```

`ECET_LLM_API_KEY=` empty is valid because the default provider is `fake`; switching to `openai` without filling it fails at startup, which is the intended behaviour.

- [ ] **Step 6: Verify every documented env var is in `.env.example`**

Run:

```bash
uv run python - <<'PY'
import pathlib, re
spec = pathlib.Path("specs/05-platform/config.md").read_text()
documented = set(re.findall(r"ECET_[A-Z0-9_]+", spec))
# The spec table writes "ECET_S3_ACCESS_KEY / SECRET_KEY" on one row.
documented.discard("ECET_SECRET_KEY")
documented.add("ECET_S3_SECRET_KEY")
example = set(re.findall(r"ECET_[A-Z0-9_]+", pathlib.Path(".env.example").read_text()))
missing = documented - example
print("missing:", sorted(missing))
assert not missing, missing
PY
```

Expected: `missing: []` and exit code 0.

- [ ] **Step 7: Close the import contract over `ecet.config`**

Now that `src/ecet/config.py` exists, add it to the `forbidden_modules` list of the
`Domain and application stay framework-free` contract in `pyproject.toml` — settings are
built in the CLI and passed down, so neither layer may import them:

```toml
    "presidio_analyzer",
    "presidio_anonymizer",
    "ecet.config",
]
```

- [ ] **Step 8: Run the full gate**

Run: `make check`
Expected: all green; `lint-imports` reports `Contracts: 2 kept, 0 broken.`

- [ ] **Step 9: Commit**

```bash
git add src/ecet/config.py tests/unit/test_config.py .env.example pyproject.toml
git commit -m "feat: add Settings and .env.example"
```

---

### Task 3: API app with `/healthz`

**Files:**
- Create: `src/ecet/interfaces/api/app.py`
- Create: `src/ecet/interfaces/api/routes/health.py`
- Test: `tests/api/test_health.py`, `tests/conftest.py`

**Interfaces:**
- Consumes: `ecet.config.Settings`.
- Produces:
  - `ecet.interfaces.api.app.create_app(settings: Settings) -> FastAPI` — app factory; stores the settings on `app.state.settings`.
  - `ecet.interfaces.api.routes.health.router: APIRouter` — serves `GET /healthz` → `{"status": "ok"}`.
  - `tests/conftest.py::settings` — pytest fixture returning a valid `Settings` built without reading `.env`.

- [ ] **Step 1: Write the shared test fixture**

`tests/conftest.py`:

```python
import pytest

from ecet.config import Settings


@pytest.fixture
def settings() -> Settings:
    """Valid settings that never read the developer's local .env file."""
    return Settings(_env_file=None, s3_event_token="test-token", api_key="test-key")
```

- [ ] **Step 2: Write the failing test**

`tests/api/test_health.py`:

```python
import httpx

from ecet.config import Settings
from ecet.interfaces.api.app import create_app


async def test_healthz_returns_ok(settings: Settings) -> None:
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_app_keeps_settings_on_state(settings: Settings) -> None:
    app = create_app(settings)

    assert app.state.settings is settings
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest tests/api/test_health.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.interfaces.api.app'`

- [ ] **Step 4: Write `src/ecet/interfaces/api/routes/health.py`**

```python
from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe. No dependencies — it must answer even when Postgres is down."""
    return {"status": "ok"}
```

- [ ] **Step 5: Write `src/ecet/interfaces/api/app.py`**

```python
from fastapi import FastAPI

from ecet import __version__
from ecet.config import Settings
from ecet.interfaces.api.routes import health


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="ECET API", version=__version__)
    app.state.settings = settings
    app.include_router(health.router)
    return app
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run pytest tests/api/test_health.py -v`
Expected: PASS — 2 tests.

- [ ] **Step 7: Run the full gate**

Run: `make check`
Expected: all green — in particular `lint-imports` still reports `2 kept, 0 broken` (`interfaces` importing `config` and `fastapi` is allowed).

- [ ] **Step 8: Commit**

```bash
git add src/ecet/interfaces/api tests/api tests/conftest.py
git commit -m "feat: add FastAPI app factory with /healthz"
```

---

### Task 4: Worker idle loop with graceful shutdown

**Files:**
- Create: `src/ecet/interfaces/worker/main.py`
- Test: `tests/unit/test_worker_main.py`

**Interfaces:**
- Consumes: `ecet.config.Settings`.
- Produces: `ecet.interfaces.worker.main.run(settings: Settings, stop: asyncio.Event | None = None) -> None` — installs SIGINT/SIGTERM handlers that set `stop`, then awaits it. Phase 4 replaces the body between the handlers and `await stop.wait()` with the AMQP consumer; the signature stays.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_worker_main.py`:

```python
import asyncio
import os
import signal

from ecet.config import Settings
from ecet.interfaces.worker.main import run


async def test_run_returns_when_stop_event_is_set(settings: Settings) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, stop))
    await asyncio.sleep(0)  # let the worker reach `await stop.wait()`

    stop.set()

    await asyncio.wait_for(task, timeout=1)
    assert task.done()


async def test_sigterm_stops_the_worker(settings: Settings) -> None:
    task = asyncio.create_task(run(settings))
    await asyncio.sleep(0.05)  # handlers must be installed before the signal lands

    os.kill(os.getpid(), signal.SIGTERM)

    await asyncio.wait_for(task, timeout=1)
    assert task.done()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_worker_main.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.interfaces.worker.main'`

- [ ] **Step 3: Write `src/ecet/interfaces/worker/main.py`**

```python
"""Worker entrypoint.

Phase 0 ships the process shell only: it starts, idles, and stops cleanly on
SIGTERM (exit code 0). The RabbitMQ consumer replaces the idle wait in Phase 4.
"""

import asyncio
import contextlib
import signal

import structlog

from ecet.config import Settings

log = structlog.get_logger(__name__)


async def run(settings: Settings, stop: asyncio.Event | None = None) -> None:
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not implemented on Windows event loops; the compose stack is Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    log.info("worker.started", env=settings.env, prefetch=settings.worker_prefetch)
    try:
        await stop.wait()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(sig)
        log.info("worker.stopped")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_worker_main.py -v`
Expected: PASS — 2 tests. If `test_sigterm_stops_the_worker` hangs, the handlers were not installed before `os.kill`; raise the `asyncio.sleep` to `0.1`.

- [ ] **Step 5: Run the full gate**

Run: `make check`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/ecet/interfaces/worker tests/unit/test_worker_main.py
git commit -m "feat: add worker idle loop with graceful shutdown"
```

---

### Task 5: Structured logging and the `ecet` CLI

**Files:**
- Create: `src/ecet/infrastructure/observability/logging.py`
- Create: `src/ecet/cli.py`
- Modify: `pyproject.toml` (add the `[project.scripts]` table)
- Test: `tests/unit/test_logging.py`, `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: `ecet.config.Settings`, `ecet.interfaces.api.app.create_app`, `ecet.interfaces.worker.main.run`.
- Produces:
  - `ecet.infrastructure.observability.logging.drop_sensitive_fields(logger, method_name, event_dict) -> dict` — structlog processor removing the keys `text`, `raw_text`, `redacted_text`, `webhook_secret`, `api_key` (ADR-001 belt-and-braces).
  - `ecet.infrastructure.observability.logging.configure_logging(level: str) -> None` — JSON logs to stdout with the redaction processor installed.
  - `ecet.cli.app: typer.Typer` — commands `api` and `worker`; console script `ecet`.

- [ ] **Step 1: Write the failing logging test**

`tests/unit/test_logging.py`:

```python
import json

import pytest
import structlog

from ecet.infrastructure.observability.logging import (
    configure_logging,
    drop_sensitive_fields,
)


def test_sensitive_fields_are_dropped() -> None:
    event_dict = {
        "event": "claim.ingested",
        "claim_id": "c-1",
        "text": "Patient John Doe, SSN 123-45-6789",
        "raw_text": "same",
        "redacted_text": "same",
        "webhook_secret": "shh",
        "api_key": "shh",
    }

    result = drop_sensitive_fields(None, "info", event_dict)

    assert result == {"event": "claim.ingested", "claim_id": "c-1"}


def test_non_sensitive_fields_survive() -> None:
    event_dict = {"event": "worker.started", "env": "dev"}

    assert drop_sensitive_fields(None, "info", event_dict) == event_dict


def test_configure_logging_emits_json_without_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")

    structlog.get_logger("test").info(
        "claim.transition", claim_id="c-1", text="Patient John Doe"
    )

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "claim.transition"
    assert payload["claim_id"] == "c-1"
    assert payload["level"] == "info"
    assert "text" not in payload
    assert "John Doe" not in line
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.infrastructure.observability.logging'`

- [ ] **Step 3: Write `src/ecet/infrastructure/observability/logging.py`**

```python
"""structlog configuration. JSON to stdout, with an ADR-001 redaction guard."""

import logging
from typing import Any

import structlog

SENSITIVE_FIELDS = frozenset(
    {"text", "raw_text", "redacted_text", "webhook_secret", "api_key"}
)


def drop_sensitive_fields(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Remove fields that must never reach a log sink (ADR-001)."""
    for key in SENSITIVE_FIELDS & event_dict.keys():
        del event_dict[key]
    return event_dict


def configure_logging(level: str) -> None:
    numeric_level = logging.getLevelNamesMapping()[level.upper()]
    logging.basicConfig(format="%(message)s", level=numeric_level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            drop_sensitive_fields,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
```

`cache_logger_on_first_use=False` keeps the test above honest — a cached bound logger would ignore a later `configure_logging` call.

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: PASS — 3 tests.

- [ ] **Step 5: Write the failing CLI test**

`tests/unit/test_cli.py`:

```python
import pathlib

import pytest
from typer.testing import CliRunner

from ecet.cli import app

runner = CliRunner()


def test_help_lists_both_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "api" in result.stdout
    assert "worker" in result.stdout


def test_api_command_starts_uvicorn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)  # no local .env
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "tok")
    monkeypatch.setenv("ECET_API_KEY", "key")
    calls: list[dict[str, object]] = []

    def fake_run(application: object, **kwargs: object) -> None:
        calls.append({"app": application, **kwargs})

    monkeypatch.setattr("uvicorn.run", fake_run)

    result = runner.invoke(app, ["api", "--port", "9999"])

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["port"] == 9999
    assert calls[0]["host"] == "0.0.0.0"


def test_worker_command_runs_the_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "tok")
    monkeypatch.setenv("ECET_API_KEY", "key")
    started: list[str] = []

    async def fake_run(settings: object, stop: object = None) -> None:
        started.append(getattr(settings, "env", "?"))

    monkeypatch.setattr("ecet.interfaces.worker.main.run", fake_run)

    result = runner.invoke(app, ["worker"])

    assert result.exit_code == 0
    assert started == ["dev"]


def test_invalid_config_exits_one_and_names_the_fields(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ECET_S3_EVENT_TOKEN", raising=False)
    monkeypatch.delenv("ECET_API_KEY", raising=False)

    result = runner.invoke(app, ["worker"])

    assert result.exit_code == 1
    assert "s3_event_token" in result.output
    assert "api_key" in result.output
```

`result.output` carries stderr as well on click ≥ 8.2. On an older click, switch those
two assertions to `result.stderr`.

- [ ] **Step 6: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ecet.cli'`

- [ ] **Step 7: Write `src/ecet/cli.py`**

```python
"""`ecet` command line: one image, two entrypoints (ADR-007)."""

import asyncio

import typer
from pydantic import ValidationError

from ecet.config import Settings
from ecet.infrastructure.observability.logging import configure_logging

app = typer.Typer(add_completion=False, help="ECET — claims extraction & triage engine")


def _load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:
        for error in exc.errors():
            field = ".".join(str(part) for part in error["loc"])
            typer.echo(f"config error: {field}: {error['msg']}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the HTTP API."""
    import uvicorn

    from ecet.interfaces.api.app import create_app

    settings = _load_settings()
    configure_logging(settings.log_level)
    uvicorn.run(create_app(settings), host=host, port=port, log_config=None)


@app.command()
def worker() -> None:
    """Run the queue worker."""
    import ecet.interfaces.worker.main as worker_main

    settings = _load_settings()
    configure_logging(settings.log_level)
    asyncio.run(worker_main.run(settings))
```

The `import ecet.interfaces.worker.main as worker_main` form (rather than `from ... import run`) is what lets the test monkeypatch `run` on the module.

- [ ] **Step 8: Add the console script to `pyproject.toml`**

Insert directly after the `dependencies = [...]` list of the `[project]` table:

```toml
[project.scripts]
ecet = "ecet.cli:app"
```

- [ ] **Step 9: Re-sync and run the CLI tests**

Run: `uv sync && uv run pytest tests/unit/test_cli.py -v`
Expected: PASS — 4 tests.

- [ ] **Step 10: Verify the installed console script works**

Run: `uv run ecet --help`
Expected: usage text listing `api` and `worker`, exit code 0.

- [ ] **Step 11: Run the full gate**

Run: `make check`
Expected: all green. `lint-imports` still `2 kept, 0 broken` — `ecet.cli` sits outside the layered contract, which is exactly why it is allowed to import `ecet.interfaces`.

- [ ] **Step 12: Commit**

```bash
git add pyproject.toml uv.lock src/ecet/cli.py src/ecet/infrastructure/observability/logging.py \
        tests/unit/test_cli.py tests/unit/test_logging.py
git commit -m "feat: add ecet CLI entrypoints and JSON logging with redaction guard"
```

---

### Task 6: Dockerfile, compose stack and `make up`

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`
- Modify: `Makefile` (add `up`, `down`, `logs`, `ps` and the `.env` rule)
- Test: manual smoke verification (steps 6–8); no automated test — a compose smoke test belongs to the `e2e` layer in Phase 6.

**Interfaces:**
- Consumes: the `ecet` console script (Task 5), `.env.example` (Task 2).
- Produces: image with `ENTRYPOINT ["ecet"]` / `CMD ["api"]`; compose services `postgres`, `rabbitmq`, `minio`, `api`, `worker`; Makefile targets `up`, `down`, `logs`, `ps`.

- [ ] **Step 1: Write `.dockerignore`**

```dockerignore
.git
.venv
.env
__pycache__
*.pyc
.mypy_cache
.ruff_cache
.pytest_cache
htmlcov
docs
specs
tests
```

- [ ] **Step 2: Write `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
# Dependency layer first so source edits do not re-resolve the world.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ ./src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim AS runtime
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN useradd --create-home --uid 1000 ecet
COPY --from=builder /opt/venv /opt/venv
USER ecet
WORKDIR /app
EXPOSE 8000
ENTRYPOINT ["ecet"]
CMD ["api"]
```

`--no-editable` installs the package into `/opt/venv` itself, so the runtime stage needs nothing from `/app/src`. The spaCy model download that the docker-compose spec describes lands here in Phase 3, together with presidio.

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ecet
      POSTGRES_PASSWORD: ecet
      POSTGRES_DB: ecet
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ecet -d ecet"]
      interval: 5s
      timeout: 3s
      retries: 10

  rabbitmq:
    image: rabbitmq:3.13-management
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitdata:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - miniodata:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 10

  api:
    build: .
    command: ["api"]
    env_file: .env
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
    deploy:
      resources:
        limits:
          memory: 2G

  worker:
    build: .
    command: ["worker"]
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    deploy:
      resources:
        limits:
          memory: 512M

volumes:
  pgdata:
  rabbitdata:
  miniodata:
```

The worker has no healthcheck in Phase 0 — its liveness probe is the `/metrics` server on 9100, which arrives with the metrics work. The api healthcheck hits `/healthz` rather than the spec's `/readyz` because `/readyz` (DB + AMQP + presidio) is a Phase 3 endpoint; switch it there.

- [ ] **Step 4: Add the compose targets to `Makefile`**

Replace the `.PHONY` line and append the new targets:

```makefile
.PHONY: install lint format typecheck imports test check up down clean logs ps

# Order-only prerequisite: create .env when absent, never regenerate it over
# a developer's customized copy when .env.example is edited later.
.env: | .env.example
	cp .env.example $@

up: .env
	docker compose up --build -d

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f api worker

ps:
	docker compose ps
```

- [ ] **Step 5: Verify the compose file parses**

Run: `make .env && docker compose config --quiet && echo OK`
Expected: `OK`, exit code 0. The `make .env` comes first because `env_file: .env` is not optional — on a clean checkout `docker compose config` fails without it.

- [ ] **Step 6: Bring the stack up**

Run: `make up`
Expected: images build; five containers start. First build pulls `python:3.12-slim` and the uv image.

- [ ] **Step 7: Verify every service is healthy and the API answers**

Run:

```bash
docker compose ps
curl -sf localhost:8000/healthz
```

Expected: `postgres`, `rabbitmq`, `minio`, `api` all show `healthy`; `worker` shows `running`; curl prints `{"status":"ok"}` and exits 0.

- [ ] **Step 8: Verify the worker logged its start line and stops cleanly**

Run:

```bash
docker compose logs worker | tail -n 3
docker compose stop worker
docker compose ps -a worker
```

Expected: a JSON line containing `"event": "worker.started"`; after `stop`, a `"worker.stopped"` line and exit code `0` in `docker compose ps -a`. Restart it with `docker compose start worker`.

- [ ] **Step 9: Tear down and commit**

```bash
make clean
git add Dockerfile .dockerignore docker-compose.yml Makefile
git commit -m "chore: add single-image Dockerfile and compose stack"
```

---

### Task 7: pre-commit hooks and CI workflow

**Files:**
- Create: `.pre-commit-config.yaml`
- Create: `.github/workflows/ci.yml`
- Modify: `Makefile` (add `hooks` target)

**Interfaces:**
- Consumes: every Makefile gate from Tasks 1–6.
- Produces: installed git hooks running ruff / mypy-strict / import-linter; a `check` CI job that runs lint, both type passes, import contracts and tests with `--cov-fail-under=85`.

- [ ] **Step 1: Write `.pre-commit-config.yaml`**

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: check-yaml
      - id: check-toml
      - id: check-added-large-files

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.16.6  # must match the ruff version uv.lock resolves
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: local
    hooks:
      - id: mypy-strict
        name: mypy --strict (domain, application)
        entry: uv run mypy --strict src/ecet/domain src/ecet/application
        language: system
        types: [python]
        pass_filenames: false
      - id: mypy
        name: mypy (src/ecet)
        entry: uv run mypy src/ecet
        language: system
        types: [python]
        pass_filenames: false
      - id: lint-imports
        name: import-linter contracts
        entry: uv run lint-imports
        language: system
        types: [python]
        pass_filenames: false
```

If `rev: v0.8.6` does not exist for ruff-pre-commit, run `uv run pre-commit autoupdate` after step 3 and commit the bumped revs.

- [ ] **Step 2: Add the `hooks` target to `Makefile`**

Add `hooks` to `.PHONY` and append:

```makefile
hooks:
	uv run pre-commit install
```

- [ ] **Step 3: Install the hooks and run them over the whole repo**

Run: `make hooks && uv run pre-commit run --all-files`
Expected: every hook passes. Formatting hooks may rewrite files on the first run — re-run until clean, then include those edits in this task's commit.

- [ ] **Step 4: Write `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true

      - name: Install Python 3.12
        run: uv python install 3.12

      - name: Sync dependencies
        run: uv sync --frozen

      - name: Lint
        run: |
          uv run ruff check .
          uv run ruff format --check .

      - name: Type check (strict on domain + application)
        run: |
          uv run mypy --strict src/ecet/domain src/ecet/application
          uv run mypy src/ecet

      - name: Import contracts
        run: uv run lint-imports

      - name: Tests
        run: uv run pytest --cov=ecet --cov-report=term-missing --cov-fail-under=85
```

Only the fast layers run here. The `slow` (testcontainers) and `e2e` (compose) jobs land with the adapters in Phase 2–3.

- [ ] **Step 5: Verify the CI command sequence passes locally**

Run:

```bash
uv sync --frozen
uv run ruff check . && uv run ruff format --check .
uv run mypy --strict src/ecet/domain src/ecet/application
uv run mypy src/ecet
uv run lint-imports
uv run pytest --cov=ecet --cov-report=term-missing --cov-fail-under=85
```

Expected: every command exits 0; the coverage line reports ≥ 85 %. If coverage falls short, the gap will be in `src/ecet/cli.py` — add the missing assertion to `tests/unit/test_cli.py` rather than lowering the threshold.

- [ ] **Step 6: Commit**

```bash
git add .pre-commit-config.yaml .github/workflows/ci.yml Makefile
git commit -m "chore: add pre-commit hooks and CI workflow"
```

- [ ] **Step 7: Verify the phase exit criteria end to end**

Run:

```bash
make check
make up
curl -sf localhost:8000/healthz && echo
docker compose ps
make down
```

Expected: `make check` green; all four healthchecked services `healthy`; `{"status":"ok"}`; clean teardown. Push the branch and confirm the GitHub Actions `check` job is green.

---

## Deviations from spec (record in the PR description)

1. `src/ecet` contains package `__init__.py` files only — no empty placeholder modules for `claim.py`, `use_cases/*.py`, adapters, etc. Each arrives with the phase that implements it.
2. Dockerfile omits the spaCy `en_core_web_lg` download and the `SPACY_MODEL` build arg; both land in Phase 3 with presidio.
3. Compose omits `minio-setup` (Phase 3) and `mock-client` (Phase 4).
4. The api container healthcheck probes `/healthz`, not `/readyz`, until `/readyz` exists in Phase 3.
5. The worker has no healthcheck until the `/metrics` server exists.
6. Coverage thresholds are enforced in CI only; local `pytest` stays fast.
7. `[tool.ruff] extend-exclude = ["docs", "specs"]` was added in Task 1: ruff 0.16 formats Python code fences inside Markdown, so `ruff format --check .` otherwise rewrites the spec files.
8. `[tool.importlinter] include_external_packages = true` was added in Task 1: import-linter 2.14 refuses to evaluate a `forbidden` contract against third-party modules without it.
9. `"ecet.config"` joins the `forbidden_modules` list in Task 2, not Task 1 — `lint-imports` cannot match a module absent from the import graph.
10. `tests/conftest.py` also carries an autouse `_no_ambient_ecet_env` fixture that strips every `ECET_*` key from `os.environ` per test — added in Task 3 after review. `Settings(_env_file=None)` suppresses only the dotenv file, so without it an exported `ECET_*` var silently overrides the defaults the tests assert on.
11. `.pre-commit-config.yaml` pins `ruff-pre-commit` to the tag matching the ruff version `uv.lock` resolves (v0.16.6), not the plan's original v0.8.6 — otherwise the hook's ruff and the ruff `make lint`/CI run are eight minor versions apart and the local gate stops predicting the CI gate.
12. uvicorn's own stdlib log records bypass the structlog processor chain: `cli.py`'s `api` command passes `log_config=None` to `uvicorn.run`, so `uvicorn.error`/`uvicorn.access` go to the root handler configured by `logging.basicConfig` and emit plain text on stderr, while structlog emits JSON on stdout. Only structlog-emitted events get the ADR-001 `drop_sensitive_fields` guard. Deferred to Phase 6, which owns the observability work; exposure today is limited to access and startup lines, which carry no clinical text. Phase 3's adapters must not rely on stdlib logging until that seam is closed.
