"""Driving the compose stack from outside: drop an object, poll for what follows.

Only published ports and `docker compose` are used. Postgres is read for exactly one
fact no API exposes — which claim an object key became.
"""

import asyncio
import json
import os
import subprocess
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import text

from ecet.infrastructure.postgres.session import create_engine
from ecet.infrastructure.storage.s3 import S3ObjectStorage

REPO_ROOT = Path(__file__).resolve().parents[2]
PDFS = REPO_ROOT / "tests" / "fixtures" / "pdfs"

API = "http://localhost:8000"
MOCK_CLIENT = "http://localhost:8081"
MINIO = "http://localhost:9000"
DATABASE_URL = "postgresql+asyncpg://ecet:ecet@localhost:5432/ecet"
BUCKET = "claims"
#: `.env.example`'s key, which `make up` copies. Not `ECET_API_KEY`: the root conftest
#: strips every `ECET_*` variable from the test environment.
API_KEY = os.environ.get("E2E_API_KEY", "dev-api-key")


async def drop(pdf: str, tenant: str) -> str:
    """Upload `tests/fixtures/pdfs/<pdf>.pdf` under a fresh key; MinIO's notification
    does the rest. Returns the key, which every later lookup is keyed on."""
    key = f"tenants/{tenant}/claims/e2e-{pdf}-{uuid4().hex}.pdf"
    storage = S3ObjectStorage(endpoint_url=MINIO, access_key="minioadmin", secret_key="minioadmin")
    async with storage.client() as client:
        await client.put_object(Bucket=BUCKET, Key=key, Body=(PDFS / f"{pdf}.pdf").read_bytes())
    return key


async def eventually[T](
    check: Callable[[], Awaitable[T | None]], *, what: str, within_s: float = 90.0
) -> T:
    """Poll `check` once a second until it returns something other than None."""
    deadline = time.monotonic() + within_s
    while True:
        result = await check()
        if result is not None:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"gave up after {within_s:.0f}s waiting for {what}")
        await asyncio.sleep(1)


async def deliveries_for(key: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(base_url=MOCK_CLIENT, timeout=10) as client:
        received: list[dict[str, Any]] = (await client.get("/received")).raise_for_status().json()
    return [d for d in received if d["payload"]["source_key"] == key]


async def delivery_for(key: str, *, decided_by: str) -> dict[str, Any] | None:
    return next(
        (d for d in await deliveries_for(key) if d["payload"]["decided_by"] == decided_by), None
    )


async def claim_row(key: str, *, status: str | None = None) -> dict[str, Any] | None:
    """The claim an object key became, optionally only once it reaches `status`."""
    engine = create_engine(DATABASE_URL)
    try:
        async with engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        text("SELECT id, status, failure_reason FROM claims WHERE key = :key"),
                        {"key": key},
                    )
                )
                .mappings()
                .first()
            )
    finally:
        await engine.dispose()
    if row is None or (status is not None and row["status"] != status):
        return None
    return dict(row)


async def api(
    method: str, path: str, *, tenant: str | None = None, **kwargs: Any
) -> httpx.Response:
    headers = {"X-API-Key": API_KEY}
    if tenant is not None:
        headers["X-Tenant-Id"] = tenant
    async with httpx.AsyncClient(base_url=API, timeout=30, follow_redirects=True) as client:
        return await client.request(method, path, headers=headers, **kwargs)


def compose_logs(service: str) -> str:
    return subprocess.run(
        ["docker", "compose", "logs", "--no-color", "--no-log-prefix", service],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def log_records(service: str) -> list[dict[str, Any]]:
    """The service's structlog JSON lines. Anything else (a traceback, a banner) is
    skipped rather than failing the parse."""
    records: list[dict[str, Any]] = []
    for line in compose_logs(service).splitlines():
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            records.append(parsed)
    return records


def worker_metrics() -> str:
    """The worker's `:9100` is deliberately not published to the host; read it from
    inside the container, the same way its healthcheck does."""
    return subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "worker",
            "python",
            "-c",
            "import urllib.request;"
            "print(urllib.request.urlopen('http://localhost:9100/metrics').read().decode())",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
