"""E2E: the real compose stack, driven from outside like a tenant and a reviewer would.

Needs the stack up first — `make e2e` does both. Everything under `tests/e2e/` is
marked `e2e`, so the default run (`-m 'not slow and not e2e'`) and the CI `slow` job
(`-m 'not e2e'`) never collect a live-stack dependency.
"""

import subprocess
import time
from pathlib import Path

import httpx
import pytest
from tests.e2e.stack import API, REPO_ROOT

THIS_DIR = Path(__file__).resolve().parent

#: A cold stack builds presidio's engine before `/readyz` answers (see the api's
#: `start_period` in docker-compose.yml); this is that budget plus MinIO's restart.
READY_TIMEOUT_S = 300


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # This hook sees the whole session's items — filter, or every test gets marked.
    for item in items:
        if THIS_DIR in item.path.parents:
            item.add_marker(pytest.mark.e2e)


@pytest.fixture(scope="session", autouse=True)
def stack_ready() -> None:
    deadline = time.monotonic() + READY_TIMEOUT_S
    while True:
        try:
            if httpx.get(f"{API}/readyz", timeout=5).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if time.monotonic() > deadline:
            pytest.fail(f"api not ready after {READY_TIMEOUT_S}s — is the stack up? (make up)")
        time.sleep(2)
    # `minio-setup` restarts MinIO to register the webhook target; an object dropped
    # before it exits either hits a refused connection or fires no notification. Not
    # `docker compose wait`: it errors with "no containers" once the one-shot has exited,
    # which on a cold stack it has long before `/readyz` answers.
    while True:
        state = subprocess.run(
            [
                "docker",
                "compose",
                "ps",
                "-a",
                "--format",
                "{{.State}} {{.ExitCode}}",
                "minio-setup",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if state == "exited 0":
            return
        if state.startswith("exited") or time.monotonic() > deadline:
            pytest.fail(f"minio-setup did not finish cleanly: {state or 'no container'}")
        time.sleep(2)
