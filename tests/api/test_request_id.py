"""One id per request, echoed back. The
[observability spec](../../specs/05-platform/observability.md) calls tracing out of
scope and this in: enough to grep one claim end to end."""

import json

import pytest
import structlog
from tests.api.conftest import ApiHarness

from ecet.infrastructure.observability.logging import configure_logging


async def test_a_caller_supplied_request_id_is_echoed(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/healthz", headers={"X-Request-Id": "req-from-caller"})

    assert response.headers["x-request-id"] == "req-from-caller"


async def test_a_request_without_one_gets_a_generated_id(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.get("/healthz")

    assert len(response.headers["x-request-id"]) == 36  # uuid4


async def test_the_request_id_is_bound_to_every_log_line_of_that_request(
    harness: ApiHarness, api_headers: dict[str, str], capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("INFO")

    async with harness.client() as client:
        await client.post(
            "/v1/claims/ingest",
            headers={**api_headers, "X-Request-Id": "req-1"},
            json={"bucket": "claims", "key": "tenants/tenant-a/claims/note-1.pdf"},
        )

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    transitions = [line for line in lines if line["event"] == "claim.transition"]
    assert transitions, "the ingest logged no transition"
    assert all(line["request_id"] == "req-1" for line in transitions)


async def test_the_context_does_not_leak_between_requests(harness: ApiHarness) -> None:
    async with harness.client() as client:
        await client.get("/healthz", headers={"X-Request-Id": "req-1"})

    assert "request_id" not in structlog.contextvars.get_contextvars()
