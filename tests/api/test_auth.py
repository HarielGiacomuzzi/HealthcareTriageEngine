"""Auth dependency coverage.

`/v1/claims/ingest` and `/v1/events/s3` do not exist until Task 11 — the over-HTTP
assertions that a wrong or missing credential 401s on those routes belong there.
This file tests the dependencies themselves directly, which needs no routes.
"""

import pytest
from fastapi import Depends, HTTPException
from tests.api.conftest import API_KEY, EVENT_TOKEN, ApiHarness

from ecet.domain.ids import TenantId
from ecet.interfaces.api.dependencies import (
    require_api_key,
    require_event_token,
    require_tenant_id,
)


async def test_healthz_needs_no_auth(harness: ApiHarness) -> None:
    async with harness.client() as client:
        assert (await client.get("/healthz")).status_code == 200


def test_require_api_key_rejects_a_missing_header(harness: ApiHarness) -> None:
    with pytest.raises(HTTPException) as excinfo:
        require_api_key(harness.container, x_api_key=None)
    assert excinfo.value.status_code == 401


def test_require_api_key_rejects_a_wrong_value(harness: ApiHarness) -> None:
    with pytest.raises(HTTPException) as excinfo:
        require_api_key(harness.container, x_api_key="wrong")
    assert excinfo.value.status_code == 401


def test_require_api_key_accepts_the_correct_value(harness: ApiHarness) -> None:
    assert require_api_key(harness.container, x_api_key=API_KEY) is None


def test_require_event_token_rejects_a_missing_header(harness: ApiHarness) -> None:
    with pytest.raises(HTTPException) as excinfo:
        require_event_token(harness.container, authorization=None)
    assert excinfo.value.status_code == 401


def test_require_event_token_rejects_a_wrong_value(harness: ApiHarness) -> None:
    with pytest.raises(HTTPException) as excinfo:
        require_event_token(harness.container, authorization="Bearer wrong")
    assert excinfo.value.status_code == 401


def test_require_event_token_rejects_a_malformed_header(harness: ApiHarness) -> None:
    with pytest.raises(HTTPException) as excinfo:
        require_event_token(harness.container, authorization=EVENT_TOKEN)
    assert excinfo.value.status_code == 401


def test_require_event_token_accepts_the_correct_bearer_token(harness: ApiHarness) -> None:
    assert require_event_token(harness.container, authorization=f"Bearer {EVENT_TOKEN}") is None


def test_require_tenant_id_returns_the_header_value_as_a_tenant_id() -> None:
    assert require_tenant_id("tenant-a") == TenantId("tenant-a")


def _guard_routes(harness: ApiHarness) -> None:
    """A throwaway pair of routes guarded by the two auth dependencies — not
    production routes (Task 11 owns those), just enough to exercise auth over HTTP."""

    @harness.app.get("/_test/api-key", dependencies=[Depends(require_api_key)])
    async def _api_key_guarded() -> dict[str, bool]:
        return {"ok": True}

    @harness.app.get("/_test/event-token", dependencies=[Depends(require_event_token)])
    async def _event_token_guarded() -> dict[str, bool]:
        return {"ok": True}


async def test_a_non_ascii_api_key_is_401_not_500(harness: ApiHarness) -> None:
    _guard_routes(harness)
    async with harness.client() as client:
        response = await client.get("/_test/api-key", headers=[(b"x-api-key", b"\xe9x")])
    assert response.status_code == 401


async def test_a_non_ascii_bearer_token_is_401_not_500(harness: ApiHarness) -> None:
    _guard_routes(harness)
    async with harness.client() as client:
        response = await client.get(
            "/_test/event-token", headers=[(b"authorization", b"Bearer \xe9x")]
        )
    assert response.status_code == 401


async def test_a_correct_api_key_still_authenticates_over_http(harness: ApiHarness) -> None:
    _guard_routes(harness)
    async with harness.client() as client:
        response = await client.get("/_test/api-key", headers={"X-API-Key": API_KEY})
    assert response.status_code == 200


async def test_a_correct_bearer_token_still_authenticates_over_http(harness: ApiHarness) -> None:
    _guard_routes(harness)
    async with harness.client() as client:
        response = await client.get(
            "/_test/event-token", headers={"Authorization": f"Bearer {EVENT_TOKEN}"}
        )
    assert response.status_code == 200


async def test_ingest_without_an_api_key_is_401(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": "claims", "key": "tenants/tenant-a/claims/note-1.pdf"},
            headers={"X-Tenant-Id": "tenant-a"},
        )
    assert response.status_code == 401


async def test_ingest_with_a_wrong_api_key_is_401(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/claims/ingest",
            json={"bucket": "claims", "key": "tenants/tenant-a/claims/note-1.pdf"},
            headers={"X-API-Key": "wrong", "X-Tenant-Id": "tenant-a"},
        )
    assert response.status_code == 401


async def test_events_without_a_bearer_token_is_401(harness: ApiHarness) -> None:
    async with harness.client() as client:
        response = await client.post("/v1/events/s3", json={"Records": []})
    assert response.status_code == 401


async def test_events_with_the_api_key_instead_of_a_bearer_token_is_401(
    harness: ApiHarness,
) -> None:
    async with harness.client() as client:
        response = await client.post(
            "/v1/events/s3", json={"Records": []}, headers={"X-API-Key": API_KEY}
        )
    assert response.status_code == 401
