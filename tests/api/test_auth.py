"""Auth dependency coverage.

`/v1/claims/ingest` and `/v1/events/s3` do not exist until Task 11 — the over-HTTP
assertions that a wrong or missing credential 401s on those routes belong there.
This file tests the dependencies themselves directly, which needs no routes.
"""

import pytest
from fastapi import HTTPException
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
