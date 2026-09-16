"""Delivery, signing and the retry policy. `backoff_seconds=(0, 0, 0)` keeps the retry
test instant — the schedule is a constructor argument precisely so a test never has to
sleep 21 seconds to prove three attempts happened."""

import hashlib
import hmac
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest
from prometheus_client import REGISTRY
from tests.pii import assert_no_pii

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.application.ports.webhook_client import WebhookClient
from ecet.domain.ids import ClaimId
from ecet.domain.tenant import Tenant
from ecet.infrastructure.webhook.httpx_client import HttpxWebhookClient

SECRET = "dev-hmac-tenant-a"
NOW = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def build_tenant() -> Tenant:
    return Tenant.model_validate(
        {
            "id": "tenant-a",
            "name": "Northwind Health Plan",
            "webhook_url": "http://mock-client:8081/hooks/northwind",
            "webhook_secret": SECRET,
        }
    )


def build_payload() -> ClientNotification:
    return ClientNotification(
        claim_id=ClaimId(uuid4()),
        tenant_id="tenant-a",
        source_key="tenants/tenant-a/claims/note-1.pdf",
        outcome="MEETS_NECESSITY",
        confidence=0.91,
        decided_by="auto",
        cited_codes=["M54.5"],
        rationale="Conservative therapy documented.",
        decided_at=NOW,
    )


def build_client(handler: Any, **overrides: Any) -> HttpxWebhookClient:
    args: dict[str, Any] = {
        "timeout_s": 5,
        "max_attempts": 3,
        "backoff_seconds": (0.0, 0.0, 0.0),
        "http_client": httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        **overrides,
    }
    return HttpxWebhookClient(**args)


async def test_the_adapter_satisfies_the_port() -> None:
    assert isinstance(build_client(lambda request: httpx.Response(200)), WebhookClient)


async def test_a_2xx_is_one_attempt() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 1
    assert str(seen[0].url) == "http://mock-client:8081/hooks/northwind"
    assert_no_pii(seen[0].content.decode("utf-8"))


async def test_the_signature_verifies_with_the_shared_secret() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    await build_client(handle).deliver(build_tenant(), build_payload())

    request = seen[0]
    timestamp = request.headers["X-ECET-Timestamp"]
    expected = hmac.new(
        SECRET.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + request.content,
        hashlib.sha256,
    ).hexdigest()
    assert request.headers["X-ECET-Signature"] == f"sha256={expected}"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["X-ECET-Delivery"]
    assert SECRET not in request.content.decode("utf-8")
    assert_no_pii(request.content.decode("utf-8"))


async def test_two_server_errors_then_success_is_three_attempts_one_delivery_id() -> None:
    statuses = iter([500, 500, 200])
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(next(statuses))

    await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 3
    assert len({request.headers["X-ECET-Delivery"] for request in seen}) == 1
    for request in seen:
        assert_no_pii(request.content.decode("utf-8"))


@pytest.mark.parametrize("status", [408, 429, 500, 503])
async def test_retryable_statuses_exhaust_into_a_transient_error(status: int) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)

    with pytest.raises(WebhookTransientError):
        await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 3
    for request in seen:
        assert_no_pii(request.content.decode("utf-8"))


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
async def test_other_client_errors_are_permanent_after_one_attempt(status: int) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)

    with pytest.raises(WebhookPermanentError):
        await build_client(handle).deliver(build_tenant(), build_payload())

    assert len(seen) == 1
    assert_no_pii(seen[0].content.decode("utf-8"))


async def test_a_transport_error_is_retried_then_transient() -> None:
    attempts = 0
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        seen.append(request)
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(WebhookTransientError):
        await build_client(handle).deliver(build_tenant(), build_payload())

    assert attempts == 3
    for request in seen:
        assert_no_pii(request.content.decode("utf-8"))


async def test_every_attempt_is_counted_by_response_class() -> None:
    before_2xx = sample("ecet_webhook_attempts_total", status_class="2xx")
    before_5xx = sample("ecet_webhook_attempts_total", status_class="5xx")
    responses = iter([httpx.Response(500), httpx.Response(200)])

    client = build_client(lambda request: next(responses), max_attempts=2, backoff_seconds=(0.0,))
    await client.deliver(build_tenant(), build_payload())

    assert sample("ecet_webhook_attempts_total", status_class="5xx") == before_5xx + 1
    assert sample("ecet_webhook_attempts_total", status_class="2xx") == before_2xx + 1


async def test_a_transport_failure_counts_as_error() -> None:
    before = sample("ecet_webhook_attempts_total", status_class="error")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(WebhookTransientError):
        await build_client(refuse, max_attempts=1).deliver(build_tenant(), build_payload())

    assert sample("ecet_webhook_attempts_total", status_class="error") == before + 1
