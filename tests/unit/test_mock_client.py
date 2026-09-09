"""The mock client is demo infrastructure, but the signature check is the only
independent proof that `HttpxWebhookClient` signs correctly — it re-derives the HMAC
from the shared secret rather than calling the sender's own `sign`.

The service directory is hyphenated (`services/mock-client/`) and therefore not
importable as a package, so the module is loaded by path.
"""

import hashlib
import hmac
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from tests.pii import assert_no_pii

APP_PATH = Path(__file__).resolve().parents[2] / "services" / "mock-client" / "app.py"
SECRET = "dev-hmac-tenant-a"

PAYLOAD: dict[str, Any] = {
    "event": "claim.triaged",
    "claim_id": "11111111-1111-4111-8111-111111111111",
    "tenant_id": "tenant-a",
    "source_key": "tenants/tenant-a/claims/note-1.pdf",
    "outcome": "MEETS_NECESSITY",
    "confidence": 0.91,
    "decided_by": "auto",
    "cited_codes": ["M54.5"],
    "rationale": "Conservative therapy documented.",
    "evidence_missing": [],
    "decided_at": "2026-09-07T12:00:00Z",
}


def load_app(monkeypatch: pytest.MonkeyPatch, **env: str) -> Any:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("mock_client_app", APP_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["mock_client_app"] = module
    spec.loader.exec_module(module)
    return module


def signed_headers(body: bytes, secret: str = SECRET) -> dict[str, str]:
    timestamp = str(int(time.time()))
    digest = hmac.new(
        secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + body, hashlib.sha256
    ).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-ECET-Delivery": "delivery-1",
        "X-ECET-Timestamp": timestamp,
        "X-ECET-Signature": f"sha256={digest}",
    }


def client(module: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=module.app), base_url="http://mock")


async def test_a_correctly_signed_delivery_is_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS=json.dumps({"northwind": SECRET}))
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        posted = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        listed = await http.get("/received")

    assert posted.status_code == 200
    received = listed.json()
    assert len(received) == 1
    assert received[0]["hook"] == "northwind"
    assert received[0]["verified"] is True
    assert received[0]["payload"]["outcome"] == "MEETS_NECESSITY"
    assert_no_pii(json.dumps(received))


async def test_a_wrongly_signed_delivery_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS=json.dumps({"northwind": SECRET}))
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        posted = await http.post(
            "/hooks/northwind", content=body, headers=signed_headers(body, "wrong-secret")
        )
        listed = await http.get("/received")

    assert posted.status_code == 401
    assert listed.json() == []
    assert_no_pii(body.decode("utf-8"))


async def test_an_unknown_hook_is_recorded_unverified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No secret configured for this hook: still useful for a demo, but honest about
    # not having checked anything.
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS="{}")
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        posted = await http.post("/hooks/anything", content=body, headers=signed_headers(body))
        listed = await http.get("/received")

    assert posted.status_code == 200
    assert listed.json()[0]["verified"] is False
    assert_no_pii(json.dumps(listed.json()))


async def test_fail_first_n_rejects_then_accepts(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS="{}", FAIL_FIRST_N="2")
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        first = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        second = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        third = await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        listed = await http.get("/received")

    assert [first.status_code, second.status_code, third.status_code] == [500, 500, 200]
    assert len(listed.json()) == 1
    assert_no_pii(json.dumps(listed.json()))


async def test_received_can_be_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_app(monkeypatch, MOCK_CLIENT_SECRETS="{}")
    body = json.dumps(PAYLOAD).encode("utf-8")

    async with client(module) as http:
        await http.post("/hooks/northwind", content=body, headers=signed_headers(body))
        await http.delete("/received")
        listed = await http.get("/received")

    assert listed.json() == []
