"""A stand-in for a tenant's claims system.

It exists so `docker compose up` demonstrates the whole loop with no external system:
the worker signs a delivery, this receives it, checks the HMAC and keeps it for
`curl localhost:8081/received | jq`.

Deliberately independent of the `ecet` package. If it imported the sender's signing
helper, a bug in that helper would verify against itself and prove nothing.
"""

import hashlib
import hmac
import json
import os
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Request

#: {"<hook name>": "<shared secret>"}. A hook with no secret is accepted and recorded
#: with `verified: false` rather than refused — an unverifiable demo is more useful
#: than a 401 nobody can debug.
SECRETS: dict[str, str] = json.loads(os.environ.get("MOCK_CLIENT_SECRETS", "{}"))

#: Reject this many deliveries per hook with a 500 before accepting, to demo the
#: sender's retry schedule.
FAIL_FIRST_N = int(os.environ.get("FAIL_FIRST_N", "0"))

app = FastAPI(title="ECET mock client")

received: list[dict[str, Any]] = []
_rejections: dict[str, int] = {}


def _verify(name: str, body: bytes, timestamp: str, signature: str) -> bool:
    secret = SECRETS.get(name)
    if secret is None:
        return False
    expected = hmac.new(
        secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, f"sha256={expected}")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/hooks/{name}")
async def receive(
    name: str,
    request: Request,
    x_ecet_signature: Annotated[str, Header()] = "",
    x_ecet_timestamp: Annotated[str, Header()] = "",
    x_ecet_delivery: Annotated[str, Header()] = "",
) -> dict[str, Any]:
    seen = _rejections.get(name, 0)
    if seen < FAIL_FIRST_N:
        _rejections[name] = seen + 1
        raise HTTPException(500, "induced failure (FAIL_FIRST_N)")

    body = await request.body()
    verified = _verify(name, body, x_ecet_timestamp, x_ecet_signature)
    if name in SECRETS and not verified:
        raise HTTPException(401, "bad signature")

    received.append(
        {
            "hook": name,
            "delivery": x_ecet_delivery,
            "verified": verified,
            "payload": json.loads(body),
        }
    )
    return {"ok": True, "verified": verified}


@app.get("/received")
async def list_received() -> list[dict[str, Any]]:
    return received


@app.delete("/received")
async def reset() -> dict[str, int]:
    count = len(received)
    received.clear()
    _rejections.clear()
    return {"cleared": count}
