"""Signed webhook delivery with a bounded retry.

The signature covers `"{timestamp}.{body}"`, not the body alone: without the timestamp
in the signed material a captured request stays replayable forever, and the receiver
has nothing to compare a freshness window against.

`X-ECET-Delivery` is generated once per `deliver`, not once per attempt, so a receiver
can deduplicate retries of the same delivery.

The retry schedule is a constructor argument rather than a constant because a test
that proves "three attempts happened" should not take 21 seconds to do it.
"""

import asyncio
import hashlib
import hmac
import time
from collections.abc import Sequence
from uuid import uuid4

import httpx
import structlog

from ecet.application.errors import WebhookPermanentError, WebhookTransientError
from ecet.application.notifications import ClientNotification
from ecet.domain.tenant import Tenant

log = structlog.get_logger(__name__)

#: Retryable 4xx. Everything else in the 4xx range means the request itself is wrong.
RETRY_STATUS = frozenset({408, 429})


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret, timestamp.encode("utf-8") + b"." + body, hashlib.sha256)
    return f"sha256={digest.hexdigest()}"


class HttpxWebhookClient:
    def __init__(
        self,
        *,
        timeout_s: int,
        max_attempts: int = 3,
        backoff_seconds: Sequence[float] = (1.0, 4.0, 16.0),
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._max_attempts = max(1, max_attempts)
        self._backoff = tuple(backoff_seconds) or (0.0,)
        # One client for the process: the worker delivers to the same few hosts over
        # and over, so connection reuse is the whole point of not building it per call.
        # `http_client` is the test seam, matching `OpenAiLlmGateway`.
        self._client = http_client or httpx.AsyncClient(timeout=float(timeout_s))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def deliver(self, tenant: Tenant, payload: ClientNotification) -> None:
        body = payload.model_dump_json().encode("utf-8")
        secret = tenant.webhook_secret.get_secret_value().encode("utf-8")
        url = str(tenant.webhook_url)
        delivery_id = str(uuid4())
        last = "no attempt"

        for attempt in range(1, self._max_attempts + 1):
            timestamp = str(int(time.time()))
            headers = {
                "Content-Type": "application/json",
                "X-ECET-Delivery": delivery_id,
                "X-ECET-Timestamp": timestamp,
                "X-ECET-Signature": sign(secret, timestamp, body),
            }
            try:
                response = await self._client.post(url, content=body, headers=headers)
            except httpx.TransportError as error:
                last = type(error).__name__
            else:
                status = response.status_code
                if 200 <= status < 300:
                    log.info(
                        "webhook.delivered",
                        tenant_id=str(tenant.id),
                        claim_id=str(payload.claim_id),
                        status=status,
                        attempt=attempt,
                        delivery=delivery_id,
                    )
                    return
                if status not in RETRY_STATUS and status < 500:
                    log.warning(
                        "webhook.rejected",
                        tenant_id=str(tenant.id),
                        claim_id=str(payload.claim_id),
                        status=status,
                        delivery=delivery_id,
                    )
                    raise WebhookPermanentError(f"status {status}")
                last = f"status {status}"

            log.warning(
                "webhook.attempt_failed",
                tenant_id=str(tenant.id),
                claim_id=str(payload.claim_id),
                attempt=attempt,
                reason=last,
                delivery=delivery_id,
            )
            if attempt < self._max_attempts:
                await asyncio.sleep(self._backoff[min(attempt - 1, len(self._backoff) - 1)])

        raise WebhookTransientError(f"{self._max_attempts} attempts failed, last: {last}")
