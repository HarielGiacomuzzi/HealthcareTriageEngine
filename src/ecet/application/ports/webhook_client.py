"""Delivery of a decided claim to the tenant's system.

The port takes the whole `Tenant` rather than a URL and a secret, because signing is
the adapter's responsibility and splitting the tenant apart at the call site is how a
secret ends up in a log line or a use-case signature.
"""

from typing import Protocol, runtime_checkable

from ecet.application.notifications import ClientNotification
from ecet.domain.tenant import Tenant


@runtime_checkable
class WebhookClient(Protocol):
    async def deliver(self, tenant: Tenant, payload: ClientNotification) -> None:
        """Raises `WebhookPermanentError` on a non-retryable 4xx and
        `WebhookTransientError` once the retry budget is exhausted."""
        ...
