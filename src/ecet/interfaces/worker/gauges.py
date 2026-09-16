"""`ecet_claims_by_status` and `ecet_review_open`: the two metrics no request path
produces, because they are statements about the whole table rather than about one
claim. The worker asks the database every 30 s, as the observability spec says.

The api does not do this. Two processes writing the same gauge would each publish
their own view, and the api's is the one that matters least.
"""

import asyncio
import contextlib
from collections.abc import Callable

import structlog

from ecet import metrics
from ecet.application.ports.unit_of_work import UnitOfWork
from ecet.domain.claim import ClaimStatus

log = structlog.get_logger(__name__)

REFRESH_SECONDS = 30.0


async def refresh_once(uow_factory: Callable[[], UnitOfWork]) -> None:
    async with uow_factory() as uow:
        claims = await uow.claims.count_by_status()
        reviews = await uow.review_tasks.count_open_by_tenant()

    for status in ClaimStatus:
        # Every status every time: a status that drops to zero rows must drop to zero
        # here too, and `count_by_status` omits it rather than reporting a zero.
        metrics.CLAIMS_BY_STATUS.labels(status=status.value).set(claims.get(status, 0))

    # A tenant whose queue empties would otherwise keep its last value forever.
    metrics.REVIEW_OPEN.clear()
    for tenant_id, count in reviews.items():
        metrics.REVIEW_OPEN.labels(tenant=str(tenant_id)).set(count)


async def run_refresher(
    uow_factory: Callable[[], UnitOfWork],
    stop: asyncio.Event,
    *,
    interval: float = REFRESH_SECONDS,
) -> None:
    """Refresh until `stop`. A failure is logged and the loop continues: a stale gauge
    is not worth killing the consumer for."""
    while not stop.is_set():
        try:
            await refresh_once(uow_factory)
        except Exception as error:
            log.warning("gauges.refresh_failed", error=type(error).__name__)
        with contextlib.suppress(TimeoutError):
            # Waiting on the event rather than sleeping: shutdown must not sit out a
            # 30 s interval before the worker can exit.
            await asyncio.wait_for(stop.wait(), timeout=interval)
