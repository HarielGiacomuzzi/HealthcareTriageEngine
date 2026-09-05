"""Worker entrypoint.

Phase 0 ships the process shell only: it starts, idles, and stops cleanly on
SIGTERM (exit code 0). The RabbitMQ consumer replaces the idle wait in Phase 4.
"""

import asyncio
import contextlib
import signal

import structlog

from ecet.config import Settings

log = structlog.get_logger(__name__)


async def run(settings: Settings, stop: asyncio.Event | None = None) -> None:
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not implemented on Windows event loops; the compose stack is Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    log.info("worker.started", env=settings.env, prefetch=settings.worker_prefetch)
    try:
        await stop.wait()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(sig)
        log.info("worker.stopped")
