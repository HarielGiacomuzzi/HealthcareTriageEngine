"""Worker entrypoint: consume `claims.evaluate` until SIGTERM, then drain and exit 0."""

import asyncio
import contextlib
import signal

import structlog

from ecet.config import Settings
from ecet.interfaces.worker.container import WorkerContainer, build_container

log = structlog.get_logger(__name__)


async def run(
    settings: Settings,
    stop: asyncio.Event | None = None,
    container: WorkerContainer | None = None,
) -> None:
    """`container` is injected by tests, mirroring `create_app(settings, container=...)`."""
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not implemented on Windows event loops; the compose stack is Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    built = container if container is not None else await build_container(settings)
    await built.consumer.start()
    log.info("worker.started", env=settings.env, prefetch=settings.worker_prefetch)
    try:
        await stop.wait()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(sig)
        if container is None:
            await built.aclose()
        log.info("worker.stopped")
