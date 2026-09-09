"""`run()` owns the signal handling and the stop event; that is all these tests are
about. The container is injected so nothing here opens a database or a broker —
`run()` builds a real one only when none is passed, which is the compose path."""

import asyncio
import os
import signal
from typing import Any, cast

from ecet.config import Settings
from ecet.interfaces.worker.container import WorkerContainer
from ecet.interfaces.worker.main import run


class _StubConsumer:
    def __init__(self) -> None:
        self.started = 0

    async def start(self) -> None:
        self.started += 1


def build_container(settings: Settings, consumer: _StubConsumer) -> WorkerContainer:
    async def aclose() -> None:  # pragma: no cover - `run` never closes an injected one
        raise AssertionError("run() must not close a container it did not build")

    return WorkerContainer(
        settings=settings,
        evaluate=cast(Any, None),
        consumer=cast(Any, consumer),
        aclose=aclose,
    )


async def test_run_returns_when_stop_event_is_set(settings: Settings) -> None:
    consumer = _StubConsumer()
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, stop, build_container(settings, consumer)))
    await asyncio.sleep(0)  # let the worker reach `await stop.wait()`

    stop.set()

    await asyncio.wait_for(task, timeout=1)
    assert task.done()
    assert consumer.started == 1


async def test_sigterm_stops_the_worker(settings: Settings) -> None:
    task = asyncio.create_task(run(settings, None, build_container(settings, _StubConsumer())))

    # Poll for the handler installation instead of a fixed sleep: a slow CI runner
    # could still lose the race, and it would kill pytest itself with no output.
    # No asyncio.Event to wait on here — this observes process-global signal
    # state that `run()` doesn't (and shouldn't) expose one for.
    async with asyncio.timeout(1):
        while signal.getsignal(signal.SIGTERM) is signal.SIG_DFL:  # noqa: ASYNC110
            await asyncio.sleep(0)

    os.kill(os.getpid(), signal.SIGTERM)

    await asyncio.wait_for(task, timeout=1)
    assert task.done()
