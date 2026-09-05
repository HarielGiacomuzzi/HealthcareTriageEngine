import asyncio
import os
import signal

from ecet.config import Settings
from ecet.interfaces.worker.main import run


async def test_run_returns_when_stop_event_is_set(settings: Settings) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, stop))
    await asyncio.sleep(0)  # let the worker reach `await stop.wait()`

    stop.set()

    await asyncio.wait_for(task, timeout=1)
    assert task.done()


async def test_sigterm_stops_the_worker(settings: Settings) -> None:
    task = asyncio.create_task(run(settings))

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
