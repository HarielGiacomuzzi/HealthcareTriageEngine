"""Container readiness without log-scraping.

A container's port opens before its API answers, and the log lines that mean "ready"
change between image releases. Retrying the call the test actually needs is both
shorter and more durable than either.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable


async def wait_until(
    check: Callable[[], Awaitable[None]],
    *,
    timeout: float = 60.0,  # noqa: ASYNC109 -- a retry deadline, not a single call to wrap
) -> None:
    """Call `check` until it returns without raising, or re-raise the last error."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            await check()
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.5)
