"""Time as a dependency. Injected so every use case is deterministic in tests and so
the domain never calls `datetime.now()` behind the caller's back."""

from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        """Timezone-aware UTC. Sync on purpose — reading a clock never blocks."""
        ...
