"""The one real `Clock`. Trivial, but it keeps `datetime.now()` out of every use
case, which is what makes them testable with `FixedClock`."""

from datetime import UTC, datetime


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)
