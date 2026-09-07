"""Local PII redaction contract (ADR-001). The only thing standing between raw
clinical text and everything that persists, queues or logs it."""

from typing import Protocol, runtime_checkable

from ecet.domain.claim import RedactedText


@runtime_checkable
class PiiRedactor(Protocol):
    async def redact(self, text: str) -> RedactedText: ...
