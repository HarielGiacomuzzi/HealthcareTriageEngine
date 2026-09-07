"""UC-02 RedactPii (ADR-001).

Deliberately thin. It exists so that *which* entities are redacted and *what*
replaces them is an application decision (`redaction_policy.py`, handed to the
adapter at construction) rather than something buried in the presidio wiring — and
so Phase 6 has one place to hang the `pii_redaction_seconds` histogram.
"""

from ecet.application.ports.pii_redactor import PiiRedactor
from ecet.domain.claim import RedactedText


class RedactPii:
    def __init__(self, redactor: PiiRedactor) -> None:
        self._redactor = redactor

    async def execute(self, text: str) -> RedactedText:
        """`text` is raw and must not be logged, stored or returned — only the
        `RedactedText` leaves this call."""
        return await self._redactor.redact(text)
