"""UC-02 RedactPii (ADR-001).

Deliberately thin. It exists so that *which* entities are redacted and *what*
replaces them is an application decision (`redaction_policy.py`, handed to the
adapter at construction) rather than something buried in the presidio wiring.
"""

from ecet import metrics
from ecet.application.ports.pii_redactor import PiiRedactor
from ecet.domain.claim import RedactedText


class RedactPii:
    def __init__(self, redactor: PiiRedactor) -> None:
        self._redactor = redactor

    async def execute(self, text: str) -> RedactedText:
        """`text` is raw and must not be logged, stored or returned — only the
        `RedactedText` leaves this call."""
        with metrics.PII_REDACTION_SECONDS.time():
            redacted = await self._redactor.redact(text)
        for entity, count in redacted.entity_counts.items():
            metrics.PII_ENTITIES_TOTAL.labels(entity=entity).inc(count)
        return redacted
