"""PDF → text contract."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class TextExtractor(Protocol):
    async def extract(self, pdf: bytes) -> str:
        """Raises `ExtractionFailed`. The returned text is **raw** — unredacted — and
        must reach `PiiRedactor` before anything else (ADR-001)."""
        ...
