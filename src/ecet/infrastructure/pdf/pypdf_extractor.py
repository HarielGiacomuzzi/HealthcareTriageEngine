"""`TextExtractor` over pypdf.

CPU-bound, so the parse runs in a worker thread and the event loop stays free to
answer the next S3 event. Every failure mode is one short token, because the token is
persisted as `Claim.failure_reason` and read by operators.
"""

import re
from io import BytesIO

import anyio.to_thread
import structlog
from pypdf import PasswordType, PdfReader

from ecet.application.errors import ExtractionFailed

log = structlog.get_logger(__name__)

#: Below this, the file is almost certainly a scan. OCR is out of scope for v1.
DEFAULT_MIN_CHARS = 50

_RUNS_OF_BLANK_LINES = re.compile(r"\n{3,}")


def normalise(text: str) -> str:
    """Strip trailing whitespace per line and collapse runs of blank lines."""
    stripped = "\n".join(line.rstrip() for line in text.splitlines())
    return _RUNS_OF_BLANK_LINES.sub("\n\n", stripped).strip()


class PypdfTextExtractor:
    def __init__(self, *, max_pages: int, min_chars: int = DEFAULT_MIN_CHARS) -> None:
        self._max_pages = max_pages
        self._min_chars = min_chars

    async def extract(self, pdf: bytes) -> str:
        return await anyio.to_thread.run_sync(self._extract, pdf)

    def _extract(self, pdf: bytes) -> str:
        try:
            reader = PdfReader(BytesIO(pdf))
        except Exception as error:  # pypdf raises a wide family of errors here
            raise ExtractionFailed("unreadable") from error

        if reader.is_encrypted and reader.decrypt("") == PasswordType.NOT_DECRYPTED:
            # An empty user password is worth one try; anything else needs a key we
            # do not have and never will.
            raise ExtractionFailed("encrypted")

        pages = len(reader.pages)
        if pages > self._max_pages:
            raise ExtractionFailed("too_many_pages")

        text = normalise("\n\n".join(page.extract_text() or "" for page in reader.pages))
        if len(text) < self._min_chars:
            raise ExtractionFailed("no_text")

        # Never log the text or a sample of it (ADR-001) — pages and length only.
        log.debug("pdf.extracted", pages=pages, characters=len(text))
        return text
