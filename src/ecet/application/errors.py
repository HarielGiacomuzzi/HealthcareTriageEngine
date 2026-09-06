"""Errors raised by application ports.

They subclass `DomainError` so the API error mapping stays a single tree, but they
live here rather than in `domain/errors.py` because no domain object raises them —
they are contract failures of an adapter (`ObjectStorage`, `TextExtractor`,
`EvaluationQueue`).
"""

from ecet.domain.errors import DomainError

__all__ = ["ExtractionFailed", "ObjectNotFound", "QueuePublishError"]


class ObjectNotFound(DomainError):
    """No object at that bucket/key. The message embeds the key — never echo it."""


class ExtractionFailed(DomainError):
    """No usable text. The message is a short token: `no_text`, `encrypted`,
    `too_many_pages`, `unreadable`, `object_unavailable`. It is persisted verbatim as
    `Claim.failure_reason`, so it must stay free of client-supplied strings."""


class QueuePublishError(DomainError):
    """The broker did not confirm the publish. UC-01 leaves the claim
    `POLICIES_ATTACHED` so a retry can re-publish it."""
