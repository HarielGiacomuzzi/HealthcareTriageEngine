"""Errors raised by application ports.

They subclass `DomainError` so the API error mapping stays a single tree, but they
live here rather than in `domain/errors.py` because no domain object raises them —
they are contract failures of an adapter (`ObjectStorage`, `TextExtractor`,
`EvaluationQueue`).
"""

from ecet.domain.errors import DomainError

__all__ = [
    "ExtractionFailed",
    "LLMError",
    "LLMInvalidOutput",
    "LLMPermanentError",
    "LLMTransientError",
    "ObjectNotFound",
    "QueuePublishError",
]


class ObjectNotFound(DomainError):
    """No object at that bucket/key. The message embeds the key — never echo it."""


class ExtractionFailed(DomainError):
    """No usable text. The message is a short token: `no_text`, `encrypted`,
    `too_many_pages`, `unreadable`, `object_unavailable`. It is persisted verbatim as
    `Claim.failure_reason`, so it must stay free of client-supplied strings."""


class QueuePublishError(DomainError):
    """The broker did not confirm the publish. UC-01 leaves the claim
    `POLICIES_ATTACHED` so a retry can re-publish it."""


class LLMError(DomainError):
    """Base for every `LLMGateway` failure, so the worker's ack policy can branch on
    one tree rather than on a list of unrelated types."""


class LLMTransientError(LLMError):
    """429, 5xx, timeout or connection failure. The message is nacked with requeue;
    RabbitMQ's `x-delivery-limit` moves it to the DLQ after five deliveries."""


class LLMPermanentError(LLMError):
    """400, 401, 403 or 404 — a request or credential the retry would repeat verbatim.
    The claim goes `EVALUATION_FAILED` and a human picks it up."""


class LLMInvalidOutput(LLMError):
    """The vendor answered, but not with something `EvaluationOutput` accepts: no tool
    call, unparseable arguments, or a schema violation."""
