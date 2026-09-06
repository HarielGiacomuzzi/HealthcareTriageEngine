"""Publish side of `claims.evaluate`. The consumer side lives in the worker (Phase 4)
and is not a port — it drives the process rather than being called by a use case."""

from typing import Protocol, runtime_checkable

from ecet.application.messages import EvaluationMessage


@runtime_checkable
class EvaluationQueue(Protocol):
    async def publish(self, message: EvaluationMessage) -> None:
        """Raises `QueuePublishError` when the broker does not confirm the publish."""
        ...
